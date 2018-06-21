#include <linux/dm-ioctl.h>
#include <linux/kdev_t.h>
#include <linux/fs.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <fcntl.h>
#include <unistd.h>

//----------------------------------------------------------------

// assuming div is a power of 2
static size_t round_up(size_t n, size_t div)
{
	size_t mask = div - 1;
	return (n + mask) & ~mask;
}

//----------------------------------------------------------------

static void *zalloc(size_t len)
{
	void *ptr = malloc(len);
	if (ptr)
		memset(ptr, 0, len);

	return ptr;
}

static void *payload(struct dm_ioctl *ctl)
{
	return ((unsigned char *) ctl) + ctl->data_start;
}

static struct dm_ioctl *alloc_ctl(size_t payload_size)
{
	size_t len = sizeof(struct dm_ioctl) + payload_size;
	struct dm_ioctl *ctl = zalloc(len);

	if (ctl) {
		ctl->version[0] = DM_VERSION_MAJOR;
		ctl->version[1] = DM_VERSION_MINOR;
		ctl->version[2] = DM_VERSION_PATCHLEVEL;
		ctl->data_size = len;
		ctl->data_start = sizeof(*ctl);
	}

	return ctl;
}

static void free_ctl(struct dm_ioctl *ctl)
{
	free(ctl);
}

// realloc only copies the dm_ioctl struct, not the payload.
// old is always freed, even in case of error.
static struct dm_ioctl *realloc_ctl(struct dm_ioctl *old, size_t extra_payload)
{
	struct dm_ioctl *ctl;
	size_t old_payload_size = old->data_size - sizeof(struct dm_ioctl);
	size_t new_payload_size = old_payload_size + extra_payload;

	ctl = alloc_ctl(new_payload_size);
	if (ctl)
		memcpy(payload(ctl), payload(old), sizeof(*ctl));

	free_ctl(old);
	return ctl;
}

//----------------------------------------------------------------

struct dm_interface {
	int fd;
};

// FIXME: pass in some form of log object?
struct dm_interface *dm_open()
{
	int fd;
	char path[1024];
	struct dm_interface *dmi;

	snprintf(path, sizeof(path), "/dev/%s/%s", DM_DIR, DM_CONTROL_NODE);
	fd = open(path, O_RDWR | O_EXCL);
	if (fd < 0)
		return NULL;

	dmi = zalloc(sizeof(*dmi));
	if (dmi)
		dmi->fd = fd;
	else
		close(fd);

	return dmi;
}

void dm_close(struct dm_interface *dmi)
{
	close(dmi->fd);
	free(dmi);
}

//----------------------------------------------------------------

struct dev_list {
	struct dev_list *next;
	unsigned major;
	unsigned minor;
	char *name;
};

void free_dev_list(struct dev_list *dl)
{
	struct dev_list *next;

	while (dl) {
		next = dl->next;
		free(dl->name);
		free(dl);
		dl = next;
	}
}

struct dev_list_builder {
	struct dev_list *head, *tail;
};

static void dlb_init(struct dev_list_builder *dlb)
{
	dlb->head = dlb->tail = NULL;
}

static int dlb_append(struct dev_list_builder *dlb,
		      unsigned major, unsigned minor, const char *name)
{
	struct dev_list *dl = malloc(sizeof(*dl));

	if (!dl)
		return -ENOMEM;

	dl->next = NULL;
	dl->major = major;
	dl->minor = minor;
	dl->name = strdup(name);

	if (dlb->head) {
		dlb->tail->next = dl;
		dlb->tail = dl;
	} else
		dlb->head = dlb->tail = dl;

	return 0;
}

static struct dev_list *dlb_get(struct dev_list_builder *dlb)
{
	return dlb->head;
}

//----------------------------------------------------------------

static int copy_string(char *dest, const char *src, size_t max)
{
	if (strlen(src) + 1 > max)
		return -ENOMEM;

	strcpy(dest, src);
	return 0;
}

static int copy_name(struct dm_ioctl *ctl, const char *name)
{
	return copy_string(ctl->name, name, DM_NAME_LEN);
}

static int copy_uuid(struct dm_ioctl *ctl, const char *uuid)
{
	return copy_string(ctl->uuid, uuid, DM_UUID_LEN);
}

//----------------------------------------------------------------

int dm_version(struct dm_interface *dmi, uint32_t *major, uint32_t *minor, uint32_t *patch)
{
	int r;
	struct dm_ioctl *ctl = alloc_ctl(0);

	if (!ctl)
		return -ENOMEM;

	r = ioctl(dmi->fd, DM_VERSION, ctl);
	*major = ctl->version[0];
	*minor = ctl->version[1];
	*patch = ctl->version[2];
	free_ctl(ctl);

	return r;
}

int dm_remove_all(struct dm_interface *dmi)
{
	int r;
	struct dm_ioctl *ctl = alloc_ctl(0);

	if (!ctl)
		return -ENOMEM;

	r = ioctl(dmi->fd, DM_REMOVE_ALL, ctl);
	free_ctl(ctl);

	return r;
}

static bool list_devices(struct dm_interface *dmi, struct dm_ioctl *ctl,
			 size_t payload_size, struct dev_list **devs,
			 int *r)
{
	struct dm_name_list *nl;
	struct dev_list_builder dlb;

	*r = ioctl(dmi->fd, DM_LIST_DEVICES, ctl);
	if (*r < 0)
		return true;

	if (ctl->flags & DM_BUFFER_FULL_FLAG) {
		free_ctl(ctl);
		return false;
	}

	dlb_init(&dlb);
	nl = (struct dm_name_list *) payload(ctl);

	if (nl->dev) {
		for (;;) {
			dlb_append(&dlb, MAJOR(nl->dev), MINOR(nl->dev), nl->name);

			if (!nl->next)
				break;

			nl = (struct dm_name_list *) (((unsigned char *) nl) + nl->next);
		}
	}

	*devs = dlb_get(&dlb);
	return true;
}

int dm_list_devices(struct dm_interface *dmi, struct dev_list **devs)
{
	int r;
	struct dm_ioctl *ctl;
	size_t payload_size = 8192;

	ctl = alloc_ctl(payload_size);
	if (!ctl)
		return -ENOMEM;

	while (!list_devices(dmi, ctl, payload_size, devs, &r)) {
		payload_size *= 2;
		ctl = realloc_ctl(ctl, payload_size);
		if (!ctl)
			return -ENOMEM;
	}

	free_ctl(ctl);
	return r;
}

// Obviously major and minor are only valid if successful.
int dm_create_device(struct dm_interface *dmi, const char *name, const char *uuid,
		     uint32_t *major_result, uint32_t *minor_result)
{
	int r;
	struct dm_ioctl *ctl = alloc_ctl(0);

	if (!ctl)
		return -ENOMEM;

	r = copy_name(ctl, name);
	if (r) {
		free_ctl(ctl);
		return r;
	}

	r = copy_uuid(ctl, name);
	if (r) {
		free_ctl(ctl);
		return r;
	}

	r = ioctl(dmi->fd, DM_DEV_CREATE, ctl);
	if (!r) {
		*major_result = MAJOR(ctl->dev);
		*minor_result = MINOR(ctl->dev);
	}
	free_ctl(ctl);
	return r;
}

static int dev_cmd(struct dm_interface *dmi, const char *name, int request, unsigned flags)
{
	int r;
	struct dm_ioctl *ctl = alloc_ctl(0);

	if (!ctl)
		return -ENOMEM;

	ctl->flags = flags;
	r = copy_name(ctl, name);
	if (r) {
		free_ctl(ctl);
		return -ENOMEM;
	}
	r = ioctl(dmi->fd, request, ctl);
	free_ctl(ctl);

	return r;
}

int dm_remove_device(struct dm_interface *dmi, const char *name)
{
	return dev_cmd(dmi, name, DM_DEV_REMOVE, 0);
}

int dm_suspend_device(struct dm_interface *dmi, const char *name)
{
	return dev_cmd(dmi, name, DM_DEV_SUSPEND, DM_SUSPEND_FLAG);
}

int dm_resume_device(struct dm_interface *dmi, const char *name)
{
	return dev_cmd(dmi, name, DM_DEV_SUSPEND, 0);
}

int dm_clear_device(struct dm_interface *dmi, const char *name)
{
	return dev_cmd(dmi, name, DM_TABLE_CLEAR, 0);
}

//----------------------------------------------------------------

struct target {
	struct target *next;

	uint64_t len;
	char *type;
	char *args;
};

void free_targets(struct target *t)
{
	while (t) {
		struct target *next = t->next;
		free(t->type);
		free(t->args);
		t = next;
	}
}

struct target_builder {
	struct target *head, *tail;
};

static void tb_init(struct target_builder *tb)
{
	tb->head = tb->tail = NULL;
}

static int tb_append(struct target_builder *tb, uint64_t len, char *type, char *args)
{
	struct target *t = malloc(sizeof(*t));
	if (!t)
		return -ENOMEM;

	t->next = NULL;
	t->len = len;
	t->type = strdup(type);
	if (!t->type) {
		free(t);
		return -ENOMEM;
	}
	t->args = strdup(args);
	if (!t->args) {
		free(t->type);
		free(t);
		return -ENOMEM;
	}

	if (tb->head) {
		tb->tail->next = t;
		tb->tail = t;
	} else
		tb->head = tb->tail = t;

	return 0;
}

static struct target *tb_get(struct target_builder *tb)
{
	return tb->head;
}

//----------------------------------------------------------------

static size_t calc_load_payload(struct target *t)
{
	size_t space = 0;

	while (t) {
		space += sizeof(struct dm_target_spec);
		space += strlen(t->args) + 16;
		t = t->next;
	}

	return space + 128;
}

static int prep_load(struct dm_ioctl *ctl, size_t payload_size,
		     const char *name, struct target *t)
{
	int r;
	uint64_t current_sector = 0;
	struct dm_target_spec *spec;

	ctl->target_count = 0;
	spec = payload(ctl);

	while (t) {
		spec->sector_start = current_sector;
		current_sector += t->len;
		spec->length = t->len;
		spec->status = 0;
		r = copy_string(spec->target_type, t->type, DM_MAX_TYPE_NAME);
		if (r)
			return r;

		r = copy_string((char *) (spec + 1), t->args, payload_size);
		if (r)
			return r;

		spec->next = sizeof(*spec) + round_up(strlen(t->args) + 1, 8);
		payload_size -= spec->next;

		spec = (struct dm_target_spec *) (((char *) spec) + spec->next);

		ctl->target_count++;
		t = t->next;
	}

	return 0;
}

int dm_load(struct dm_interface *dmi, const char *name,
		struct target *targets)
{
	int r;
	size_t payload_size = calc_load_payload(targets);
	struct dm_ioctl *ctl = alloc_ctl(payload_size);

	if (!ctl)
		return -ENOMEM;

	r = prep_load(ctl, payload_size, name, targets);
	if (r) {
		free_ctl(ctl);
		return r;
	}

	r = copy_name(ctl, name);
	if (r) {
		free_ctl(ctl);
		return -ENOMEM;
	}

	r = ioctl(dmi->fd, DM_TABLE_LOAD, ctl);
	free_ctl(ctl);

	return r;
}

static bool get_status(struct dm_interface *dmi, struct dm_ioctl *ctl,
		       const char *name, unsigned flags,
		       int *result)
{
	*result = copy_name(ctl, name);
	if (*result) {
		free_ctl(ctl);
		return true;
	}

	ctl->flags = flags;
	ctl->target_count = 0;

	*result = ioctl(dmi->fd, DM_TABLE_STATUS, ctl);
	if (*result)
		return true;

	if (ctl->flags & DM_BUFFER_FULL_FLAG)
		return false;

	return true;
}

static int unpack_status(struct dm_ioctl *ctl, struct target **result)
{
	unsigned i;
	struct target_builder tb;
	struct dm_target_spec *spec = payload(ctl);
	char *spec_start = (char *) spec;

	tb_init(&tb);
	for (i = 0; i < ctl->target_count; i++) {
		tb_append(&tb, spec->length, spec->target_type, (char *) (spec + 1));
		spec = (struct dm_target_spec *) (spec_start + spec->next);
	}

	*result = tb_get(&tb);
	return 0;
}

static int status_cmd(struct dm_interface *dmi, const char *name,
		      struct target **targets, unsigned flags)
{
	int r;
	size_t payload_size = 8192;
	struct dm_ioctl *ctl = NULL;

	ctl = alloc_ctl(payload_size);
	if (!ctl)
		return -ENOMEM;

retry:
	if (!get_status(dmi, ctl, name, flags, &r)) {
		payload_size *= 2;
		ctl = realloc_ctl(ctl, payload_size);
		if (!ctl)
			return -ENOMEM;

		goto retry;
	}

	if (r)
		return r;

	r = unpack_status(ctl, targets);
	free_ctl(ctl);
	return r;
}

int dm_status(struct dm_interface *dmi, const char *name, struct target **targets)
{
	return status_cmd(dmi, name, targets, 0);
}

int dm_table(struct dm_interface *dmi, const char *name, struct target **targets)
{
	return status_cmd(dmi, name, targets, DM_STATUS_TABLE_FLAG);
}

#if 0
int dm_info(struct dm_interface *dmi, const char *name, struct target **targets)
{
	return status_cmd(dmi, name, targets, DM_STATUS_INFO_FLAG);
}
#endif

int dm_message(struct dm_interface *dmi, const char *name, uint64_t sector,
	       const char *msg_str)
{
	int r;
	size_t msg_len = strlen(msg_str) + 1;
	size_t payload_size = msg_len + 32;
	struct dm_ioctl *ctl = alloc_ctl(payload_size);
	struct dm_target_msg *msg;

	if (!ctl)
		return -ENOMEM;
	msg = payload(ctl);
	copy_name(ctl, name);
	msg->sector = sector;
	memcpy(msg->message, msg_str, msg_len);

	r = ioctl(dmi->fd, DM_TARGET_MSG, ctl);
	free_ctl(ctl);

	return r;
}

int get_dev_size(const char *path, uint64_t *sectors)
{
	int r, fd;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -EINVAL;

	r = ioctl(fd, BLKGETSIZE64, sectors);
	(*sectors) /= 512;
	close(fd);
	return r;
}

int discard(const char *path, uint64_t sector_b, uint64_t sector_e)
{
	int r, fd;
	uint64_t payload[2];

	fd = open(path, O_RDWR);
	if (fd < 0) {
        	fprintf(stderr, "couldn't open %s", path);
		return -EINVAL;
	}

	payload[0] = sector_b;
	payload[1] = sector_e;

	r = ioctl(fd, BLKDISCARD, payload);
	close(fd);

	return r;
}

//----------------------------------------------------------------
