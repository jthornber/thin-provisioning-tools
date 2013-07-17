/*
 * Copyright (C) 2013 Red Hat, GmbH
 * 
 * Calculates device-mapper thin privisioning
 * metadata device size based on pool, block size and
 * maximum expected thin provisioned devices and snapshots.
 *
 * This file is part of the thin-provisioning-tools source.
 *
 * thin-provisioning-tools is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * thin-provisioning-tools is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with thin-provisioning-tools.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 */

#include <getopt.h>
#include <libgen.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "version.h"

/*----------------------------------------------------------------*/

enum numeric_options { BLOCKSIZE, POOLSIZE, MAXTHINS, NUMERIC, OPT_END};
enum return_units { RETURN_BYTES, RETURN_SECTORS };
struct global {
	char *prg; /* program name */

	struct {
		char *chars;
		char **strings;
		unsigned long long *factors;
	} unit;

	struct options {
		unsigned unit_idx;
		char *s[OPT_END];
		unsigned long long n[OPT_END];
	} options;
};

static void exit_prg(struct global *g, int ret)
{
	if (g)
		free(g);

	exit(ret);
}

static void abort_prg(struct global *g, const char *msg)
{
	fprintf(stderr, "%s - %s\n", g->prg, msg);
	exit_prg(g, 1);
}

static struct global *init_prg(char *prg_path)
{
	unsigned u;
	static char *unit_chars = "bskKmMgGtTpPeEzZyY";
	static char *unit_strings[] = { "bytes", "sectors",
					"kilobytes", "kibibytes", "megabytes",  "mebibytes",
					"gigabytes", "gibibytes", "terabytes",  "tebibytes",
					"petabytes", "pebibytes", "exabytes",   "ebibytes",
					"zetabytes", "zebibytes", "yottabytes", "yobibytes", NULL };
	static unsigned long long unit_factors[18] = { 1, 512, 1024, 1000 };
	struct global *r = malloc(sizeof(*r));

	if (!r)
		abort_prg(r, "failed to allocate global context!");

	memset(r, 0, sizeof(*r));

	for (u = 4; unit_strings[u]; u += 2) {
		unit_factors[u] = unit_factors[u-2] * unit_factors[2];
		unit_factors[u+1] = unit_factors[u-1] * unit_factors[3];
	}

	r->prg = basename(prg_path);
	r->unit.chars = unit_chars;
	r->unit.strings = unit_strings;
	r->unit.factors = unit_factors;

	return r;
}
#define bytes_per_sector(g) (g)->unit.factors[get_index(g, "sectors")]

static int get_index(struct global *g, char *unit_string)
{
	unsigned len;

	if (!unit_string)
		return get_index(g, "sectors");

	len = strlen(unit_string);
	if (len == 1) {
		char *o = strchr(g->unit.chars, *unit_string);

		if (o)
			return o - g->unit.chars;

	} else {
		char **s;

		for (s = g->unit.strings; *s; s++)
			if (!strncmp(*s, unit_string, len))
				return s - g->unit.strings;
	}

	return -1;
}

static void check_opts(struct global *g)
{
	struct options *o = &g->options;

	if (!o->n[BLOCKSIZE] || !o->n[POOLSIZE] || !o->n[MAXTHINS])
		abort_prg(g, "3 arguments required!");
	else if (o->n[BLOCKSIZE] & (o->n[BLOCKSIZE] - 1))
  		abort_prg(g, "block size must be 2^^N");
	else if (o->n[POOLSIZE] <= o->n[BLOCKSIZE])
  		abort_prg(g, "POOLSIZE must be larger than BLOCKSIZE");
	else if (!o->n[MAXTHINS])
		abort_prg(g, "maximum number of thin provisioned devices must be > 0");
}

static unsigned long long to_bytes(struct global *g, char *sz, enum return_units unit, int *index)
{
	int idx;
	unsigned long long r;
	char *us;

	for (us = sz; *us; us++) {
		if (!isdigit(*us))
			break;
	}

	if (*us) {
		idx = get_index(g, us);
		if (idx < 0)
			abort_prg(g, "Invalid unit specifier!");

		*us = 0;
		*index = idx;
	} else {
		idx = get_index(g, NULL);
		us = NULL;
		*index = -1;
	}

	r = atoll(sz) * g->unit.factors[idx];
	return (!us || unit == RETURN_SECTORS) ? r / bytes_per_sector(g) : r;
}

static void printf_aligned(struct global *g, char *a, char *b, char *c, int units)
{
	char buf[80];

	strcpy(buf, b);
	if (units)
		strcat(buf, "["), strcat(buf, g->unit.chars), strcat(buf, "]");

	printf("\t%-4s%-44s%s\n", a, buf, c);
}

static void help(struct global *g)
{
	printf ("Thin Provisioning Metadata Device Size Calculator.\nUsage: %s [options]\n", g->prg);
	printf_aligned(g, "-b", "--block-size BLOCKSIZE", "Block size of thin provisioned devices.", 1);
	printf_aligned(g, "-s", "--pool-size SIZE", "Size of pool device.", 1);
	printf_aligned(g, "-m", "--max-thins #MAXTHINS", "Maximum sum of all thin devices and snapshots.", 1);
	printf_aligned(g, "-u", "--unit ", "Output unit specifier.", 1);
	printf_aligned(g, "-n", "--numeric-only[=unit]", "Output numeric value only (optionally with unit identifier).", 0);
	printf_aligned(g, "-h", "--help", "This help.", 0);
	printf_aligned(g, "-V", "--version", "Print thin provisioning tools version.", 0);
	exit_prg(g, 0);
}

static void version(struct global *g)
{
	printf("%s\n", THIN_PROVISIONING_TOOLS_VERSION);
	exit_prg(g, 1);
}

static void check_unit(struct global *g, char *arg)
{
	int idx = get_index(g, arg);

	if (idx < 0)
		abort_prg(g, "output unit specifier invalid!");

      	g->options.unit_idx = idx;
}

static void check_numeric_option(struct global *g, char *arg)
{
	if (g->options.n[NUMERIC])
		abort_prg(g, "-n already given!");

	g->options.n[NUMERIC] = 1;

	if (arg) {
		if (!*arg || strncmp("unit", arg, strlen(arg)))
			abort_prg(g, "-n invalid option argument");

		g->options.n[NUMERIC]++;
	}
}

static void check_size(struct global *g, enum numeric_options o, char *arg)
{
	int idx;

	if (g->options.n[o])
		abort_prg(g, "option already given!");

	g->options.n[o] = to_bytes(g, arg, o == MAXTHINS ? RETURN_BYTES : RETURN_SECTORS, &idx);
	g->options.s[o] = malloc(strlen(arg) + ((idx > -1) ? strlen(g->unit.strings[idx]) : 0) + 1);
	if (!g->options.s[o])
		abort_prg(g, "failed to allocate string!");

	strcpy(g->options.s[o], arg);
	if (idx > -1)
		strcat(g->options.s[o], g->unit.strings[idx]);

}

static void parse_command_line(struct global *g, int argc, char **argv)
{
	int c;
	static struct option long_options[] = {
		{"block-size",	required_argument, NULL, 'b' },
		{"pool-size",	required_argument, NULL, 's' },
		{"max-thins",	required_argument, NULL, 'm' },
		{"unit",	required_argument, NULL, 'u' },
		{"numeric-only",optional_argument, NULL, 'n' },
		{"help",	no_argument,       NULL, 'h' },
		{"version",	no_argument,       NULL, 'V' },
		{NULL,		0,		   NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, "b:s:m:u:n::hV", long_options, NULL)) != -1) {
		switch (c) {
		case 'b':
			check_size(g, BLOCKSIZE, optarg);
			break;
		case 's':
			check_size(g, POOLSIZE, optarg);
			break;
		case 'm':
			check_size(g, MAXTHINS, optarg);
			break;
		case 'u':
			check_unit(g, optarg);
			break;
		case 'n':
			check_numeric_option(g, optarg);
			break;
		case 'h':
			help(g); /* exits */
		case 'V':
			version(g); /* exits */
		default:
			abort_prg(g, "Invalid option!");
		}
	}

	check_opts(g);
}

static const unsigned mappings_per_block(void)
{
	const struct {
		const unsigned node;
		const unsigned node_header;
		const unsigned entry;
	} btree_size = { 4096, 64, 16 };

	return (btree_size.node - btree_size.node_header) / btree_size.entry;
}

static void printf_precision(struct global *g, double r, unsigned idx)
{
	int full = !g->options.n[NUMERIC];
	double rtrunc = truncl(r);

	if (full)
		printf("%s - estimated metadata area size [blocksize=%s,poolsize=%s,maxthins=%s] is ",
		       g->prg, g->options.s[BLOCKSIZE], g->options.s[POOLSIZE], g->options.s[MAXTHINS]);

	if (r == rtrunc)
		printf("%llu", (unsigned long long) r);
	else
		printf(r - truncl(r) < 1E-2 ? "%0.2e" : "%0.2f", r);

	if (full)
		printf(" %s", g->unit.strings[idx]);
	else if (g->options.n[NUMERIC] > 1)
		printf("%c", g->unit.chars[idx]);

	putchar('\n');
}

static void print_estimated_result(struct global *g)
{
	unsigned idx = g->options.unit_idx;
	double r;

	/* double-fold # of nodes, because they aren't fully populated in average */
	r = (1.0 + (2 * g->options.n[POOLSIZE] / g->options.n[BLOCKSIZE] / mappings_per_block() + g->options.n[MAXTHINS])) * 8 * bytes_per_sector(g); /* in bytes! */
	r /= g->unit.factors[idx]; /* in requested unit */

	printf_precision(g, r, idx);
}

int main(int argc, char **argv)
{
	struct global *g = init_prg(*argv);

	parse_command_line(g, argc, argv);
	print_estimated_result(g);
	exit_prg(g, 0);
	return 0; /* Doesn't get here... */
}
