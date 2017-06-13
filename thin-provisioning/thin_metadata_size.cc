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

#include "thin-provisioning/commands.h"

#include <getopt.h>
#include <libgen.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "version.h"

#include <search.h>

using namespace thin_provisioning;

/*----------------------------------------------------------------*/

enum numeric_options { BLOCKSIZE, POOLSIZE, MAXTHINS, NUMERIC, OPT_END};
enum return_units { RETURN_BYTES, RETURN_SECTORS };
enum numeric_type { NO_NUMBER, NUMBER, NUMBER_SHORT, NUMBER_LONG };

struct options_ {
	unsigned unit_idx;
	char *s[OPT_END];
	unsigned long long n[OPT_END];
};

struct global {
	char *prg; /* program name */

	/* Unit representations in characters, strings and numeric factors. */
	struct {
		char *chars;
		char **strings;
		unsigned long long *factors;
	} unit;

	/* Command line option properties. */
	options_ options;
};

static void exit_prg(struct global *g, int ret)
{
	if (g) {
		unsigned u = OPT_END;

		while (u--) {
			if (g->options.s[u])
				free (g->options.s[u]);
		}

		free(g);
	}

	exit(ret);
}

static void abort_prg(struct global *g, const char *msg)
{
	fprintf(stderr, "%s - %s\n", g ? g->prg : "fatal", msg);
	exit_prg(g, 1);
}

static int unit_index(struct global *g, char const *unit_string)
{
	unsigned len;

	if (!unit_string)
		return unit_index(g, "sectors");

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

#define ARRAY_SIZE(a) (sizeof(a)/sizeof(a[0]))
static struct global *init_prg(char *prg_path)
{
	unsigned u;
	static char const *unit_chars = "bskKmMgGtTpPeEzZyY";
	static char const *unit_strings[] = { "bytes", "sectors",
					      "kibibytes", "kilobytes", "mebibytes", "megabytes",
					      "gibibytes", "gigabytes", "tebibytes", "terabytes",
					      "pebibytes", "petabytes", "ebibytes",  "exabytes",
					      "zebibytes", "zetabytes", "yobibytes", "yottabytes", NULL };
	static unsigned long long unit_factors[ARRAY_SIZE(unit_strings) - 1] = { 1, 512, 1024, 1000 };
	struct global *r = static_cast<global *>(malloc(sizeof(*r)));

	if (!r)
		abort_prg(r, "failed to allocate global context!");

	memset(r, 0, sizeof(*r));

	for (u = 4; unit_strings[u]; u += 2) {
		unit_factors[u]   = unit_factors[u-2] * unit_factors[2];
		unit_factors[u+1] = unit_factors[u-1] * unit_factors[3];
	}

	r->prg = basename(prg_path);
	r->unit.chars = const_cast<char *>(unit_chars);
	r->unit.strings = const_cast<char **>(unit_strings);
	r->unit.factors = unit_factors;
	r->options.unit_idx = unit_index(r, NULL);

	return r;
}

static unsigned long long bytes_per_sector(struct global *g)
{
	return g->unit.factors[unit_index(g, "sectors")];
}

static void check_opts(struct global *g)
{
	options_ *o = &g->options;

	if (!o->n[BLOCKSIZE])
		abort_prg(g, "block size required!");
	else if (!o->n[POOLSIZE])
		abort_prg(g, "pool size required!");
	else if (!o->n[MAXTHINS])
		abort_prg(g, "max thins required!");
	else if (o->n[BLOCKSIZE] & (o->n[BLOCKSIZE] - 1))
  		abort_prg(g, "block size must be 2^^N");
	else if (o->n[POOLSIZE] <= o->n[BLOCKSIZE])
  		abort_prg(g, "pool size must be larger than block size");
}

static unsigned long long to_bytes(struct global *g, char *sz, enum return_units unit, int *index)
{
	int idx;
	unsigned long long r;
	char *us = sz;

	/* Get pointer to unit identifier. */
	us += strspn(sz, "0123456789");
	if (*us) {
		idx = unit_index(g, us);
		if (idx < 0)
			abort_prg(g, "Invalid unit specifier!");

		*us = 0;
		*index = idx;
	} else {
		idx = unit_index(g, NULL);
		us = NULL;
		*index = -1;
	}

	r = atoll(sz) * g->unit.factors[idx];
	return (!us || unit == RETURN_SECTORS) ? r / bytes_per_sector(g) : r;
}

static void printf_aligned(struct global *g, char const *a, char const *b, char const *c, bool units, bool mandatory)
{
	char buf[80];

	strcpy(buf, b);
	if (units)
		strcat(buf, mandatory ? "{" :"["), strcat(buf, g->unit.chars), strcat(buf, mandatory ? "}" : "]");

	printf("\t%-4s%-44s%s\n", a, buf, c);
}

static void help(struct global *g)
{
	printf ("Thin Provisioning Metadata Device Size Calculator.\nUsage: %s [options]\n", g->prg);
	printf_aligned(g, "-b", "--block-size BLOCKSIZE", "Block size of thin provisioned devices.", true, false);
	printf_aligned(g, "-s", "--pool-size SIZE", "Size of pool device.", true, false);
	printf_aligned(g, "-m", "--max-thins #MAXTHINS", "Maximum sum of all thin devices and snapshots.", true, false);
	printf_aligned(g, "-u", "--unit ", "Output unit specifier.", true, true);
	printf_aligned(g, "-n", "--numeric-only [short|long]", "Output numeric value only (optionally with short/long unit identifier).", false, false);
	printf_aligned(g, "-h", "--help", "This help.", false, false);
	printf_aligned(g, "-V", "--version", "Print thin provisioning tools version.", false, false);
	exit_prg(g, 0);
}

static void version(struct global *g)
{
	printf("%s\n", THIN_PROVISIONING_TOOLS_VERSION);
	exit_prg(g, 1);
}

static void check_unit(struct global *g, char *arg)
{
	int idx = unit_index(g, arg);

	if (idx < 0)
		abort_prg(g, "output unit specifier invalid!");

      	g->options.unit_idx = idx;
}

static void check_numeric_option(struct global *g, char *arg)
{
	if (g->options.n[NUMERIC] != NO_NUMBER)
		abort_prg(g, "-n already given!");

	g->options.n[NUMERIC] = NUMBER;

	if (arg) {
		bool unit_long = !strncmp("long", arg, strlen(arg));

		if (!*arg || (strncmp("short", arg, strlen(arg)) && !unit_long))
			abort_prg(g, "-n invalid option argument");

		g->options.n[NUMERIC] = unit_long ? NUMBER_LONG : NUMBER_SHORT;
	}
}

static void check_size(struct global *g, enum numeric_options o, char *arg)
{
	int idx;
	bool valid_index = true;

	if (g->options.n[o])
		abort_prg(g, "option already given!");

	g->options.n[o] = to_bytes(g, arg, o == MAXTHINS ? RETURN_BYTES : RETURN_SECTORS, &idx);
	if (idx < 0) {
		valid_index = false;
		idx = g->options.unit_idx;
	}

	g->options.s[o] = static_cast<char *>(malloc(strlen(arg) + strlen(g->unit.strings[idx]) + 1));
	if (!g->options.s[o])
		abort_prg(g, "failed to allocate string!");

	strcpy(g->options.s[o], arg);
	if (o != MAXTHINS || valid_index)
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
		{NULL,		0,		   NULL, 0   }
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
			exit_prg(g, 1);
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

static void print_precision(struct global *g, double r, unsigned idx)
{
	bool full = g->options.n[NUMERIC] == NO_NUMBER;
	double rtrunc = floor(r);

	if (full)
		printf("%s - ", g->prg);

	if (r == rtrunc)
		printf("%llu", (unsigned long long) r);
	else
		printf(r - rtrunc < 1E-2 ? "%0.2e" : "%0.2f", r);

	if (full)
		printf(" %s", g->unit.strings[idx]);
	else if (g->options.n[NUMERIC] > NUMBER) {
		if (g->options.n[NUMERIC] == NUMBER_SHORT)
			printf("%c", g->unit.chars[idx]);
		else
			printf("%s", g->unit.strings[idx]);
	}

	if (full)
		printf(" estimated metadata area size for \"--block-size=%s --pool-size=%s --max-thins=%s\"",
		       g->options.s[BLOCKSIZE], g->options.s[POOLSIZE], g->options.s[MAXTHINS]);

	putchar('\n');
}

static void print_estimated_result(struct global *g)
{
	double r;

	/* double-fold # of nodes, because they aren't fully populated in average */
	r = (1.0 + (2 * g->options.n[POOLSIZE] / g->options.n[BLOCKSIZE] / mappings_per_block() + g->options.n[MAXTHINS])); /* in 4k blocks */
	r *=  8 * bytes_per_sector(g); /* in bytes! */
	r /= g->unit.factors[g->options.unit_idx]; /* in requested unit */

	print_precision(g, r, g->options.unit_idx);
}

//----------------------------------------------------------------

thin_metadata_size_cmd::thin_metadata_size_cmd()
	: command("thin_metadata_size")
{
}

void
thin_metadata_size_cmd::usage(std::ostream &out) const
{
	// FIXME: finish
}

int
thin_metadata_size_cmd::run(int argc, char **argv)
{
	struct global *g = init_prg(*argv);

	parse_command_line(g, argc, argv);
	print_estimated_result(g);
	exit_prg(g, 0);
	return 0; /* Doesn't get here... */
}

//----------------------------------------------------------------
