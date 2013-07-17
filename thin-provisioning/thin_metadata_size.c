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

static char *prg; /* program name */

enum numeric_options { BLOCKSIZE, POOLSIZE, MAXTHINS, NUMERIC, OPT_END};
enum return_units { RETURN_BYTES, RETURN_SECTORS };
struct global {
	#define UNIT_ARRAY_SZ	18
	struct {
		char *chars;
		char *strings[UNIT_ARRAY_SZ];
		unsigned long long factors[UNIT_ARRAY_SZ];
	} unit;

	struct options {
		unsigned long long n[OPT_END];
		char *s[OPT_END];
		char unit;
	} options;
};
#define bytes_per_sector g->unit.factors[1]

static struct global *init_prg(void)
{
	unsigned u = 0;
	static struct global r;
	static char *unit_strings[] = { "bytes", "sectors",
					"kilobytes", "kibibytes", "megabytes",  "mebibytes",
					"gigabytes", "gibibytes", "terabytes",  "tebibytes",
					"petabytes", "pebibytes", "exabytes",   "ebibytes",
					"zetabytes", "zebibytes", "yottabytes", "yobibytes" };

	memset(&r, 0, sizeof(r));
	r.unit.chars = "bskKmMgGtTpPeEzZyY";
	r.unit.factors[u++] = 1;
	r.unit.factors[u++] = 512;
	r.unit.factors[u++] = 1024;
	r.unit.factors[u++] = 1000;
	for ( ; u < UNIT_ARRAY_SZ; u += 2) {
		r.unit.factors[u] = r.unit.factors[2] * r.unit.factors[u - 2];
		r.unit.factors[u+1] = r.unit.factors[3] * r.unit.factors[u - 1];
	}

	for  (u = UNIT_ARRAY_SZ; u--; )
		r.unit.strings[u] = unit_strings[u];

	r.options.unit = 's';

	return &r;
}

static void exit_prg(struct global *g)
{
	unsigned u;

	for (u = OPT_END; u--; ) {
		if (g->options.s[u])
			free(g->options.s[u]);
	}
}

static unsigned get_index(struct global *g, char unit_char)
{
	char *o = strchr(g->unit.chars, unit_char);

	return o ? o - g->unit.chars : 1;
}

static void abort_prg(const char *msg)
{
	fprintf(stderr, "%s - %s\n", prg, msg);
	exit(1);
}

static void check_opts(struct options *options)
{
	if (!options->n[BLOCKSIZE] || !options->n[POOLSIZE] || !options->n[MAXTHINS])
		abort_prg("3 arguments required!");
	else if (options->n[BLOCKSIZE] & (options->n[BLOCKSIZE] - 1))
  		abort_prg("block size must be 2^^N");
	else if (options->n[POOLSIZE] < options->n[BLOCKSIZE])
  		abort_prg("POOLSIZE must be much larger than BLOCKSIZE");
	else if (!options->n[MAXTHINS])
		abort_prg("maximum number of thin provisioned devices must be > 0");
}

static unsigned long long to_bytes(struct global *g, char *sz, enum return_units unit)
{
	unsigned len = strlen(sz);
	unsigned long long r;
	char uc = 's', *us = strchr(g->unit.chars, sz[len-1]);

	if (us)
		uc = *us, sz[len-1] = 0;

	r = atoll(sz) * g->unit.factors[get_index(g, uc)];
	return (!us || unit == RETURN_SECTORS) ? r / bytes_per_sector : r;
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
	printf ("Thin Provisioning Metadata Device Size Calculator.\nUsage: %s [options]\n", prg);
	printf_aligned(g, "-b", "--block-size BLOCKSIZE", "Block size of thin provisioned devices.", 1);
	printf_aligned(g, "-s", "--pool-size SIZE", "Size of pool device.", 1);
	printf_aligned(g, "-m", "--max-thins #MAXTHINS", "Maximum sum of all thin devices and snapshots.", 1);
	printf_aligned(g, "-u", "--unit ", "Output unit specifier.", 1);
	printf_aligned(g, "-n", "--numeric-only[=unit]", "Output numeric value only (optionally with unit identifier).", 0);
	printf_aligned(g, "-h", "--help", "This help.", 0);
	printf_aligned(g, "-V", "--version", "Print thin provisioning tools version.", 0);
	exit(0);
}

static void check_unit(struct global *g, char *arg)
{
	if (*(arg + 1))
		abort_prg("only one unit specifier character allowed!");
	else if (!strchr(g->unit.chars, *arg))
		abort_prg("output unit specifier character invalid!");

      	g->options.unit = *arg;
}

static void check_numeric_option(struct global *g, char *arg)
{
	if (g->options.n[NUMERIC])
		abort_prg("-n already given!");

	g->options.n[NUMERIC] = 1;

	if (arg) {
		if (!*arg || strncmp("unit", arg, strlen(arg)))
			abort_prg("-n invalid option argument");

		g->options.n[NUMERIC]++;
	}
}

static void check_size(struct global *g, enum numeric_options o, enum return_units unit, char *arg)
{
	if (g->options.n[o])
		abort_prg("option already given!");

	g->options.s[o] = strdup(arg);
	if (!g->options.s[o])
		abort_prg("failed to allocate string!");

	g->options.n[o] = to_bytes(g, arg, unit);
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
		if (c == 'b')
			check_size(g, BLOCKSIZE, RETURN_SECTORS, optarg);
		else if (c == 's')
			check_size(g, POOLSIZE, RETURN_SECTORS, optarg);
		else if (c == 'm')
			check_size(g, MAXTHINS, RETURN_BYTES, optarg);
		else if (c == 'u')
			check_unit(g, optarg);
		else if (c == 'n')
			check_numeric_option(g, optarg);
		else if (c == 'h')
			help(g);
		else if (c == 'V') {
			printf("%s\n", THIN_PROVISIONING_TOOLS_VERSION);
			exit(0);
		} else
			abort_prg("Invalid option!");
	}

	check_opts(&g->options);
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
		       prg, g->options.s[BLOCKSIZE],  g->options.s[POOLSIZE],  g->options.s[MAXTHINS]);

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
	unsigned idx = get_index(g, g->options.unit);
	double r;

	/* double-fold # of nodes, because they aren't fully populated in average */
	r = (1.0 + (2 * g->options.n[POOLSIZE] / g->options.n[BLOCKSIZE] / mappings_per_block() + g->options.n[MAXTHINS])) * 8 * bytes_per_sector; /* in bytes! */
	r /= g->unit.factors[idx]; /* in requested unit */

	printf_precision(g, r, idx);
}

int main(int argc, char **argv)
{
	struct global *g = init_prg();

	prg = basename(argv[0]);
	parse_command_line(g, argc, argv);
	print_estimated_result(g);
	exit_prg(g);
	return 0;
}
