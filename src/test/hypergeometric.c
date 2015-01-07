/* hypergeometric.c
   Minor tool to help find a break even point
   Adam C. Emerson <aemerson@cohortfs.com>
   Copyright © 2015 CohortFS, LLC

   Until we have data movement, our placement strategy is an
   unweighted hypergeometric draw of k OSDs from a cluster of size
   n. In practice, the two obvious ways to do this are a brute-force
   draw with retry on repetition and a Fisher-Yates shuffle. The
   former is obviously the right choice when n is much larger than k,
   and the latter is obviously the right choice when n is not much
   larger than k.

   The question is, how much larger is 'much larger'? That's what this
   file is for. We run rounds of tests and time them to see exactly
   which method works best when.

   It's also my debugged version of the two draw functions. And the
   test harness might be useful for other things.

   In actual placement the seeds will be te result of some hash
   function, but I just read random data from /dev/urandom and use
   it. */

#define _XOPEN_SOURCE 700
#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

#include <sys/stat.h>
#include <sys/times.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

/* This is a 64-bit linear congruential generator. */

static inline uint32_t lcg(uint64_t *const seed)
{
	const uint64_t a = 6364136223846793005; /* If it's good for Knuth */
	const uint64_t c = 1442695040888963407; /* It's good enough for me */

	*seed = a * (*seed) + c;
	return *seed >> 32;
}


/* These definitions are used only by the brute force hypergeometric
   draw. */

struct element {
	uint32_t i;
	struct element* n;
};

/* This function attempts to insert an integer into the set. If the
   integer is already a member, it returns false. Otherwise it
   returns true. */

static inline bool insert(struct element *const e,
			  struct element** const set)
{
	/* We use a sorted list of integers since in the general case
	   the number of OSDs picked is going to be much, much smaller
	   than the number of OSDs available, plus all we care about
	   is membership. We use insertion sort since we're inserting
	   one at a time and the number is extremely small.

	   The caller should not set e->n, since it's never read and
	   always set by this function. */

	struct element **cur = set;

	while (*cur) {
		if (unlikely((*cur)->i == e->i)) {
			return false;
		} else if ((*cur)->n && ((*cur)->n->i > e->i)) {
			e->n = (*cur)->n;
			(*cur)->n = e;
			return true;
		} else {
			cur = &(*cur)->n;
		}
	}
	*cur = e;
	e->n = NULL;
	return true;
}

/* This is a brute-force hypergeometric draw. Its worst case time
   complexity is pretty awful, but it's average time complexity
   approaches k as n increases. In practice even in the worst case it
   isn't that bad since the LCG will have everything come up sooner
   rather than later. If someone wants to stripe 256 stripes across
   256 servers the way hotmail apparently does, you likely don't care
   about pseudorandom placement at all. */

static void hypergeometric_brutality(const uint32_t k,
				     const uint32_t n,
				     uint64_t seed,
				     bool (*cb)(uint32_t n))
{
	struct element elements[k];
	struct element *set = NULL;
	for (uint32_t draw = 0; draw < k; ++draw) {
		do {
			elements[draw].i = lcg(&seed) % n;
		} while (!insert(elements + draw, &set));
		if (unlikely(!cb(elements[draw].i))) {
			return;
		}
	}
}

/* This is a Fisher-Yates shuffle. It has exactly O(n) complexity no
   matter what. Of k is close to n, this is pretty awesome, since
   that's exactly when the brute-force hypergeometric draw fails. */

static void gone_fisher_yatesing(const uint32_t k,
				 const uint32_t n,
				 uint64_t seed,
				 bool (*cb)(uint32_t n))
{
	uint32_t a[n];
	for (uint32_t i = 0; i < n; ++i) {
		uint32_t j = lcg(&seed) % (i + 1);
		if (likely(j != i)) {
			a[i] = a[j];
		}
		a[j] = i;
	}

	for (uint32_t i = 0; i < k; ++i) {
		if (unlikely(!cb(a[i]))) {
			return;
		}
	}
}

/* Definitions below here are used by the driver */

/* Print usage information. As far as I'm concerned, if you actually
   request help with -h it's not an error and shouldn't be printed on
   stderr. */

static void usage(FILE *const stream, const char* argv0)
{
	fprintf(stream, "Usage: %s [-h] [-f] [-p | -c] [-k k] [-n n] "
		"[-t trials]\n",
		argv0);
	fprintf(stream, "-h: show this help.\n");
	fprintf(stream, "-f: use Fisher-Yates shuffle instead of brute-force "
		"hypergeometric draw.\n");
	fprintf(stream, "-p: show performance data rather than printing"
		"draws. Defaults to printing draws.\n");
	fprintf(stream,
		"-c: comparison run, compare Fisher-Yates shuffle to\n"
		"    brute-force hypergeometric draw. Iterate all values of\n"
		"    n up to that specified and all values of k inclusive.\n");
	fprintf(stream, "-k k: number of draws to make from candidate set,"
		"must be a positive integer less than n. Defaults to 3.\n");
	fprintf(stream, "-n n: size of the candidate set, must be a postive "
		"integer greater than k. Defaults to 100\n");
	fprintf(stream, "-t trials: number of trials to run.\n");
}
/* Boiler plate that would otherwise show up in all the options. */

static bool parse_num(const char *str, uint32_t *const n, const char *param)
{
	char *end;
	int64_t trial;
	if (*str == 0) {
		fprintf(stderr, "%s requires a positive integer.\n", param);
		return false;
	}

	trial = strtoll(str, &end, 0);
	if (!(*end == 0)) {
		fprintf(stderr, "%s requires a positive integer, not `%s'.\n",
			param, str);
		return false;
	}

	if (trial < 0) {
		fprintf(stderr, "%s requires a positive integer.\n", param);
		return false;
	}

	if (trial == 0) {
		fprintf(stderr, "%s requires a positive integer, not a "
		       "non-negative integer. Pay attention!\n", param);
		return false;
	}

	*n = (uint32_t) trial;

	return true;
}

/* Parse and verify all command line options. When this function
   completes everything the user can set is guaranteed to be valid. */

static bool parseopts(int argc, char *const argv[],
		      uint32_t *const k, uint32_t *const n,
		      uint32_t *const t, bool *const fisher,
		      bool *const compare, bool *const timing,
		      bool *const help)
{
	int opt;
	bool kspec = false;

	while ((opt = getopt(argc, argv, "hfcpk:n:t:")) != -1) {
		switch (opt) {
		case 'h':
			*help = true;
			break;

		case 'f':
			*fisher = true;
			break;

		case 'c':
			*compare = true;
			break;

		case 'p':
			*timing = true;
			break;

		case 'k':
			if (!parse_num(optarg, k, "-k")) {
				return false;
			}
			kspec = true;
			break;

		case 'n':
			if (!parse_num(optarg, n, "-n")) {
				return false;
			}
			break;

		case 't':
			if (!parse_num(optarg, t, "-t")) {
				return false;
			}
			break;

		default:
			return false;
		}
	}

	if (optind < argc) {
		fprintf(stderr, "Non-option arguments are neither desired "
			"nor permitted.\n");
		return false;
	}

	if (*n < *k) {
		fprintf(stderr, "Funny thing about drawing without "
			"replacement: you can't draw more things than there "
			"are things.\n");
		return false;
	}
	return true;

	if (*compare && *fisher) {
		fprintf(stderr, "The -c option implies both methods.\n");
		return false;
	}

	if (*compare && kspec) {
		fprintf(stderr, "The -c option implies all k, 0 < k ≤ n.\n");
		return false;
	}

	if (*compare && *timing) {
		fprintf(stderr, "The -c option is exclusive with -p.");
		return false;
	}
}

/* Read t 64-bit random values from the /dev/urandom device. If this
   function returns non-NULL, our seeds are fully populated. The
   memory pointed to must be freed with free(3). */

static uint64_t *read_seeds(const uint32_t t)
{
	uint64_t *seeds = NULL;
	size_t toread = t * sizeof(*seeds);
	char *cursor;
	int random_fd = -1;

	seeds = malloc(toread);
	if (!seeds) {
		fprintf(stderr, "Unable to allocate memory.\n");
		return false;
	}

	random_fd = open("/dev/urandom", O_RDONLY);
	if (random_fd == -1) {
		perror("open: /dev/urandom");
		goto error;
	}

	cursor = (char *)seeds;
	while (toread) {
		ssize_t r = read(random_fd, cursor, toread);
		if (unlikely(r < 0)) {
			perror("read");
			goto error;
		} else if (unlikely(r == 0)) {
			fprintf(stderr, "There is no randomness left anywhere "
				"in the world. May god us keep from single "
				"vision and Newton's sleep.\n");
			goto error;
		} else {
			size_t size = (unsigned) r;
			toread -= size;
			cursor += size;
		}
	}

	if (close(random_fd)) {
		perror("close:/dev/urandom");
		random_fd = -1;
		goto error;
	}

	return seeds;

error:

	if (seeds) {
		free(seeds);
	}

	if (random_fd != -1) {
		if (close(random_fd) < 0) {
			perror("close:/dev/urandom");
		}
	}

	return NULL;
}

/* Print n and ask for more. */

static bool noisy_cb(const uint32_t n)
{
	printf(" %8" PRIu32, n);
	return true;
}

/* Do nothing. We don't want to mess about when being timed. */

static bool boring_cb(const uint32_t n __attribute__((unused)))
{
	return true;
}


static void run_and_print(const uint32_t k, const uint32_t n,
			  const uint32_t t, const uint64_t *const seeds,
			  const char m)
{
	struct tms t1, t2;
	uint64_t csecs;
	void (*drawer)(const uint32_t, const uint32_t,
		       uint64_t, bool (*)(uint32_t))
		= (m == 'f') ? gone_fisher_yatesing : hypergeometric_brutality;
	assert((m == 'f') || (m == 'b'));

	times(&t1);
	for (uint32_t trial = 0; trial < t; ++trial) {
		drawer(k, n, seeds[trial], boring_cb);
	}
	times(&t2);
	csecs = (unsigned) (t2.tms_utime - t1.tms_utime);
	csecs *= 100;
	csecs /= (unsigned) sysconf(_SC_CLK_TCK);
	printf(" %c %8"PRIu32" %8"PRIu32" %8"PRIu32" %8"PRIu64"\n",
	       m, k, n, t, csecs);
}

static void comparison_run(const uint32_t maxn, const uint32_t trials,
			   const uint64_t *const seeds)
{
	printf(" %c %8c %8c %8s %8s\n", 'M', 'k', 'n', "trials", "time");
	printf("---------------------------------------\n");

	for (uint32_t n = 1; n <= maxn; ++n) {
		for (uint32_t k = 1; k <= n; ++k) {
			run_and_print(k, n, trials, seeds, 'b');
			run_and_print(k, n, trials, seeds, 'f');
		}
	}
}

/* The function that kicks everything off. */

int main(const int argc, char *const argv[])
{
	uint64_t *seeds = NULL;
	uint32_t k = 3;
	uint32_t n = 100;
	uint32_t t = 1;
	bool fisher = false;
	bool compare= false;
	bool timing = false;
	bool help = false;
	void (*drawer)(const uint32_t, const uint32_t,
		       uint64_t, bool (*)(uint32_t));
	bool (*cb)(uint32_t);


	if (!parseopts(argc, argv, &k, &n, &t, &fisher, &compare,
		       &timing, &help)) {
		usage(stderr, argv[0]);
		exit(EXIT_FAILURE);
	}

	if (help) {
		usage(stdout, argv[0]);
		exit(EXIT_SUCCESS);
	}

	seeds = read_seeds(t);
	if (!seeds) {
		exit(EXIT_FAILURE);
	}

	if (!compare) {
		printf("Using ");
		if (fisher) {
			drawer = gone_fisher_yatesing;
			printf("Fisher-Yates shuffle");
		} else {
			drawer = hypergeometric_brutality;
			printf("brute-force hypergeometric draw");
		}
		printf(" to pick %" PRIu32 " draws from %" PRIu32
		       " possibilities in %" PRIu32" trials.\n\n",
		       k, n, t);

		if (timing) {
			struct tms before, after;
			uint64_t csecs;
			cb = boring_cb;
			times(&before);
			for (uint32_t trial = 0; trial < t; ++trial) {
				drawer(k, n, seeds[trial], cb);
			}
			times(&after);
			csecs = (unsigned) (after.tms_utime -
					    before.tms_utime);
			csecs *= 100;
			csecs /= (unsigned) sysconf(_SC_CLK_TCK);
			printf("Completed in %"PRIu64".%"PRIu64" seconds.\n",
			       csecs / 100, csecs % 100);
		} else {
			cb = noisy_cb;
			for (uint32_t trial = 0; trial < t; ++trial) {
				printf("%8" PRIu32 " %016" PRIx64 ": ", trial,
				       seeds[trial]);
				drawer(k, n, seeds[trial], cb);
				printf("\n");
			}
		}
	} else {
		comparison_run(n, t, seeds);
	}

	free(seeds);

	exit(EXIT_SUCCESS);
}
