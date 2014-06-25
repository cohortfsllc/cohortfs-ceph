/*
 *  Ceph PageSet benchmark
 */

#include <chrono>

#include "include/buffer.h"
#include "include/types.h"
#include "os/PageSet.h"

#include "global/global_init.h"
#include "common/debug.h"

#include "common/errno.h"
#include "common/strtol.h"
#include "common/ceph_argparse.h"

#define dout_subsys ceph_subsys_filestore

static void usage()
{
	derr << "usage: pagesetbench [flags]\n"
		"  --size\n"
		"        total size in bytes\n"
		"  --block-size\n"
		"        block size in bytes for each write\n"
		"  --repeats\n"
		"        number of times to repeat the write cycle\n"
		"  --threads\n"
		"        number of threads to carry out this workload\n" << dendl;
	generic_server_usage();
}

// helper class for bytes with units
struct byte_units {
	size_t v;
	byte_units(size_t v) : v(v) {}

	bool parse(const std::string &val);

	operator size_t() const { return v; }
};

bool byte_units::parse(const string &val)
{
	char *endptr;
	errno = 0;
	unsigned long long ret = strtoull(val.c_str(), &endptr, 10);
	if (errno == ERANGE && ret == ULLONG_MAX)
		return false;
	if (errno && ret == 0)
		return false;
	if (endptr == val.c_str())
		return false;

	// interpret units
	int lshift = 0;
	switch (*endptr) {
	case 't':
	case 'T':
		lshift += 10;
		// cases fall through
	case 'g':
	case 'G':
		lshift += 10;
	case 'm':
	case 'M':
		lshift += 10;
	case 'k':
	case 'K':
		lshift += 10;
		if (*++endptr)
			return false;
	case 0:
		break;

	default:
		return false;
	}

	// test for overflow
	typedef std::numeric_limits<unsigned long long> limits;
	if (ret & ~((1 << (limits::digits - lshift))-1))
		return false;

	v = ret << lshift;
	return true;
}

std::ostream& operator<<(std::ostream &out, const byte_units &amount)
{
	static const char* units[] = { "B", "KB", "MB", "GB", "TB" };
	static const int max_units = sizeof(units)/sizeof(*units);

	int unit = 0;
	auto v = amount.v;
	while (v >= 1024 && unit < max_units) {
		// preserve significant bytes
		if (v < 1048576 && (v % 1024 != 0))
			break;
		v >>= 10;
		unit++;
	}
	return out << v << ' ' << units[unit];
}

byte_units size = 1048576;
byte_units block_size = 4096;
int repeats = 1;
int n_threads = 1;
static const size_t PageSize = 64 << 10;
typedef PageSet<PageSize> page_set;
page_set pages;


class OBS_Worker : public Thread
{
public:
    OBS_Worker() { }

    void *entry() {
	bufferlist data;
	data.append(buffer::create(block_size));

	dout(0) << "Writing " << size << " in blocks of " << block_size
		<< dendl;

	for (int ix = 0; ix < repeats; ++ix) {
	    uint64_t offset = 0;
	    size_t len = size;

	    std::cout << "Write cycle " << ix << std::endl;

	    page_set::iterator p = pages.begin();
	    while (len) {
		assert(p != pages.end());
		size_t count = len < block_size ? len : (size_t)block_size;

		const size_t page_off = offset - p->offset;
		if (count > PageSize - page_off)
			count = PageSize - page_off;

		memcpy(p->data + page_off, data.c_str(), count);

		offset += count;
		len -= count;
		if ((offset % PageSize) == 0)
			++p;
	    }
	}
	return 0;
    }
};

int main(int argc, const char *argv[])
{
	// command-line arguments
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	global_init(NULL, args, CEPH_ENTITY_TYPE_OSD,
			CODE_ENVIRONMENT_UTILITY, 0);

	string val;
	vector<const char*>::iterator i = args.begin();
	while (i != args.end()) {
		if (ceph_argparse_double_dash(args, i))
			break;

		if (ceph_argparse_witharg(args, i, &val, "--size", (char*)NULL)) {
			if (!size.parse(val)) {
				derr << "error parsing size: It must be an int." << dendl;
				usage();
			}
		} else if (ceph_argparse_witharg(args, i, &val, "--block-size", (char*)NULL)) {
			if (!block_size.parse(val)) {
				derr << "error parsing block-size: It must be an int." << dendl;
				usage();
			}
		} else if (ceph_argparse_witharg(args, i, &val, "--repeats", (char*)NULL)) {
		    repeats = atoi(val.c_str());
		} else if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)NULL)) {
		    n_threads = atoi(val.c_str());
		} else {
			derr << "Error: can't understand argument: " << *i <<
				"\n" << dendl;
			usage();
		}
	}

	common_init_finish(g_ceph_context);

	// create object store
	dout(0) << "size " << size << dendl;
	dout(0) << "block-size " << block_size << dendl;
	dout(0) << "repeats " << repeats << dendl;
	dout(0) << "threads " << n_threads << dendl;

	pages.alloc_range(0, size);

	int thr_ix;
	OBS_Worker *workers = new OBS_Worker[n_threads];
	auto t1 = std::chrono::high_resolution_clock::now();
	for (thr_ix = 0; thr_ix < n_threads; ++thr_ix) {
	    workers[thr_ix].create();
	}
	for (thr_ix = 0; thr_ix < n_threads; ++thr_ix) {
	    workers[thr_ix].join();
	}
	delete[] workers;
	auto t2 = std::chrono::high_resolution_clock::now();

	pages.free_pages_after(0);

	using std::chrono::duration_cast;
	using std::chrono::microseconds;
	auto duration = duration_cast<microseconds>(t2 - t1);
	byte_units total = size * repeats * n_threads;
	byte_units rate = (1000000LL * total) / duration.count();
	dout(0) << "Wrote " << total << " in "
		<< duration.count() << "us, at a rate of " << rate << "/s"
		<< dendl;

	return 0;
}
