/*
 *  Ceph ObjectStore benchmark
 */

#include <chrono>

#include "os/ObjectStore.h"

#include "global/global_init.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"


#define dout_subsys ceph_subsys_filestore

static void usage()
{
	derr << "usage: objectstore-bench [flags]\n"
		"  --size\n"
		"        total size in bytes\n"
		"  --block-size\n"
		"        block size in bytes for each write\n" << dendl;
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


int main(int argc, const char *argv[])
{
	// command-line arguments
	byte_units size = 1048576;
	byte_units block_size = 4096;
	int repeats = 1;

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
		} else {
			derr << "Error: can't understand argument: " << *i <<
				"\n" << dendl;
			usage();
		}
	}

	common_init_finish(g_ceph_context);

	// create object store
	dout(0) << "objectstore " << g_conf->osd_objectstore << dendl;
	dout(0) << "data " << g_conf->osd_data << dendl;
	dout(0) << "journal " << g_conf->osd_journal << dendl;
	dout(0) << "size " << size << dendl;
	dout(0) << "block-size " << block_size << dendl;
	dout(0) << "repeats " << repeats << dendl;

	ObjectStore *fs = ObjectStore::create(g_ceph_context,
			g_conf->osd_objectstore,
			g_conf->osd_data,
			g_conf->osd_journal);
	if (fs == NULL) {
		derr << "bad objectstore type " << g_conf->osd_objectstore << dendl;
		return 1;
	}
	if (fs->mkfs() < 0) {
		derr << "mkfs failed" << dendl;
		return 1;
	}
	if (fs->mount() < 0) {
		derr << "mount failed" << dendl;
		return 1;
	}

	dout(10) << "created objectstore " << fs << dendl;

	ObjectStore::Transaction ft;
	ft.create_collection(coll_t());
	fs->apply_transaction(ft);

	sobject_t poid(object_t("osbench"), 0);

	C_GatherBuilder gather(g_ceph_context);

	bufferlist data;
	data.append(buffer::create(block_size));

	dout(0) << "Writing " << size << " in blocks of " << block_size << dendl;
	auto t1 = std::chrono::high_resolution_clock::now();

	for (int ix = 0; ix < repeats; ++ix) {
	    uint64_t offset = 0;
	    size_t len = size;

	    C_GatherBuilder gather(g_ceph_context);
	    list<ObjectStore::Transaction*> tls;

	    std::cout << "Write cycle " << ix << std::endl;
	    while (len) {
		size_t count = len < block_size ? len : (size_t)block_size;

		ObjectStore::Transaction *t = new ObjectStore::Transaction;
		t->write(coll_t(), hobject_t(poid), offset, count, data);
		tls.push_back(t);

		offset += count;
		len -= count;
	    }

	    // try running each repeat set at once
	    fs->queue_transactions(NULL, tls, NULL, gather.new_sub());

	    if (gather.has_subs()) {
		// wait for all writes to be committed
		Mutex lock("osbench");
		Cond cond;
		bool done = false;

		gather.set_finisher(new C_SafeCond(&lock, &cond, &done));
		gather.activate();

		lock.Lock();
		while (!done)
			cond.Wait(lock);
		lock.Unlock();
	    }

	    // we must delete all transactions
	    while (! tls.empty()) {
		ObjectStore::Transaction* t = tls.front();
		tls.pop_front();
		delete t;
	    }
	}


	auto t2 = std::chrono::high_resolution_clock::now();

	// remove the object
	ObjectStore::Transaction t;
	t.remove(coll_t(), hobject_t(poid));
	fs->apply_transaction(t);

	fs->umount();
	delete fs;

	using std::chrono::duration_cast;
	using std::chrono::microseconds;
	auto duration = duration_cast<microseconds>(t2 - t1);
	byte_units rate = (1000000LL * size * repeats) / duration.count();
	dout(0) << "Wrote " << size * repeats << " in " << duration.count()
		<< "us, at a rate of " << rate << "/s" << dendl;

	return 0;
}
