#include "os/FragTreeIndex.h"
#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#define dout_subsys ceph_subsys_filestore

using namespace cohort;

namespace {

CephContext *cct;

class TestFragTreeIndex : public FragTreeIndex {
 public:
  TestFragTreeIndex(int split_threshold, int split_bits)
    : FragTreeIndex(::cct, split_threshold, split_bits)
  {}

  // expose split/merge functions
  int split(frag_t frag, int bits, bool async=true) {
    return FragTreeIndex::split(frag, bits, async);
  }
  int merge(frag_t frag, bool async=true) {
    return FragTreeIndex::merge(frag, async);
  }
  void do_split(frag_path path, int bits, frag_size_map &size_updates) {
    return FragTreeIndex::do_split(path, bits, size_updates);
  }
  void do_merge(frag_path path, int bits) {
    return FragTreeIndex::do_merge(path, bits);
  }
  void finish_split(frag_t frag, const frag_size_map &size_updates) {
    return FragTreeIndex::finish_split(frag, size_updates);
  }
  void finish_merge(frag_t frag) {
    return FragTreeIndex::finish_merge(frag);
  }
  void restart_migrations(bool async=true) {
    return FragTreeIndex::restart_migrations(async);
  }

  // helpers for synchronous split/merge
  int split_sync(frag_t frag, int bits) {
    frag_path path = {};
    path.build(tree, frag.value());
    assert(path.frag == frag);

    int r = FragTreeIndex::split(frag, bits, false);
    if (r) return r;

    frag_size_map size_updates;
    FragTreeIndex::do_split(path, bits, size_updates);
    FragTreeIndex::finish_split(path.frag, size_updates);
    return 0;
  }
  int merge_sync(frag_t frag, int bits) {
    int r = FragTreeIndex::merge(frag, false);
    if (r) return r;

    frag_path path = {};
    path.build(tree, frag.value());
    assert(path.frag == frag);

    FragTreeIndex::do_merge(path, bits);
    FragTreeIndex::finish_merge(path.frag);
    return 0;
  }

  // expose frag size map
  using FragTreeIndex::frag_size_map;
  const frag_size_map& get_size_map() const { return sizes; }

  // simulate a crash by clearing state without unmounting
  void crash() {
    committed.tree.clear();
    committed.splits.clear();
    committed.merges.clear();
    tree.clear();
    sizes.clear();
    assert(migration_threads.empty()); // no async migrations
    ::close(rootfd);
    rootfd = -1;
  }
};

// helper class to create a temporary directory and clean it up on exit
class tmpdir_with_cleanup {
 private:
  std::string path;
  void recursive_rmdir(int fd, DIR *dir);
 public:
  tmpdir_with_cleanup(const char *path);
  ~tmpdir_with_cleanup();
  operator const std::string&() const { return path; }
};

tmpdir_with_cleanup::tmpdir_with_cleanup(const char *path)
  : path(path)
{
  int r = ::mkdir(path, 0755);
  assert(r == 0); // blow up so we don't delete existing files
}

tmpdir_with_cleanup::~tmpdir_with_cleanup()
{
  int fd = ::open(path.c_str(), O_RDONLY);
  assert(fd >= 0);
  DIR *dir = ::fdopendir(fd);
  assert(dir);
  recursive_rmdir(fd, dir);
  ::closedir(dir);
  int r = ::rmdir(path.c_str());
  assert(r == 0);
}

void tmpdir_with_cleanup::recursive_rmdir(int fd, DIR *dir)
{
  struct dirent *dn;
  while ((dn = ::readdir(dir)) != NULL) {
    if (dn->d_type == DT_DIR) {
      // skip . and ..
      if (dn->d_name[0] == '.' && dn->d_name[1] == 0)
        continue;
      if (dn->d_name[0] == '.' && dn->d_name[1] == '.' && dn->d_name[2] == 0)
        continue;

      int fd2 = ::openat(fd, dn->d_name, O_RDONLY);
      assert(fd2 >= 0);
      DIR *dir2 = ::fdopendir(fd2);
      assert(dir2);

      recursive_rmdir(fd2, dir2);
      ::closedir(dir2); // closes fd2 also

      int r = ::unlinkat(fd, dn->d_name, AT_REMOVEDIR);
      assert(r == 0);
    } else {
      int r = ::unlinkat(fd, dn->d_name, 0);
      assert(r == 0);
    }
  }
}

} // anonymous namespace

#if 1
TEST(OsFragTreeIndex, FragPathBuild)
{
  frag_t root;
  frag_t a = root.make_child(0, 2);
  frag_t b = root.make_child(3, 2);

  fragtree_t tree;
  tree.split(root, 2); // 0 1 2 3
  tree.split(a, 1); // 0/0 0/1
  tree.split(b, 1); // 3/0 3/1

  frag_path path;

  ASSERT_EQ(path.build(tree, 0), 0);
  ASSERT_EQ(path.path, std::string("0/0/"));

  ASSERT_EQ(path.build(tree, 0x200000), 0);
  ASSERT_EQ(path.path, std::string("0/1/"));

  ASSERT_EQ(path.build(tree, 0x400000), 0);
  ASSERT_EQ(path.path, std::string("1/"));

  ASSERT_EQ(path.build(tree, 0x800000), 0);
  ASSERT_EQ(path.path, std::string("2/"));

  ASSERT_EQ(path.build(tree, 0xC00000), 0);
  ASSERT_EQ(path.path, std::string("3/0/"));

  ASSERT_EQ(path.build(tree, 0xE00000), 0);
  ASSERT_EQ(path.path, std::string("3/1/"));

  // TODO: test frag_path::append() error handling
}

TEST(OsFragTreeIndex, OpenStatUnlink)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex");

  TestFragTreeIndex index(1024, 2);
  struct stat st;

  ASSERT_EQ(index.init(path), 0);
  ASSERT_EQ(index.mount(path), 0);
  ASSERT_EQ(index.stat("stat-noent", 0, &st), -ENOENT);
  int fd = -1;
  ASSERT_EQ(index.open("open-noent", 0, false, &fd), -ENOENT);
  ASSERT_EQ(index.open("open-create", 0, true, &fd), 0);
  ASSERT_EQ(index.stat("open-create", 0, &st), 0);
  ::close(fd);
  ASSERT_EQ(index.unlink("open-create", 0), 0);
  ASSERT_EQ(index.unmount(), 0);
}

TEST(OsFragTreeIndex, Split)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-split");

  TestFragTreeIndex index(1024, 2);
  ASSERT_EQ(index.init(path), 0);
  ASSERT_EQ(index.mount(path), 0);

  const std::string filename("0000000000000000-foo");
  const int64_t hash = 0;

  int fd = -1;
  ASSERT_EQ(index.open(filename, hash, true, &fd), 0);
  ::close(fd);

  // start a split (async=false)
  frag_path p = {};
  ASSERT_EQ(index.split(p.frag, 1, false), 0);

  // make sure we can find it before rename
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // make sure we can't start another split or merge
  ASSERT_TRUE(index.split(p.frag, 1, false) != 0);
  ASSERT_TRUE(index.merge(p.frag, false) != 0);

  // do the rename
  TestFragTreeIndex::frag_size_map size_updates;
  index.do_split(p, 1, size_updates);

  // make sure we can still find it after rename
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // complete the split
  index.finish_split(p.frag, size_updates);
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // verify the size map
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag.make_child(0, 1)] = 1;
  sizes[p.frag.make_child(1, 1)] = 0;
  ASSERT_EQ(index.get_size_map(), sizes);

  ASSERT_EQ(index.unmount(), 0);
}

TEST(OsFragTreeIndex, Merge)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-merge");

  TestFragTreeIndex index(1024, 2);
  ASSERT_EQ(index.init(path), 0);
  ASSERT_EQ(index.mount(path), 0);

  // do an initial split
  const frag_path p = {};
  ASSERT_EQ(index.split_sync(p.frag, 1), 0);

  // create a file in 0/
  const std::string filename("0000000000000000-foo");
  const int64_t hash = 0;
  int fd = -1;
  ASSERT_EQ(index.open(filename, hash, true, &fd), 0);
  ::close(fd);

  // start a merge (async=false)
  ASSERT_EQ(index.merge(p.frag, false), 0);

  // make sure we can find it before rename
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // make sure we can't start another split or merge
  ASSERT_TRUE(index.split(p.frag, 1, false) != 0);
  ASSERT_TRUE(index.merge(p.frag, false) != 0);

  // do the rename
  index.do_merge(p, 1);

  // make sure we can still find it after rename
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // complete the merge
  index.finish_merge(p.frag);
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // verify the size map
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag] = 1;
  ASSERT_EQ(index.get_size_map(), sizes);

  ASSERT_EQ(index.unmount(), 0);
}
#endif
TEST(OsFragTreeIndex, SplitRecovery)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-split-recovery");

  TestFragTreeIndex index(1024, 2);
  ASSERT_EQ(index.init(path), 0);
  ASSERT_EQ(index.mount(path), 0);

  const std::string filename("0000000000000000-foo");
  const int64_t hash = 0;

  int fd = -1;
  ASSERT_EQ(index.open(filename, hash, true, &fd), 0);
  ::close(fd);

  // start a split (async=false)
  frag_path p = {};
  ASSERT_EQ(index.split(p.frag, 1, false), 0);

  // make sure we can find it before rename
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // crash before starting renames
  index.crash();

  // remount without starting recovery
  ASSERT_EQ(index.mount(path, false), 0);

  // verify the size map before recovery to test count_sizes()
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag.make_child(0, 1)] = 1;
  sizes[p.frag.make_child(1, 1)] = 0;
  ASSERT_EQ(index.get_size_map(), sizes);

  // do the synchronous recovery
  index.restart_migrations(false);

  // make sure we can still find it after recovery
  ASSERT_EQ(index.lookup(filename, hash), 0);

  // verify the size map after recovery
  ASSERT_EQ(index.get_size_map(), sizes);

  ASSERT_EQ(index.unmount(), 0);
}

TEST(OsFragTreeIndex, CountSizes)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-size-recovery");

  TestFragTreeIndex index(1024, 2);
  ASSERT_EQ(index.init(path), 0);
  ASSERT_EQ(index.mount(path), 0);

  const frag_t root;
  const frag_t f0 = root.make_child(0, 2);
  const frag_t f00 = f0.make_child(0, 1);
  const frag_t f01 = f0.make_child(1, 1);
  const frag_t f1 = root.make_child(1, 2);
  const frag_t f10 = f1.make_child(0, 1);
  const frag_t f11 = f1.make_child(1, 1);
  const frag_t f2 = root.make_child(2, 2);
  const frag_t f20 = f2.make_child(0, 1);
  const frag_t f21 = f2.make_child(1, 1);
  const frag_t f3 = root.make_child(3, 2);
  const frag_t f30 = f3.make_child(0, 1);
  const frag_t f31 = f3.make_child(1, 1);
  ASSERT_EQ(index.split_sync(root, 2), 0);
  ASSERT_EQ(index.split_sync(f0, 1), 0);
  ASSERT_EQ(index.split_sync(f00, 1), 0);
  ASSERT_EQ(index.split_sync(f01, 1), 0);
  ASSERT_EQ(index.split_sync(f1, 1), 0);
  ASSERT_EQ(index.split_sync(f10, 1), 0);
  ASSERT_EQ(index.split_sync(f11, 1), 0);
  ASSERT_EQ(index.split_sync(f2, 1), 0);
  ASSERT_EQ(index.split_sync(f20, 1), 0);
  ASSERT_EQ(index.split_sync(f21, 1), 0);
  ASSERT_EQ(index.split_sync(f3, 1), 0);
  ASSERT_EQ(index.split_sync(f30, 1), 0);
  ASSERT_EQ(index.split_sync(f31, 1), 0);

  struct filehash { uint64_t hash; const char *name; };
  filehash a { 0x0000000000000000LL, "0000000000000000-a" };
  filehash b { 0x0000000000100000LL, "0000000000100000-b" };
  filehash c { 0x0000000000200000LL, "0000000000200000-c" };
  filehash d { 0x0000000000300000LL, "0000000000300000-d" };
  filehash e { 0x0000000000400000LL, "0000000000400000-e" };
  filehash f { 0x0000000000500000LL, "0000000000500000-f" };
  filehash g { 0x0000000000600000LL, "0000000000600000-g" };
  filehash h { 0x0000000000700000LL, "0000000000700000-h" };
  filehash i { 0x0000000000800000LL, "0000000000800000-i" };
  filehash j { 0x0000000000900000LL, "0000000000900000-j" };
  filehash k { 0x0000000000A00000LL, "0000000000A00000-k" };
  filehash l { 0x0000000000B00000LL, "0000000000B00000-l" };
  filehash m { 0x0000000000C00000LL, "0000000000C00000-m" };
  filehash n { 0x0000000000D00000LL, "0000000000D00000-n" };
  filehash o { 0x0000000000E00000LL, "0000000000E00000-o" };
  filehash p { 0x0000000000F00000LL, "0000000000F00000-p" };

  int fd = -1;
  ASSERT_EQ(index.open(a.name, a.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(b.name, b.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(c.name, c.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(d.name, d.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(e.name, e.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(f.name, f.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(g.name, g.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(h.name, h.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(i.name, i.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(j.name, j.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(k.name, k.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(l.name, l.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(m.name, m.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(n.name, n.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(o.name, o.hash, true, &fd), 0); ::close(fd);
  ASSERT_EQ(index.open(p.name, p.hash, true, &fd), 0); ::close(fd);

  // verify the size map after creates
  TestFragTreeIndex::frag_size_map sizes;
  sizes[f00.make_child(0, 1)] = 1;
  sizes[f00.make_child(1, 1)] = 1;
  sizes[f01.make_child(0, 1)] = 1;
  sizes[f01.make_child(1, 1)] = 1;
  sizes[f10.make_child(0, 1)] = 1;
  sizes[f10.make_child(1, 1)] = 1;
  sizes[f11.make_child(0, 1)] = 1;
  sizes[f11.make_child(1, 1)] = 1;
  sizes[f20.make_child(0, 1)] = 1;
  sizes[f20.make_child(1, 1)] = 1;
  sizes[f21.make_child(0, 1)] = 1;
  sizes[f21.make_child(1, 1)] = 1;
  sizes[f30.make_child(0, 1)] = 1;
  sizes[f30.make_child(1, 1)] = 1;
  sizes[f31.make_child(0, 1)] = 1;
  sizes[f31.make_child(1, 1)] = 1;
  ASSERT_EQ(index.get_size_map(), sizes);

  index.crash();

  // remount without starting recovery
  ASSERT_EQ(index.mount(path, false), 0);

  // verify the size map before recovery to test count_sizes()
  ASSERT_EQ(index.get_size_map(), sizes);

  ASSERT_EQ(index.unmount(), 0);
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);
  ::testing::InitGoogleTest(&argc, argv);

  int r = RUN_ALL_TESTS();
  if (r >= 0)
    dout(0) << "There are no failures in the test case" << dendl;
  else
    derr << "There are some failures" << dendl;
  return 0;
}
