#include "os/FragTreeIndex.h"
#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "os/chain_xattr.h"
#include "global/global_init.h"

#define dout_subsys ceph_subsys_filestore

using namespace cohort;

namespace {

CephContext *cct;

class TestFragTreeIndex : public FragTreeIndex {
 public:
  TestFragTreeIndex(uint32_t initial_split = 0)
    : FragTreeIndex(::cct, initial_split)
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
    ::close(rootfd);
    rootfd = -1;
  }
};

hoid_t mkhoid(const char *name, uint64_t hash)
{
  hoid_t oid;
  oid.oid.name.assign(name);
  oid.hk = hash;
  return oid;
}

// helper class to create a temporary directory and clean it up on exit
class tmpdir_with_cleanup {
 private:
  std::string path;
  void recursive_rmdir(int fd, DIR *dir);
 public:
  tmpdir_with_cleanup(const char *path);
  ~tmpdir_with_cleanup();
  operator const std::string&() const { return path; }
  const char* c_str() const { return path.c_str(); }
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
  if (fd < 0)
    return;
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

  ASSERT_EQ(0, path.build(tree, 0));
  ASSERT_EQ(path.path, std::string("0/0/"));

  ASSERT_EQ(0, path.build(tree, 0x20000000000000));
  ASSERT_EQ(std::string("0/1/"), path.path);

  ASSERT_EQ(0, path.build(tree, 0x40000000000000));
  ASSERT_EQ(std::string("1/"), path.path);

  ASSERT_EQ(0, path.build(tree, 0x80000000000000));
  ASSERT_EQ(std::string("2/"), path.path);

  ASSERT_EQ(0, path.build(tree, 0xC0000000000000));
  ASSERT_EQ(std::string("3/0/"), path.path);

  ASSERT_EQ(0, path.build(tree, 0xE0000000000000));
  ASSERT_EQ(std::string("3/1/"), path.path);

  // TODO: test frag_path::append() error handling
}

TEST(OsFragTreeIndex, OpenStatUnlink)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex");

  TestFragTreeIndex index;
  struct stat st;

  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));
  ASSERT_EQ(-ENOENT, index.stat(mkhoid("stat-noent", 0), &st));
  int fd = -1;
  ASSERT_EQ(-ENOENT, index.open(mkhoid("open-noent", 0), false, &fd));
  ASSERT_EQ(0, index.open(mkhoid("open-create", 0), true, &fd));
  ASSERT_EQ(0, index.stat(mkhoid("open-create", 0), &st));
  ::close(fd);
  ASSERT_EQ(0, index.unlink(mkhoid("open-create", 0)));
  ASSERT_EQ(0, index.unmount());
}

TEST(OsFragTreeIndex, Split)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-split");

  TestFragTreeIndex index;
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

  const hoid_t oid = mkhoid("foo", 0);

  int fd = -1;
  ASSERT_EQ(0, index.open(oid, true, &fd));
  ::close(fd);

  // start a split (async=false)
  frag_path p = {};
  ASSERT_EQ(0, index.split(p.frag, 1, false));

  // make sure we can find it before rename
  ASSERT_EQ(0, index.lookup(oid));

  // make sure we can't start another split or merge
  ASSERT_TRUE(index.split(p.frag, 1, false) != 0);
  ASSERT_TRUE(index.merge(p.frag, false) != 0);

  // do the rename
  TestFragTreeIndex::frag_size_map size_updates;
  index.do_split(p, 1, size_updates);

  // make sure we can still find it after rename
  ASSERT_EQ(0, index.lookup(oid));

  // complete the split
  index.finish_split(p.frag, size_updates);
  ASSERT_EQ(0, index.lookup(oid));

  // verify the size map
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag.make_child(0, 1)] = 1;
  sizes[p.frag.make_child(1, 1)] = 0;
  ASSERT_EQ(sizes, index.get_size_map());

  ASSERT_EQ(0, index.unmount());
}

TEST(OsFragTreeIndex, SplitAsync)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-split-async");

  TestFragTreeIndex index;
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

  const hoid_t oid = mkhoid("foo", 0);

  int fd = -1;
  ASSERT_EQ(0, index.open(oid, true, &fd));
  ::close(fd);

  // start an async split
  frag_path p = {};
  ASSERT_EQ(0, index.split(p.frag, 1));

  // wait a bit to make sure the migration started
  std::this_thread::sleep_for(10ms);

  // unmount to block until it completes
  ASSERT_EQ(0, index.unmount());

  // remount without starting recovery
  ASSERT_EQ(0, index.mount(path, false));

  // make sure we can still find it
  ASSERT_EQ(0, index.lookup(oid));

  // verify the size map
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag.make_child(0, 1)] = 1;
  sizes[p.frag.make_child(1, 1)] = 0;
  ASSERT_EQ(sizes, index.get_size_map());

  ASSERT_EQ(0, index.unmount());
}

TEST(OsFragTreeIndex, Merge)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-merge");

  TestFragTreeIndex index;
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

  // do an initial split
  const frag_path p = {};
  ASSERT_EQ(0, index.split_sync(p.frag, 1));

  // create a file in 0/
  const hoid_t oid = mkhoid("foo", 0);
  int fd = -1;
  ASSERT_EQ(0, index.open(oid, true, &fd));
  ::close(fd);

  // start a merge (async=false)
  ASSERT_EQ(0, index.merge(p.frag, false));

  // make sure we can find it before rename
  ASSERT_EQ(0, index.lookup(oid));

  // make sure we can't start another split or merge
  ASSERT_TRUE(index.split(p.frag, 1, false) != 0);
  ASSERT_TRUE(index.merge(p.frag, false) != 0);

  // do the rename
  index.do_merge(p, 1);

  // make sure we can still find it after rename
  ASSERT_EQ(0, index.lookup(oid));

  // complete the merge
  index.finish_merge(p.frag);
  ASSERT_EQ(0, index.lookup(oid));

  // verify the size map
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag] = 1;
  ASSERT_EQ(sizes, index.get_size_map());

  ASSERT_EQ(0, index.unmount());
}

TEST(OsFragTreeIndex, MergeAsync)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-merge-async");

  TestFragTreeIndex index;
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

  // do an initial split
  const frag_path p = {};
  ASSERT_EQ(0, index.split_sync(p.frag, 1));

  // create a file in 0/
  const hoid_t oid = mkhoid("foo", 0);
  int fd = -1;
  ASSERT_EQ(0, index.open(oid, true, &fd));
  ::close(fd);

  // start an async merge
  ASSERT_EQ(0, index.merge(p.frag));

  // wait a bit to make sure the migration started
  std::this_thread::sleep_for(10ms);

  // unmount to block until it completes
  ASSERT_EQ(0, index.unmount());

  // remount without starting recovery
  ASSERT_EQ(0, index.mount(path, false));

  // make sure we can still find it
  ASSERT_EQ(0, index.lookup(oid));

  // verify the size map
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag] = 1;
  ASSERT_EQ(sizes, index.get_size_map());

  ASSERT_EQ(0, index.unmount());
}

TEST(OsFragTreeIndex, SplitRecovery)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-split-recovery");

  TestFragTreeIndex index;
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

  const hoid_t oid = mkhoid("foo", 0);

  int fd = -1;
  ASSERT_EQ(0, index.open(oid, true, &fd));
  ::close(fd);

  // start a split (async=false)
  frag_path p = {};
  ASSERT_EQ(0, index.split(p.frag, 1, false));

  // make sure we can find it before rename
  ASSERT_EQ(0, index.lookup(oid));

  // crash before starting renames
  index.crash();

  // remount without starting recovery
  ASSERT_EQ(0, index.mount(path, false));

  // verify the size map before recovery to test count_sizes()
  TestFragTreeIndex::frag_size_map sizes;
  sizes[p.frag] = 1;
  ASSERT_EQ(sizes, index.get_size_map());

  // do the synchronous recovery
  index.restart_migrations(false);

  // make sure we can still find it after recovery
  ASSERT_EQ(0, index.lookup(oid));

  // verify the size map after recovery
  sizes.clear();
  sizes[p.frag.make_child(0, 1)] = 1;
  sizes[p.frag.make_child(1, 1)] = 0;
  ASSERT_EQ(sizes, index.get_size_map());

  ASSERT_EQ(0, index.unmount());
}

TEST(OsFragTreeIndex, CountSizes)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-size-recovery");

  TestFragTreeIndex index;
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

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
  ASSERT_EQ(0, index.split_sync(root, 2));
  ASSERT_EQ(0, index.split_sync(f0, 1));
  ASSERT_EQ(0, index.split_sync(f00, 1));
  ASSERT_EQ(0, index.split_sync(f01, 1));
  ASSERT_EQ(0, index.split_sync(f1, 1));
  ASSERT_EQ(0, index.split_sync(f10, 1));
  ASSERT_EQ(0, index.split_sync(f11, 1));
  ASSERT_EQ(0, index.split_sync(f2, 1));
  ASSERT_EQ(0, index.split_sync(f20, 1));
  ASSERT_EQ(0, index.split_sync(f21, 1));
  ASSERT_EQ(0, index.split_sync(f3, 1));
  ASSERT_EQ(0, index.split_sync(f30, 1));
  ASSERT_EQ(0, index.split_sync(f31, 1));

  const hoid_t a = mkhoid("a", 0x0000000000000000ULL);
  const hoid_t b = mkhoid("b", 0x0010000000000000ULL);
  const hoid_t c = mkhoid("c", 0x0020000000000000ULL);
  const hoid_t d = mkhoid("d", 0x0030000000000000ULL);
  const hoid_t e = mkhoid("e", 0x0040000000000000ULL);
  const hoid_t f = mkhoid("f", 0x0050000000000000ULL);
  const hoid_t g = mkhoid("g", 0x0060000000000000ULL);
  const hoid_t h = mkhoid("h", 0x0070000000000000ULL);
  const hoid_t i = mkhoid("i", 0x0080000000000000ULL);
  const hoid_t j = mkhoid("j", 0x0090000000000000ULL);
  const hoid_t k = mkhoid("k", 0x00A0000000000000ULL);
  const hoid_t l = mkhoid("l", 0x00B0000000000000ULL);
  const hoid_t m = mkhoid("m", 0x00C0000000000000ULL);
  const hoid_t n = mkhoid("n", 0x00D0000000000000ULL);
  const hoid_t o = mkhoid("o", 0x00E0000000000000ULL);
  const hoid_t p = mkhoid("p", 0x00F0000000000000ULL);

  int fd = -1;
  ASSERT_EQ(0, index.open(a, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(b, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(c, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(d, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(e, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(f, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(g, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(h, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(i, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(j, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(k, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(l, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(m, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(n, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(o, true, &fd)); ::close(fd);
  ASSERT_EQ(0, index.open(p, true, &fd)); ::close(fd);

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
  ASSERT_EQ(sizes, index.get_size_map());

  index.crash();

  // remount without starting recovery
  ASSERT_EQ(0, index.mount(path, false));

  // verify the size map before recovery to test count_sizes()
  ASSERT_EQ(sizes, index.get_size_map());

  ASSERT_EQ(0, index.unmount());
}

TEST(OsFragTreeIndex, Destroy)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-destroy");

  TestFragTreeIndex index(2);
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

  int fd = -1;
  ASSERT_EQ(0, index.open(mkhoid("a", 0x0000000000000000ULL), true, &fd));
  ::close(fd);
  ASSERT_EQ(0, index.open(mkhoid("b", 0x0040000000000000ULL), true, &fd));
  ::close(fd);
  ASSERT_EQ(0, index.open(mkhoid("c", 0x0080000000000000ULL), true, &fd));
  ::close(fd);
  ASSERT_EQ(0, index.open(mkhoid("d", 0x00C0000000000000ULL), true, &fd));
  ::close(fd);

  ASSERT_EQ(0, index.destroy(path));

  // directory must not exist
  struct stat st;
  ASSERT_EQ(-1, ::stat(path.c_str(), &st));
  ASSERT_EQ(ENOENT, errno);
}

TEST(OsFragTreeIndex, Names)
{
  tmpdir_with_cleanup path("tmp-fragtreeindex-names");

  TestFragTreeIndex index(0);
  ASSERT_EQ(0, index.init(path));
  ASSERT_EQ(0, index.mount(path));

  // construct an oid with a name too long for a filename
  const std::string name(MAX_CEPH_OBJECT_NAME_LEN, 'a');
  const hoid_t oid = mkhoid(name.c_str(), 0);

  int fd = -1;
  ASSERT_EQ(0, index.open(oid, true, &fd));

  // query xattr
  std::string xattr(MAX_CEPH_OBJECT_NAME_LEN, 0);
  char *buf = const_cast<char*>(xattr.data());
  ssize_t len = chain_fgetxattr(fd, "user.full_object_name",
                                buf, xattr.size());
  ASSERT_TRUE(len > 0);
  xattr.resize(len);
  ASSERT_EQ(xattr, name);
  ::close(fd);

  ASSERT_EQ(0, index.unmount());
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
