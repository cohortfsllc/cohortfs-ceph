// vim: ts=8 sw=2 smarttab
#include <algorithm>
#include "DirRegMap.h"
#include "MDSRegMap.h"
#include "common/Finisher.h"
#include "include/cephfs/libcephfs.h"

#define dout_subsys ceph_subsys_client

#undef dout_prefix
#define dout_prefix *_dout << "client.dirreg(" << vino << ") "

DirRegMap::DirRegMap(CephContext *cct, MDSRegMap *mdsregs, vinodeno_t vino)
  : cct(cct),
    mdsregs(mdsregs),
    mtx("DirRegMap"),
    vino(vino)
{
  memset(&layout, 0, sizeof(layout));
}

DirRegMap::~DirRegMap()
{
  assert(regs.empty());
}

bool DirRegMap::add_registration(uint32_t regid, void *place,
				 void *recall, void *user)
{
  Finisher *finisher = mdsregs->add_dir_registration(regid, this);
  if (!finisher)
    return false;

  Mutex::Locker lock(mtx);

  registration &reg = regs[regid];
  reg.place = place;
  reg.recall = recall;
  reg.user = user;
  reg.async = finisher;

  ldout(cct, 10) << "added registration " << regid << dendl;

  update(reg);
  return true;
}

void DirRegMap::remove_registration(uint32_t regid)
{
  Mutex::Locker lock(mtx);

  ldout(cct, 10) << "removing registration " << regid << dendl;

  reg_map::iterator i = regs.find(regid);
  assert(i != regs.end());

  mdsregs->remove_dir_registration(regid, this);
  cleanup(i->second);

  regs.erase(i);
}

void DirRegMap::recall_registration(uint32_t regid)
{
  Mutex::Locker lock(mtx);

  ldout(cct, 10) << "recalling registration " << regid << dendl;

  reg_map::iterator i = regs.find(regid);
  assert(i != regs.end());

  cleanup(i->second);

  regs.erase(i);
}

class C_DRM_Placement : public Context {
 private:
  dir_placement_cb callback;
  vinodeno_t vino;
  uint64_t seed;
  vector<int> stripes;
  void *user;
 public:
  C_DRM_Placement(dir_placement_cb callback, vinodeno_t vino, uint64_t seed,
                  const int *stripes_begin, const int *stripes_end, void *user)
    : callback(callback), vino(vino), seed(seed),
      stripes(stripes_begin, stripes_end), user(user) {}
  void finish(int r) {
    callback(vino, seed, stripes.size(), stripes.data(), user);
  }
};

void DirRegMap::update(registration &reg)
{
  ldout(cct, 10) << "sending placement for dir " << vino << dendl;
  dir_placement_cb callback = reinterpret_cast<dir_placement_cb>(reg.place);
  const int *stripe_begin = layout.dl_stripe_auth;
  const int *stripe_end = layout.dl_stripe_auth + layout.dl_stripe_count;
  reg.async->queue(new C_DRM_Placement(callback, vino, layout.dl_hash_seed,
				       stripe_begin, stripe_end, reg.user));
}

class C_DRM_Recall : public Context {
 private:
  dir_recall_cb callback;
  vinodeno_t vino;
  void *user;
 public:
  C_DRM_Recall(dir_recall_cb callback, vinodeno_t vino, void *user)
    : callback(callback), vino(vino), user(user) {}
  void finish(int r) { callback(vino, user); }
};

void DirRegMap::cleanup(registration &reg)
{
  ldout(cct, 10) << "sending recall for dir " << vino << dendl;
  dir_recall_cb callback = reinterpret_cast<dir_recall_cb>(reg.recall);
  reg.async->queue(new C_DRM_Recall(callback, vino, reg.user));
}

static bool operator==(const ceph_dir_layout &lhs, const ceph_dir_layout &rhs)
{
  return lhs.dl_hash_seed == rhs.dl_hash_seed
    && lhs.dl_dir_hash == rhs.dl_dir_hash
    && lhs.dl_stripe_count == rhs.dl_stripe_count
    && equal(lhs.dl_stripe_auth, lhs.dl_stripe_auth + lhs.dl_stripe_count,
	     rhs.dl_stripe_auth);
}

void DirRegMap::update(const ceph_dir_layout &dl)
{
  ldout(cct, 10) << "update " << dl.dl_stripe_count << dendl;

  if (layout == dl)
    return;

  layout = dl;

  // schedule callbacks
  for (reg_map::iterator r = regs.begin(); r != regs.end(); ++r)
    update(r->second);
}

void DirRegMap::close()
{
  // schedule recalls
  for (reg_map::iterator r = regs.begin(); r != regs.end(); ++r) {
    cleanup(r->second);
    mdsregs->remove_dir_registration(r->first, this);
  }
  regs.clear();
}

