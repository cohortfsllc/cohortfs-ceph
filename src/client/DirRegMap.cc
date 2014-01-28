// vim: ts=8 sw=2 smarttab
#include "DirRegMap.h"
#include "MDSRegMap.h"
#include "common/Finisher.h"
#include "include/cephfs/libcephfs.h"

#define dout_subsys ceph_subsys_client

#undef dout_prefix
#define dout_prefix *_dout << "client.dirreg(" << vino << ')'

DirRegMap::DirRegMap(CephContext *cct, MDSRegMap *mdsregs, vinodeno_t vino)
  : cct(cct),
    mdsregs(mdsregs),
    mtx("DirRegMap"),
    vino(vino),
    hash_seed(0)
{
}

DirRegMap::~DirRegMap()
{
  assert(regs.empty());
}

void DirRegMap::add_registration(uint32_t regid, void *place,
				 void *recall, void *user)
{
  Mutex::Locker lock(mtx);

  registration &reg = regs[regid];
  reg.place = place;
  reg.recall = recall;
  reg.user = user;
  reg.async = mdsregs->add_dir_registration(regid, this);

  ldout(cct, 10) << "added registration " << regid << dendl;

  update(reg);
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
  C_DRM_Placement(dir_placement_cb callback, vinodeno_t vino,
                  uint64_t seed, const vector<int> &stripes, void *user)
    : callback(callback), vino(vino), seed(seed),
      stripes(stripes), user(user) {}
  void finish(int r) {
    callback(vino, seed, stripes.size(), stripes.data(), user);
  }
};

void DirRegMap::update(registration &reg)
{
  ldout(cct, 10) << "sending placement for dir " << vino << dendl;
  dir_placement_cb callback = reinterpret_cast<dir_placement_cb>(reg.place);
  reg.async->queue(new C_DRM_Placement(callback, vino, hash_seed,
				       stripe_auth, reg.user));
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

void DirRegMap::update(const vector<int> &stripes, uint64_t seed)
{
  if (stripe_auth == stripes && hash_seed == seed)
    return;

  stripe_auth = stripes;
  hash_seed = seed;

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

