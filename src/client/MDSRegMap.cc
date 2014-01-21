// vim: ts=8 sw=2 smarttab
#include "MDSRegMap.h"

#include "../mds/MDSMap.h"
#include "include/cephfs/libcephfs.h"

#define dout_subsys ceph_subsys_client

#undef dout_prefix
#define dout_prefix *_dout << "client.mdsreg "


MDSRegMap::MDSRegMap(CephContext *cct)
  : cct(cct),
    mtx("MDSRegMap"),
    next_regid(0),
    placement_seq(0)
{
}

MDSRegMap::~MDSRegMap()
{
  assert(regs.empty()); // must call shutdown() first
}

// callback registration
uint32_t MDSRegMap::add_registration(void *add, void *remove,
				     void *place, void *user)
{
  Mutex::Locker lock(mtx);

  const uint32_t regid = next_regid++;

  registration &reg = regs[regid];
  reg.add = add;
  reg.remove = remove;
  reg.place = place;
  reg.user = user;
  reg.placement_seq = 0;
  reg.async = new Finisher(cct);
  reg.async->start();

  ldout(cct, 10) << "added registration " << regid << dendl;

  // schedule a callback for each active mds
  update(reg);

  return regid;
}

void MDSRegMap::remove_registration(uint32_t regid)
{
  Mutex::Locker lock(mtx);

  ldout(cct, 10) << "removing registration " << regid << dendl;

  reg_map::iterator i = regs.find(regid);
  assert(i != regs.end());

  cleanup(i->second);

  regs.erase(i);
}

void MDSRegMap::cleanup(registration &reg)
{
  ldout(cct, 10) << "waiting for registration to finish callbacks" << dendl;

  // wait for any outstanding callbacks
  reg.async->wait_for_empty();
  reg.async->stop();

  ldout(cct, 10) << "callbacks finished" << dendl;
}


void MDSRegMap::update(const MDSMap *mdsmap)
{
  Mutex::Locker lock(mtx);

  // update inode placement
  if (placement_seq == 0 || placement != mdsmap->inode_placement) {
    placement = mdsmap->inode_placement;
    placement_seq++;

    devices.resize(placement.count);

    ldout(cct, 10) << "update placement seq=" << placement_seq
      << " count=" << placement.count << dendl;
  }

  // update cached devices
  for (int i = 0; i < placement.count; i++) {
    if (mdsmap->get_state(i) != MDSMap::STATE_ACTIVE) {
      // free the device if there was one
      if (devices[i])
	ldout(cct, 10) << "mds." << i << " going down" << dendl;
      devices[i].reset();
    } else if (!devices[i]) {
      const MDSMap::mds_info_t &info = mdsmap->get_mds_info(i);
      mds_info_ptr mds(new ceph_mds_info_t);
      mds->index = i;
      mds->addr_count = 1;
      memcpy(&mds->addrs, &info.addr.addr, sizeof(mds->addrs));
      // hack: set port to nfs:2049
      if (info.addr.addr.ss_family == AF_INET)
	((sockaddr_in*)&mds->addrs[0])->sin_port = htons(2049);
      else if (info.addr.addr.ss_family == AF_INET6)
	((sockaddr_in6*)&mds->addrs[0])->sin6_port = htons(2049);
      devices[i] = mds;
      ldout(cct, 10) << "mds." << i << " coming up" << dendl;
    }
  }

  // schedule callbacks for all new/removed devices
  for (reg_map::iterator r = regs.begin(); r != regs.end(); ++r)
    update(r->second);
}


// Context objects for Finisher
class C_DM_AddMDS : public Context {
 private:
  mds_add_cb callback;
  mds_info_ptr dev;
  void *user;
 public:
  C_DM_AddMDS(mds_add_cb callback, mds_info_ptr dev, void *user)
    : callback(callback), dev(dev), user(user) {}
  void finish(int r) { callback(dev.get(), user); }
};

class C_DM_RemoveMDS : public Context {
 private:
  mds_remove_cb callback;
  int index;
  void *user;
 public:
  C_DM_RemoveMDS(mds_remove_cb callback, int index, void *user)
    : callback(callback), index(index), user(user) {}
  void finish(int r) { callback(index, user); }
};

class C_DM_Placement : public Context {
 private:
  mds_placement_cb callback;
  ceph_ino_placement_t placement;
  void *user;
 public:
  C_DM_Placement(mds_placement_cb callback, const mds_inode_placement_t &p,
      void *user) : callback(callback), user(user)
  {
    // convert mds_inode_placement_t to ceph_ino_placement_t
    placement.count = p.count;
    placement.shift = p.shift;
    placement.offset = p.offset;
  }
  void finish(int r) { callback(&placement, user); }
};

// assumes caller has locked mtx
void MDSRegMap::update(registration &reg)
{
  // send removal callbacks first
  for (size_t i = 0; i < reg.known.size(); i++) {
    if (i < devices.size() && devices[i]) // device still up
      continue;
    if (!reg.known[i])
      continue;

    ldout(cct, 10) << "sending callback for removed mds." << i << dendl;
    mds_remove_cb callback = reinterpret_cast<mds_remove_cb>(reg.remove);
    reg.async->queue(new C_DM_RemoveMDS(callback, i, reg.user));
    reg.known[i] = false;
  }

  reg.known.resize(devices.size());

  // if placement has changed, send a callback 
  if (ceph_seq_cmp(reg.placement_seq, placement_seq) < 0) {
    ldout(cct, 10) << "sending callback for placement seq="
      << placement_seq << dendl;
    mds_placement_cb callback = reinterpret_cast<mds_placement_cb>(reg.place);
    reg.async->queue(new C_DM_Placement(callback, placement, reg.user));
    reg.placement_seq = placement_seq;
  }

  // send add callbacks
  for (size_t i = 0; i < devices.size(); i++) {
    if (!devices[i])
      continue;
    if (reg.known[i])
      continue;

    ldout(cct, 10) << "sending callback for added mds." << i << dendl;
    mds_add_cb callback = reinterpret_cast<mds_add_cb>(reg.add);
    reg.async->queue(new C_DM_AddMDS(callback, devices[i], reg.user));
    reg.known[i] = true;
  }
}

void MDSRegMap::shutdown()
{
  Mutex::Locker lock(mtx);

  // clean up and remove registrations
  reg_map::iterator i = regs.begin();
  while (i != regs.end()) {
    cleanup(i->second);
    regs.erase(i++);
  }
}

