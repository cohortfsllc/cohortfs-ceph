// vim: ts=8 sw=2 smarttab
#include "MDSRegMap.h"

#include "../mds/MDSMap.h"
#include "include/cephfs/libcephfs.h"


MDSRegMap::MDSRegMap(CephContext *cct)
  : cct(cct),
    mtx("MDSRegMap"),
    next_regid(0)
{
}

MDSRegMap::~MDSRegMap()
{
  assert(regs.empty()); // must call shutdown() first
}

// callback registration
uint32_t MDSRegMap::add_registration(void *add, void *remove, void *user)
{
  Mutex::Locker lock(mtx);

  const uint32_t regid = next_regid++;

  registration &reg = regs[regid];
  reg.add = add;
  reg.remove = remove;
  reg.user = user;
  reg.async = new Finisher(cct);

  // schedule a callback for each active mds
  update(reg);

  return regid;
}

void MDSRegMap::remove_registration(uint32_t regid)
{
  Mutex::Locker lock(mtx);

  reg_map::iterator i = regs.find(regid);
  assert(i != regs.end());

  cleanup(i->second);

  regs.erase(i);
}

void MDSRegMap::cleanup(registration &reg)
{
  // wait for any outstanding callbacks
  reg.async->wait_for_empty();
  reg.async->stop();
}


void MDSRegMap::update(const MDSMap *mdsmap)
{
  Mutex::Locker lock(mtx);

  unsigned count = mdsmap->get_max_mds();
  devices.resize(count);

  const map<uint64_t, MDSMap::mds_info_t> &info = mdsmap->get_mds_info();

  // update cached devices
  for (uint64_t i = 0; i < count; i++) {
    map<uint64_t, MDSMap::mds_info_t>::const_iterator m = info.find(i);
    if (m == info.end() || m->second.state != MDSMap::STATE_ACTIVE) {
      // free the device if there was one
      devices[i].reset();
    } else if (!devices[i]) {
      mds_info_ptr mds(new ceph_mds_info_t);
      mds->deviceid = i;
      mds->addr_count = 1;
      memcpy(&mds->addrs, &m->second.addr.addr, sizeof(mds->addrs));
      // hack: set port to nfs:2049
      if (m->second.addr.addr.ss_family == AF_INET)
	((sockaddr_in*)&mds->addrs[0])->sin_port = htons(2049);
      else if (m->second.addr.addr.ss_family == AF_INET6)
	((sockaddr_in6*)&mds->addrs[0])->sin6_port = htons(2049);
      devices[i] = mds;
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
  int deviceid;
  void *user;
 public:
  C_DM_RemoveMDS(mds_remove_cb callback, int deviceid, void *user)
    : callback(callback), deviceid(deviceid), user(user) {}
  void finish(int r) { callback(deviceid, user); }
};

// assumes caller has locked mtx
void MDSRegMap::update(registration &reg)
{
  size_t count = devices.size();
  if (reg.known.size() < count)
    reg.known.resize(count);

  for (size_t i = 0; i < reg.known.size(); i++) {
    if (i < count && devices[i]) {
      if (!reg.known[i]) {
	mds_add_cb callback = reinterpret_cast<mds_add_cb>(reg.add);
	reg.async->queue(new C_DM_AddMDS(callback, devices[i], reg.user));
	reg.known[i] = true;
      }
    } else {
      if (reg.known[i]) {
	mds_remove_cb callback = reinterpret_cast<mds_remove_cb>(reg.remove);
	reg.async->queue(new C_DM_RemoveMDS(callback, i, reg.user));
	reg.known[i] = false;
      }
    }
  }

  // resize after loop in case there were known devices past count
  reg.known.resize(count);
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

