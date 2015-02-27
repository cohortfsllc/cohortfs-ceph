// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "testfilestore_backend.h"
#include "global/global_init.h"
#include "os/ObjectStore.h"

extern CephContext* cct;

struct C_DeleteTransWrapper : public Context {
  std::function<void(int)> c;
  ObjectStore::Transaction *t;
  C_DeleteTransWrapper(
    ObjectStore::Transaction *t, std::function<void(int)>&& c) : c(c), t(t) {}
  void finish(int r) {
    c(r);
    delete t;
  }
};

struct C_DumbWrapper : public Context {
  std::function<void(int)> c;
  C_DumbWrapper(std::function<void(int)>&& c) : c(c) {}
  void finish(int r) {
    c(r);
  }
};

TestFileStoreBackend::TestFileStoreBackend(
  ObjectStore *os, bool write_infos)
  : os(os), finisher(cct), write_infos(write_infos)
{
  finisher.start();
}

void TestFileStoreBackend::write(
  const string &oid,
  uint64_t offset,
  const bufferlist &bl,
  std::function<void(int)>&& on_applied,
  std::function<void(int)>&& on_commit)
{
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  size_t sep = oid.find("/");
  assert(sep != string::npos);
  assert(sep + 1 < oid.size());
  string coll_str(oid.substr(0, sep));

  // inefficient, but clear
  coll_t c(coll_str);
  hoid_t h(oid_t(oid.substr(sep+1)));
  (void) t->push_cid(c);
  (void) t->push_oid(h);
  t->write(offset, bl.length(), bl); // uses current c_ix and h_ix

  if (write_infos) {
    bufferlist bl2;
    for (uint64_t j = 0; j < 128; ++j) bl2.append(0);
    coll_t meta("meta");
    hoid_t info(oid_t(string("info_")+coll_str));
    (void) t->push_cid(meta);
    (void) t->push_oid(info);
    t->write(0, bl2.length(), bl2);
  }

  os->queue_transaction(
    t,
    new C_DeleteTransWrapper(t, std::move(on_applied)),
    new C_DumbWrapper(std::move(on_commit)));
}

void TestFileStoreBackend::read(
  const string &oid,
  uint64_t offset,
  uint64_t length,
  bufferlist *bl,
  std::function<void(int)>&& on_complete)
{
  size_t sep = oid.find("/");
  assert(sep != string::npos);
  assert(sep + 1 < oid.size());
  coll_t c(oid.substr(0, sep));
  hoid_t h(oid_t(oid.substr(sep+1)));
  ObjectStore::CollectionHandle ch = os->open_collection(c);
  if (! ch) {
    derr << "TestFileStoreBackend::read: error opening collection " << c
	 << dendl;
    return;
  }
  ObjectStore::ObjectHandle oh = os->get_object(ch, h);
  if (! oh) {
    derr << "TestFileStoreBackend::read: error instantiating object " << c
	 << dendl;
    os->close_collection(ch);
    return;
  }
  os->read(ch, oh, offset, length, *bl);
  os->put_object(oh);
  os->close_collection(ch);
  finisher.queue(new C_DumbWrapper(std::move(on_complete)));
}
