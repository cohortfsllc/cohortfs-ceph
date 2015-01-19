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

  if (!osrs.count(coll_str))
    osrs.insert(make_pair(coll_str, ObjectStore::Sequencer(coll_str)));
  ObjectStore::Sequencer *osr = &(osrs.find(coll_str)->second);


  coll_t c(coll_str);
  oid_t h(oid.substr(sep+1));
  t->write(c, h, offset, bl.length(), bl);

  if (write_infos) {
    bufferlist bl2;
    for (uint64_t j = 0; j < 128; ++j) bl2.append(0);
    coll_t meta("meta");
    oid_t info(string("info_")+coll_str);
    t->write(meta, info, 0, bl2.length(), bl2);
  }

  os->queue_transaction(
    osr,
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
  oid_t h(oid.substr(sep+1));
  os->read(c, h, offset, length, *bl);
  finisher.queue(new C_DumbWrapper(std::move(on_complete)));
}
