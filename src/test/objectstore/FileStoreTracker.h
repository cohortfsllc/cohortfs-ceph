// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef FILESTORE_TRACKER_H
#define FILESTORE_TRACKER_H
#include <mutex>
#include "test/common/ObjectContents.h"
#include "os/file/FileStore.h"
#include "os/kv/KeyValueDB.h"
#include <boost/scoped_ptr.hpp>
#include <list>
#include <map>
#include <tuple>

class FileStoreTracker {
  const static uint64_t SIZE = 4 * 1024;
  ObjectStore *store;
  KeyValueDB *db;
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  uint64_t restart_seq;

public:
  typedef std::tuple<CollectionHandle, string> obj_desc_t;
  typedef std::pair<FileStoreTracker::obj_desc_t, uint64_t> trans_elt;
  typedef std::list<trans_elt> trans_list;

  struct OutTransaction {
    FileStoreTracker::trans_list *in_flight;
    Transaction *t;
  };

  FileStoreTracker(ObjectStore *store, KeyValueDB *db)
    : store(store), db(db), restart_seq(0) {}

  class Transaction {
    class Op {
    public:
      virtual void operator()(FileStoreTracker *harness,
			      OutTransaction *out) = 0;
      virtual ~Op() {};
    };
    list<Op*> ops;
    class Write : public Op {
    public:
      CollectionHandle ch;
      string oid;
      Write(CollectionHandle _ch,
	    const string &oid)
	: ch(_ch), oid(oid) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->write(obj_desc_t(ch, oid), out);
      }
    };
    class CloneRange : public Op {
    public:
      CollectionHandle ch;
      string from;
      string to;
      CloneRange(CollectionHandle _ch,
		 const string &from,
		 const string &to)
	: ch(_ch), from(from), to(to) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->clone_range(obj_desc_t(ch, from), obj_desc_t(ch, to),
			     out);
      }
    };
    class Clone : public Op {
    public:
      CollectionHandle ch;
      string from;
      string to;
      Clone(CollectionHandle _ch,
	    const string &from,
	    const string &to)
	: ch(_ch), from(from), to(to) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->clone(obj_desc_t(ch, from), obj_desc_t(ch, to),
			     out);
      }
    };
    class Remove: public Op {
    public:
      CollectionHandle ch;
      string oid;
      Remove(CollectionHandle _ch,
	     const string &oid)
	: ch(_ch), oid(oid) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->remove(obj_desc_t(ch, oid),
			out);
      }
    };
  public:
    void write(CollectionHandle ch, const string &oid) {
      ops.push_back(new Write(ch, oid));
    }
    void clone_range(CollectionHandle ch, const string &from,
		     const string &to) {
      ops.push_back(new CloneRange(ch, from, to));
    }
    void clone(CollectionHandle ch, const string &from,
	       const string &to) {
      ops.push_back(new Clone(ch, from, to));
    }
    void remove(CollectionHandle ch, const string &oid) {
      ops.push_back(new Remove(ch, oid));
    }
    friend class FileStoreTracker;
  };

  int init();
  void submit_transaction(Transaction &t);
  void verify(const obj_desc_t& obj, bool on_start = false);

private:
  ObjectContents get_current_content(const obj_desc_t &obj);
  pair<uint64_t, uint64_t> get_valid_reads(const obj_desc_t &obj);
  ObjectContents get_content(const obj_desc_t &obj, uint64_t version);

  void committed(const obj_desc_t &obj, uint64_t seq);
  void applied(const obj_desc_t &obj, uint64_t seq);
  uint64_t set_content(const obj_desc_t &obj, ObjectContents &content);

  // ObjectContents Operations
  void write(const obj_desc_t &obj, OutTransaction *out);
  void remove(const obj_desc_t &obj, OutTransaction *out);
  void clone_range(const obj_desc_t &from,
		   const obj_desc_t &to,
		   OutTransaction *out);
  void clone(const obj_desc_t &from,
	     const obj_desc_t &to,
	     OutTransaction *out);
  friend class OnApplied;
  friend class OnCommitted;
};

inline std::ostream& operator<<(std::ostream& out,
				const FileStoreTracker::obj_desc_t& obj) {
  using std::get;
  out << make_pair(get<0>(obj)->get_cid(), get<1>(obj));
  return out;
}

#endif
