// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef FILESTORE_TRACKER_H
#define FILESTORE_TRACKER_H
#include <mutex>
#include "test/common/ObjectContents.h"
#include "os/FileStore.h"
#include "os/KeyValueDB.h"
#include <boost/scoped_ptr.hpp>
#include <list>
#include <map>

class FileStoreTracker {
  const static uint64_t SIZE = 4 * 1024;
  ObjectStore *store;
  KeyValueDB *db;
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  uint64_t restart_seq;

  struct OutTransaction {
    list<pair<pair<string, string>, uint64_t> > *in_flight;
    ObjectStore::Transaction *t;
  };
public:
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
      string coll;
      string oid;
      Write(const string &coll,
	    const string &oid)
	: coll(coll), oid(oid) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->write(make_pair(coll, oid), out);
      }
    };
    class CloneRange : public Op {
    public:
      string coll;
      string from;
      string to;
      CloneRange(const string &coll,
		 const string &from,
		 const string &to)
	: coll(coll), from(from), to(to) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->clone_range(make_pair(coll, from), make_pair(coll, to),
			     out);
      }
    };
    class Clone : public Op {
    public:
      string coll;
      string from;
      string to;
      Clone(const string &coll,
		 const string &from,
		 const string &to)
	: coll(coll), from(from), to(to) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->clone(make_pair(coll, from), make_pair(coll, to),
			     out);
      }
    };
    class Remove: public Op {
    public:
      string coll;
      string oid;
      Remove(const string &coll,
	     const string &oid)
	: coll(coll), oid(oid) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->remove(make_pair(coll, oid),
			out);
      }
    };
  public:
    void write(const string &coll, const string &oid_t) {
      ops.push_back(new Write(coll, oid_t));
    }
    void clone_range(const string &coll, const string &from,
		     const string &to) {
      ops.push_back(new CloneRange(coll, from, to));
    }
    void clone(const string &coll, const string &from,
	       const string &to) {
      ops.push_back(new Clone(coll, from, to));
    }
    void remove(const string &coll, const string &oid_t) {
      ops.push_back(new Remove(coll, oid_t));
    }
    friend class FileStoreTracker;
  };

  int init();
  void submit_transaction(Transaction &t);
  void verify(const string &coll,
	      const string &from,
	      bool on_start = false);

private:
  ObjectContents get_current_content(const pair<string, string> &oid);
  pair<uint64_t, uint64_t> get_valid_reads(const pair<string, string> &oid);
  ObjectContents get_content(const pair<string, string> &oid, uint64_t version);

  void committed(const pair<string, string> &oid, uint64_t seq);
  void applied(const pair<string, string> &oid, uint64_t seq);
  uint64_t set_content(const pair<string, string> &oid, ObjectContents &content);

  // ObjectContents Operations
  void write(const pair<string, string> &oid, OutTransaction *out);
  void remove(const pair<string, string> &oid, OutTransaction *out);
  void clone_range(const pair<string, string> &from,
		   const pair<string, string> &to,
		   OutTransaction *out);
  void clone(const pair<string, string> &from,
	     const pair<string, string> &to,
	     OutTransaction *out);
  friend class OnApplied;
  friend class OnCommitted;
};

#endif
