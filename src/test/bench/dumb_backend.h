// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef DUMBBACKEND
#define DUMBBACKEND

#include "backend.h"
#include "include/Context.h"
#include "os/ObjectStore.h"
#include "common/WorkQueue.h"
#include "common/Semaphore.h"

#include <deque>

class DumbBackend : public Backend {
	const string path;

  struct write_item {
    const string oid;
    bufferlist bl;
    uint64_t offset;
    Context *on_applied;
    Context *on_commit;
    write_item(
      const string &oid,
      const bufferlist &bl,
      uint64_t offset,
      Context *on_applied,
      Context *on_commit) :
      oid(oid), bl(bl), offset(offset), on_applied(on_applied),
      on_commit(on_commit) {}
  };

  Semaphore sem;

  bool do_fsync;
  bool do_sync_file_range;
  bool do_fadvise;
  unsigned sync_interval;
  int sync_fd;
  ThreadPool tp;

  class SyncThread : public Thread {
    DumbBackend *backend;
  public:
    SyncThread(DumbBackend *backend) : backend(backend) {}
    void *entry() {
      backend->sync_loop();
      return 0;
    }
  } thread;
  friend class SyncThread;

  std::mutex sync_loop_mutex;
  std::condition_variable sync_loop_cond;
  int sync_loop_stop; // 0 for running, 1 for stopping, 2 for stopped
  void sync_loop();

  std::mutex pending_commit_mutex;
  set<Context*> pending_commits;

  class WriteQueue : public ThreadPool::WorkQueue<write_item> {
    deque<write_item*> item_queue;
    DumbBackend *backend;

  public:
    WriteQueue(
      DumbBackend *backend,
      ceph::timespan ti,
      ThreadPool *tp) :
      ThreadPool::WorkQueue<write_item>("DumbBackend::queue", ti, ti*10, tp),
      backend(backend) {}
    bool _enqueue(write_item *item) {
      item_queue.push_back(item);
      return true;
    }
    void _dequeue(write_item*) { assert(0); }
    write_item *_dequeue() {
      if (item_queue.empty())
	return 0;
      write_item *retval = item_queue.front();
      item_queue.pop_front();
      return retval;
    }
    bool _empty() {
      return item_queue.empty();
    }
    void _process(write_item *item) {
      return backend->_write(
	item->oid,
	item->offset,
	item->bl,
	item->on_applied,
	item->on_commit);
    }
    void _clear() {
      return item_queue.clear();
    }
  } queue;
  friend class WriteQueue;

  string get_full_path(const string &oid_t);

  void _write(
    const string &oid_t,
    uint64_t offset,
    const bufferlist &bl,
    Context *on_applied,
    Context *on_commit);

public:
  DumbBackend(
    const string &path,
    bool do_fsync,
    bool do_sync_file_range,
    bool do_fadvise,
    unsigned sync_interval,
    int sync_fd,
    unsigned worker_threads,
    CephContext *cct)
    : path(path), do_fsync(do_fsync),
      do_sync_file_range(do_sync_file_range),
      do_fadvise(do_fadvise),
      sync_interval(sync_interval),
      sync_fd(sync_fd),
      tp(cct, "DumbBackend::tp", worker_threads),
      thread(this),
      sync_loop_stop(0),
      queue(this, 20s, &tp) {
    thread.create();
    tp.start();
    for (unsigned i = 0; i < 10*worker_threads; ++i) {
      sem.Put();
    }
  }
  ~DumbBackend() {
    {
      unique_lock sll(sync_loop_mutex);
      if (sync_loop_stop == 0)
	sync_loop_stop = 1;
      sync_loop_cond.wait(sll, [&](){ return sync_loop_stop >= 2; });
    }
    tp.stop();
    thread.join();
  }
  void write(
    const string &oid_t,
    uint64_t offset,
    const bufferlist &bl,
    Context *on_applied,
    Context *on_commit) {
    sem.Get();
    queue.queue(
      new write_item(
	oid_t, bl, offset, on_applied, on_commit));
  }

  void read(
    const string &oid_t,
    uint64_t offset,
    uint64_t length,
    bufferlist *bl,
    Context *on_complete);
};

#endif
