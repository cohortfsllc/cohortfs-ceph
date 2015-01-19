// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "bencher.h"
#include <unistd.h>

template<typename T>
struct Holder {
  T obj;
  Holder(T obj) : obj(obj) {}
  void operator()(int r) {
    return;
  }
};

struct OnDelete {
  std::function<void(int)> c;
  OnDelete(std::function<void(int)>&& _c) : c(std::move(_c)) {}
  ~OnDelete() { c(0); }
};

struct OnWriteApplied {
  Bencher *bench;
  uint64_t seq;
  std::shared_ptr<OnDelete> on_delete;
  OnWriteApplied(
    Bencher *bench, uint64_t seq,
    std::shared_ptr<OnDelete> on_delete
    ) : bench(bench), seq(seq), on_delete(on_delete) {}
  void operator()(int r) {
    bench->stat_collector->write_applied(seq);
  }
};

struct OnWriteCommit {
  Bencher *bench;
  uint64_t seq;
  std::shared_ptr<OnDelete> on_delete;
  OnWriteCommit(
    Bencher *bench, uint64_t seq,
    std::shared_ptr<OnDelete> on_delete
    ) : bench(bench), seq(seq), on_delete(on_delete) {}
  void operator()(int r) {
    bench->stat_collector->write_committed(seq);
  }
};

struct OnReadComplete {
  Bencher *bench;
  uint64_t seq;
  bufferlist* bl;
  OnReadComplete(Bencher *bench, uint64_t seq, bufferlist *bl) :
    bench(bench), seq(seq), bl(bl) {}
  void operator()(int r) {
    bench->stat_collector->read_complete(seq);
    bench->complete_op();
    delete bl;
  }
};

void Bencher::start_op() {
  unique_lock l(lock);
  open_ops_cond.wait(l, [&](){ return open_ops < max_in_flight; });
  ++open_ops;
}

void Bencher::drain_ops() {
  unique_lock l(lock);
  open_ops_cond.wait(l, [&](){ return !open_ops; });
}

void Bencher::complete_op() {
  lock_guard l(lock);
  assert(open_ops > 0);
  --open_ops;
  open_ops_cond.notify_all();
}

struct OnFinish {
  bool *done;
  std::mutex *lock;
  std::condition_variable *cond;
  OnFinish(
    bool *done,
    std::mutex *lock,
    std::condition_variable *cond) :
    done(done), lock(lock), cond(cond) {}
  ~OnFinish() {
    std::unique_lock<std::mutex> l(*lock);
    *done = true;
    cond->notify_all();
  }
};

void Bencher::init(
  const std::set<std::string> &objects,
  uint64_t size,
  std::ostream *out
  )
{
  bufferlist bl;
  for (uint64_t i = 0; i < size; ++i) {
    bl.append(0);
  }
  std::mutex lock;
  std::condition_variable cond;
  bool done = 0;
  {
    std::shared_ptr<OnFinish> on_finish(
      new OnFinish(&done, &lock, &cond));
    uint64_t num = 0;
    for (auto i = objects.begin();
	 i != objects.end();
	 ++i, ++num) {
      if (!(num % 20))
	*out << "Creating " << num << "/" << objects.size() << std::endl;
      backend->write(
	*i,
	0,
	bl,
	Holder<std::shared_ptr<OnFinish> >(on_finish),
	Holder<std::shared_ptr<OnFinish> >(on_finish)
	);
    }
  }
  {
    unique_lock l(lock);
    cond.wait(l, [&](){ return done; });
  }
}

void Bencher::run_bench()
{
  time_t end = time(0) + max_duration;
  uint64_t ops = 0;

  bufferlist bl;

  while ((!max_duration || time(0) < end) && (!max_ops || ops < max_ops)) {
    start_op();
    uint64_t seq = stat_collector->next_seq();
    boost::tuple<std::string, uint64_t, uint64_t, OpType> next =
      (*op_dist)();
    std::string obj_name = next.get<0>();
    uint64_t offset = next.get<1>();
    uint64_t length = next.get<2>();
    OpType op_type = next.get<3>();
    switch (op_type) {
      case WRITE: {
	std::shared_ptr<OnDelete> on_delete(
	  new OnDelete([this](int r) { complete_op(); }));
	stat_collector->start_write(seq, length);
	while (bl.length() < length) {
	  bl.append(rand());
	}
	backend->write(
	  obj_name,
	  offset,
	  bl,
	  OnWriteApplied(
	    this, seq, on_delete),
	  OnWriteCommit(
	    this, seq, on_delete)
	  );
	break;
      }
      case READ: {
	stat_collector->start_read(seq, length);
	bufferlist *read_bl = new bufferlist;
	backend->read(
	  obj_name,
	  offset,
	  length,
	  read_bl,
	  OnReadComplete(
	    this, seq, read_bl)
	  );
	break;
      }
      default: {
	assert(0);
      }
    }
  }
  drain_ops();
}
