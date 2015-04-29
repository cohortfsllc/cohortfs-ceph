// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */


#ifndef CEPH_CONTEXT_H
#define CEPH_CONTEXT_H

#include <cassert>
#include <condition_variable>
#include <iomanip>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <boost/intrusive/list.hpp>

/*
 * GenContext - abstract callback class
 */
template <typename T>
class GenContext {
  GenContext(const GenContext& other);
  const GenContext& operator=(const GenContext& other);

 protected:
  virtual void finish(T t) = 0;

 public:
  GenContext() {}
  virtual ~GenContext() {}	 // we want a virtual destructor!!!
  virtual void complete(T t) {
    finish(t);
    delete this;
  }
};

/*
 * Context - abstract callback class
 */
class Context {
  Context(const Context& other);
  const Context& operator=(const Context& other);

  // intrusive list entry
  typedef boost::intrusive::link_mode<boost::intrusive::auto_unlink> link_mode;
  typedef boost::intrusive::list_member_hook<link_mode> list_hook_type;
  list_hook_type q;

  typedef boost::intrusive::member_hook<Context, list_hook_type,
                                        &Context::q> MemberOption;
  typedef boost::intrusive::constant_time_size<false> SizeOption;

 protected:
  virtual void finish(int r) = 0;

 public:
  typedef boost::intrusive::list<Context, MemberOption, SizeOption> List;

  Context() {}
  virtual ~Context() {}	      // we want a virtual destructor!!!
  virtual void complete(int r) {
    finish(r);
    delete this;
  }
  // Make all contexts also function objects
  void operator()(int r) {
    complete(r);
  }
};

// output operator to print context pointer
inline std::ostream& operator<<(std::ostream &out, const Context &c) {
  return out << std::hex << reinterpret_cast<uintptr_t>(&c);
}

/**
 * Simple context holding a single object
 */
template<class T>
class ContainerContext : public Context {
  T obj;
public:
  ContainerContext(T &obj) : obj(obj) {}
  void finish(int r) {}
};

template <class T>
struct Wrapper : public Context {
  Context *to_run;
  T val;
  Wrapper(Context *to_run, T val) : to_run(to_run), val(val) {}
  void finish(int r) {
    if (to_run)
      to_run->complete(r);
  }
};
struct RunOnDelete {
  Context *to_run;
  RunOnDelete(Context *to_run) : to_run(to_run) {}
  ~RunOnDelete() {
    if (to_run)
      to_run->complete(0);
  }
};
typedef std::shared_ptr<RunOnDelete> RunOnDeleteRef;

/*
 * finish and destroy a sequence of Contexts
 */
inline void finish_contexts(Context::List &finished, int result = 0)
{
  Context::List contexts;
  contexts.swap(finished); // swap out of place to avoid weird loops

  while (!contexts.empty()) {
    auto i = contexts.begin();
    Context &c = *i;
    contexts.erase(i);
    c.complete(result);
  }
}

class C_NoopContext : public Context {
public:
  void finish(int r) { }
};

/*
 * C_Contexts - list of Contexts
 */
class C_Contexts : public Context {
 private:
  Context::List contexts;
 public:
  C_Contexts() {}
  C_Contexts(Context::List &&contexts)
    : contexts(std::move(contexts)) {}

  void add(Context* c) {
    contexts.push_back(*c);
  }
  void take(Context::List &other) {
    contexts.splice(contexts.end(), other);
  }
  void finish(int r) {
    finish_contexts(contexts, r);
  }
  bool empty() const { return contexts.empty(); }

  static Context* list_to_context(Context::List &contexts) {
    if (contexts.size() == 0)
      return 0;
    if (contexts.size() == 1) {
      Context *c = &contexts.front();
      contexts.clear();
      return c;
    }
    C_Contexts *c = new C_Contexts();
    c->take(contexts);
    return c;
  }
};

/*
 * C_Gather
 *
 * BUG:? only reports error from last sub to have an error return
 */
class C_Gather : public Context {
private:
  int result;
  Context *onfinish;
  int sub_created_count;
  int sub_existing_count;
  std::recursive_mutex lock;
  bool activated;

  void sub_finish(Context* sub, int r) {
    std::unique_lock<std::recursive_mutex> l(lock);
    --sub_existing_count;
    if (r < 0 && result == 0)
      result = r;
    if ((activated == false) || (sub_existing_count != 0)) {
      l.unlock();
      return;
    }
    l.unlock();
    delete_me();
  }

  void delete_me() {
    if (onfinish) {
      onfinish->complete(result);
      onfinish = 0;
    }
    delete this;
  }

  class C_GatherSub : public Context {
    C_Gather *gather;
  public:
    C_GatherSub(C_Gather *g) : gather(g) {}
    void finish(int r) {
      gather->sub_finish(this, r);
      gather = 0;
    }
    ~C_GatherSub() {
      if (gather)
	gather->sub_finish(this, 0);
    }
  };

  C_Gather(Context *onfinish_)
    : result(0), onfinish(onfinish_),
      sub_created_count(0), sub_existing_count(0),
      activated(false) {}
public:
  ~C_Gather() {}
  void set_finisher(Context *onfinish_) {
    std::lock_guard<std::recursive_mutex> l(lock);
    assert(!onfinish);
    onfinish = onfinish_;
  }
  void activate() {
    std::unique_lock<std::recursive_mutex> l(lock);
    assert(activated == false);
    activated = true;
    if (sub_existing_count != 0) {
      l.unlock();
      return;
    }
    l.unlock();
    delete_me();
  }
  Context *new_sub() {
    std::lock_guard<std::recursive_mutex> l(lock);
    assert(activated == false);
    sub_created_count++;
    sub_existing_count++;
    Context *s = new C_GatherSub(this);
    return s;
  }
  void finish(int r) {
    assert(0);	  // nobody should ever call me.
  }
  friend class C_GatherBuilder;
};

/*
 * How to use C_GatherBuilder:
 *
 * 1. Create a C_GatherBuilder on the stack
 * 2. Call gather_bld.new_sub() as many times as you want to create new subs
 *    It is safe to call this 0 times, or 100, or anything in between.
 * 3. If you didn't supply a finisher in the C_GatherBuilder constructor,
 *    set one with gather_bld.set_finisher(my_finisher)
 * 4. Call gather_bld.activate()
 *
 * The finisher may be called at any point after step 4, including immediately
 * from the activate() function.
 * The finisher will never be called before activate().
 *
 * Note: Currently, subs must be manually freed by the caller (for some reason.)
 */
class C_GatherBuilder
{
public:
  C_GatherBuilder()
    : c_gather(NULL), finisher(NULL), activated(false)
  {
  }
  C_GatherBuilder(Context *finisher_)
    : c_gather(NULL), finisher(finisher_), activated(false)
  {
  }
  ~C_GatherBuilder() {
    if (c_gather) {
      assert(activated); // Don't forget to activate your C_Gather!
    }
    else {
      delete finisher;
    }
  }
  Context *new_sub() {
    if (!c_gather) {
      c_gather = new C_Gather(finisher);
    }
    return c_gather->new_sub();
  }
  void activate() {
    if (!c_gather)
      return;
    assert(finisher != NULL);
    activated = true;
    c_gather->activate();
  }
  void set_finisher(Context *finisher_) {
    finisher = finisher_;
    if (c_gather)
      c_gather->set_finisher(finisher);
  }
  C_Gather *get() const {
    return c_gather;
  }
  bool has_subs() const {
    return (c_gather != NULL);
  }
  int num_subs_created() {
    assert(!activated);
    if (c_gather == NULL)
      return 0;
    std::lock_guard<std::recursive_mutex> l(c_gather->lock);
    return c_gather->sub_created_count;
  }
  int num_subs_remaining() {
    assert(!activated);
    if (c_gather == NULL)
      return 0;
    std::lock_guard<std::recursive_mutex> l(c_gather->lock);
    return c_gather->sub_existing_count;
  }

private:
  C_Gather *c_gather;
  Context *finisher;
  bool activated;
};

/**
 * context to signal a cond
 *
 * Generic context to signal a cond and store the return value.  We
 * assume the caller is holding the appropriate lock.
 */
class C_Cond : public Context {
  std::condition_variable *cond;
  bool *done; ///< true if finish() has been called
  int *rval; ///< return value

public:
  C_Cond(std::condition_variable *c, bool *d, int *r)
    : cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) {
    *done = true;
    *rval = r;
    cond->notify_all();
  }
};

/**
 * context to signal a cond, protected by a lock
 *
 * Generic context to signal a cond under a specific lock. We take the
 * lock in the finish() callback, so the finish() caller must not
 * already hold it.
 */
class C_SafeCond : public Context {
  std::mutex *lock; ///< Mutex to take
  std::condition_variable *cond; ///< Cond to signal
  bool *done; ///< true after finish() has been called
  int *rval; ///< return value (optional)
public:
  C_SafeCond(std::mutex *l, std::condition_variable *c, bool *d, int *r=0)
    : lock(l), cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) {
    std::unique_lock<std::mutex> l(*lock);
    if (rval)
      *rval = r;
    *done = true;
    cond->notify_all();
    l.unlock();
  }
};

/**
 * Context providing a simple wait() mechanism to wait for completion
 *
 * The context will not be deleted as part of complete and must live
 * until wait() returns.
 */
class C_SaferCond : public Context {
  std::mutex lock; ///< Mutex to take
  std::condition_variable cond; ///< Cond to signal
  bool done; ///< true after finish() has been called
  int rval; ///< return value
public:
  C_SaferCond() : lock(), done(false), rval(0) {}
  void finish(int r) { complete(r); }

  /// We overload complete in order to not delete the context
  void complete(int r) {
    std::unique_lock<std::mutex> l(lock);
    done = true;
    rval = r;
    cond.notify_all();
  }

  /// Returns rval once the Context is called
  int wait() {
    std::unique_lock<std::mutex> l(lock);
    while (!done)
      cond.wait(l);
    return rval;
  }
};

#endif
