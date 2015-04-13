// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MULTICALLBACK_H
#define COHORT_MULTICALLBACK_H

namespace cohort {

  class MultiCallback {
  public:
    template<typename T, typename... Args>
    static T& create(Args&&... args) {
      return *(new T(std::forward<Args>(args)...));
    }
  };

  // This is the simple case, for when there's no per-call data or
  // association between worker and call.

  // What I really want is a 'fill in the blanks' class rather than
  // inheritance (i.e. everything is known at compile-time, we use the
  // std::function to present an interface to the caller) but C++
  // doesn't support that yet through either templates or constexpr,
  // as far as I know.

  // This gets allocated on the heap similarly to a Context (we store
  // std:refs in the ack/commit so they can't have ownership). It's
  // still better than Gather since only ONE THING gets allocated on
  // the heap.

  template<typename... Args>
  class SimpleMultiCallback {
    std::mutex lock;
    std::condition_variable cond;
    uint32_t count;
    bool active;

  protected:
    SimpleMultiCallback() : count(0), active(false) { }

  public:
    virtual ~SimpleMultiCallback() { }
    operator std::function<void(Args...)>() {
      std::unique_lock<std::mutex> l(lock);
      ++count;
      return std::ref(*this);
    }

    // Stupid stupid stupid stupid stupid
    // But I'll have time to rewrite the world later
    class C_MultiHackery : public Context {
      SimpleMultiCallback<int>& c;
    public:
      C_MultiHackery(SimpleMultiCallback<int>& _c) : c(_c) {}
      void finish(int r) {
	c(r);
      }
    };
    // Obviously this won't work if your Args are anything but a
    // single int.
    operator Context*() {
      std::unique_lock<std::mutex> l(lock);
      ++count;
      return new C_MultiHackery(*this);
    }
    void activate() {
      std::unique_lock<std::mutex> l(lock);
      active = true;
      cond.notify_all();
    }

    void operator()(Args ...args) {
      std::unique_lock<std::mutex> l(lock);
      cond.wait(l, [&](){ return active; });
      work(std::forward<Args>(args)...);
      if (--count == 0) {
	l.unlock();
	finish();
	delete this;
      }
    }

    bool empty() {
      return count == 0;
    }

    void discard() {
      // It's up to the caller to make sure there aren't dangling
      // references.
      delete this;
    }

    virtual void work(Args ...args) = 0;
    virtual void finish(void) = 0;
  };

}; // cohort
#endif // COHORT_MULTICALLBACK_H
