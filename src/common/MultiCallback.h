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

    std::reference_wrapper<SimpleMultiCallback> subsidiary() {
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

  namespace detail {
    template<size_t...> struct tuple_indices {};

    template<size_t S, class IntTuple, size_t E>
    struct make_indices_imp;

    template<size_t S, size_t ...Indices, size_t E>
    struct make_indices_imp<S, tuple_indices<Indices...>, E> {
      typedef typename make_indices_imp<S + 1, tuple_indices<Indices..., S>,
					E>::type type;
    };

    template<size_t E, size_t ...Indices>
    struct make_indices_imp<E, tuple_indices<Indices...>, E> {
      typedef tuple_indices<Indices...> type;
    };

    template<size_t E, size_t S = 0>
    struct make_tuple_indices {
      static_assert(S <= E, "make_tuple_indices input error");
      typedef typename make_indices_imp<S, tuple_indices<>, E>::type type;
    };
  };

  // This is the complicated case, for when there is per-call data and
  // an association between worker and call.

  template<typename... Signature>
  class ComplicatedMultiCallback;

  template<typename... Data, typename... Args>
  class ComplicatedMultiCallback<void(Args...), Data...> {
    std::mutex lock;
    std::condition_variable cond;
    uint32_t count;
    bool active;

  protected:
    ComplicatedMultiCallback() : count(0), active(false) { }

    class Subsidiary {
      friend ComplicatedMultiCallback;
      ComplicatedMultiCallback& cb;
      std::tuple<Data...> data;
      typedef typename detail::make_tuple_indices<
	sizeof...(Data)>::type indices;

      Subsidiary(ComplicatedMultiCallback& _cb, std::tuple<Data...> _data)
	: cb(_cb), data(std::forward<std::tuple<Data...>>(_data)) { }

    public:
      void operator()(Args ...args) {
	cb.do_work(std::forward<std::tuple<Data...>>(data),
		   indices(), std::forward<Args>(args)...);
      }
    };

  public:
    virtual ~ComplicatedMultiCallback() { }
    operator std::function<void(Args...)>() {
      std::unique_lock<std::mutex> l(lock);
      ++count;
      return std::ref(*this);
    }

    void activate() {
      std::unique_lock<std::mutex> l(lock);
      active = true;
      cond.notify_all();
    }

    Subsidiary subsidiary(Data... data) {
      std::unique_lock<std::mutex> l(lock);
      ++count;
      return Subsidiary(*this, std::forward_as_tuple(data...));
    }

    template<size_t ...Index>
    void do_work(std::tuple<Data...>&& data, detail::tuple_indices<Index...>,
		 Args ...args) {
      std::unique_lock<std::mutex> l(lock);
      cond.wait(l, [&](){ return active; });
      work(std::forward<Data...>(std::get<Index>(data))...,
	   std::forward<Args>(args)...);
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

    virtual void work(Data ...data, Args ...args) = 0;
    virtual void finish(void) = 0;
  };
}; // cohort
#endif // COHORT_MULTICALLBACK_H
