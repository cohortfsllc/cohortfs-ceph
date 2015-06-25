// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_FUNQUEUE_H
#define COHORT_FUNQUEUE_H

#include <boost/intrusive/slist.hpp>

#include "common/cohort_function.h"

namespace cohort {

  namespace detail {

    template<typename _Signature>
    class _FunQueue_base;

    template<typename Res, typename... Args>
    class _FunQueue_base<Res(Args...)>
    {
    protected:
      typedef typename cohort::function<Res(Args...),
					3 * sizeof(void*), false, true> F;

      boost::intrusive::slist<F,
	boost::intrusive::linear<true>,
	boost::intrusive::cache_last<true>,
	boost::intrusive::constant_time_size<false>> queue;

      const size_t keep_free;

      boost::intrusive::slist<
	F,
	boost::intrusive::linear<true>,
	boost::intrusive::constant_time_size<true>> free_list;

      struct fun_disposer {
	void operator()(F *f){
	  delete f;
	}
      } fd;

      void free(F& el) {
	// This must only be called AFTER el is unlinked
	if (free_list.size() >= keep_free) {
	  delete &el;
	} else {
	  el = nullptr;
	  free_list.push_front(el);
	}
      }

      struct fun_freer {
	_FunQueue_base& fq;

	fun_freer(_FunQueue_base& _fq) : fq(_fq) {}
	void operator()(F& f) {
	  fq.free(f);
	}
      } ff;

    public:

      // 13 is an arbitrary value, we can tune it and try to find a
      // good match.
      _FunQueue_base(const size_t kf = 13) : keep_free(kf), ff(*this) {}
      _FunQueue_base(const _FunQueue_base&) = delete;
      _FunQueue_base(_FunQueue_base&& fq) : keep_free(fq.keep_free),
					    ff(*this) {
	queue.clear_and_dispose(fd);
	queue.swap(fq.queue);
      }
      ~_FunQueue_base() {
	queue.clear_and_dispose(fd);
	free_list.clear_and_dispose(fd);
      }

      const _FunQueue_base& operator =(const _FunQueue_base&) = delete;
      const _FunQueue_base& operator =(_FunQueue_base&& fq) {
	queue.clear_and_dispose(fd);
	queue.swap(fq.queue);
      };

      void swap(_FunQueue_base& fq) {
	queue.swap(fq.queue);
      }
      friend void swap(_FunQueue_base& fq1, _FunQueue_base& fqa) {
	fq1.swap(fqa);
      }

      template<typename Callable>
      void add(Callable&& f) {
	if (!free_list.empty()) {
	  F& el = free_list.front();
	  free_list.pop_front();
	  el.assign(std::forward<Callable>(f));
	  queue.insert(queue.end(), el);
	  assert(el);
	} else {
	  F& el = *(new F(std::forward<Callable>(f)));
	  queue.insert(queue.end(), el);
	  assert(el);
	}
      }

      typedef cohort::function<Res(Args...)> LLF;
      void add(LLF&& f) {
	if (!free_list.empty()) {
	  F& el = free_list.front();
	  free_list.pop_front();
	  el = std::forward<LLF>(f);
	  queue.insert(queue.end(), el);
	  assert(el);
	} else {
	  F& el = *(new F(std::forward<LLF>(f)));
	  queue.insert(queue.end(), el);
	  assert(el);
	}
      }

      void clear() {
	queue.clear_and_dispose(ff);
      };

      bool empty() {
	return queue.empty();
      }

      void splice(_FunQueue_base& fq) {
	queue.splice_before(queue.end(),
			    fq.queue);
      }

      void execute(Args&&... args) {
	while (!queue.empty()) {
	  F& el = queue.front();
	  queue.pop_front();
	  el(std::forward<Args>(args)...);
	  free(el);
	}
      }

      void operator()(Args&&... args) {
	execute(std::forward<Args>(args)...);
      }
    };
  }; // detail

  template<typename _Signature>
  class FunQueue;

  template<typename... Args>
  class FunQueue<void(Args...)> : public detail::_FunQueue_base<void(Args...)>
  {
    using detail::_FunQueue_base<void(Args...)>::queue;
    using detail::_FunQueue_base<void(Args...)>::free;
    typedef typename detail::_FunQueue_base<void(Args...)>::F F;

  public:

    // 13 is an arbitrary value, we can tune it and try to find a
    // good match.
    FunQueue(const size_t kf = 13)
      : detail::_FunQueue_base<void(Args...)>(kf) {}

    void execute_one(Args&&... args) {
      if (!queue.empty()) {
	F& el = queue.front();
	queue.pop_front();
	el(std::forward<Args>(args)...);
	free(el);
      } else {
	throw std::bad_function_call();
      }
    }
  };

  template<typename Res, typename... Args>
  class FunQueue<Res(Args...)> : public detail::_FunQueue_base<Res(Args...)>
  {
    using detail::_FunQueue_base<Res(Args...)>::queue;
    using detail::_FunQueue_base<Res(Args...)>::free;
    typedef typename detail::_FunQueue_base<Res(Args...)>::F F;

  public:

    // 13 is an arbitrary value, we can tune it and try to find a
    // good match.
    FunQueue(const size_t kf = 13)
      : detail::_FunQueue_base<Res(Args...)>(kf) {}

    Res execute_one(Args&&... args) {
      if (!queue.empty()) {
	F& el = queue.front();
	queue.pop_front();
	Res r = el(std::forward<Args>(args)...);
	free(el);
	return r;
      } else {
	throw std::bad_function_call();
      }
    }

    template<typename FRes>
    FRes execute(const std::function<FRes(Res, FRes)>& f,
		 const FRes& seed,
		 Args&&... args) {
      FRes a = seed;
      while (!queue.empty()) {
	F& el = queue.front();
	queue.pop_front();
	a = f(el(std::forward<Args>(args)...), a);
	free(el);
      }
      return a;
    }
  };
}; // cohort
#endif // COHORT_FUNQUEUE_H
