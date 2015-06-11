/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MCAS_CACHE_H
#define COHORT_MCAS_CACHE_H

#include <atomic>
#include <mutex>
#include <type_traits>

#include <boost/intrusive_ptr.hpp>

#include <mcas/mcas.h>
#include <mcas/osi_mcas_obj_cache.h>
#include <mcas/set_queue_adt.h>

#include "include/ceph_assert.h"

namespace cohort {
namespace mcas {

class gc_global {
  gc_global_t *const handle;
 public:
  gc_global() : handle(_init_gc_subsystem()) {
    _init_osi_cas_skip_subsystem(handle);
  }
  ~gc_global() { _destroy_gc_subsystem(handle); }

  operator gc_global_t*() const { return handle; }
};

class gc_guard {
  ptst_t *const handle;
 public:
  gc_guard(const gc_global &gc) : handle(critical_enter(gc)) {}
  ~gc_guard() { critical_exit(handle); }

  operator ptst_t*() const { return handle; }
};


struct skip_stats {
  int gets;
  int gets_created;
  int gets_stillborn;
  int gets_existing;
  int puts;
  int puts_last;
  int maxsize;
  int size;
};

namespace detail {

// base skiplist code that doesn't depend on the template parameter
class skiplist_base {
 private:
  pthread_key_t tls_key;

 public:
  // per-thread stats stored in tls
  struct thread_stats {
    skiplist_base *skiplist;
    skip_stats stats, stats_last;
    int deleted;
    thread_stats* next;
  };

  CACHE_PAD(0);
  std::atomic<int> size;
  CACHE_PAD(1);
  std::atomic<int> unused;
  CACHE_PAD(2);
  std::mutex mutex;
  thread_stats* thread_list;
  skip_stats stats; // accumulated thread stats protected by mutex

  skiplist_base()
    : size(0), unused(0), thread_list(nullptr)
  {
    memset(&stats, 0, sizeof(stats));
    int r = pthread_key_create(&tls_key, sumup_and_free);
    assert(r == 0);
  }
  ~skiplist_base()
  {
    int r = pthread_key_delete(tls_key);
    assert(r == 0);
  }

  skip_stats* get_mythread_stats()
  {
    thread_stats *a = static_cast<thread_stats*>(pthread_getspecific(tls_key));
    if (a && a->deleted) // probably "can't happen"
      return 0;

    if (!a) {
      a = new thread_stats();
      memset(&a->stats, 0, sizeof(a->stats));
      memset(&a->stats_last, 0, sizeof(a->stats_last));
      a->deleted = 0;
      a->skiplist = this;
      {
        std::lock_guard<std::mutex> lock(mutex);
        a->next = thread_list;
        thread_list = a;
      }
      int r = pthread_setspecific(tls_key, a);
      assert(r == 0);
    }
    return &a->stats;
  }

  static void sumup(thread_stats *p)
  {
    skip_stats &stats = p->skiplist->stats;
    skip_stats &s = p->stats;
    skip_stats &l = p->stats_last;

    stats.gets += s.gets - l.gets;
    stats.gets_created += s.gets_created - l.gets_created;
    stats.gets_stillborn += s.gets_stillborn - l.gets_stillborn;
    stats.gets_existing += s.gets_existing - l.gets_existing;
    stats.puts += s.puts - l.puts;
    stats.puts_last += s.puts_last - l.puts_last;
    if (stats.maxsize < s.maxsize)
      stats.maxsize = s.maxsize;
    l = s;
  }

  static void sumup_and_free(void *arg)
  {
    thread_stats *p = static_cast<thread_stats*>(arg);
    thread_stats** pp, *q;

    skiplist_base *skiplist = p->skiplist;
    if (!p->deleted) {
      std::lock_guard<std::mutex> lock(skiplist->mutex);
      sumup(p);
      for (pp = &skiplist->thread_list; (q = *pp); pp = &q->next) {
        if (q == p) {
          *pp = p->next;
          break;
        }
      }
      assert(q);
    }
    delete p;
  }

  void get_stats(skip_stats *s)
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (thread_stats *p = thread_list; p; p = p->next)
      sumup(p);

    *s = stats;
    s->size = size;
  }
};

} // namespace detail

// T must implement a move constructor and inherit from skiplist::object
template <typename T>
class skiplist : private detail::skiplist_base {
 private:
  CACHE_PAD(0);
  const gc_global &gc;
  osi_set_t *const skip;
  osi_mcas_obj_cache_t cache;
  CACHE_PAD(1);

 public:
  class object {
    std::atomic<int> ref_count;
    skiplist<T> *parent;
    friend class skiplist<T>;
   public:
    object() : ref_count(0), parent(nullptr) {}
    int get() { return ++ref_count; }
    int put() {
      skip_stats *s = parent->get_mythread_stats();
      gc_guard guard(parent->gc);

      ++s->puts;
      int i = --ref_count;
      if (i == 0) {
        ++s->puts_last;
        ++parent->unused;
      }
      return i;
    }
  };

  skiplist(const gc_global &gc, osi_set_cmp_func cmp, char *name)
    : gc(gc),
      skip(osi_cas_skip_alloc(cmp))
  {
    static_assert(std::is_base_of<skiplist::object, T>::value,
                  "template type T must inherit from skiplist::object");
    osi_mcas_obj_cache_create(gc, &cache, sizeof(T), name);
  }

  ~skiplist()
  {
    {
      std::lock_guard<std::mutex> lock(mutex);
      for (thread_stats *p = thread_list; p; p = p->next)
        p->deleted = 1;
    }
    osi_cas_skip_free(gc, skip);
    osi_mcas_obj_cache_destroy(cache);
  }

  using skiplist_base::get_stats; // void get_stats(skip_stats *s)

  boost::intrusive_ptr<T> get(T&& search_template)
  {
    skip_stats *s = get_mythread_stats();
    ++s->gets;
    gc_guard guard(gc);
    T *node = static_cast<T*>(osi_cas_skip_lookup(gc, skip, &search_template));
    if (!node) {
      // create new node
      void *x = gc_alloc(guard, cache);
      node = new (x) T(std::move(search_template));
      node->parent = this;
      T *a = static_cast<T*>(osi_cas_skip_update(gc, skip, node, node, 0));
      if (a == nullptr || a == node) {
        // new node inserted successfully
        ++s->gets_created;
        int i = ++size;
        if (s->maxsize < i) s->maxsize = i;
        return node;
      }
      // lost the race to insert
      ++s->gets_stillborn;
      node->~T();
      gc_free(guard, node, cache);
      node = a;
    }
    // using existing node
    if (node->get() == 1)
      --unused;
    ++s->gets_existing;
    return boost::intrusive_ptr<T>(node, false); // don't add another ref
  }
};

} // namespace mcas
} // namespace cohort

#endif // COHORT_MCAS_CACHE_H
