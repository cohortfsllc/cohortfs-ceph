/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MCAS_CACHE_H
#define COHORT_MCAS_CACHE_H

#include <atomic>
#include <mutex>
#include <ostream>
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
  int gets_miss;
  int gets_deleted;
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
  const gc_global &gc;
  osi_set_t *const skip;
  osi_mcas_obj_cache_t cache;
  CACHE_PAD(1);
  std::atomic<int> size;
  CACHE_PAD(2);
  std::atomic<int> unused;
  CACHE_PAD(3);
  std::mutex mutex;
  thread_stats* thread_list;
  skip_stats stats; // accumulated thread stats protected by mutex

  skiplist_base(const gc_global &gc, osi_set_cmp_func cmp,
                size_t object_size, const char *name)
    : gc(gc),
      skip(osi_cas_skip_alloc(cmp)),
      size(0),
      unused(0),
      thread_list(nullptr)
  {
    assert(skip);
    memset(&stats, 0, sizeof(stats));
    osi_mcas_obj_cache_create(gc, &cache, object_size, name);

    int r = pthread_key_create(&tls_key, sumup_and_free);
    assert(r == 0);
  }
  ~skiplist_base()
  {
    {
      std::lock_guard<std::mutex> lock(mutex);
      for (thread_stats *p = thread_list; p; p = p->next)
        p->deleted = 1;
    }
    osi_cas_skip_free(gc, skip);
    osi_mcas_obj_cache_destroy(cache);

    int r = pthread_key_delete(tls_key);
    assert(r == 0);
  }

  class object {
    std::atomic<int> ref_count;
    skiplist_base *parent;
    int deleted; // XXX: does this need to be atomic?
    friend class skiplist_base;
   public:
    object() : ref_count(0), parent(nullptr), next(nullptr) {}
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

  // object accessors for skiplist<T>
  void node_init(object *node) { node->parent = this; }
  bool node_deleted(const object *node) const { return node->deleted; }

  skip_stats* get_mythread_stats();
  void get_stats(skip_stats *s);

  static void sumup_and_free(void *arg); // destructor for tls
  static void sumup(thread_stats *p);
};

template <typename T>
void dump_foreach(osi_set_t *skip, setval_t k, setval_t v, void *a)
{
  *static_cast<std::ostream*>(a) << ' ' << *static_cast<const T*>(v);
}

} // namespace detail


// T must implement a move constructor and inherit from skiplist::object
template <typename T>
class skiplist : private detail::skiplist_base {
 public:
  skiplist(const gc_global &gc, osi_set_cmp_func cmp, const char *name)
    : skiplist_base(gc, cmp, sizeof(T), name)
  {
    static_assert(std::is_base_of<skiplist::object, T>::value,
                  "template type T must inherit from skiplist::object");
  }

  using skiplist_base::object;
  using skiplist_base::get_stats; // void get_stats(skip_stats *s)

  // find an entry matching the search template, or move contruct an
  // entry from the template and insert it. never returns null
  boost::intrusive_ptr<T> get_or_create(T&& search_template)
  {
    skip_stats *s = get_mythread_stats();
    ++s->gets;
    gc_guard guard(gc);
    T *node = static_cast<T*>(osi_cas_skip_lookup(gc, skip, &search_template));
    if (!node) {
      // create new node
      void *x = gc_alloc(guard, cache);
      node = new (x) T(std::move(search_template));
      init_node(node);
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
    ++s->gets_existing;
    if (node->get() == 1)
      --unused;
    return boost::intrusive_ptr<T>(node, false); // don't add another ref
  }

  // find an entry matching the search template, or return null
  boost::intrusive_ptr<T> get(T&& search_template)
  {
    skip_stats *s = get_mythread_stats();
    ++s->gets;
    gc_guard guard(gc);
    T *node = static_cast<T*>(osi_cas_skip_lookup(gc, skip, &search_template));
    if (!node) {
      ++s->gets_miss;
      return nullptr;
    }
    if (node_deleted(node)) {
      ++s->gets_deleted;
      return nullptr;
    }
    // using existing node
    ++s->gets_existing;
    if (node->get() == 1)
      --unused;
    return boost::intrusive_ptr<T>(node, false); // don't add another ref
  }

  void dump(std::ostream &stream) const
  {
    osi_cas_skip_for_each(gc, skip, detail::dump_foreach<T>, &stream);
    stream << std::endl;
  }
};

} // namespace mcas
} // namespace cohort

#endif // COHORT_MCAS_CACHE_H
