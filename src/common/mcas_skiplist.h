/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MCAS_CACHE_H
#define COHORT_MCAS_CACHE_H

#include <atomic>
#include <type_traits>
#include <utility>

#include <mcas/mcas.h>
#include <mcas/osi_mcas_obj_cache.h>
#include <mcas/set_queue_adt.h>


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

// T must implement a move constructor and inherit from skiplist::object
template <typename T>
class skiplist {
 private:
  const gc_global &gc;
  osi_set_t *const skip;
  osi_mcas_obj_cache_t cache;
  pthread_key_t tls_key;

 public:
  class object {
    std::atomic<int> ref_count;
   public:
    object() : ref_count(1) {}
    void get() { ++ref_count; }
    void put() { --ref_count; }
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
    osi_cas_skip_free(gc, skip);
    osi_mcas_obj_cache_destroy(cache);
  }

  T* get(T&& search_template) {
    gc_guard guard(gc);
    T *node = static_cast<T*>(osi_cas_skip_lookup(gc, skip, &search_template));
    if (!node) {
      // create new node
      void *x = gc_alloc(guard, cache);
      node = new (x) T(std::move(search_template));
      T *a = static_cast<T*>(osi_cas_skip_update(gc, skip, node, node, 0));
      if (a == nullptr || a == node) {
        // new node inserted successfully
        return node;
      }
      // lost the race to insert
      node->~T();
      gc_free(guard, node, cache);
      node = a;
    }
    // using existing node
    node->get();
    return node;
  }
};

} // namespace mcas
} // namespace cohort

#endif // COHORT_MCAS_CACHE_H
