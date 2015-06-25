/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "common/mcas_skiplist.h"

using namespace cohort::mcas;
using namespace detail;

struct skiplist_base::thread_stats {
  skiplist_base *skiplist;
  skip_stats stats, stats_last;
  int deleted;
  thread_stats* next;
};

skiplist_base::skiplist_base(const gc_global &gc, osi_set_cmp_func cmp,
                             const obj_cache &cache, destructor_fn destructor,
                             int highwater, int lowwater)
  : gc(gc),
    cache(cache),
    skip(osi_cas_skip_alloc(cmp)),
    size(0),
    unused(0),
    shutdown(false),
    thread_list(nullptr),
    destructor(destructor)
{
  assert(skip);
  memset(&stats, 0, sizeof(stats));

  int r = pthread_key_create(&tls_key, sumup_and_free);
  assert(r == 0);

  if (highwater > 0)
    reaper = std::thread(&skiplist_base::reaper_thread, this,
                         destructor, lowwater, highwater);
}

skiplist_base::~skiplist_base()
{
  if (reaper.joinable()) {
    std::unique_lock<std::mutex> lock(mutex);
    shutdown = true;
    cond.notify_one();
    lock.unlock();
    reaper.join();
  } else {
    // run the reaper synchronously to clean up all entries
    shutdown = true;
    reaper_thread(destructor, 0, 0);
  }
  osi_cas_skip_free(gc, skip);
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (thread_stats *p = thread_list; p; p = p->next)
      p->deleted = 1;
  }
  int r = pthread_key_delete(tls_key);
  assert(r == 0);
}

skip_stats* skiplist_base::get_mythread_stats()
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

void skiplist_base::sumup(thread_stats *p)
{
  skip_stats &stats = p->skiplist->stats;
  skip_stats &s = p->stats;
  skip_stats &l = p->stats_last;

  stats.gets += s.gets - l.gets;
  stats.gets_created += s.gets_created - l.gets_created;
  stats.gets_stillborn += s.gets_stillborn - l.gets_stillborn;
  stats.gets_existing += s.gets_existing - l.gets_existing;
  stats.gets_miss += s.gets_miss - l.gets_miss;
  stats.gets_deleted += s.gets_deleted - l.gets_deleted;
  stats.puts += s.puts - l.puts;
  stats.puts_last += s.puts_last - l.puts_last;
  stats.destroys += s.destroys - l.destroys;
  stats.reaped += s.reaped - l.reaped;
  stats.reaper_passes += s.reaper_passes - l.reaper_passes;
  stats.reaper_activity_only += s.reaper_activity_only - l.reaper_activity_only;
  stats.reaper_shape += s.reaper_shape - l.reaper_shape;
  if (stats.maxsize < s.maxsize)
    stats.maxsize = s.maxsize;
  l = s;
}

void skiplist_base::sumup_and_free(void *arg)
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

void skiplist_base::get_stats(skip_stats *s)
{
  std::lock_guard<std::mutex> lock(mutex);
  for (thread_stats *p = thread_list; p; p = p->next)
    sumup(p);

  *s = stats;
  s->size = size;
}

namespace {

struct reaper_arg {
  skiplist_base *skip;
  skiplist_base::object **freelist;
  int shape;
  bool shutdown;
  bool activity_only;
  ptst_t *ptst;
  int removed;
};

} // anonymous namespace

void skiplist_base::reap_entry(osi_set_t *skip, setval_t k, setval_t v, void *a)
{
  reaper_arg *arg = static_cast<reaper_arg*>(a);
  skiplist_base *base = arg->skip;
  object *node = static_cast<object*>(v);

  if (node->ref_count > 0)
    return;

  if (!arg->shutdown) {
    // activity decay
    int i = node->activity;
    node->activity.compare_exchange_weak(i, i>>1, std::memory_order_relaxed);

    // ignore error - not worth contesting exact age
    if (arg->activity_only || node->activity > arg->shape)
      return;
  }

  // ok, we're deleting this one
  if (!base->node_set_deleted(node))
    return; // skiplist::destroy() won the race to free

  auto removed = osi_cas_skip_remove_critical(arg->ptst, skip, node);
  assert(removed == node);
  --base->size;

  node->next = *arg->freelist;
  *arg->freelist = node;
  ++arg->removed;
}

void skiplist_base::reaper_thread(destructor_fn destructor,
                                  int lowwater, int highwater)
{
  skip_stats *const s = get_mythread_stats();
  s->reaper_shape = 3;

  skiplist_base::object *freelist = nullptr;

  for (;;) {
    if (size < highwater && !shutdown) {
      std::unique_lock<std::mutex> lock(mutex);
      if (size < highwater && !shutdown)
        cond.wait_for(lock, std::chrono::milliseconds(5));
    }

    reaper_arg arg = {this, &freelist, s->reaper_shape, shutdown};
    {
      gc_guard guard(gc);
      arg.ptst = guard;

      const int total = size;
      const int target = total - lowwater; // target number to remove

      arg.activity_only = total < highwater;
      if (arg.activity_only)
        ++s->reaper_activity_only;
      else
        ++s->reaper_passes;

      osi_cas_skip_for_each_critical(arg.ptst, skip, reap_entry, &arg);
      if (arg.removed > (5*target)/4) {
        ++s->reaper_shape;
      } else if (arg.removed < (4*target)/5) {
        if (s->reaper_shape > 0)
          --s->reaper_shape;
      }
    }
    std::this_thread::yield();
    {
      gc_guard guard(gc);

      // free items in the free list
      object **prev = nullptr;
      for (object *next, *node = freelist; node; node = next) {
        next = node->next;
        if (node->ref_count == 0) {
          ++s->reaped;
          destructor(node); // call destructor
          gc_free(guard, node, cache);
        } else {
          // we can't free this yet, so keep it linked
          if (prev)
            *prev = node;
          else
            freelist = node;
          prev = &node->next;
        }
      }
      if (prev)
        *prev = nullptr;
      else
        freelist = nullptr;
    }
    if (arg.shutdown && !freelist)
      break;
  }
  assert(size == 0);
}
