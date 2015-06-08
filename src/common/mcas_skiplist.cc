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
                             size_t object_size, const char *name,
                             destructor_fn destructor,
                             int lowwater, int highwater)
  : gc(gc),
    skip(osi_cas_skip_alloc(cmp)),
    size(0),
    unused(0),
    shutdown(false),
    thread_list(nullptr)
{
  assert(skip);
  memset(&stats, 0, sizeof(stats));
  osi_mcas_obj_cache_create(gc, &cache, object_size, name);

  int r = pthread_key_create(&tls_key, sumup_and_free);
  assert(r == 0);

  if (highwater > 0)
    reaper = std::thread(&skiplist_base::reaper_thread, this,
                         destructor, lowwater, highwater);
}

skiplist_base::~skiplist_base()
{
  if (reaper.joinable()) {
    shutdown = true;
    reaper.join();
  }
  osi_cas_skip_free(gc, skip);
  osi_mcas_obj_cache_destroy(cache);
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
  int shape;
  bool shutdown;
  bool activity_only;
  skiplist_base::object *freelist;
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
  // XXX: does gc_guard guarantee that we can't get() another ref after the check?
  node->deleted = 1;
  object *n = static_cast<object*>(osi_cas_skip_remove(base->gc, skip, node));
  assert(n == node);
  --base->size;

  node->next = arg->freelist;
  arg->freelist = node;
  ++arg->removed;
}

void skiplist_base::reaper_thread(destructor_fn destructor,
                                  int lowwater, int highwater)
{
  skip_stats *const s = get_mythread_stats();
  s->reaper_shape = 3;

  for (;;) {
    if (size < highwater && !shutdown)
      std::this_thread::sleep_for(std::chrono::milliseconds(5));

    reaper_arg arg = {this, s->reaper_shape, shutdown};
    {
      gc_guard guard(gc);

      const int total = size;
      const int target = total - lowwater; // target number to remove

      arg.activity_only = total < highwater;
      if (arg.activity_only)
        ++s->reaper_activity_only;
      else
        ++s->reaper_passes;

      osi_cas_skip_for_each(gc, skip, reap_entry, &arg);
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
      for (object *next, *node = arg.freelist; node; node = next) {
        next = node->next;
        ++s->reaped;
        destructor(node); // call destructor
        gc_free(guard, node, cache);
      }
    }
    if (arg.shutdown)
      break;
  }
  assert(size == 0);
}
