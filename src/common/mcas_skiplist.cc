/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "common/mcas_skiplist.h"


using namespace cohort::mcas;
using namespace detail;

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
  stats.puts += s.puts - l.puts;
  stats.puts_last += s.puts_last - l.puts_last;
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
