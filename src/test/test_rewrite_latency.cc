// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <cassert>
#include <cstring>
#include <map>

#include <errno.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "include/ceph_time.h"
#include "common/errno.h"

using namespace std;

int main(int argc, const char **argv)
{
  const char *fn = argv[1];
  multimap<ceph::timespan, ceph::mono_time> latency;
  unsigned max = 10;

  int fd = ::open(fn, O_CREAT|O_RDWR, 0644);
  if (fd < 1) {
    int err = errno;
    cerr << "failed to open " << fn << " with " << cpp_strerror(err)
	 << std::endl;
    return -1;
  }

  while (true) {
    auto now = ceph::mono_clock::now();
    int r = ::pwrite(fd, fn, strlen(fn), 0);
    assert(r >= 0);
    ceph::timespan lat = ceph::mono_clock::now() - now;
    ceph::timespan oldmin;
    if (!latency.empty())
      oldmin = latency.begin()->first;
    latency.insert(make_pair(lat, now));
    ceph::timespan newmin = latency.begin()->first;
    while (latency.size() > max)
      latency.erase(latency.begin());
    if (oldmin == newmin) {
      cout << "latency\tat" << std::endl;
      for (auto p = latency.rbegin(); p != latency.rend(); ++p) {
	cout << p->first << "\t" << p->second << std::endl;
      }
    }
  }
}
