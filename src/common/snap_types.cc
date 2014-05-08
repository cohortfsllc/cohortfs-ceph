
#include "snap_types.h"
#include "common/Formatter.h"

// -----

bool SnapContext::is_valid() const
{
  // seq is a valid snapid
  if (seq > CEPH_MAXSNAP)
    return false;
  if (!snaps.empty()) {
    // seq >= snaps[0]
    if (snaps[0] > seq)
      return false;
    // snaps[] is descending
    snapid_t t = snaps[0];
    for (unsigned i=1; i<snaps.size(); i++) {
      if (snaps[i] >= t || t == 0)
	return false;
      t = snaps[i];
    }
  }
  return true;
}

void SnapContext::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("snaps");
  for (vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();
}

void SnapContext::generate_test_instances(list<SnapContext*>& o)
{
  o.push_back(new SnapContext);
  vector<snapid_t> v;
  o.push_back(new SnapContext(10, v));
  v.push_back(18);
  v.push_back(3);
  v.push_back(1);
  o.push_back(new SnapContext(20, v));
}
