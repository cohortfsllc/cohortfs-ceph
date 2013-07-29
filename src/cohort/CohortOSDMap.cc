// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "CohortOSDMap.h"

void CohortOSDMap::thrash(Monitor* mon,
			  OSDMap::Incremental& pending_inc_orig)
{
// Incremental& pending_inc = dynamic_cast<Incremental&>(pending_inc_orig);

// Nothing, for now.
}

uint64_t CohortOSDMap::get_features(uint64_t *mask) const
{
  uint64_t features = CEPH_FEATURE_OSDHASHPSPOOL;

  if (mask)
    *mask = features;
  return features;
}

void CohortOSDMap::Incremental::encode(bufferlist& bl,
				       uint64_t features) const
{
  inherited::encode(bl, features);
}

void CohortOSDMap::Incremental::decode(bufferlist::iterator& p)
{
  inherited::decode(p);
}


void CohortOSDMap::Incremental::dump(Formatter *f) const
{
  inherited::dump(f);
}


void CohortOSDMap::encode(bufferlist& bl, uint64_t features) const
{
  encodeOSDMap(bl, features);
}


void CohortOSDMap::decode(bufferlist::iterator &p)
{
  decodeOSDMap(p, 65535);
}


void CohortOSDMap::dump(Formatter *f) const
{
  inherited::dump(f);
}


void CohortOSDMap::print(ostream& out) const
{
  inherited::print(out);
}

void CohortOSDMap::set_epoch(epoch_t e)
{
  inherited::set_epoch(e);
}


CohortOSDMap::Incremental* CohortOSDMap::newIncremental() const
{
  return new CohortOSDMap::Incremental();
}

int CohortOSDMap::apply_incremental_subclass(
  const OSDMap::Incremental& incOrig)
{
  const CohortOSDMap::Incremental& inc =
    dynamic_cast<const CohortOSDMap::Incremental&>(incOrig);

  return 0;
}

void CohortOSDMap::build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
				int num_osd)
{
}

int CohortOSDMap::build_simple_from_conf(CephContext *cct, epoch_t e,
					 uuid_d &fsid)
{
  return 0;
}

int CohortOSDMap::get_oid_osd(const Objecter* objecter,
			      const object_t& oid,
			      const ceph_file_layout* layout)
{
  return 0;
}

int CohortOSDMap::get_file_stripe_address(vector<ObjectExtent>& extents,
					  vector<entity_addr_t>& address)
{
  return 0;
}
