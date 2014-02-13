// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-


#ifndef CEPH_COHORTOSDMAP_H
#define CEPH_COHORTOSDMAP_H

#include "osd/OSDMap.h"

class CohortOSDMap : public OSDMap {

private:
  typedef OSDMap inherited;

protected:

  void populate_simple(CephContext *cct) { }

  void populate_simple_from_conf(CephContext *cct) { }


public:
  void thrash(Monitor* mon, OSDMap::Incremental& pending_inc_orig);
  virtual uint64_t get_features(uint64_t *mask) const;


  class Incremental : public OSDMap::Incremental {
  private:
    typedef OSDMap::Incremental inherited;

  public:
    void encode(bufferlist& bl,
		uint64_t features = CEPH_FEATURES_ALL) const;
    void decode(bufferlist::iterator &p);
    void decode(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    void dump(Formatter *f) const;

    Incremental(epoch_t e = 0) :
      OSDMap::Incremental(e)
    {
      // empty
    }
    Incremental(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    Incremental(bufferlist::iterator &p) {
      decode(p);
    }

    OSDMapRef newOSDMap() const {
      return OSDMapRef(new CohortOSDMap());
    }
  };

public:

  CohortOSDMap()
    : OSDMap()
  {
    // empty for now
  }

  ~CohortOSDMap() {}

  friend class CohortOSDMonitor;

  void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
  void decode(bufferlist::iterator &p);
  void dump(Formatter *f) const;
  void print(ostream& out) const;

  void set_epoch(epoch_t e);

  virtual Incremental* newIncremental() const;

  int apply_incremental_subclass(const OSDMap::Incremental& inc);

  void build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
		    int num_osd);
  int build_simple_from_conf(CephContext *cct, epoch_t e, uuid_d &fsid);

  int get_oid_osd(const object_t& oid,
		  const int rule_index,
		  vector<int> &osds);

  int get_file_stripe_address(const vector<ObjectExtent>& extents,
			      const vector<entity_addr_t>& address,
			      vector<int> &osds);
};
WRITE_CLASS_ENCODER_FEATURES(CohortOSDMap)
WRITE_CLASS_ENCODER_FEATURES(CohortOSDMap::Incremental)

typedef std::tr1::shared_ptr<const CohortOSDMap> CohortOSDMapRef;

#endif // CEPH_COHORTOSDMAP_H
