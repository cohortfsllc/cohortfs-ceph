// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// #include "gtest/gtest.h"
#include "osd/OSDMap.h"
#include "vol/Volume.h"
#include "cohort/CohortVolume.h"
#include "include/buffer_ptr.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#include "OSDMap.capnp.h"

#include <iostream>
#include <map>
#include <sys/time.h>

using namespace std;
using namespace kj;


typedef kj::ArrayPtr<const kj::ArrayPtr<const ::capnp::word>> segments_t;

void dump_segments(const segments_t &segments) {
  unsigned long total = 0;
  std::cout << "segments: " << segments.size() << " segments" << std::endl;
  for (const kj::ArrayPtr<const ::capnp::word> *c = segments.begin();
       c != segments.end();
       ++c) {
    total += c->size();
    std::cout << "    size: " << c->size() << " words" << std::endl;
  }

  std::cout << "  segments total: " << total << " words (" <<
    (total * 8) << " bytes)" << std::endl;
}


void dump_bufferlist(const bufferlist &bl) {
  const std::list<bufferptr>& buffers = bl.buffers();
  std::cout << "bufferlist: " << buffers.size() << " buffers" << std::endl;
  for (std::list<bufferptr>::const_iterator it = buffers.begin();
       it != buffers.end();
       ++it) {
    std::cout << "    size: " << it->length() << " bytes" << std::endl;
  }

  std::cout << "  bufferlist total: " << bl.length() <<
    " bytes" << std::endl;
}

long time_diff_milliseconds(timeval start, timeval end) {
  timeval result;
  timersub(&end, &start, &result);
  return result.tv_sec * 1000 + result.tv_usec / 1000;
}


void encodeUuid(Captain::Uuid::Builder to,
				const boost::uuids::uuid &from) {
  ::capnp::Data::Reader data(reinterpret_cast<const byte*>(&from),
							 sizeof(from));
  to.setUuid(data);
}


void decodeUuid(boost::uuids::uuid &uuid,
				const Captain::Uuid::Reader &from) {
  memcpy(&uuid, from.getUuid().begin(), from.getUuid().size());
}


boost::uuids::uuid decodeUuid(const Captain::Uuid::Reader &from) {
  boost::uuids::uuid result;
  decodeUuid(result, from);
  return result;
}


void encodeEntityAddr(Captain::EntityAddr::Builder to,
					  const entity_addr_t &from) {
  to.setType(from.type);
  to.setNonce(from.nonce);

  ::capnp::Data::Reader addr(reinterpret_cast<const byte*>(&from.addr),
							 sizeof(from.addr));
  to.setAddr(addr);
}


void decodeEntityAddr(entity_addr_t &to,
					  Captain::EntityAddr::Reader from) {
  to.type = from.getType();
  to.nonce = from.getNonce();
  assert(sizeof(to.addr) == from.getAddr().size());
  memcpy((void*) &to.addr, from.getAddr().begin(), sizeof(to.addr));
}


entity_addr_t decodeEntityAddr(Captain::EntityAddr::Reader from) {
  entity_addr_t result;
  decodeEntityAddr(result, from);
  return result;
}


void encodeListEntityAddr(::capnp::List<Captain::EntityAddr>::Builder to,
						  vector<std::shared_ptr<entity_addr_t> > from) {
  vector<std::shared_ptr<entity_addr_t> >::const_iterator c;
  int i;
  for (c = from.begin(), i = 0; c != from.end(); ++i, ++c) {
	Captain::EntityAddr::Builder to2 = to[i];
	encodeEntityAddr(to2, **c);
  }
}


void decodeListEntityAddr(vector<std::shared_ptr<entity_addr_t> > &to,
						  ::capnp::List<Captain::EntityAddr>::Reader from) {
  for (auto c = from.begin(); c != from.end(); ++c) {
	entity_addr_t *entityAddr = new entity_addr_t();
	decodeEntityAddr(*entityAddr, *c);
	to.push_back(shared_ptr<entity_addr_t>(entityAddr));
  }
}


void encodeUTime(Captain::UTime::Builder to, const utime_t &from) {
  Captain::UTime::Tv::Builder tv = to.initTv();
  tv.setTvSec(from.tv.tv_sec);
  tv.setTvNsec(from.tv.tv_nsec);
}


utime_t decodeUTime(Captain::UTime::Reader from) {
  utime_t result;
  result.tv.tv_sec = from.getTv().getTvSec();
  result.tv.tv_nsec = from.getTv().getTvNsec();
  return result;
}


/*
 * Struct OSDMapCapnP exists so that the OSDMap class can "friend" it
 * thereby eliminating the need to find another means to private
 * elements.
 */
struct OSDMapCapnP {

  static void encodeOSDMap(const OSDMap &map,
						   Captain::OSDMap::Builder &msg) {
	const int volumeVersion = 0;

	encodeUuid(msg.initFsid(), map.get_fsid());
	msg.initEpoch().setEpoch(map.get_epoch());
	encodeUTime(msg.initCreated(), map.get_created());
	encodeUTime(msg.initModified(), map.get_modified());
	msg.setFlags(map.get_flags());
	msg.setMaxOsd(map.get_max_osd());

	// osd states and weights

	::capnp::List<uint8_t>::Builder osdState =
		msg.initOsdState(map.get_max_osd());
	::capnp::List<uint32_t>::Builder osdWeight =
		msg.initOsdWeight(map.get_max_osd());
	::capnp::List<Captain::OsdInfo>::Builder osdInfos =
		msg.initOsdInfo(map.get_max_osd());
	::capnp::List<Captain::OsdXInfo>::Builder osdXInfos =
		msg.initOsdXInfo(map.get_max_osd());
	for (int i = 0; i < map.get_max_osd(); ++i) {
	  osdState.set(i, map.get_state(i));
	  osdWeight.set(i, map.get_weight(i));

	  {
		Captain::OsdInfo::Builder osdInfo = osdInfos[i];
		osdInfo.initUpFrom().setEpoch(map.get_up_from(i));
		osdInfo.initUpThru().setEpoch(map.get_up_thru(i));
		osdInfo.initDownAt().setEpoch(map.get_down_at(i));
	  }

	  {
		Captain::OsdXInfo::Builder osdXInfo = osdXInfos[i];
		const osd_xinfo_t &xinfo = map.get_xinfo(i);

		encodeUTime(osdXInfo.initDownStamp(), xinfo.down_stamp);
		osdXInfo.setLaggyProbability(xinfo.laggy_probability * 0xffffffffu);
		osdXInfo.setLaggyInterval(xinfo.laggy_interval);
		osdXInfo.setFeatures(xinfo.features);
	  }
	}

	// volumes

	{ // scope block so i and c are only valid for loop
	  const std::map<boost::uuids::uuid,VolumeRef> &mapVolumes =
		map.get_volumes();
	  ::capnp::List<Captain::Volume>::Builder volumes =
		  msg.initVolumes(mapVolumes.size());
	  int i;
	  std::map<boost::uuids::uuid,VolumeRef>::const_iterator c;
	  for (i = 0, c = mapVolumes.begin();
		   c != mapVolumes.end();
		   ++i, ++c) {
		const Volume &v = *(c->second);
		Captain::Volume::Builder v2 = volumes[i];

		v2.setVersion(volumeVersion);
		switch(v.type) {
		case CohortVol:
		  v2.setType(Captain::Volume::VolType::COHORT_VOL);
		  break;
		case NotAVolType:
		  v2.setType(Captain::Volume::VolType::NOT_A_VOL_TYPE);
		  break;
		}
		encodeUuid(v2.initId(), v.id);
		v2.setName(v.name);
		v2.initLastUpdate().setEpoch(v.last_update);
	  }
	} // scope block

	// osd addrs

	{
	  const vector<std::shared_ptr<entity_addr_t> > *addrs;

	  addrs = & map.get_osd_addrs()->hb_back_addr;
	  encodeListEntityAddr(msg.initHbBackAddr(addrs->size()), *addrs);

	  addrs = & map.get_osd_addrs()->cluster_addr;
	  encodeListEntityAddr(msg.initClusterAddr(addrs->size()), *addrs);

	  addrs = & map.get_osd_addrs()->hb_front_addr;
	  encodeListEntityAddr(msg.initHbFrontAddr(addrs->size()), *addrs);
	}

	// blacklist

	{ // scope block
	  const std::unordered_map<entity_addr_t,utime_t> &blacklist =
		map.get_blacklist();
	  ::capnp::List<Captain::EntityAddrUTimePair>::Builder pairs =
		  msg.initBlacklist(blacklist.size());

	  std::unordered_map<entity_addr_t,utime_t>::const_iterator cfrom;
	  ::capnp::List<Captain::EntityAddrUTimePair>::Builder::Iterator cto;
	  for (cfrom = blacklist.begin(), cto = pairs.begin();
		   cfrom != blacklist.end() && cto != pairs.end();
		   ++cfrom, ++cto) {
	    encodeEntityAddr(cto->initEntityAddr(), cfrom->first);
		encodeUTime(cto->initTime(), cfrom->second);
	  }
	} // scope block
  } // buildOSDMapMessage


  static OSDMap decodeOSDMap(segments_t segments) {
	capnp::SegmentArrayMessageReader reader(segments);
	Captain::OSDMap::Reader r = reader.getRoot<Captain::OSDMap>();

	OSDMap m;
	int maxOsd = r.getMaxOsd();

	m.build_simple(g_ceph_context,
				   0,
				   decodeUuid(r.getFsid()),
				   maxOsd);

	// fsid set by call to build_simple above
	m.epoch = r.getEpoch().getEpoch();
	m.created = decodeUTime(r.getCreated());
	m.modified = decodeUTime(r.getModified());
	m.flags = r.getFlags();
	// max osd set by call to build_simple above
	
	for (int i = 0; i < maxOsd; ++i) {
	  m.set_state(i, r.getOsdState()[i]);
	  m.set_weight(i, r.getOsdWeight()[i]);
	  {
		osd_info_t &m_info = m.osd_info[i];
		Captain::OsdInfo::Reader r_info = r.getOsdInfo()[i];

		m_info.up_from = r_info.getUpFrom().getEpoch();
		m_info.up_thru = r_info.getUpThru().getEpoch();
		m_info.down_at = r_info.getDownAt().getEpoch();
	  }
	  {
		osd_xinfo_t &m_xinfo = m.osd_xinfo[i];
		Captain::OsdXInfo::Reader r_xinfo = r.getOsdXInfo()[i];

		m_xinfo.down_stamp = decodeUTime(r_xinfo.getDownStamp());
		m_xinfo.laggy_probability = r_xinfo.getLaggyProbability();
		m_xinfo.laggy_interval =
		  (float)r_xinfo.getLaggyInterval() / (float)0xffffffff;
		m_xinfo.features = r_xinfo.getFeatures();
	  }
	} // for loop

	// volumes

	{ // scope block so i and c are only valid for loop
	  for (auto c = r.getVolumes().begin();
		   c != r.getVolumes().end();
		   ++c) {
		Volume *v;
		switch(c->getType()) {
		case Captain::Volume::VolType::COHORT_VOL:
		  // v = new CohortVolume();
		  v->type = CohortVol;
		  break;
		case Captain::Volume::VolType::NOT_A_VOL_TYPE:
		  v->type = NotAVolType;
		  break;
		} // switch


		v->id = decodeUuid(c->getId());
		v->name = c->getName();
		v->last_update = c->getLastUpdate().getEpoch();

		VolumeRef vr(v);
		m.add_volume(vr);
	  }
	} // scope block

	// osd addrs

	{
	  decodeListEntityAddr(m.get_osd_addrs()->hb_back_addr,
						   r.getHbBackAddr());

	  decodeListEntityAddr(m.get_osd_addrs()->cluster_addr,
						   r.getClusterAddr());

	  decodeListEntityAddr(m.get_osd_addrs()->hb_front_addr,
						   r.getHbFrontAddr());
	}

	// blacklist

	{ // scope block
	  ::capnp::List<Captain::EntityAddrUTimePair>::Reader pairs =
		  r.getBlacklist();

	  for (auto c = pairs.begin();
		   c != pairs.end();
		   ++c) {
		entity_addr_t entityAddr = decodeEntityAddr(c->getEntityAddr());
		utime_t utime = decodeUTime(c->getTime());
		m.blacklist.insert({{entityAddr, utime}});
	  }
	} // scope block

	return m;
  } // function decodeOSDMap

}; // struct OSDMapCapnP


void set_up_volumes(OSDMap &osdmap, int num_volumes) {
  // boost::uuids::uuid uuid_memo;
  for (int i = 0; i < num_volumes; ++i) {
    // VolumeRef vr(new CohortVolume(CohortVol));
    // osdmap.create_volume(vr, uuid_memo);
  }
}

void set_up_map(OSDMap &osdmap, int num_osds) {
  boost::uuids::uuid fsid;
  osdmap.build_simple(g_ceph_context, 0, fsid, num_osds);
  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
  pending_inc.fsid = osdmap.get_fsid();
  entity_addr_t sample_addr;
  boost::uuids::uuid sample_uuid;
  for (int i = 0; i < num_osds; ++i) {
    sample_uuid.data[i] = i;
    sample_addr.nonce = i;
    pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
    pending_inc.new_up_client[i] = sample_addr;
    pending_inc.new_up_cluster[i] = sample_addr;
    pending_inc.new_hb_back_up[i] = sample_addr;
    pending_inc.new_hb_front_up[i] = sample_addr;
    pending_inc.new_weight[i] = CEPH_OSD_IN;
    pending_inc.new_uuid[i] = sample_uuid;
  }
  osdmap.apply_incremental(pending_inc);
}


void test_present_encode(const OSDMap &osdMap, int iterations = 1000) {
  timeval start, end;
  gettimeofday(&start, NULL);
  for (int i = 0; i < iterations; ++i) {
    bufferlist bl;
    osdMap.encode(bl);
  }
  gettimeofday(&end, NULL);

  std::cout << "Present encode: " <<
	time_diff_milliseconds(start, end) << " milliseconds for " <<
    iterations << " iterations." << std::endl;
}


void test_present_decode(bufferlist &bl, int iterations = 1000) {
  timeval start, end;
  gettimeofday(&start, NULL);
  for (int i = 0; i < iterations; ++i) {
	OSDMap map;
    map.decode(bl);
  }
  gettimeofday(&end, NULL);

  std::cout << "Present decode: " <<
	time_diff_milliseconds(start, end) << " milliseconds for " <<
    iterations << " iterations." << std::endl;
}


segments_t map_to_segments(const OSDMap &osdMap) {
  ::capnp::MallocMessageBuilder message;
  Captain::OSDMap::Builder osdMapMsg =
	message.initRoot<Captain::OSDMap>();

  OSDMapCapnP::encodeOSDMap(osdMap, osdMapMsg);

  segments_t segments = message.getSegmentsForOutput();

  return segments;
}


void test_capnp_encode(const OSDMap &osdMap, int iterations = 1000) {
  timeval start, end;

  gettimeofday(&start, NULL);
  for (int i = 0; i < iterations; ++i) {
    (void) map_to_segments(osdMap); // ignore result
  }
  gettimeofday(&end, NULL);

  std::cout << "Capn Protocol encode: " <<
    time_diff_milliseconds(start, end) << " milliseconds for " <<
    iterations << " iterations." << std::endl;
}


void test_capnp_decode(const segments_t &segments, int iterations = 1000) {
  timeval start, end;

  gettimeofday(&start, NULL);
  for (int i = 0; i < iterations; ++i) {
	OSDMap map = OSDMapCapnP::decodeOSDMap(segments);
  }
  gettimeofday(&end, NULL);

  std::cout << "Capn Protocol decode: " <<
    time_diff_milliseconds(start, end) << " milliseconds for " <<
    iterations << " iterations." << std::endl;
}


int main(int argc, char* argv[]) {
  const int num_osds = 100;
  const int num_volumes = 20;
  const int iterations = 10000;
  OSDMap osdmap;
  std::vector<const char *> preargs;
  std::vector<const char*> args(argv, argv+argc);
  global_init(&preargs, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
			  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  // make sure we have 3 copies, or some tests won't work
  g_ceph_context->_conf->set_val("osd_pool_default_size", "3", false);
  // our map is flat, so just try and split across OSDs, not hosts or whatever
  g_ceph_context->_conf->set_val("osd_crush_chooseleaf_type", "0", false);

  set_up_map(osdmap, num_osds);
  set_up_volumes(osdmap, num_volumes);

  test_present_encode(osdmap, iterations);
  test_capnp_encode(osdmap, iterations);

  bufferlist bl;
  osdmap.encode(bl);
  dump_bufferlist(bl);

  segments_t segments = map_to_segments(osdmap);
  dump_segments(segments);

  test_present_decode(bl, iterations);
  test_capnp_decode(segments, iterations);

  OSDMap map2 = OSDMapCapnP::decodeOSDMap(segments);
}
