// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// #include "gtest/gtest.h"

#include <sys/types.h>
#include <unistd.h>

#include "osd/OSDMap.h"
#include "vol/Volume.h"
#include "cohort/CohortVolume.h"
#include "include/buffer_ptr.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#include <iostream>
#include <map>
#include <sys/time.h>

#include "OSDMap.capnp.h"
#include "capnp-common.h"


using namespace std;
using namespace kj;


typedef kj::ArrayPtr<const kj::ArrayPtr<const ::capnp::word>> segments_t;

size_t total_size(const segments_t &segments) {
  size_t total = 0;
  for (auto c = segments.begin();
       c != segments.end();
       ++c) {
    total += c->size();
  }
  return total;
}


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


/*
 * Struct OSDMapCapnP exists so that the OSDMap class can "friend" it
 * thereby eliminating the need to find another means to private
 * elements.
 */
struct OSDMapCapnP {

  static void set_up_volumes(OSDMap &osdmap, int num_volumes) {
    // boost::uuids::uuid uuid_memo;
    for (int i = 0; i < num_volumes; ++i) {
      // VolumeRef vr(new CohortVolume(CohortVol));
      // osdmap.create_volume(vr, uuid_memo);
    }
  }

  static void set_up_map(OSDMap &osdmap, int num_osds) {
    boost::uuids::uuid fsid;
    osdmap.build_simple(g_ceph_context, 0, fsid, num_osds);
    osdmap.set_flag(1<<3);
    osdmap.set_flag(1<<10);
    osdmap.set_flag(1<<21);
    std::cout << "creating fake flags " << std::hex <<
      osdmap.get_flags() << std::endl;

    osdmap.created.tv.tv_sec = 999;
    osdmap.created.tv.tv_nsec = 888;

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

  static void encodeOSDMap(const OSDMap &map,
			   Captain::OSDMap::Builder &msg) {
    const int volumeVersion = 0;

    msg.setMaxOsd(map.get_max_osd());
    encodeUuid(msg.initFsid(), map.get_fsid());

    std::cout << "encoding maxOSD to " << map.get_max_osd() << std::endl;
    std::cout << "encoding flags " << std::hex << map.get_flags() << std::endl;
    std::cout << "encoding fsid " << map.get_fsid() << std::endl;

    msg.initEpoch().setEpoch(map.get_epoch());
    encodeUTime(msg.initCreated(), map.get_created());
    encodeUTime(msg.initModified(), map.get_modified());
    msg.setFlags(map.get_flags());

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
    return decodeOSDMap(r);
  }

  static OSDMap decodeOSDMap(Captain::OSDMap::Reader r) {
    OSDMap m;
    int32_t maxOsd = r.getMaxOsd();
    std::cout << "decoding maxOSD to " << maxOsd << std::endl;

    std::cout << "decoding flags " << std::hex << r.getFlags() << std::endl;

    std::cout << "decoding created " << decodeUTime(r.getCreated()) << std::endl;

    boost::uuids::uuid fsid;
    {
      Captain::Uuid::Reader r2 = r.getFsid();
      fsid = decodeUuid(r2);
    }
    std::cout << "decoding fsid " << fsid << std::endl;

    m.build_simple(g_ceph_context,
		   0,
		   fsid,
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


/*
  kj::ArrayPtr<::capnp::word> map_to_segment(const OSDMap &osdMap) {
  ::capnp::MallocMessageBuilder message;
  Captain::OSDMap::Builder osdMapMsg =
  message.initRoot<Captain::OSDMap>();

  OSDMapCapnP::encodeOSDMap(osdMap, osdMapMsg);

  kj::CappedArray<::capnp::word,1024*1024> array;
  array.setSize(1 + osdMapMsg.totalSize().wordCount);
  copyToUnchecked(message, array);
  return array.asPtr();
  }
*/


Captain::OSDMap::Reader map_to_reader(const OSDMap &osdMap) {
  ::capnp::MallocMessageBuilder message;
  Captain::OSDMap::Builder osdMapMsg =
    message.initRoot<Captain::OSDMap>();

  OSDMapCapnP::encodeOSDMap(osdMap, osdMapMsg);
  return osdMapMsg.asReader();
}


void map_to_file(const OSDMap &osdMap, int fd) {
  ::capnp::MallocMessageBuilder messageBuilder;
  Captain::OSDMap::Builder message =
    messageBuilder.initRoot<Captain::OSDMap>();

  OSDMapCapnP::encodeOSDMap(osdMap, message);
  ::capnp::writePackedMessageToFd(fd, messageBuilder);
}


Captain::OSDMap::Reader file_to_reader(int fd) {
  ::capnp::PackedFdMessageReader messageReader(fd);
  Captain::OSDMap::Reader mapReader =
    messageReader.getRoot<Captain::OSDMap>();
  return mapReader;
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


void test_capnp_decode(Captain::OSDMap::Reader reader,
		       int iterations = 1000) {
  timeval start, end;

  gettimeofday(&start, NULL);
  for (int i = 0; i < iterations; ++i) {
    OSDMap map = OSDMapCapnP::decodeOSDMap(reader);
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

  OSDMapCapnP::set_up_map(osdmap, num_osds);
  OSDMapCapnP::set_up_volumes(osdmap, num_volumes);

  const bool skip = true;
  if (!skip) {
    test_present_encode(osdmap, iterations);
    test_capnp_encode(osdmap, iterations);

    bufferlist bl;
    osdmap.encode(bl);
    // dump_bufferlist(bl);

    segments_t segments = map_to_segments(osdmap);
    // dump_segments(segments);

    test_present_decode(bl, iterations);
  }

  int fd = open("/tmp/capnp.message", O_RDWR | O_CREAT, 0666);
  map_to_file(osdmap, fd);
  lseek(fd, 0, SEEK_SET); // seek to start so we can read it back in
  Captain::OSDMap::Reader reader = file_to_reader(fd);
  close(fd);

  test_capnp_decode(reader, iterations);
}
