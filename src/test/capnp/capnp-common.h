// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "ceph-common.capnp.h"


using namespace kj;


inline void encodeUuid(Captain::Uuid::Builder to,
                       const boost::uuids::uuid &from) {
  std::cout << "size of uuid is " << sizeof(from) << std::endl;
  to.initUuid(sizeof(from));
  ::capnp::Data::Reader data(reinterpret_cast<const byte*>(&from),
			     sizeof(from));
  to.setUuid(data);
}


inline void decodeUuid(boost::uuids::uuid &uuid,
                       const Captain::Uuid::Reader &from) {
  memcpy(&uuid, from.getUuid().begin(), from.getUuid().size());
}


inline boost::uuids::uuid decodeUuid(const Captain::Uuid::Reader &from) {
  boost::uuids::uuid result;
  decodeUuid(result, from);
  return result;
}


inline void encodeEntityAddr(Captain::EntityAddr::Builder to,
                             const entity_addr_t &from) {
  to.setType(from.type);
  to.setNonce(from.nonce);

  ::capnp::Data::Reader addr(reinterpret_cast<const byte*>(&from.addr),
			     sizeof(from.addr));
  to.setAddr(addr);
}


inline void decodeEntityAddr(entity_addr_t &to,
                             Captain::EntityAddr::Reader from) {
  to.type = from.getType();
  to.nonce = from.getNonce();
  assert(sizeof(to.addr) == from.getAddr().size());
  memcpy((void*) &to.addr, from.getAddr().begin(), sizeof(to.addr));
}


inline entity_addr_t decodeEntityAddr(Captain::EntityAddr::Reader from) {
  entity_addr_t result;
  decodeEntityAddr(result, from);
  return result;
}


inline void encodeListEntityAddr(::capnp::List<Captain::EntityAddr>::Builder to,
                                 vector<std::shared_ptr<entity_addr_t> > from) {
  vector<std::shared_ptr<entity_addr_t> >::const_iterator c;
  int i;
  for (c = from.begin(), i = 0; c != from.end(); ++i, ++c) {
    Captain::EntityAddr::Builder to2 = to[i];
    encodeEntityAddr(to2, **c);
  }
}


inline void decodeListEntityAddr(vector<std::shared_ptr<entity_addr_t> > &to,
                                 ::capnp::List<Captain::EntityAddr>::Reader from) {
  for (auto c = from.begin(); c != from.end(); ++c) {
    entity_addr_t *entityAddr = new entity_addr_t();
    decodeEntityAddr(*entityAddr, *c);
    to.push_back(std::shared_ptr<entity_addr_t>(entityAddr));
  }
}


inline void encodeUTime(Captain::UTime::Builder to, const utime_t &from) {
  Captain::UTime::Tv::Builder tv = to.initTv();
  tv.setTvSec(from.tv.tv_sec);
  tv.setTvNsec(from.tv.tv_nsec);
}


inline utime_t decodeUTime(Captain::UTime::Reader from) {
  utime_t result;
  result.tv.tv_sec = from.getTv().getTvSec();
  result.tv.tv_nsec = from.getTv().getTvNsec();
  return result;
}
