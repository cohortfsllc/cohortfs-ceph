// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <capnp/message.h>
// #include <capnp/serialize-packed.h>
#include <capnp/serialize.h>

#include <memory>
#include <iostream>
#include <map>
#include <sys/time.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>


#include "msg/msg_types.h"
#include "include/utime.h"
#include "test-small.capnp.h"
#include "capnp-common.h"


using namespace std;
using namespace kj;


void build(const char* path) {
  ::capnp::MallocMessageBuilder messageBuilder;
  Test::Foo::Builder message =
    messageBuilder.initRoot<Test::Foo>();

  message.setMaxOsd(0xDAD);
  message.initEpoch().setEpoch(0xBED);

  utime_t created_time;
  created_time.tv.tv_sec = 0xcad;
  created_time.tv.tv_nsec = 0xddd;
  encodeUTime(message.initCreated(), created_time);

  static boost::uuids::random_generator brg;
  boost::uuids::uuid fsid = brg();
  std::cout << "generated uuid " << fsid << std::endl;
  encodeUuid(message.initFsid(), fsid);

  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0666);

  ::capnp::writeMessageToFd(fd, messageBuilder);

  close(fd);
}


void read(const char* path) {
  int fd = open("/tmp/test-small.message", O_RDONLY);

  ::capnp::StreamFdMessageReader messageReader(fd);
  Test::Foo::Reader reader =
    messageReader.getRoot<Test::Foo>();
  close(fd);

  int maxOsd = reader.getMaxOsd();
  std::cout << "max osd is " << std::hex << maxOsd << std::endl;

  std::cout << "epoch is " <<
    std::hex << reader.getEpoch().getEpoch() << std::endl;

  utime_t created = decodeUTime(reader.getCreated());

  std::cout << "created is " <<
    std::hex << created.tv.tv_sec << " sec & " <<
    std::hex << created.tv.tv_nsec << " nsec" <<
    std::endl;

  std::cout << "uuid read is " << decodeUuid(reader.getFsid()) << std::endl;
}


int main(int argc, char* argv[]) {
  const char* path = "/tmp/test-small.message";

  build(path);
  read(path);
}
