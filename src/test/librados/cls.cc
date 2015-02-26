#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using ceph::buffer;
using std::map;
using std::ostringstream;
using std::string;

TEST(LibRadosCls, DNE) {
  Rados cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(volume_name.c_str(), ioctx);

  // create an object
  string oid_t = "foo";
  bufferlist bl;
  ASSERT_EQ(0, ioctx.write(oid_t, bl, bl.length(), 0));

  // call a bogus class
  ASSERT_EQ(-EOPNOTSUPP, ioctx.exec(oid_t, "doesnotexistasdfasdf", "method", bl, bl));

  // call a bogus method on existent class
  ASSERT_EQ(-EOPNOTSUPP, ioctx.exec(oid_t, "lock", "doesnotexistasdfasdfasdf", bl, bl));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, cluster));
}
