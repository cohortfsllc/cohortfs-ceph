#include "include/rados/librados.h"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <vector>

#define VOLUME_LIST_BUF_SZ 32768

#if 0
TEST(LibRadosVolumes, VolumeList) {
  std::vector<char> volume_list_buf(VOLUME_LIST_BUF_SZ, '\0');
  char *buf = &volume_list_buf[0];
  rados_t cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  ASSERT_LT(rados_volume_list(cluster, buf, VOLUME_LIST_BUF_SZ), VOLUME_LIST_BUF_SZ);

  bool found_volume = false;
  while (buf[0] != '\0') {
    if ((found_volume == false) && (strcmp(buf, volume_name.c_str()) == 0)) {
      found_volume = true;
    }
    buf += strlen(buf) + 1;
  }
  ASSERT_EQ(found_volume, true);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}
#endif

int64_t rados_volume_lookup(rados_t cluster, const char *volume_name);

TEST(LibRadosVolumes, VolumeLookup) {
  rados_t cluster;
  std::string volume_name = get_temp_volume_name();
  uint8_t volume_id[16];
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  ASSERT_GT(rados_volume_by_name(cluster, volume_name.c_str(),
				 volume_id), 0);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRadosVolumes, VolumeLookup2) {
  rados_t cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  uint8_t volume_id[16];
  ASSERT_GT(rados_volume_by_name(cluster, volume_name.c_str(),
				 volume_id), 0);
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(cluster, volume_name.c_str(), &ioctx));
  uint8_t volume_id2[16];
  rados_ioctx_get_id(ioctx, volume_id2);
  ASSERT_EQ(0, memcmp(volume_id, volume_id2, 16));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRadosVolumes, VolumeDelete) {
  rados_t cluster;
  std::string volume_name = get_temp_volume_name();
  uint8_t volume_id[16];
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  ASSERT_EQ(0, rados_volume_delete(cluster, volume_name.c_str()));
  ASSERT_GT(rados_volume_by_name(cluster, volume_name.c_str(),
				 volume_id), 0);
  ASSERT_EQ(0, rados_volume_create(cluster, volume_name.c_str()));
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRadosVolumes, VolumeCreateDelete) {
  rados_t cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));

  std::string n = volume_name + "abc123";
  ASSERT_EQ(0, rados_volume_create(cluster, n.c_str()));
  ASSERT_EQ(-EEXIST, rados_volume_create(cluster, n.c_str()));
  ASSERT_EQ(0, rados_volume_delete(cluster, n.c_str()));
  ASSERT_EQ(-ENOENT, rados_volume_delete(cluster, n.c_str()));

  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}
