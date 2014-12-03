#include <chrono>

#include "global/global_init.h"

#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_osd


int main(int argc, const char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD,
              CODE_ENVIRONMENT_UTILITY, 0);

  common_init_finish(g_ceph_context);

  int the_int = 4351352;
  string the_string = "cohortFS";
  auto t1 = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < 100000; i++) {
    dout(1) << the_int << the_string << the_int
      << the_string << the_int<< the_string 
      << the_int << the_string << dendl; 
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  auto duration = duration_cast<milliseconds>(t2 - t1);
  std::cout << "Duration is: " << duration.count() << " ms" << std::endl;
  return 0;
}

 


























