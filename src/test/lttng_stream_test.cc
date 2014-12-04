#include <chrono>

#include "global/global_init.h"

#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_osd
/*
struct example_message {
  example_message(int num_ints, int num_strings) 
        : num_ints(num_ints), num_strings(num_strings) 
        {
        }

  int num_ints;
  int num_strings;  
};
*/

int main(int argc, const char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD,
              CODE_ENVIRONMENT_UTILITY, 0);

  common_init_finish(g_ceph_context);

//  thread_local string foo("", 1024); // stand-in for thread-local buffer

  int the_int = 4351352;
  string the_string = "cohortFS";
  auto t1 = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < 100000; i++) {
    dout(1) << the_int << the_string << the_int
      << the_string << the_int << the_string
      << the_int << the_string << dendl;
    /*
#if 0
#else
    foo = "cohortFS";
    foo += "cohortFS";
    foo += "cohortFS";
    foo += "cohortFS";
    foo += the_int;
    foo += the_int;
    foo += the_int;
    foo += the_int;
    dout(1) << foo << dendl;
#endif
*/
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  auto duration = duration_cast<milliseconds>(t2 - t1);
  std::cout << "Duration is: " << duration.count() << " ms" << std::endl;
  return 0;
}


