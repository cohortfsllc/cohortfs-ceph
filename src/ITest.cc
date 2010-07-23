#include "common/Mutex.h"
#include "messages/MMonMap.h"
#include "common/common_init.h"
#include "msg/SimpleMessenger.h"
#include "client/Client.h"

int initialize(Client** client);

int main(void)
{
     Client *client;
     vinodeno_t vi(1099511699986, CEPH_NOSNAP);
     int r;

     initialize(&client);
     
     r = client->ll_fetch(vi);
     cout << r << std::endl;
     return 0;
}

int initialize(Client** client)
{
     MonClient *monclient;
     SimpleMessenger *messenger;
     const char* foo=NULL;
     const char** bar=&foo;
     char *argv[2];
     int argc=1;
     
     char procname[]="FSAL_CEPH";
     char mount[]="localhost:/";

     argv[0]=procname;
     argv[1]=mount;
     
     vector<const char*> args;
     argv_to_vec(argc, (const char**) argv, args);

     common_set_defaults(false);
     common_init(args, "libceph", true);

     if (g_conf.clock_tare) {
	  g_clock.tare();
     }
     
     monclient = new MonClient();
     if (monclient->build_initial_monmap() < 0) {
	  delete monclient;
	  return -1; //error!
     }
     //network connection
     messenger = new SimpleMessenger();
     messenger->register_entity(entity_name_t::CLIENT());
     
     //at last the client
     *client = new Client(messenger, monclient);
     
     messenger->start();
     
     (*client)->init();
}
