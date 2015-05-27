#ifndef _MDSINT_H
#define _MDSINT_H 1
#define IN_TREE_BUILD 1
#include "msg/Dispatcher.h"
#include "common/Timer.h"
#include <mds/MDS.h>
class MDSimpl : public MDS, Dispatcher {
public:
    string name;
    int whoami;
    Messenger *messenger;
    MonClient *monc;
    MDSMap *mdsmap;
    Objecter *objecter;
    cohort::Timer<ceph::mono_clock> beacon_timer;
protected:
    int last_state, state, want_state;
    Context *createwaitingforosd;
    ceph_tid_t last_tid;
public:
    int get_state() { return state; }
    int get_want_state() { return want_state; }
    void request_state(int s);
    ceph_tid_t issue_tid() { return ++last_tid; }
    version_t beacon_last_seq;
    bool was_laggy;
    uint64_t beacon_sender;
    MDSimpl(const std::string &n, Messenger *m, MonClient *mc);
    ~MDSimpl();
    int get_nodeid() { return whoami; }
    MDSMap *get_mds_map() { return mdsmap; }
    int init();
    void beacon_start();
    void beacon_send();
    // void handle_mds_beacon(MMDSBeacon *m);
    void handle_signal(int signum);
    bool ms_dispatch(Message *m);
    bool ms_handle_reset(Connection *con);
    void ms_handle_remote_reset(Connection *con);
    void shutdown();
};
#endif /* _MDSINT_H */
