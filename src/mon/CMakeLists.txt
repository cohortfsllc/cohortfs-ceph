set (no_fun_mon_files
	../mon/Monitor.cc 
	../mon/PGMonitor.cc 
	../mon/PGMap.cc 
)

set(lib_mon_srcs
	../auth/cephx/CephxKeyServer.cc 
	../auth/cephx/CephxServiceHandler.cc 
	../auth/cephx/CephxSessionHandler.cc 
	../auth/AuthServiceHandler.cc 
	../auth/AuthSessionHandler.cc 
	../mon/Paxos.cc 
	../mon/PaxosService.cc 
	../mon/OSDMonitor.cc 
	../mon/MDSMonitor.cc 
	../mon/MonmapMonitor.cc 
	../mon/LogMonitor.cc 
	../mon/AuthMonitor.cc 
	../mon/Elector.cc 
	../mon/MonitorStore.cc 
	../os/LevelDBStore.cc 
	../mon/HealthMonitor.cc 
	../mon/DataHealthService.cc 
	../mon/ConfigKeyService.cc 
	../common/util.cc
# ${no_fun_mon_files}
)


include_directories("..")

add_library(mon ${lib_mon_srcs})
