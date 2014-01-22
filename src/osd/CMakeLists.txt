set (no_fun_osd_files
	../osd/PG.cc 
	../osd/PGLog.cc 
	../osd/ReplicatedPG.cc 
)
set (cohort_osd_files 
	../cohort/CohortOSD.cc
	../cohort/CohortOSD.h
	../cohort/OSDVol.cc
	../cohort/OSDVol.h
	../cohort/VolLog.cc
	../cohort/VolLog.h
)

set(osd_srcs
	../osd/Ager.cc 
	../osd/OSD.cc 
	../osd/OSDCap.cc 
	../osd/Watch.cc 
	../osd/ClassHandler.cc 
	../osd/OpRequest.cc 
	../osd/SnapMapper.cc
# ${no_fun_osd_files}
${cohort_osd_files}
)

include_directories("..")

add_library(osd ${osd_srcs})

target_link_libraries(osd dl leveldb)
