# LRC plugin

set(lrc_srcs
  ErasureCodePluginLrc.cc
  ErasureCodeLrc.cc
  $<TARGET_OBJECTS:erasure_code_objs>
)

add_library(ec_Lrc SHARED ${lrc_srcs})
set_target_properties(ec_Lrc PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ec_Lrc DESTINATION lib/erasure-code)
