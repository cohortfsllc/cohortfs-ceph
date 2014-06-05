set(osdc_files
	Objecter.cc 
	ObjectCacher.cc 
#	Filer.cc For MDS integration later.
	Striper.cc
)

add_library(osdc ${osdc_files})
