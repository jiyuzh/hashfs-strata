define load_sl
	sharedlibrary libshim.so
	sharedlibrary libmlfs.so
end

set auto-solib-add on
set environment LD_PRELOAD ../build/libmlfs.so:../lib/jemalloc-4.5.0/lib/libjemalloc.so.2
set environment LD_LIBRARY_PATH ../lib/nvml/src/nondebug/:../build/:../src/storage/spdk/
#set environment DEV_ID 4

set follow-fork-mode child

# loading python modules.
source ../../gdb_python_modules/load_modules.py

# this is macro to setup for breakpoint
define setup_br
	b 60
# run programe
	r w aa 40K

	l add_to_log
	b 541  if $caller_is("posix_write", 8)
	#b balloc if $caller_is("posix_write", 8)
end
