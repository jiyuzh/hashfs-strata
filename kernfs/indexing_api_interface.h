#ifndef __INDEXING_API_INTERFACE__
#define __INDEXING_API_INTERFACE__ 1

#ifdef __cplusplus
extern "C" {
#endif

#include <malloc.h>
#include <memory.h>
#include <string.h>
#include "shared.h"
#include "fs.h"
#include "global/util.h"
#include "balloc.h"

#include "file_indexing.h"

ssize_t nvm_write(paddr_t blk, off_t off, size_t nbytes, const char* buf);

ssize_t nvm_read(paddr_t blk, off_t off, size_t nbytes, char* buf);

ssize_t alloc_metadata_blocks(size_t nblocks, paddr_t* pblk);

ssize_t alloc_data_blocks(size_t nblocks, paddr_t *pblk);

ssize_t dealloc_metadata_blocks(size_t nblocks, paddr_t pblk);

ssize_t dealloc_data_blocks(size_t nblocks, paddr_t pblk);

int get_dev_info(device_info_t* di);

extern mem_man_fns_t strata_mem_man;
extern callback_fns_t strata_callbacks;
extern idx_spec_t strata_idx_spec;

#ifdef __cplusplus
};
#endif

#endif //__INDEXING_API_INTERFACE__
