#ifndef __libspdk_h__
#define __libspdk_h__

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include <rte_config.h>
#include <rte_mempool.h>
#include <rte_malloc.h>

#include "spdk/nvme.h"
//#include "spdk/pci.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BLOCK_SIZE 4096

struct ctrlr_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct ctrlr_entry	*next;
	char			name[1024];
};

struct ns_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
	struct ns_entry		*next;
  // (iangneal): array of qpairs, one per core.
	struct spdk_nvme_qpair  **qpairs;
  // (iangneal): each qpair needs a mutex -- needed for waiting on completions,
  // as that touches every qpair.
  pthread_mutex_t **qtexs;
  int nqpairs;
};

typedef struct mlfs_spdk_stats
{
	uint64_t memcpy_tsc;
} spdk_stats_t;

extern uint8_t g_enable_perf_stats;
extern spdk_stats_t g_spdk_perf_stats;
extern struct ctrlr_entry *g_controllers;
extern struct ns_entry *g_namespaces;

void register_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns);
bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
		struct spdk_nvme_ctrlr_opts *opts);
void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
		struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts);

unsigned long upper_power_of_two(unsigned int);
int libspdk_init(void);
void libspdk_exit(void);

unsigned int libspdk_get_n_lbas(void);

void show_spdk_stats(void);
float spdk_get_cpu_clock_speed(void);
long spdk_get_num_cpus(void);

extern long ncpus;
extern int max_io_queues;

inline int qpair_idx(void) {
#ifdef CONCURRENT
  return ((int)syscall(SYS_gettid)) % max_io_queues;
#else
  return 0;
#endif
}

#ifdef __cplusplus
}
#endif

#endif
