#ifndef __CACHE_STATS_H__
#define __CACHE_STATS_H__

#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE // has to be before any headers
#endif

#include <stdio.h>
#include <sched.h>
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <linux/perf_event.h>
#include <linux/hw_breakpoint.h>
#include <asm/unistd.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <json-c/json.h>

#include "global/global.h"
#include "global/util.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cache_stats {
    double l1_accesses;
    double l1_misses;
    double li_misses;
    double l2_accesses;
    double l2_misses;
    double l3_accesses;
    double l3_misses;
    double perf_event_time;
} cache_stats_t;

typedef struct perf_event_attr perf_event_attr_t;

static perf_event_attr_t cache_access_attr;
static perf_event_attr_t cache_misses_attr;
static perf_event_attr_t l1_cache_access_attr;
static perf_event_attr_t l1_cache_misses_attr;
static perf_event_attr_t li_cache_misses_attr;

extern int cache_access_fd;
extern int cache_misses_fd;
extern int l1_cache_access_fd;
extern int l1_cache_misses_fd;
extern int li_cache_misses_fd;

extern bool enable_cache_stats;
extern bool cache_stats_done_init;

#define LL_CA PERF_COUNT_HW_CACHE_LL \
                | (PERF_COUNT_HW_CACHE_OP_READ << 8) \
                | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16)

#define LL_CM PERF_COUNT_HW_CACHE_LL \
                | (PERF_COUNT_HW_CACHE_OP_READ << 8) \
                | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)

#define L1_CA PERF_COUNT_HW_CACHE_L1D \
                | (PERF_COUNT_HW_CACHE_OP_READ << 8) \
                | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16)

#define L1_CM PERF_COUNT_HW_CACHE_L1D \
                | (PERF_COUNT_HW_CACHE_OP_READ << 8) \
                | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)

#define LI_CM PERF_COUNT_HW_CACHE_L1I \
                | (PERF_COUNT_HW_CACHE_OP_READ << 8) \
                | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)

static void perf_event_attr_init(perf_event_attr_t *pe, uint64_t config){
    memset(pe, 0, sizeof(*pe));
    pe->type = PERF_TYPE_HW_CACHE;
    pe->size = sizeof(*pe);
    pe->config = config;
    pe->disabled = 1;
    //pe->mmap = 1;
    pe->comm = 1;
    pe->read_format = PERF_FORMAT_TOTAL_TIME_RUNNING;
    pe->inherit = 1;
    pe->exclude_kernel = 1;
    pe->exclude_hv = 1;

    // Sampling
    pe->sample_period = 1000;
}

static inline int perf_event_open(perf_event_attr_t *hw_event, pid_t pid,
                    int cpu, int group_fd, unsigned long flags) {
   return syscall(__NR_perf_event_open, hw_event, pid, cpu,
                  group_fd, flags);
}

static bool get_is_cache_perf_enabled(void) {
    const char *env = getenv("MLFS_CACHE_PERF");

    if (!env) {
        printf("MLFS_CACHE_PERF not set -> disabling cache measurements!\n");
        return false;
    }

    if (!strcmp(env, "1") ||
        !strcmp(env, "TRUE") || !strcmp(env, "true") ||
        !strcmp(env, "YES") || !strcmp(env, "yes")) {
        printf("%s -> enabling cache perf!\n", env);
        return true;
    } 
    
    printf("%s -> disabling cache measurements!\n", env);
    return false;
}

static void cache_stats_init() {
    enable_cache_stats = get_is_cache_perf_enabled();
    if (!enable_cache_stats) return;
    if (!cache_stats_done_init) {

        perf_event_attr_init(&cache_access_attr, LL_CA);
        perf_event_attr_init(&cache_misses_attr, LL_CM);
        perf_event_attr_init(&l1_cache_access_attr, L1_CA);
        perf_event_attr_init(&l1_cache_misses_attr, L1_CM);
        perf_event_attr_init(&li_cache_misses_attr, LI_CM);

        cache_access_fd = perf_event_open(&cache_access_attr, 0, -1, -1, 0);
        if (cache_access_fd < 0) {
            perror("Opening cache access FD");
            panic("Could not open perf event!");
        }

        cache_misses_fd = perf_event_open(&cache_misses_attr, 0, -1, -1, 0);
        if (cache_misses_fd < 0) {
            perror("Opening cache miss FD");
            panic("Could not open perf event!");
        }

        l1_cache_access_fd = perf_event_open(&l1_cache_access_attr, 0, -1, -1, 0);
        if (l1_cache_access_fd < 0) {
            perror("Opening cache access FD");
            panic("Could not open perf event!");
        }

        l1_cache_misses_fd = perf_event_open(&l1_cache_misses_attr, 0, -1, -1, 0);
        if (l1_cache_misses_fd < 0) {
            perror("Opening cache miss FD");
            panic("Could not open perf event!");
        }

        li_cache_misses_fd = perf_event_open(&li_cache_misses_attr, 0, -1, -1, 0);
        if (li_cache_misses_fd < 0) {
            perror("Opening cache miss FD");
            panic("Could not open perf event!");
        }

        cache_stats_done_init = true;
    }
}

static void start_events(int count, ...){
    va_list argp;
    int i;
    int fd = 0;
    va_start(argp, count);
    for( i = 0; i < count; i++ ){
        fd = va_arg(argp, int);
        int err = ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
        if (err) {
            printf("IOCTL ENABLE (%d): %s\n", errno, strerror(errno)); 
            panic("IOCTL ENABLE");
        }
    }
    va_end(argp);
}

static void reset_events(int count, ...){
    va_list argp;
    int i;
    int fd = 0;
    va_start(argp, count);
    for( i = 0; i < count; i++ ){
        fd = va_arg(argp, int);
        int err = ioctl(fd, PERF_EVENT_IOC_RESET, 0);
        if (err) {
            printf("IOCTL RESET fd=%d (%d): %s\n", fd, errno, strerror(errno)); 
            panic("IOCTL RESET");
        }
    }
    va_end(argp);
}

static void end_events(int count, ...){
    va_list argp;
    int i;
    int fd = 0;
    va_start(argp, count);
    for( i = 0; i < count; i++ ){
        fd = va_arg(argp, int);
        int err = ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
        if (err) {
            printf("IOCTL DISABLE (%d): %s\n", errno, strerror(errno)); 
            panic("IOCTL");
        }
    }
    va_end(argp);
}

static void start_cache_stats(void) {
    if (!enable_cache_stats) return;
    //printf("start!\n");
    start_events(5, cache_access_fd, cache_misses_fd, l1_cache_access_fd,
            l1_cache_misses_fd, li_cache_misses_fd);
}


typedef struct read_format {
	uint64_t value;         /* The value of the event */
	//uint64_t time_enabled;  /* if PERF_FORMAT_TOTAL_TIME_ENABLED */
	uint64_t time_running;  /* if PERF_FORMAT_TOTAL_TIME_RUNNING */
	//uint64_t id;            /* if PERF_FORMAT_ID */
} read_format_t;

static ssize_t __read(int fd, char *buf, size_t size) {
    ssize_t err;
    //int err = read(fd, &res, sizeof(res));
    asm("mov %1, %%edi;"
        "mov %2, %%rsi;"
        "mov %3, %%rdx;"
        "mov %4, %%eax;"
        "syscall;\n\t"
        "mov %%rax, %0;\n\t"
        :"=r"(err)
        :"r"(fd), "m"(buf), "r"(size), "r"(__NR_read)
        :"rax", "rdi", "rsi", "rdx"
        );

    return err;
}

static void get_stat(int fd, uint64_t *count, uint64_t *time) {
    read_format_t res;
    read_format_t *res_ptr = &res;

    ssize_t err = __read(fd, (char*)&res, sizeof(res));

    if (err < sizeof(res)) {
        printf("Could not read perf stats! err(%ld) < size(%lu)\n",
                err, sizeof(res));
        ssize_t err = __read(fd, (char*)(&res) + 8, sizeof(res));
        panic("could not read stats");
    }

	*count = res.value;
	*time = res.time_running;
}

static void get_cache_stats(cache_stats_t *cs) {
    if (!enable_cache_stats) return;
    uint64_t count, time;
    get_stat(li_cache_misses_fd, &count, &time);
    cs->li_misses = (double)count;

    get_stat(l1_cache_access_fd, &count, &time);
    cs->l1_accesses = (double)count;
    get_stat(l1_cache_misses_fd, &count, &time);
    cs->l1_misses = (double)count;
    cs->l2_accesses = (double)count;
    cs->perf_event_time = (double)time;

    get_stat(cache_access_fd, &count, &time);
    cs->l3_accesses = (double)count;
    cs->l2_misses = (double)count;

    get_stat(cache_misses_fd, &count, &time);
    cs->l3_misses = (double)count;
}

static void end_cache_stats(void) {
    if (!enable_cache_stats) return;
    //printf("end!\n");
    end_events(5, l1_cache_misses_fd, cache_access_fd, cache_misses_fd, 
            l1_cache_access_fd, li_cache_misses_fd);
}

static void reset_cache_stats(void) {
    if (!enable_cache_stats) return;
    //printf("reset!\n");
    reset_events(5, l1_cache_misses_fd, cache_access_fd, cache_misses_fd, 
            l1_cache_access_fd, li_cache_misses_fd);
}

static void print_cache_stats(cache_stats_t *cs) 
{
    if (!enable_cache_stats) return;
    double l1_hits = cs->l1_accesses - cs->l1_misses;
    double l2_hits = cs->l2_accesses - cs->l2_misses;
    double l3_hits = cs->l3_accesses - cs->l3_misses;

    printf("l1 (%.2f %.2f %.2f) (%.0f)\nl2 (%.2f %.2f %.2f) (%.0f)\nl3 (%.2f %.2f %.2f) (%.0f)\n",
            cs->l1_accesses, l1_hits, cs->l1_misses, (cs->l1_misses * 100.0) / cs->l1_accesses,
            cs->l2_accesses, l2_hits, cs->l2_misses, (cs->l2_misses * 100.0) / cs->l2_accesses,
            cs->l3_accesses, l3_hits, cs->l3_misses, (cs->l3_misses * 100.0) / cs->l3_accesses);
}

void add_cache_stats_to_json(json_object *root, const char *object_name, cache_stats_t *cs);

#ifdef __cplusplus
}
#endif

#endif //__CACHE_STATS_H__
