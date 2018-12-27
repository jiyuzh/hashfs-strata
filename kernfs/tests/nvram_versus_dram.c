#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdint.h>
#include <emmintrin.h>
#include <sched.h>

void set_affin() {
  cpu_set_t  mask;
  CPU_ZERO(&mask);
  CPU_SET(0, &mask);
  int result = sched_setaffinity(0, sizeof(mask), &mask);
  if (result) {
    perror("affin");
  }
}

#include "time_stat.h"

#define KB (1024)
#define MB (1024 * KB)
#define GB (1024 * MB)

#define DEVSIZE 266352984064
#define unlikely(x)     __builtin_expect((x),0)

#define _CPUFREQ 1000LLU /* MHz */

#define NS2CYCLE(__ns) (((__ns) * _CPUFREQ) / 1000)
#define CYCLE2NS(__cycles) (((__cycles) * 1000) / _CPUFREQ)

static inline unsigned long long asm_rdtscp(void)
{
	unsigned hi, lo;
	__asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi)::"rcx");
	return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

void write_mem(uint8_t *addr, size_t s, struct time_stats* ts) {
  for (int i = 0; i < s; i++) {
    uint64_t start, stop;
    addr[i] = 0;
    asm volatile ("clflush (%0)\n" :: "r"(addr + i));
    asm volatile ("sfence\n" : : );

    start = asm_rdtscp();
    addr[i] = 42;
    asm volatile ("clflush (%0)\n" :: "r"(addr + i));
    asm volatile ("sfence\n" : : );
    stop = asm_rdtscp();

    // hackz
    ts->time_v[ts->count++] = ((double)CYCLE2NS(stop - start)) / (1.0e9);
  }
}

void read_mem(uint8_t *addr, size_t s, struct time_stats* ts) {
  uint8_t *dest = (uint8_t*)malloc(s);
  if (!dest) {
    perror("malloc");
    return;
  }

  for(register int i = 0; i < s; i++) {
    uint64_t start, stop;
    asm volatile ("clflush (%0)\n" :: "r"(addr + i));
    asm volatile ("mfence\n" : : );

    start = asm_rdtscp();
    register uint8_t x = addr[i];
    asm volatile ("lfence\n" : : );
    stop = asm_rdtscp();

    if (x != 42) {
      fprintf(stderr, "Bad write\n");
      return;
    }

    // more hackz
    ts->time_v[ts->count++] = ((double)CYCLE2NS(stop - start)) / (1.0e9);
  }

}


void mem_test() {
  uint8_t *addr;
  size_t size = 1 * MB;
  struct time_stats wstats, rstats;

  addr = (uint8_t*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, 0, 0);

  if (addr == MAP_FAILED) {
    perror("mem_test (mmap)");
    goto end;
  }

  time_stats_init(&wstats, size);

  write_mem(addr, size, &wstats);
  time_stats_print(&wstats, "mem_test (write)");

  time_stats_init(&rstats, size);

  read_mem(addr, size, &rstats);
  time_stats_print(&rstats, "mem_test (read)");

end:
  munmap(addr, size);
}

void dax_test(char *dev_name) {
  int ret, f;
  uint8_t *addr;
  size_t size = 1 * MB;
  struct time_stats wstats, rstats;

  f = open(dev_name, O_RDWR);
  if (f < 0) {
    perror("dax test (open)");
    goto end;
  }

  addr = (uint8_t*)mmap(NULL, DEVSIZE, PROT_READ | PROT_WRITE, MAP_SHARED, f, 0);

  if (addr == MAP_FAILED) {
    perror("dax test (mmap)");
    goto end;
  }

  time_stats_init(&wstats, size);

  write_mem(addr, size, &wstats);
  time_stats_print(&wstats, "dax_test (write)");

  time_stats_init(&rstats, size);

  read_mem(addr, size, &rstats);
  time_stats_print(&rstats, "dax_test (read)");

  end:
    close(f);
}

int main(char argc, char **argv) {
  set_affin();

  mem_test();

  dax_test("/dev/dax5.0");

  return 0;
}
