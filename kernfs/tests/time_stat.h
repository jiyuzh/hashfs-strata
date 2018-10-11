#ifndef __TIME_STAT_H__
#define __TIME_STAT_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <stdlib.h>

struct time_stats {
   struct timeval time_start;
   int n, count;
   double* time_v;
};

void time_stats_init(struct time_stats*, int);
void time_stats_start(struct time_stats*);
void time_stats_stop(struct time_stats*);
void time_stats_print(struct time_stats*, char*);
double time_stats_get_avg(struct time_stats*);

#ifdef __cplusplus
}

typedef struct time_stats TimeStats;
#endif

#endif  //__TIME_STAT_H__
