#include "cache_stats.h"

int cache_access_fd = 0;
int cache_misses_fd = 0;
int l1_cache_access_fd = 0;
int l1_cache_misses_fd = 0;
int li_cache_misses_fd = 0;

bool enable_cache_stats = false;
bool cache_stats_done_init = false;

