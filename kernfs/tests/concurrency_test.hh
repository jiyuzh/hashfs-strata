#pragma once

#include <iostream>
#include <map>
#include <thread>
#include <vector>

#include <sys/time.h>
#include <sys/resource.h>

#include "test_gen.h"
#include "time_stat.h"

class ConcurrencyTasks {

  public:
    static void CreateBlocks(
        TestGenerator& testGen,
        inode_t *inode,
        mlfs_lblk_t start,
        mlfs_lblk_t end,
        TimeStats *out_stats);
};

class ConcurrencyTest {

  public:
    ConcurrencyTest(int n);

    double run_multi_block_test(
        int insert_seq,
        int lookup_seq,
        int nr_block);

  private:
    int numThreads;
    TestGenerator testGen_;
};
