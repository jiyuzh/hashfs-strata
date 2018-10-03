#pragma once

#include "extent_test.h"

class ConcurrencyTasks {

  public:
    static void CreateBlocks(int inum, mlfs_lblk_t start, mlfs_lblk_t end);
};

class ConcurrencyTest {

  public:
    ConcurrencyTest(int n);

    double run_multi_block_test(int insert_seq, int lookup_seq, int nr_block);

  private:
    int numThreads;
    ExtentTest extTest;
};
