// vim: set ft=cpp:
#ifndef __TEST_GEN_H__
#define __TEST_GEN_H__ 1

#include <algorithm>
#include <iostream>
#include <list>
#include <memory>
#include <random>

#include "kernfs_interface.h"
#include "../io/block_io.h"
#include "../io/buffer_head.h"
#include "../extents.h"
#include "../extents_bh.h"
#include "../fs.h"
#include "../balloc.h"
#include "../mlfs/mlfs_user.h"
#include "../global/global.h"
#include "../global/util.h"
#include "../global/defs.h"
#include "../storage/storage.h"
#include "../extents.h"
#include "../extents_bh.h"
#include "../slru.h"
#include "../migrate.h"

#ifdef HASHTABLE
#include "../inode_hash.h"
extern uint64_t reads;
extern uint64_t writes;
#endif

/*
 * We support operations on logical blocks in the following sequences:
 * - SEQUENTIAL: 0 -> NUM_BLOCKS, by intervals of RANGE
 * - REVERSE:    NUM_BLOCKS - RANGE -> 0, by intervals of RANGE (allocation
 *   still done sequentially, so if range size is 100 then you'll allocate
 *   blocks 900->999, 800->899, etc.)
 * - RANDOM:     SHUFFLE(0 -> NUM_BLOCKS, by intervals of RANGE). So the blocks
 *   are random, but aligned to RANGE.
 */
enum SequenceType {
  SEQUENTIAL = 0, REVERSE, RANDOM
};

extern const char* SequenceTypeNames[];
extern const char* SequenceTypeAbbr[];

struct TestInput {
  std::shared_ptr<inode_t> Inode;
  mlfs_lblk_t LogicalBlock;

  TestInput(std::shared_ptr<inode_t> inode, mlfs_lblk_t lblk):
    Inode(inode), LogicalBlock(lblk) {}
};

struct TestOutput {
  mlfs_fsblk_t PhysicalBlockExpected = 0;
  mlfs_fsblk_t PhysicalBlockActual = 0;

  bool IsCorrect() {
    return PhysicalBlockExpected == PhysicalBlockActual;
  }
};

struct TestCase {
  std::shared_ptr<TestInput> In;
  std::shared_ptr<TestOutput> Out;

  TestCase(std::shared_ptr<TestInput>& i, std::shared_ptr<TestOutput>& o) :
    In(i), Out(o) { }
};

class TestGenerator {
  private:
    std::random_device rd_;
    std::mt19937 mt_;
    handle_t dev_handle_;

    bool done_init_;

    void BaseInit();

  public:
    TestGenerator(uint8_t dev = g_root_dev): rd_(), mt_(rd_()),
      done_init_(false), dev_handle_({.dev=dev}) {}

    ~TestGenerator() {}

    std::vector<std::shared_ptr<inode_t>> CreateInodes(
        uint32_t inum_start, size_t num);

    std::vector<mlfs_lblk_t> CreateBlockListSequential(size_t num, size_t skip);

    std::list<TestCase> CreateTests(SequenceType s, size_t num_files,
        size_t blocks_per_file, size_t block_range_size);


};
#endif  //__TEST_GEN_H__
