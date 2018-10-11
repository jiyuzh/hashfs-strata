#include "concurrency_test.hh"

using namespace std;

void ConcurrencyTasks::CreateBlocks(
    TestGenerator& testGen,
    inode_t *inode,
    mlfs_lblk_t start,
    mlfs_lblk_t end,
    TimeStats *out_stats) {

  int err;
  struct buffer_head bh_got;
  struct mlfs_map_blocks map;
  TimeStats hs;
  handle_t handle = {.dev = g_root_dev};

  time_stats_init(&hs, end - start);

  std::map<mlfs_lblk_t, mlfs_fsblk_t> res_check;

  /* create all logical blocks */
  for (mlfs_lblk_t lb = start; lb < end; ++lb) {
    map.m_lblk = lb;
    map.m_len = 1;
    time_stats_start(&hs);
    err = mlfs_ext_get_blocks(&handle, inode, &map,
      MLFS_GET_BLOCKS_CREATE);
#ifndef HASHTABLE
    // already persisted in get blocks
    sync_all_buffers(g_bdev[g_root_dev]);
#endif
    time_stats_stop(&hs);

    if (err < 0 || map.m_pblk == 0) {
      fprintf(stderr, "err: %s, lblk %x, fsblk: %lx\n",
        strerror(-err), lb, map.m_pblk);
      exit(-1);
    }

    if (map.m_len != 1) {
      cout << "request nr_block " << 1 << " received nr_block "
        << map.m_len << endl;
      exit(-1);
    }

    res_check[lb] = map.m_pblk;
  }

#ifdef HASHTABLE
  cout << "Hashtable load factor: " << check_load_factor(inode) << endl;
  cout << "Reads: " << reads << " Writes: " << writes << endl;
  cout << "Total blocks written: " << blocks << endl;
  reads = 0; writes = 0; blocks = 0;
#endif
}

ConcurrencyTest::ConcurrencyTest(int n) : numThreads(n), testGen_() { }

double ConcurrencyTest::run_multi_block_test(
    int insert_seq, int lookup_seq, int nr_block) {

  vector<shared_ptr<inode_t>> vec = testGen_.CreateInodes(100, numThreads);

  auto task = [&] (inode_t *inode) {
    TimeStats stats;
    ConcurrencyTasks::CreateBlocks(testGen_, inode, 0, nr_block, &stats);
  };

  time_stats ts;
  vector<thread> t;

  time_stats_init(&ts, 1);
  time_stats_start(&ts);

  for (auto ptr : vec) {
    t.emplace_back(task, ptr.get());
  }

  for (int i = 0; i < numThreads; ++i) {
    t[i].join();
  }

  time_stats_stop(&ts);

  return time_stats_get_avg(&ts);
}

int main(int argc, char **argv)
{
#if 1
  if (argc < 5) {
    cerr << "Usage: " << argv[0] << " INSERT_ORDER LOOKUP_ORDER" <<
     " NUM_BLOCKS NUM_THREADS" << endl;
    return 1;
  }

  int i = atoi(argv[1]);
  int l = atoi(argv[2]);
  int b = atoi(argv[3]);
  int n = atoi(argv[4]);

  ConcurrencyTest c(n);
  double time = c.run_multi_block_test(i, l, b);
  printf("%d %f\n", n, time);
#else
  TestGenerator gen;

  auto vec = gen.CreateInodes(100, 2);

  for (auto ptr : vec) {
    cout << ptr.get() << endl;
  }
#endif

  return 0;
}
