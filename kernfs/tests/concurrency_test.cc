#include "concurrency_test.hh"

#include <thread>
#include <vector>

ConcurrencyTest::ConcurrencyTest(int n) : numThreads(n) {
  extTest.initialize(n);
}

double ConcurrencyTest::run_multi_block_test(int i, int l, int b) {
  auto task = [&] (int id) -> tuple<double, double> {

    list<mlfs_lblk_t> insert = extTest.genLogicalBlockSequence(
        (SequenceType)i, 0, 100000, b);
    list<mlfs_lblk_t> lookup = extTest.genLogicalBlockSequence(
        (SequenceType)l, insert);

    return extTest.run_multi_block_test(insert, lookup, b, id);
  };

  time_stats ts;
  vector<thread> t;

  time_stats_init(&ts, 1);
  time_stats_start(&ts);

  for (int i = 0; i < numThreads; ++i) {
    t.emplace_back(task, i);
  }

  for (int i = 0; i < numThreads; ++i) {
    t[i].join();
  }

  time_stats_stop(&ts);

  return time_stats_get_avg(&ts);
}

int main(int argc, char **argv)
{
  if (argc < 5) {
    cerr << "Usage: " << argv[0] << " INSERT_ORDER LOOKUP_ORDER" <<
     " BLOCK_CHUCK_SIZE INSERT_OUTPUT_FILE LOOKUP_OUTPUT_FILE" << endl;
    return 1;
  }

  int i = atoi(argv[1]);
  int l = atoi(argv[2]);
  int b = atoi(argv[3]);
  int n = atoi(argv[4]);

  for (int x = 1; x <= n; ++x) {
    ConcurrencyTest c(x);
    double time = c.run_multi_block_test(i, l, b);
    printf("%d %f\n", x, time);
  }

  return 0;
}
