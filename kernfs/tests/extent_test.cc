#include "extent_test.h"

int main(int argc, char **argv)
{
  ExtentTest cExtTest;

  //cExtTest.async_io_test();
  //cExtTest.run_read_block_test(3, 0, 20 * g_block_size_bytes, 10);
  //cExtTest.run_multi_block_test(0, 100000 * g_block_size_bytes, 4);
  //cExtTest.run_multi_block_test(0, 100000 * g_block_size_bytes, 1);
  //cExtTest.run_ftruncate_test(1 * g_block_size_bytes, 10 * g_block_size_bytes, 5);

  if (argc < 6) {
    cerr << "Usage: " << argv[0] << " INSERT_ORDER LOOKUP_ORDER" <<
     " BLOCK_CHUCK_SIZE INSERT_OUTPUT_FILE LOOKUP_OUTPUT_FILE" <<
     " TRUNCATE_OUTPUT_FILE" << endl;
    return 1;
  }

  int i = atoi(argv[1]);
  int l = atoi(argv[2]);
  int b = atoi(argv[3]);
  string insert_data_file(argv[4]);
  string lookup_data_file(argv[5]);
  //string truncate_data_file(argv[6]);

  if (b <= 0) {
    cerr << "ERROR: number of blocks (arg 3) must be positive, not " << b
      << "!" << endl;
    return 1;
  }

  cExtTest.initialize();

  printf("Insert: %s, Lookup: %s\n", SequenceTypeNames[i],
      SequenceTypeNames[l]);

  list<mlfs_lblk_t> insert = cExtTest.genLogicalBlockSequence(
      (SequenceType)i, 0, 100000, b);
  list<mlfs_lblk_t> lookup = cExtTest.genLogicalBlockSequence(
      (SequenceType)l, insert);

  tuple<double, double> res = cExtTest.run_multi_block_test(insert, lookup, b);

  {
    FILE* f = fopen(insert_data_file.c_str(), "a");
    assert(f);
    fprintf(f," ( %d, %.2f ) ", (3 * i) + l, get<0>(res) * 1000000.0);
    fclose(f);
  }
  {
    FILE* f = fopen(lookup_data_file.c_str(), "a");
    assert(f);
    fprintf(f," ( %d, %.2f ) ", (3 * i) + l, get<1>(res) * 1000000.0);
    fclose(f);
  }

  return 0;
}
