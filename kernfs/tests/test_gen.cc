#include "test_gen.h"

const char* SequenceTypeNames[] = { "sequential", "reverse", "random" };
const char* SequenceTypeAbbr[] = { "seq", "rev", "rand" };

using namespace std;

void TestGenerator::BaseInit() {
  if (done_init_) {
    return;
  }

#if 1
	int i;
	const char *perf_profile;

	g_ssd_dev = 2;
	g_hdd_dev = 3;
	g_log_dev = 4;

  cerr << "Start init" << endl;
#ifdef USE_SLAB
	mlfs_slab_init(3UL << 30);
#endif

	device_init();
  cerr << "Devices initialized." << endl;

	init_device_lru_list();

	shared_memory_init();
	cache_init(g_root_dev);

	for (i = 0; i < g_n_devices; i++)
		sb[i] = (super_block*)mlfs_zalloc(sizeof(struct super_block));

	read_superblock(g_root_dev);
	read_root_inode(g_root_dev);
	balloc_init(g_root_dev, sb[g_root_dev]);

#ifdef USE_SSD
	read_superblock(g_ssd_dev);
	balloc_init(g_ssd_dev, sb[g_ssd_dev]);
#endif
#ifdef USE_HDD
	read_superblock(g_hdd_dev);
	balloc_init(g_hdd_dev, sb[g_hdd_dev]);
#endif

	memset(&g_perf_stats, 0, sizeof(kernfs_stats_t));

	inode_version_table =
		(uint16_t *)mlfs_zalloc(sizeof(uint16_t) * NINODES);

	perf_profile = getenv("MLFS_PROFILE");

	if (perf_profile) {
		enable_perf_stats = 1;
  } else {
		enable_perf_stats = 0;
  }

	mlfs_debug("%s\n", "LIBFS is initialized");

  done_init_ = true;
#endif
}

vector<shared_ptr<inode_t>> TestGenerator::CreateInodes(
    uint32_t inum_start,
    size_t num) {

  assert(num > 0);

  BaseInit();

  vector<shared_ptr<inode_t>> inode_ptrs;

  cerr << "Begin init" << endl;

  for (int i = 0; i < num; ++i) {
    shared_ptr<inode_t> inode(ialloc(g_root_dev, T_FILE, i ));
    mlfs_assert(inode.get());
    cerr << "inode " << i << " allocated" << endl;

    struct mlfs_extent_header *ihdr;

    ihdr = ext_inode_hdr(&dev_handle_, inode.get());
    cerr << "header allocated" << endl;

    // First creation of dinode of file
    if (ihdr->eh_magic != MLFS_EXT_MAGIC) {
      mlfs_debug("create new inode %u\n", inode->inum);
      memset(inode->l1.i_data, 0, sizeof(uint32_t) * 15);
      cerr << "i_data set" << endl;
      mlfs_ext_tree_init(&dev_handle_, inode.get());
      cerr << "ext tree initialized" << endl;

      inode->i_writeback = NULL;
      memset(inode->i_uuid, 0x01 + i, sizeof(inode->i_uuid));
      inode->i_csum = mlfs_crc32c(~0, inode->i_uuid, sizeof(inode->i_uuid));
      inode->i_csum =
        mlfs_crc32c(inode->i_csum, &inode->inum, sizeof(inode->inum));
      inode->i_csum = mlfs_crc32c(inode->i_csum, &inode->i_generation,
        sizeof(inode->i_generation));

      write_ondisk_inode(g_root_dev, inode.get());
    }

    inode_ptrs.push_back(inode);
  }

  cout << "Init finished." << endl;
  mlfs_debug("%s\n", "LIBFS is initialized");

  return inode_ptrs;
}

std::vector<mlfs_lblk_t> TestGenerator::CreateBlockListSequential(
    size_t num, size_t skip) {
  assert(num);
  assert(skip);

  std::vector<mlfs_lblk_t> blocks;

  for (mlfs_fsblk_t b = 0; b < num; b += skip) {
    blocks.push_back(b);
  }

  return blocks;
}

std::list<TestCase> TestGenerator::CreateTests(
    SequenceType s,
    size_t num_files,
    size_t blocks_per_file,
    size_t block_range_size) {

  std::list<TestCase> cases;
#if 0
  assert(blocks_per_file % block_range_size == 0);
  assert(s == SEQUENTIAL || s == REVERSE || s == RANDOM);

  size_t ntests = num_files * (blocks_per_file / block_range_size);

  std::vector<mlfs_lblk_t> blocks = CreateBlockListSequential(blocks_per_file,
      block_range_size);


  if (s == REVERSE) {
    std::reverse(blocks.begin(), blocks.end());
  }

  for (uint32_t inum = 0; inum < num_files; ++inum) {
    if (s == RANDOM) {
      std::random_shuffle(blocks.begin(), blocks.end());
    }

    std::shared_ptr<inode_t> inode = CreateInode(inum);

    for (const mlfs_lblk_t& lblk : blocks) {
      std::shared_ptr<TestInput> in(new TestInput(inode, lblk));
      std::shared_ptr<TestOutput> out(new TestOutput());

      cases.emplace_back(in, out);
    }
  }

  assert(cases.size() == ntests);
#endif
  return cases;
}
