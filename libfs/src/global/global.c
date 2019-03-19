#include "global/global.h"

char pwd[MAX_PATH + 1];

indexing_choice_t g_idx_choice;

indexing_choice_t get_indexing_choice(void) {
    const char *env = getenv("MLFS_IDX_STRUCT");

    if (env != NULL && !strcmp(env, "EXTENT_TREES")) {
        printf("%s -> using API extent trees!\n", env);
        return EXTENT_TREES;
    } else if (env != NULL && !strcmp(env, "GLOBAL_HASH_TABLE")) {
        printf("%s -> using API global hash table!\n", env);
        return GLOBAL_HASH_TABLE;
    } else if (env != NULL && !strcmp(env, "LEVEL_HASH_TABLES")) {
        printf("%s -> using API level hashing, which is per-inode!\n", env);
        return LEVEL_HASH_TABLES;
    } else if (env != NULL && !strcmp(env, "GLOBAL_RADIX_TREE")) {
        printf("%s -> using API global radix tree!\n", env);
        return GLOBAL_RADIX_TREE;
    } else {
        printf("%s -> using Strata default indexing!\n", env);
        return NONE;
    }
}
