#include "fd_map.h"

int fd_map[1024];
char fn_map[1024][1024];

void init_maps() {
    printf("init to 0 %p\n", fd_map);
    for (int i = 0; i < 1024; ++i) {
        fd_map[i] = 0;
    }
    printf("init to 0 done\n");
    fflush(stdout);
}
