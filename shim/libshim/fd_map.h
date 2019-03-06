#ifndef __FD_MAP_H__
#define __FD_MAP_H__

#include <stdio.h>

extern int fd_map[1024];
extern char fn_map[1024][1024];

void init_maps();

#endif
