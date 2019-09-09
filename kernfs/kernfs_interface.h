#ifndef _KERN_FS_INTERFACE_
#define _KERN_FS_INTERFACE_
#define KERNFSPIDPATH "/tmp/kernfs_ian.pid"

#ifdef __cplusplus
extern "C" {
#endif

void init_fs(void);
void init_fs_callback(void (*callback_fn)(void));
void reset_kernfs_stats(void);
void show_kernfs_stats(void);
void shutdown_fs(void);

#ifdef __cplusplus
}
#endif

#endif
