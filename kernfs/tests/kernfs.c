#include <stdio.h>
#include <sys/types.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include "kernfs_interface.h"

static inline void safe_exit() {
    shutdown_fs();
    remove(KERNFSPIDPATH);
    exit(0);
}

static void sig_handler(int signum) {
    printf("%s @ %s: SIGNAL is %d\n", __FILE__, __FUNCTION__, signum);
    switch (signum) {
        case SIGINT:
            printf("\tSIGINT, show stats\n");
            show_kernfs_stats();
            break;
        case SIGUSR2:
            printf("\tSIGUSR, show and reset stats\n");
            show_kernfs_stats();
            printf("\n\n\t\tNOW RESET\n\n");
            reset_kernfs_stats();
            break;
        case SIGQUIT:
            printf("\tSIGQUIT, show stats and shutdown\n");
            show_kernfs_stats();
            printf("%s @ %s: safe_exit...\n", __FILE__, __FUNCTION__);
            safe_exit();
        default:
            ;
    }
    printf("%s @ %s: SIGNAL handling complete\n", __FILE__, __FUNCTION__);
}

static void regist_sighandler() {
    struct sigaction new_action;
    new_action.sa_handler = sig_handler;
    sigemptyset(&new_action.sa_mask);
    new_action.sa_flags = 0;
    if (sigaction(SIGINT, &new_action, NULL) != 0) {
        perror("SIGINT error");
        exit(-1);
    }
    if(sigaction(SIGUSR2, &new_action, NULL) != 0) {
        perror("SIGUSR1 error");
        exit(-1);
    }
    if (sigaction(SIGQUIT, &new_action, NULL) != 0) {
        perror("SIGQUIT error");
        exit(-1);
    }
}

static void write_pid(char *path) {
    pid_t pid = getpid();
    FILE *f = fopen(path, "w");
    if (f == NULL) {
        perror("open pidfile failed");
        exit(-1);
    }
    fprintf(f, "%d\n", pid);
    fclose(f);
}

static void write_pid_callback(void) { write_pid(KERNFSPIDPATH); }

int main(void)
{
    regist_sighandler();
	printf("initialize file system\n");

	init_fs_callback(write_pid_callback);

    safe_exit();
}
