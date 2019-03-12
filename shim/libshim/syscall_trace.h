#ifndef _SYSCALL_TRACE_H_
#define _SYSCALL_TRACE_H_

#include <stdio.h>
#include "macromap.h"

void do_trace(const char *name, int ret, int args, ...);

//#define SYS_TRACE

#ifdef SYS_TRACE
#define syscall_trace(name, ret, args, ...) \
	do_trace(name, ret, args, __VA_ARGS__);
#else
#define syscall_trace(name, ret, args, ...)
#endif

#define MAP_TUPLE_SEP(m, sep, term, first, second, ...)        \
  m(first, second)                                             \
  IF_ELSE(HAS_ARGS(__VA_ARGS__))(                              \
    sep DEFER2(_MAP_TUPLE_SEP)()(m, sep, term, __VA_ARGS__)    \
  )(                                                           \
    term                                                       \
  )
#define _MAP_TUPLE_SEP() MAP_TUPLE_SEP
#define MAP_TUPLE_COMMA(m, first, second, ...)                 \
  m(first, second)                                             \
  IF_ELSE(HAS_ARGS(__VA_ARGS__))(                              \
    , DEFER2(_MAP_TUPLE_COMMA)()(m, __VA_ARGS__)               \
  )(                                                           \
    /* Do nothing, just terminate */                           \
  )
#define _MAP_TUPLE_COMMA() MAP_TUPLE_COMMA
#define EXPAND_FMT(fmt, var) #var "=" fmt
#define DO_EXPAND_FMT(...) MAP_TUPLE_SEP(EXPAND_FMT, ", ", "\n", __VA_ARGS__)
#define EXPAND_OPT(fmt, var, ...) var
#define DO_EXPAND_OPT(...) MAP_TUPLE_COMMA(EXPAND_OPT, __VA_ARGS__)

#define always_warn(fmt, ...) do {\
    pid_t tid = syscall(SYS_gettid);\
    fprintf(stderr, "(%d)[%s:%d] " fmt, tid, __func__, __LINE__, __VA_ARGS__); \
    } while(0)
#define syscall_abort(fmt, ...) do { always_warn(fmt, __VA_ARGS__); abort(); } while (0)
#ifdef SYS_TRACE
#define syscall_warn(fmt, ...) always_warn(fmt, __VA_ARGS__)
#define syscall_dump(...) syscall_warn(EVAL(DO_EXPAND_FMT(__VA_ARGS__)), EVAL(DO_EXPAND_OPT(__VA_ARGS__)))
#else
#define syscall_warn(fmt, ...)
#define syscall_dump(...)
#endif

#endif
