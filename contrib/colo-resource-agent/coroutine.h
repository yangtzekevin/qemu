/*
 * COLO background daemon coroutine core
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef COROUTINE_H
#define COROUTINE_H

#define CO co->

#define co_begin(yield_type, yield_ret) \
    unsigned int *coroutine_line = \
        &(coroutine->stack[coroutine->stack_index].line); \
    yield_type coroutine_yield_ret = (yield_ret); \
    switch(*coroutine_line) { case 0:;

#define co_end }

#define _co_yield(value) \
        do { \
            *coroutine_line=__LINE__; \
            return (value); case __LINE__:; \
        } while (0)

#define _co_yieldV \
        do { \
            *coroutine_line=__LINE__; \
            return; case __LINE__:; \
        } while (0)

typedef struct Coroutine Coroutine;

#endif /* COROUTINE_H */
