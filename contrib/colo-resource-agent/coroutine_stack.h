/*
 * COLO background daemon coroutine stack management
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef COROUTINE_STACK_H
#define COROUTINE_STACK_H

#include <assert.h>

#include <glib-2.0/glib.h>

#include "daemon.h"
#include "coutil.h"
#include "client.h"
#include "qmp.h"

typedef struct CoroutineStack {
    unsigned int line;
    union {
        ColodClientCo clientco;
        CoroutineUtilCo utilco;
        ColodQmpCo qmpco;
        ColodCo colodco;
        ColodArrayCo colodarrayco;
    } data;
} CoroutineStack;

typedef struct CoroutineCallback {
    GSourceFunc plain;
    GIOFunc iofunc;
} CoroutineCallback;

typedef struct Coroutine {
    int quit;
    int yield;
    void *yield_value;
    unsigned int stack_index;
    CoroutineStack stack[16];
    CoroutineCallback cb;
} Coroutine;

#define coroutine_stack_size(co) (sizeof(co->stack)/sizeof(co->stack[0]))

#define co_stack(field) (&(coroutine->stack[coroutine->stack_index].data.field))

#define co_yield(value) \
    do { \
        coroutine->yield = 1; \
        coroutine->yield_value = (void *) (value); \
        _co_yield(coroutine_yield_ret); \
    } while (0)

#define co_yield_int(value) \
    co_yield(GINT_TO_POINTER(value))

#define co_enter(ret, coroutine, func, ...) \
    do { \
        assert(!(coroutine)->quit); \
        (coroutine)->yield = 0; \
        assert((coroutine)->stack_index == 0); \
        (ret) = func((coroutine), ##__VA_ARGS__); \
        assert((coroutine)->stack_index == 0); \
        if (!(coroutine)->yield) { \
            (coroutine)->stack[(coroutine)->stack_index].line = 0; \
            (coroutine)->quit = 1; \
        } \
    } while(0)

#define co_call_co(ret, func, ...) \
    while(1) { \
        coroutine->stack_index++; \
        assert(coroutine->stack_index < coroutine_stack_size(coroutine)); \
        (ret) = func(coroutine, ##__VA_ARGS__); \
        coroutine->stack_index--; \
        if (coroutine->yield) { \
            _co_yield(coroutine_yield_ret); \
        } else { \
            coroutine->stack[coroutine->stack_index + 1].line = 0; \
            break; \
        } \
    }

#endif // COROUTINE_STACK_H
