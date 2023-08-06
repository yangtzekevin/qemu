/*
 * ram colo related functions
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef QEMU_MIGRATION_RAM_COLO_H
#define QEMU_MIGRATION_RAM_COLO_H

#include "qemu/thread.h"

typedef struct ColoFlushParams {
    QemuThread thread;
    QemuSemaphore sem;
    QemuMutex mutex;
    bool quit;
    unsigned long thread_mask;
    unsigned long thread_bits;
} ColoFlushParams;

/* 4096 * 4k = 16m bytes chunk size */
#define COLO_FLUSH_CHUNK_SHIFT 12
#define COLO_FLUSH_CHUNK_SIZE (1ul << COLO_FLUSH_CHUNK_SHIFT)

void colo_flush_threads_init(void);
void colo_flush_threads_cleanup(void);
void colo_flush_threads_run(void);
void colo_flush_threads_wait(void);

#endif // QEMU_MIGRATION_RAM_COLO.H
