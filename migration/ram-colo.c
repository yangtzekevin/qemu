/*
 * ram colo related functions
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include "qemu/osdep.h"
#include "qemu/cutils.h"

#include "ram-colo.h"

#include "ram.h"
#include "options.h"

typedef struct FlushThreads {
    int num_threads;
    QemuSemaphore wait_sem;
    struct ColoFlushParams* threads;
} FlushThreads;

FlushThreads *colo_flush_threads;

static void *colo_flush_ram_cache_thread(void *opaque) {
    ColoFlushParams *thread = opaque;

    rcu_register_thread();
    while (true) {
        qemu_sem_wait(&thread->sem);

        qemu_mutex_lock(&thread->mutex);
        if (thread->quit) {
            qemu_sem_post(&colo_flush_threads->wait_sem);
            break;
        }
        qemu_mutex_unlock(&thread->mutex);

        qemu_sem_post(&colo_flush_threads->wait_sem);
    }

    return NULL;
}

void colo_flush_threads_run(void) {
    int num_threads = colo_flush_threads->num_threads;
    for (int n = 0; n < num_threads; n++) {
        struct ColoFlushParams *thread = &colo_flush_threads->threads[n];
        qemu_sem_post(&thread->sem);
    }

    for (int n = 0; n < num_threads; n++) {
        qemu_sem_wait(&colo_flush_threads->wait_sem);
    }
}

void colo_flush_threads_cleanup(void) {
    int num_threads = colo_flush_threads->num_threads;

    for (int n = 0; n < num_threads; n++) {
        ColoFlushParams *thread = &colo_flush_threads->threads[n];

        qemu_mutex_lock(&thread->mutex);
        thread->quit = true;
        qemu_mutex_unlock(&thread->mutex);
        qemu_sem_post(&thread->sem);
        qemu_sem_wait(&colo_flush_threads->wait_sem);

        qemu_thread_join(&thread->thread);
        qemu_sem_destroy(&thread->sem);
        qemu_mutex_destroy(&thread->mutex);
    }

    if (num_threads) {
        g_free(colo_flush_threads->threads);
    }

    g_free(colo_flush_threads);
}

void colo_flush_threads_init(void) {
    int num_threads = migrate_colo_flush_threads();
    unsigned long thread_mask = num_threads;
    thread_mask -= 1; // before: 0100 now: 0011
    thread_mask <<= COLO_FLUSH_CHUNK_SHIFT;

    colo_flush_threads = g_new0(FlushThreads, 1);
    colo_flush_threads->num_threads = num_threads;
    if (num_threads == 0) {
        return;
    }

    qemu_sem_init(&colo_flush_threads->wait_sem, 0);
    colo_flush_threads->threads = g_new0(ColoFlushParams, num_threads);

    for (int n = 0; n < num_threads; n++) {
        ColoFlushParams *thread = &colo_flush_threads->threads[n];
        qemu_sem_init(&thread->sem, 0);
        qemu_mutex_init(&thread->mutex);
        thread->quit = false;
        thread->thread_mask = thread_mask;
        thread->thread_bits = n << COLO_FLUSH_CHUNK_SHIFT;

        qemu_thread_create(&thread->thread, "flush thread",
                           colo_flush_ram_cache_thread, thread,
                           QEMU_THREAD_JOINABLE);
    }
}
