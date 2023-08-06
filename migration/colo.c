/*
 * COarse-grain LOck-stepping Virtual Machines for Non-stop Service (COLO)
 * (a.k.a. Fault Tolerance or Continuous Replication)
 *
 * Copyright (c) 2016 HUAWEI TECHNOLOGIES CO., LTD.
 * Copyright (c) 2016 FUJITSU LIMITED
 * Copyright (c) 2016 Intel Corporation
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or
 * later.  See the COPYING file in the top-level directory.
 */

#include "qemu/osdep.h"
#include "sysemu/sysemu.h"
#include "qapi/error.h"
#include "qapi/qapi-commands-migration.h"
#include "migration.h"
#include "qemu-file.h"
#include "savevm.h"
#include "migration/colo.h"
#include "block.h"
#include "io/channel-buffer.h"
#include "trace.h"
#include "qemu/error-report.h"
#include "qemu/main-loop.h"
#include "qemu/rcu.h"
#include "migration/failover.h"
#include "migration/ram.h"
#include "block/replication.h"
#include "net/colo-compare.h"
#include "net/colo.h"
#include "block/block.h"
#include "qapi/qapi-events-migration.h"
#include "sysemu/cpus.h"
#include "sysemu/runstate.h"
#include "net/filter.h"
#include "options.h"
#include "migration-stats.h"

static bool vmstate_loading;
static bool colo_running = false;

/* User need to know colo mode after COLO failover */
static COLOMode last_colo_mode;

#define COLO_BUFFER_BASE_SIZE (4 * 1024 * 1024)

bool migration_in_colo_state(void)
{
    MigrationState *s = migrate_get_current();

    return (s->state == MIGRATION_STATUS_COLO);
}

bool migration_incoming_in_colo_state(void)
{
    MigrationIncomingState *mis = migration_incoming_get_current();

    return mis && (mis->state == MIGRATION_STATUS_COLO);
}

static bool colo_runstate_is_stopped(void)
{
    return runstate_check(RUN_STATE_COLO) || !runstate_is_running();
}

static void _colo_checkpoint_notify(void *opaque)
{
    MigrationState *s = opaque;
    int64_t next_notify_time;

    qatomic_inc(&s->colo_checkpoint_request);
    qemu_event_set(&s->colo_checkpoint_event);
    s->colo_checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    next_notify_time = s->colo_checkpoint_time + migrate_checkpoint_delay();
    timer_mod(s->colo_delay_timer, next_notify_time);
}

void colo_checkpoint_notify(void)
{
    if (!colo_running) {
        return;
    }

    _colo_checkpoint_notify(migrate_get_current());
}

static void colo_dirty_check_notify(void *opaque)
{
    MigrationState *s = opaque;
    int64_t next_notify_time;

    qemu_event_set(&s->colo_checkpoint_event);
    next_notify_time = qemu_clock_get_ms(QEMU_CLOCK_HOST) +
                       s->parameters.x_dirty_check_delay;
    timer_mod(s->colo_dirty_check_timer, next_notify_time);
}

void colo_checkpoint_delay_set(void)
{
    MigrationState *s = migrate_get_current();

    if (migration_in_colo_state()) {
        _colo_checkpoint_notify(s);
        colo_dirty_check_notify(s);
    }
}

static void secondary_vm_do_failover(void)
{
/* COLO needs enable block-replication */
    int old_state;
    MigrationIncomingState *mis = migration_incoming_get_current();
    Error *local_err = NULL;

    /* Can not do failover during the process of VM's loading VMstate, Or
     * it will break the secondary VM.
     */
    if (vmstate_loading) {
        old_state = failover_set_state(FAILOVER_STATUS_ACTIVE,
                        FAILOVER_STATUS_RELAUNCH);
        if (old_state != FAILOVER_STATUS_ACTIVE) {
            error_report("Unknown error while do failover for secondary VM,"
                         "old_state: %s", FailoverStatus_str(old_state));
        }
        return;
    }

    migrate_set_state(&mis->state, MIGRATION_STATUS_COLO,
                      MIGRATION_STATUS_COMPLETED);

    replication_stop_all(true, &local_err);
    if (local_err) {
        error_report_err(local_err);
        local_err = NULL;
    }

    /* Notify all filters of all NIC to do checkpoint */
    colo_notify_filters_event(COLO_EVENT_FAILOVER, &local_err);
    if (local_err) {
        error_report_err(local_err);
    }

    if (!autostart) {
        error_report("\"-S\" qemu option will be ignored in secondary side");
        /* recover runstate to normal migration finish state */
        autostart = true;
    }
    /*
     * Make sure COLO incoming thread not block in recv or send,
     * If mis->from_src_file and mis->to_src_file use the same fd,
     * The second shutdown() will return -1, we ignore this value,
     * It is harmless.
     */
    if (mis->from_src_file) {
        qemu_file_shutdown(mis->from_src_file);
    }
    if (mis->to_src_file) {
        qemu_file_shutdown(mis->to_src_file);
    }

    old_state = failover_set_state(FAILOVER_STATUS_ACTIVE,
                                   FAILOVER_STATUS_COMPLETED);
    if (old_state != FAILOVER_STATUS_ACTIVE) {
        error_report("Incorrect state (%s) while doing failover for "
                     "secondary VM", FailoverStatus_str(old_state));
        return;
    }
    /* Notify COLO incoming thread that failover work is finished */
    qemu_sem_post(&mis->colo_incoming_sem);

    /* For Secondary VM, jump to incoming co */
    if (mis->colo_incoming_co) {
        qemu_coroutine_enter(mis->colo_incoming_co);
    }
}

static void primary_vm_do_failover(void)
{
    MigrationState *s = migrate_get_current();
    int old_state;
    Error *local_err = NULL;

    migrate_set_state(&s->state, MIGRATION_STATUS_COLO,
                      MIGRATION_STATUS_COMPLETED);
    /*
     * kick COLO thread which might wait at
     * qemu_sem_wait(&s->colo_checkpoint_sem).
     */
    _colo_checkpoint_notify(s);

    /*
     * Wake up COLO thread which may blocked in recv() or send(),
     * The s->rp_state.from_dst_file and s->to_dst_file may use the
     * same fd, but we still shutdown the fd for twice, it is harmless.
     */
    if (s->to_dst_file) {
        qemu_file_shutdown(s->to_dst_file);
    }
    if (s->rp_state.from_dst_file) {
        qemu_file_shutdown(s->rp_state.from_dst_file);
    }

    old_state = failover_set_state(FAILOVER_STATUS_ACTIVE,
                                   FAILOVER_STATUS_COMPLETED);
    if (old_state != FAILOVER_STATUS_ACTIVE) {
        error_report("Incorrect state (%s) while doing failover for Primary VM",
                     FailoverStatus_str(old_state));
        return;
    }

    replication_stop_all(true, &local_err);
    if (local_err) {
        error_report_err(local_err);
        local_err = NULL;
    }

    /* Notify COLO thread that failover work is finished */
    qemu_sem_post(&s->colo_exit_sem);
}

COLOMode get_colo_mode(void)
{
    if (migration_in_colo_state()) {
        return COLO_MODE_PRIMARY;
    } else if (migration_incoming_in_colo_state()) {
        return COLO_MODE_SECONDARY;
    } else {
        return COLO_MODE_NONE;
    }
}

void colo_do_failover(void)
{
    /* Make sure VM stopped while failover happened. */
    if (!colo_runstate_is_stopped()) {
        vm_stop_force_state(RUN_STATE_COLO);
    }

    switch (last_colo_mode = get_colo_mode()) {
    case COLO_MODE_PRIMARY:
        primary_vm_do_failover();
        break;
    case COLO_MODE_SECONDARY:
        secondary_vm_do_failover();
        break;
    default:
        error_report("colo_do_failover failed because the colo mode"
                     " could not be obtained");
    }
}

void qmp_xen_set_replication(bool enable, bool primary,
                             bool has_failover, bool failover,
                             Error **errp)
{
    ReplicationMode mode = primary ?
                           REPLICATION_MODE_PRIMARY :
                           REPLICATION_MODE_SECONDARY;

    if (has_failover && enable) {
        error_setg(errp, "Parameter 'failover' is only for"
                   " stopping replication");
        return;
    }

    if (enable) {
        replication_start_all(mode, errp);
    } else {
        if (!has_failover) {
            failover = NULL;
        }
        replication_stop_all(failover, failover ? NULL : errp);
    }
}

ReplicationStatus *qmp_query_xen_replication_status(Error **errp)
{
    Error *err = NULL;
    ReplicationStatus *s = g_new0(ReplicationStatus, 1);

    replication_get_error_all(&err);
    if (err) {
        s->error = true;
        s->desc = g_strdup(error_get_pretty(err));
    } else {
        s->error = false;
    }

    error_free(err);
    return s;
}

void qmp_xen_colo_do_checkpoint(Error **errp)
{
    Error *err = NULL;

    replication_do_checkpoint_all(&err);
    if (err) {
        error_propagate(errp, err);
        return;
    }
    /* Notify all filters of all NIC to do checkpoint */
    colo_notify_filters_event(COLO_EVENT_CHECKPOINT, errp);
}

COLOStatus *qmp_query_colo_status(Error **errp)
{
    COLOStatus *s = g_new0(COLOStatus, 1);

    s->mode = get_colo_mode();
    s->last_mode = last_colo_mode;

    switch (failover_get_state()) {
    case FAILOVER_STATUS_NONE:
        s->reason = COLO_EXIT_REASON_NONE;
        break;
    case FAILOVER_STATUS_COMPLETED:
        s->reason = COLO_EXIT_REASON_REQUEST;
        break;
    default:
        if (migration_in_colo_state()) {
            s->reason = COLO_EXIT_REASON_PROCESSING;
        } else {
            s->reason = COLO_EXIT_REASON_ERROR;
        }
    }

    return s;
}

static void colo_send_message(QEMUFile *f, COLOMessage msg,
                              Error **errp)
{
    int ret;

    if (msg >= COLO_MESSAGE__MAX) {
        error_setg(errp, "%s: Invalid message", __func__);
        return;
    }
    qemu_put_be32(f, msg);
    qemu_fflush(f);

    ret = qemu_file_get_error(f);
    if (ret < 0) {
        error_setg_errno(errp, -ret, "Can't send COLO message");
    }
    trace_colo_send_message(COLOMessage_str(msg));
}

static void colo_send_message_value(QEMUFile *f, COLOMessage msg,
                                    uint64_t value, Error **errp)
{
    Error *local_err = NULL;
    int ret;

    colo_send_message(f, msg, &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return;
    }
    qemu_put_be64(f, value);
    qemu_fflush(f);

    ret = qemu_file_get_error(f);
    if (ret < 0) {
        error_setg_errno(errp, -ret, "Failed to send value for message:%s",
                         COLOMessage_str(msg));
    }
}

static COLOMessage colo_receive_message(QEMUFile *f, Error **errp)
{
    COLOMessage msg;
    int ret;

    msg = qemu_get_be32(f);
    ret = qemu_file_get_error(f);
    if (ret < 0) {
        error_setg_errno(errp, -ret, "Can't receive COLO message");
        return msg;
    }
    if (msg >= COLO_MESSAGE__MAX) {
        error_setg(errp, "%s: Invalid message", __func__);
        return msg;
    }
    trace_colo_receive_message(COLOMessage_str(msg));
    return msg;
}

static void colo_receive_check_message(QEMUFile *f, COLOMessage expect_msg,
                                       Error **errp)
{
    COLOMessage msg;
    Error *local_err = NULL;

    msg = colo_receive_message(f, &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return;
    }
    if (msg != expect_msg) {
        error_setg(errp, "Unexpected COLO message %d, expected %d",
                          msg, expect_msg);
    }
}

static uint64_t colo_receive_message_value(QEMUFile *f, uint32_t expect_msg,
                                           Error **errp)
{
    Error *local_err = NULL;
    uint64_t value;
    int ret;

    colo_receive_check_message(f, expect_msg, &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return 0;
    }

    value = qemu_get_be64(f);
    ret = qemu_file_get_error(f);
    if (ret < 0) {
        error_setg_errno(errp, -ret, "Failed to get value for COLO message: %s",
                         COLOMessage_str(expect_msg));
    }
    return value;
}

static int colo_do_checkpoint_transaction(MigrationState *s,
                                          QIOChannelBuffer *bioc,
                                          QEMUFile *fb)
{
    Error *local_err = NULL;
    int ret = -1;
    uint64_t start = 0, total_start = 0, now = 0, total = 0, message = 0,
        prep = 0, replication = 0, memory = 0, vmstate = 0, apply_mem = 0,
        apply_dev = 0, net_notify = 0;

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    total_start = now;
    start = now;

    colo_send_message(s->to_dst_file, COLO_MESSAGE_CHECKPOINT_REQUEST,
                      &local_err);
    if (local_err) {
        goto out;
    }

    colo_receive_check_message(s->rp_state.from_dst_file,
                    COLO_MESSAGE_CHECKPOINT_REPLY, &local_err);
    if (local_err) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    start = now;

    /* Reset channel-buffer directly */
    qio_channel_io_seek(QIO_CHANNEL(bioc), 0, 0, NULL);
    bioc->usage = 0;

    qemu_mutex_lock_iothread();
    if (failover_get_state() != FAILOVER_STATUS_NONE) {
        qemu_mutex_unlock_iothread();
        goto out;
    }
    vm_stop_force_state(RUN_STATE_COLO);
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("run", "stop");
    /*
     * Failover request bh could be called after vm_stop_force_state(),
     * So we need check failover_request_is_active() again.
     */
    if (failover_get_state() != FAILOVER_STATUS_NONE) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    prep += now - start;
    start = now;

    qemu_mutex_lock_iothread();

    replication_do_checkpoint_all(&local_err);
    if (local_err) {
        qemu_mutex_unlock_iothread();
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    replication += now - start;
    start = now;

    /* Note: device state is saved into buffer */
    ret = qemu_save_device_state(fb);

    qemu_mutex_unlock_iothread();
    if (ret < 0) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    apply_dev += now - start;
    start = now;

    colo_send_message(s->to_dst_file, COLO_MESSAGE_VMSTATE_SEND, &local_err);
    if (local_err) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    start = now;

    if (migrate_auto_converge()) {
        mig_throttle_counter_reset();
    }
    /*
     * Only save VM's live state, which not including device state.
     * TODO: We may need a timeout mechanism to prevent COLO process
     * to be blocked here.
     */
    qemu_savevm_live_state(s->to_dst_file);

    qemu_fflush(fb);

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    memory += now - start;
    start = now;

    /*
     * We need the size of the VMstate data in Secondary side,
     * With which we can decide how much data should be read.
     */
    colo_send_message_value(s->to_dst_file, COLO_MESSAGE_VMSTATE_SIZE,
                            bioc->usage, &local_err);
    if (local_err) {
        goto out;
    }

    qemu_put_buffer(s->to_dst_file, bioc->data, bioc->usage);
    qemu_fflush(s->to_dst_file);
    ret = qemu_file_get_error(s->to_dst_file);
    if (ret < 0) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    vmstate += now - start;
    start = now;

    colo_receive_check_message(s->rp_state.from_dst_file,
                       COLO_MESSAGE_VMSTATE_RECEIVED, &local_err);
    if (local_err) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    start = now;

    qemu_event_reset(&s->colo_checkpoint_event);
    colo_notify_compares_event(NULL, COLO_EVENT_CHECKPOINT, &local_err);
    if (local_err) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    net_notify += now - start;
    start = now;

    colo_receive_check_message(s->rp_state.from_dst_file,
                       COLO_MESSAGE_VMSTATE_LOADED, &local_err);
    if (local_err) {
        goto out;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    start = now;

    ret = 0;

    qemu_mutex_lock_iothread();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    prep += now - start;
    total = now - total_start;
    trace_colo_checkpoint_stats(total, message, prep, replication,
                                memory, vmstate, apply_mem, apply_dev,
                                net_notify);

out:
    if (local_err) {
        error_report_err(local_err);
    }

    s->colo_last_transferred_bytes =
            migration_transferred_bytes(s->to_dst_file);
    return ret;
}

typedef enum ColoAction {
    ACTION_NONE,
    ACTION_BACKGROUND,
    ACTION_CHECKPOINT
} ColoAction;

static ColoAction colo_dirty_check(MigrationState *s)
{
    uint64_t pending_bytes, pend_pre, pend_post, transferred_bytes;

    qemu_savevm_state_pending_exact(&pend_pre, &pend_post);
    pending_bytes = pend_pre + pend_post;

    transferred_bytes = migration_transferred_bytes(s->to_dst_file) -
            s->colo_last_transferred_bytes;

    trace_colo_need_migrate_ram_background(pending_bytes, transferred_bytes);

    if (pending_bytes + transferred_bytes >= migrate_dirty_checkpoint()) {
        return ACTION_CHECKPOINT;
    } else if (pending_bytes >= migrate_dirty_threshold()) {
        return ACTION_BACKGROUND;
    } else {
        return ACTION_NONE;
    }
}

static void colo_process_checkpoint(MigrationState *s)
{
    QIOChannelBuffer *bioc;
    QEMUFile *fb = NULL;
    Error *local_err = NULL;
    int ret;

    if (get_colo_mode() != COLO_MODE_PRIMARY) {
        error_report("COLO mode must be COLO_MODE_PRIMARY");
        return;
    }

    failover_init_state();

    s->rp_state.from_dst_file = qemu_file_get_return_path(s->to_dst_file);
    if (!s->rp_state.from_dst_file) {
        error_report("Open QEMUFile from_dst_file failed");
        goto out;
    }
    qemu_file_set_delay(s->to_dst_file, false);

    colo_running = true;

    /*
     * Wait for Secondary finish loading VM states and enter COLO
     * restore.
     */
    colo_receive_check_message(s->rp_state.from_dst_file,
                       COLO_MESSAGE_CHECKPOINT_READY, &local_err);
    if (local_err) {
        goto out;
    }
    bioc = qio_channel_buffer_new(COLO_BUFFER_BASE_SIZE);
    fb = qemu_file_new_output(QIO_CHANNEL(bioc));
    object_unref(OBJECT(bioc));

    qemu_mutex_lock_iothread();
    replication_start_all(REPLICATION_MODE_PRIMARY, &local_err);
    if (local_err) {
        qemu_mutex_unlock_iothread();
        goto out;
    }

    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    timer_mod(s->colo_delay_timer, qemu_clock_get_ms(QEMU_CLOCK_HOST) +
              migrate_checkpoint_delay());
    timer_mod(s->colo_dirty_check_timer, qemu_clock_get_ms(QEMU_CLOCK_HOST) +
              migrate_dirty_check_delay());

    while (s->state == MIGRATION_STATUS_COLO) {
        if (failover_get_state() != FAILOVER_STATUS_NONE) {
            error_report("failover request");
            goto out;
        }

        qemu_event_wait(&s->colo_checkpoint_event);

        if (s->state != MIGRATION_STATUS_COLO) {
            goto out;
        }
        if (qatomic_xchg(&s->colo_checkpoint_request, 0)) {
            /* start a colo checkpoint */
            ret = colo_do_checkpoint_transaction(s, bioc, fb);
            if (ret < 0) {
                goto out;
            }
        } else {
            ColoAction action = colo_dirty_check(s);

            if (action == ACTION_CHECKPOINT) {
                qemu_event_reset(&s->colo_checkpoint_event);
                ret = colo_do_checkpoint_transaction(s, bioc, fb);
                if (ret < 0) {
                    goto out;
                }
            } else if (action == ACTION_BACKGROUND) {
                colo_send_message(s->to_dst_file,
                                  COLO_MESSAGE_MIGRATE_RAM_BACKGROUND,
                                  &local_err);
                if (local_err) {
                    goto out;
                }

                qemu_savevm_state_iterate(s->to_dst_file, false);
                qemu_put_byte(s->to_dst_file, QEMU_VM_EOF);
                ret = qemu_file_get_error(s->to_dst_file);
                if (ret < 0) {
                    error_setg_errno(&local_err, -ret,
                        "Failed to send dirty pages backgroud");
                    goto out;
                }
            } else {
                qemu_event_reset(&s->colo_checkpoint_event);
            }
        }
    }

out:
    /* Throw the unreported error message after exited from loop */
    if (local_err) {
        error_report_err(local_err);
    }

    if (fb) {
        qemu_fclose(fb);
    }

    /*
     * There are only two reasons we can get here, some error happened
     * or the user triggered failover.
     */
    switch (failover_get_state()) {
    case FAILOVER_STATUS_COMPLETED:
        qapi_event_send_colo_exit(COLO_MODE_PRIMARY,
                                  COLO_EXIT_REASON_REQUEST);
        break;
    default:
        qapi_event_send_colo_exit(COLO_MODE_PRIMARY,
                                  COLO_EXIT_REASON_ERROR);
    }

    /* Hope this not to be too long to wait here */
    qemu_sem_wait(&s->colo_exit_sem);
    qemu_sem_destroy(&s->colo_exit_sem);

    /*
     * It is safe to unregister notifier after failover finished.
     * Besides, colo_delay_timer and colo_checkpoint_sem can't be
     * released before unregister notifier, or there will be use-after-free
     * error.
     */
    colo_running = false;
    timer_free(s->colo_delay_timer);
    timer_free(s->colo_dirty_check_timer);
    qemu_event_destroy(&s->colo_checkpoint_event);

    /*
     * Must be called after failover BH is completed,
     * Or the failover BH may shutdown the wrong fd that
     * re-used by other threads after we release here.
     */
    if (s->rp_state.from_dst_file) {
        qemu_fclose(s->rp_state.from_dst_file);
        s->rp_state.from_dst_file = NULL;
    }
}

void migrate_start_colo_process(MigrationState *s)
{
    qemu_mutex_unlock_iothread();
    qemu_event_init(&s->colo_checkpoint_event, false);
    s->colo_checkpoint_request = 0;
    s->colo_delay_timer =  timer_new_ms(QEMU_CLOCK_HOST,
                                _colo_checkpoint_notify, s);
    s->colo_dirty_check_timer = timer_new_ms(QEMU_CLOCK_HOST,
                                    colo_dirty_check_notify, s);
    s->colo_last_transferred_bytes =
            migration_transferred_bytes(s->to_dst_file);

    qemu_sem_init(&s->colo_exit_sem, 0);
    colo_process_checkpoint(s);
    qemu_mutex_lock_iothread();
}

static void colo_incoming_process_checkpoint(MigrationIncomingState *mis,
                      QEMUFile *fb, QIOChannelBuffer *bioc, Error **errp)
{
    uint64_t total_size;
    uint64_t value;
    Error *local_err = NULL;
    int ret;
    uint64_t start = 0, total_start = 0, now = 0, total = 0, message = 0,
        prep = 0, replication = 0, memory = 0, vmstate = 0,
        apply_mem = 0, apply_dev = 0, net_notify = 0, start_mem = 0;

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    total_start = now;
    start = now;

    qemu_mutex_lock_iothread();
    vm_stop_force_state(RUN_STATE_COLO);
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("run", "stop");

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    prep += now - start;
    start = now;

    /* FIXME: This is unnecessary for periodic checkpoint mode */
    colo_send_message(mis->to_src_file, COLO_MESSAGE_CHECKPOINT_REPLY,
                 &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return;
    }

    colo_receive_check_message(mis->from_src_file,
                       COLO_MESSAGE_VMSTATE_SEND, &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    start = now;

    qemu_mutex_lock_iothread();
    cpu_synchronize_all_states();
    ret = qemu_loadvm_state_main(mis->from_src_file, mis);
    qemu_mutex_unlock_iothread();

    if (ret < 0) {
        error_setg(errp, "Load VM's live state (ram) error");
        return;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    memory += now - start;
    start = now;

    value = colo_receive_message_value(mis->from_src_file,
                             COLO_MESSAGE_VMSTATE_SIZE, &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    start = now;

    /*
     * Read VM device state data into channel buffer,
     * It's better to re-use the memory allocated.
     * Here we need to handle the channel buffer directly.
     */
    if (value > bioc->capacity) {
        bioc->capacity = value;
        bioc->data = g_realloc(bioc->data, bioc->capacity);
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    apply_dev += now - start;
    start = now;

    total_size = qemu_get_buffer(mis->from_src_file, bioc->data, value);
    if (total_size != value) {
        error_setg(errp, "Got %" PRIu64 " VMState data, less than expected"
                    " %" PRIu64, total_size, value);
        return;
    }
    bioc->usage = total_size;
    qio_channel_io_seek(QIO_CHANNEL(bioc), 0, 0, NULL);

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    vmstate += now - start;
    start = now;

    colo_send_message(mis->to_src_file, COLO_MESSAGE_VMSTATE_RECEIVED,
                 &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    start = now;
    start_mem = now;

    qemu_mutex_lock_iothread();
    vmstate_loading = true;
    colo_flush_ram_cache_begin();

    replication_get_error_all(&local_err);
    if (local_err) {
        colo_flush_ram_cache_wait();
        error_propagate(errp, local_err);
        vmstate_loading = false;
        qemu_mutex_unlock_iothread();
        return;
    }

    /* discard colo disk buffer */
    replication_do_checkpoint_all(&local_err);
    if (local_err) {
        colo_flush_ram_cache_wait();
        error_propagate(errp, local_err);
        vmstate_loading = false;
        qemu_mutex_unlock_iothread();
        return;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    replication += now - start;
    start = now;

    /* Notify all filters of all NIC to do checkpoint */
    colo_notify_filters_event(COLO_EVENT_CHECKPOINT, &local_err);

    if (local_err) {
        error_propagate(errp, local_err);
        vmstate_loading = false;
        qemu_mutex_unlock_iothread();
        return;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    net_notify += now - start;
    start = now;

    colo_flush_ram_cache_wait();

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    apply_mem += now - start_mem;
    start = now;

    ret = qemu_load_device_state(fb);
    if (ret < 0) {
        error_setg(errp, "COLO: load device state failed");
        vmstate_loading = false;
        qemu_mutex_unlock_iothread();
        return;
    }

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    apply_dev += now - start;
    start = now;

    vmstate_loading = false;
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    prep += now - start;
    start = now;

    if (failover_get_state() == FAILOVER_STATUS_RELAUNCH) {
        return;
    }

    colo_send_message(mis->to_src_file, COLO_MESSAGE_VMSTATE_LOADED,
                 &local_err);
    error_propagate(errp, local_err);

    now = qemu_clock_get_ms(QEMU_CLOCK_REALTIME);
    message += now - start;
    total = now - total_start;
    trace_colo_checkpoint_stats(total, message, prep, replication,
                                memory, vmstate, apply_mem, apply_dev,
                                net_notify);
}

static void colo_wait_handle_message(MigrationIncomingState *mis,
                QEMUFile *fb, QIOChannelBuffer *bioc, Error **errp)
{
    COLOMessage msg;
    Error *local_err = NULL;

    msg = colo_receive_message(mis->from_src_file, &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        return;
    }

    switch (msg) {
    case COLO_MESSAGE_CHECKPOINT_REQUEST:
        colo_incoming_process_checkpoint(mis, fb, bioc, errp);
        break;
    case COLO_MESSAGE_MIGRATE_RAM_BACKGROUND:
        if (qemu_loadvm_state_main(mis->from_src_file, mis) < 0) {
            error_setg(errp, "Load ram background failed");
        }
        break;
    default:
        error_setg(errp, "Got unknown COLO message: %d", msg);
        break;
    }
}

void colo_shutdown(void)
{
    MigrationIncomingState *mis = NULL;
    MigrationState *s = NULL;

    switch (get_colo_mode()) {
    case COLO_MODE_PRIMARY:
        s = migrate_get_current();
        qemu_event_set(&s->colo_checkpoint_event);
        qemu_sem_post(&s->colo_exit_sem);
        break;
    case COLO_MODE_SECONDARY:
        mis = migration_incoming_get_current();
        qemu_sem_post(&mis->colo_incoming_sem);
        break;
    default:
        break;
    }
}

static void *colo_process_incoming_thread(void *opaque)
{
    MigrationIncomingState *mis = opaque;
    QEMUFile *fb = NULL;
    QIOChannelBuffer *bioc = NULL; /* Cache incoming device state */
    Error *local_err = NULL;

    rcu_register_thread();
    qemu_sem_init(&mis->colo_incoming_sem, 0);

    migrate_set_state(&mis->state, MIGRATION_STATUS_ACTIVE,
                      MIGRATION_STATUS_COLO);

    if (get_colo_mode() != COLO_MODE_SECONDARY) {
        error_report("COLO mode must be COLO_MODE_SECONDARY");
        return NULL;
    }

    failover_init_state();

    mis->to_src_file = qemu_file_get_return_path(mis->from_src_file);
    if (!mis->to_src_file) {
        error_report("COLO incoming thread: Open QEMUFile to_src_file failed");
        goto out;
    }
    /*
     * Note: the communication between Primary side and Secondary side
     * should be sequential, we set the fd to unblocked in migration incoming
     * coroutine, and here we are in the COLO incoming thread, so it is ok to
     * set the fd back to blocked.
     */
    qemu_file_set_blocking(mis->from_src_file, true);
    qemu_file_set_delay(mis->from_src_file, false);

    colo_incoming_start_dirty_log();

    bioc = qio_channel_buffer_new(COLO_BUFFER_BASE_SIZE);
    fb = qemu_file_new_input(QIO_CHANNEL(bioc));
    object_unref(OBJECT(bioc));

    qemu_mutex_lock_iothread();
    replication_start_all(REPLICATION_MODE_SECONDARY, &local_err);
    if (local_err) {
        qemu_mutex_unlock_iothread();
        goto out;
    }
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    colo_send_message(mis->to_src_file, COLO_MESSAGE_CHECKPOINT_READY,
                      &local_err);
    if (local_err) {
        goto out;
    }

    while (mis->state == MIGRATION_STATUS_COLO) {
        colo_wait_handle_message(mis, fb, bioc, &local_err);
        if (local_err) {
            error_report_err(local_err);
            break;
        }

        if (failover_get_state() == FAILOVER_STATUS_RELAUNCH) {
            failover_set_state(FAILOVER_STATUS_RELAUNCH,
                            FAILOVER_STATUS_NONE);
            failover_request_active(NULL);
            break;
        }

        if (failover_get_state() != FAILOVER_STATUS_NONE) {
            error_report("failover request");
            break;
        }
    }

out:
    /*
     * There are only two reasons we can get here, some error happened
     * or the user triggered failover.
     */
    switch (failover_get_state()) {
    case FAILOVER_STATUS_COMPLETED:
        qapi_event_send_colo_exit(COLO_MODE_SECONDARY,
                                  COLO_EXIT_REASON_REQUEST);
        break;
    default:
        qapi_event_send_colo_exit(COLO_MODE_SECONDARY,
                                  COLO_EXIT_REASON_ERROR);
    }

    if (fb) {
        qemu_fclose(fb);
    }

    /* Hope this not to be too long to loop here */
    qemu_sem_wait(&mis->colo_incoming_sem);
    qemu_sem_destroy(&mis->colo_incoming_sem);

    rcu_unregister_thread();
    return NULL;
}

int coroutine_fn colo_incoming_co(void)
{
    MigrationIncomingState *mis = migration_incoming_get_current();
    Error *local_err = NULL;
    QemuThread th;

    assert(qemu_mutex_iothread_locked());

    if (!migrate_colo()) {
        return 0;
    }

    /* Make sure all file formats throw away their mutable metadata */
    bdrv_activate_all(&local_err);
    if (local_err) {
        error_report_err(local_err);
        return -EINVAL;
    }

    qemu_thread_create(&th, "COLO incoming", colo_process_incoming_thread,
                       mis, QEMU_THREAD_JOINABLE);

    mis->colo_incoming_co = qemu_coroutine_self();
    qemu_coroutine_yield();
    mis->colo_incoming_co = NULL;

    qemu_mutex_unlock_iothread();
    /* Wait checkpoint incoming thread exit before free resource */
    qemu_thread_join(&th);
    qemu_mutex_lock_iothread();

    /* We hold the global iothread lock, so it is safe here */
    colo_release_ram_cache();

    return 0;
}
