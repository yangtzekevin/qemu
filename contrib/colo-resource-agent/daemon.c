/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <syslog.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <assert.h>

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-unix.h>

#include <corosync/cpg.h>
#include <corosync/corotypes.h>

#include "base_types.h"
#include "daemon.h"
#include "coroutine.h"
#include "coutil.h"
#include "json_util.h"
#include "util.h"
#include "client.h"
#include "qmp.h"
#include "coroutine_stack.h"

static FILE *trace = NULL;
static gboolean do_syslog = FALSE;

enum ColodEvent {
    EVENT_NONE = 0,
    EVENT_FAILED,
    EVENT_QEMU_QUIT,
    EVENT_PEER_FAILOVER,
    EVENT_FAILOVER_SYNC,
    EVENT_PEER_FAILED,
    EVENT_FAILOVER_WIN,
    EVENT_QUIT,
    EVENT_AUTOQUIT,
    EVENT_YELLOW,
    EVENT_START_MIGRATION,
    EVENT_DID_FAILOVER
};

static const gchar *event_str(ColodEvent event) {
    switch (event) {
        case EVENT_NONE: return "EVENT_NONE";
        case EVENT_FAILED: return "EVENT_FAILED";
        case EVENT_QEMU_QUIT: return "EVENT_QEMU_QUIT";
        case EVENT_PEER_FAILOVER: return "EVENT_PEER_FAILOVER";
        case EVENT_FAILOVER_SYNC: return "EVENT_FAILOVER_SYNC";
        case EVENT_PEER_FAILED: return "EVENT_PEER_FAILED";
        case EVENT_FAILOVER_WIN: return "EVENT_FAILOVER_WIN";
        case EVENT_QUIT: return "EVENT_QUIT";
        case EVENT_AUTOQUIT: return "EVENT_AUTOQUIT";
        case EVENT_YELLOW: return "EVENT_YELLOW";
        case EVENT_START_MIGRATION: return "EVENT_START_MIGRATION";
        case EVENT_DID_FAILOVER: return "EVENT_DID_FAILOVER";
    }
    abort();
}

static gboolean event_escalate(ColodEvent event) {
    switch (event) {
        case EVENT_NONE:
        case EVENT_FAILED:
        case EVENT_QEMU_QUIT:
        case EVENT_PEER_FAILOVER:
        case EVENT_QUIT:
        case EVENT_AUTOQUIT:
        case EVENT_YELLOW:
        case EVENT_START_MIGRATION:
        case EVENT_DID_FAILOVER:
            return TRUE;
        break;

        default:
            return FALSE;
        break;
    }
}

static gboolean event_critical(ColodEvent event) {
    switch (event) {
        case EVENT_NONE:
        case EVENT_FAILOVER_WIN:
        case EVENT_YELLOW:
        case EVENT_START_MIGRATION:
        case EVENT_DID_FAILOVER:
            return FALSE;
        break;

        default:
            return TRUE;
        break;
    }
}

static gboolean event_failed(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED:
        case EVENT_QEMU_QUIT:
        case EVENT_PEER_FAILOVER:
            return TRUE;
        break;

        default:
            return FALSE;
        break;
    }
}

static gboolean event_failover(ColodEvent event) {
    return event == EVENT_FAILOVER_SYNC || event == EVENT_PEER_FAILED;
}

static gboolean colod_event_pending(ColodContext *ctx) {
    return !queue_empty(&ctx->events) || !queue_empty(&ctx->critical_events);
}

#define colod_event_queue(ctx, event, reason) \
    _colod_event_queue((ctx), (event), (reason), __func__, __LINE__)
static void _colod_event_queue(ColodContext *ctx, ColodEvent event,
                               const gchar *reason, const gchar *func,
                               int line) {
    ColodQueue *queue;

    colod_trace("%s:%u: queued %s (%s)\n", func, line, event_str(event),
                reason);

    if (event_critical(event)) {
        queue = &ctx->critical_events;
    } else {
        queue = &ctx->events;
    }

    if (queue_empty(queue) && ctx->main_coroutine) {
        colod_trace("%s:%u: Waking main coroutine\n", __func__, __LINE__);
        g_idle_add(ctx->main_coroutine->cb.plain, ctx->main_coroutine);
    }

    if (!queue_empty(queue)) {
        // ratelimit
        if (queue_peek(queue) == event) {
            colod_trace("%s:%u: Ratelimiting events\n", __func__, __LINE__);
            return;
        }
    }

    queue_add(queue, event);
    assert(colod_event_pending(ctx));
}

#define colod_event_wait(result, ctx) \
    co_call_co(result, _colod_event_wait, ctx, __func__, __LINE__)
static ColodEvent _colod_event_wait(Coroutine *coroutine, ColodContext *ctx,
                                    const gchar *func, int line) {
    ColodQueue *queue = &ctx->events;

    if (!colod_event_pending(ctx)) {
        coroutine->yield = TRUE;
        coroutine->yield_value = GINT_TO_POINTER(G_SOURCE_REMOVE);
        return EVENT_FAILED;
    }

    if (!queue_empty(&ctx->critical_events)) {
        queue = &ctx->critical_events;
    }

    ColodEvent event = queue_remove(queue);
    colod_trace("%s:%u: got %s\n", func, line, event_str(event));
    return event;
}

static gboolean colod_critical_pending(ColodContext *ctx) {
    return !queue_empty(&ctx->critical_events);
}

#define colod_qmp_event_wait_co(result, ctx, timeout, event, errp) \
    co_call_co(result, _colod_qmp_event_wait_co, ctx, timeout, event, errp)
static int _colod_qmp_event_wait_co(Coroutine *coroutine, ColodContext *ctx,
                                    guint timeout, const gchar* match,
                                    GError **errp) {
    int ret;
    GError *local_errp = NULL;

    while (TRUE) {
        ret = _qmp_wait_event_co(coroutine, ctx->qmp, timeout, match,
                                 &local_errp);
        if (coroutine->yield) {
            return -1;
        }
        if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
            assert(colod_event_pending(ctx));
            if (!colod_critical_pending(ctx)) {
                g_error_free(local_errp);
                local_errp = NULL;
                continue;
            }
            g_propagate_error(errp, local_errp);
            return ret;
        } else if (ret < 0) {
            g_propagate_error(errp, local_errp);
            return ret;
        }

        break;
    }

    return ret;
}

void colod_trace(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    if (trace) {
        vfprintf(trace, fmt, args);
        fflush(trace);
    }

    va_end(args);
}

void colod_syslog(int pri, const char *fmt, ...) {
    va_list args;

    if (trace) {
        va_start(args, fmt);
        vfprintf(trace, fmt, args);
        fwrite("\n", 1, 1, trace);
        fflush(trace);
        va_end(args);
    }

    va_start(args, fmt);
    if (do_syslog) {
        vsyslog(pri, fmt, args);
    } else {
        vfprintf(stderr, fmt, args);
        fwrite("\n", 1, 1, stderr);
    }
    va_end(args);
}

void colod_set_migration_commands(ColodContext *ctx, JsonNode *commands) {
    if (ctx->migration_commands) {
        json_node_unref(ctx->migration_commands);
    }
    ctx->migration_commands = json_node_ref(commands);
}

void colod_set_primary_commands(ColodContext *ctx, JsonNode *commands) {
    if (ctx->failover_primary_commands) {
        json_node_unref(ctx->failover_primary_commands);
    }
    ctx->failover_primary_commands = json_node_ref(commands);
}

void colod_set_secondary_commands(ColodContext *ctx, JsonNode *commands) {
    if (ctx->failover_secondary_commands) {
        json_node_unref(ctx->failover_secondary_commands);
    }
    ctx->failover_secondary_commands = json_node_ref(commands);
}

int _colod_yank_co(Coroutine *coroutine, ColodContext *ctx, GError **errp) {
    int ret;
    GError *local_errp = NULL;

    ret = _qmp_yank_co(coroutine, ctx->qmp, &local_errp);
    if (coroutine->yield) {
        return -1;
    }
    if (ret < 0) {
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
    } else {
        qmp_clear_yank(ctx->qmp);
        colod_event_queue(ctx, EVENT_FAILOVER_SYNC, "did yank");
    }

    return ret;
}

static void colod_watchdog_refresh(ColodWatchdog *state);
ColodQmpResult *_colod_execute_nocheck_co(Coroutine *coroutine,
                                          ColodContext *ctx,
                                          GError **errp,
                                          const gchar *command) {
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    colod_watchdog_refresh(ctx->watchdog);

    result = _qmp_execute_nocheck_co(coroutine, ctx->qmp, &local_errp, command);
    if (coroutine->yield) {
        return NULL;
    }
    if (!result) {
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return NULL;
    }

    ret = qmp_get_error(ctx->qmp, &local_errp);
    if (ret < 0) {
        qmp_result_free(result);
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return NULL;
    }

    if (qmp_get_yank(ctx->qmp)) {
        qmp_clear_yank(ctx->qmp);
        colod_event_queue(ctx, EVENT_FAILOVER_SYNC, "did yank");
    }

    return result;
}

ColodQmpResult *_colod_execute_co(Coroutine *coroutine,
                                  ColodContext *ctx,
                                  GError **errp,
                                  const gchar *command) {
    ColodQmpResult *result;

    result = _colod_execute_nocheck_co(coroutine, ctx, errp, command);
    if (coroutine->yield) {
        return NULL;
    }
    if (!result) {
        return NULL;
    }
    if (has_member(result->json_root, "error")) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP,
                    "qmp command returned error: %s %s",
                    command, result->line);
        qmp_result_free(result);
        return NULL;
    }

    return result;
}

#define colod_execute_array(ret, ctx, array, ignore_errors, errp) \
    co_call_co((ret), _colod_execute_array_co, (ctx), (array), \
               (ignore_errors), (errp))
static int _colod_execute_array_co(Coroutine *coroutine, ColodContext *ctx,
                                   JsonNode *array_node, gboolean ignore_errors,
                                   GError **errp) {
    ColodArrayCo *co = co_stack(colodarrayco);
    int ret = 0;
    GError *local_errp = NULL;

    co_begin(int, -1);

    assert(!errp || !*errp);
    assert(JSON_NODE_HOLDS_ARRAY(array_node));

    CO array = json_node_get_array(array_node);
    CO count = json_array_get_length(CO array);
    for (CO i = 0; CO i < CO count; CO i++) {
        JsonNode *node = json_array_get_element(CO array, CO i);
        assert(node);

        gchar *tmp = json_to_string(node, FALSE);
        CO line = g_strdup_printf("%s\n", tmp);
        g_free(tmp);

        ColodQmpResult *result;
        colod_execute_co(result, ctx, &local_errp, CO line);
        if (ignore_errors &&
                g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
            colod_syslog(LOG_WARNING, "Ignoring qmp error: %s",
                         local_errp->message);
            g_error_free(local_errp);
            local_errp = NULL;
        } else if (!result) {
            g_propagate_error(errp, local_errp);
            g_free(CO line);
            ret = -1;
            break;
        }
        qmp_result_free(result);
    }

    co_end;

    return ret;
}

static gboolean qemu_runnng(const gchar *status) {
    return !strcmp(status, "running")
            || !strcmp(status, "finish-migrate")
            || !strcmp(status, "colo")
            || !strcmp(status, "prelaunch")
            || !strcmp(status, "paused");
}

#define qemu_query_status_co(result, qmp, primary, replication, errp) \
    co_call_co((result), _qemu_query_status_co, (qmp), (primary), \
               (replication), (errp))
static int _qemu_query_status_co(Coroutine *coroutine, ColodContext *ctx,
                                 gboolean *primary, gboolean *replication,
                                 GError **errp) {
    ColodCo *co = co_stack(colodco);

    co_begin(int, -1);

    colod_execute_co(CO qemu_status, ctx, errp,
                   "{'execute': 'query-status'}\n");
    if (!CO qemu_status) {
        return -1;
    }

    colod_execute_co(CO colo_status, ctx, errp,
                   "{'execute': 'query-colo-status'}\n");
    if (!CO colo_status) {
        qmp_result_free(CO qemu_status);
        return -1;
    }

    co_end;

    const gchar *status, *colo_mode, *colo_reason;
    status = get_member_member_str(CO qemu_status->json_root,
                                   "return", "status");
    colo_mode = get_member_member_str(CO colo_status->json_root,
                                      "return", "mode");
    colo_reason = get_member_member_str(CO colo_status->json_root,
                                        "return", "reason");
    if (!status || !colo_mode || !colo_reason) {
        colod_error_set(errp, "Failed to parse query-status "
                        "and query-colo-status output");
        qmp_result_free(CO qemu_status);
        qmp_result_free(CO colo_status);
        return -1;
    }

    if (!strcmp(status, "inmigrate") || !strcmp(status, "shutdown")) {
        *primary = FALSE;
        *replication = FALSE;
    } else if (qemu_runnng(status) && !strcmp(colo_mode, "none")
               && (!strcmp(colo_reason, "none")
                   || !strcmp(colo_reason, "request"))) {
        *primary = TRUE;
        *replication = FALSE;
    } else if (qemu_runnng(status) &&!strcmp(colo_mode, "primary")) {
        *primary = TRUE;
        *replication = TRUE;
    } else if (qemu_runnng(status) && !strcmp(colo_mode, "secondary")) {
        *primary = FALSE;
        *replication = TRUE;
    } else {
        colod_error_set(errp, "Unknown qemu status: %s, %s",
                        CO qemu_status->line, CO colo_status->line);
        qmp_result_free(CO qemu_status);
        qmp_result_free(CO colo_status);
        return -1;
    }

    qmp_result_free(CO qemu_status);
    qmp_result_free(CO colo_status);
    return 0;
}

int _colod_check_health_co(Coroutine *coroutine, ColodContext *ctx,
                           GError **errp) {
    gboolean primary;
    gboolean replication;
    int ret;
    GError *local_errp = NULL;

    ret = _qemu_query_status_co(coroutine, ctx, &primary, &replication,
                                &local_errp);
    if (coroutine->yield) {
        return -1;
    }
    if (ret < 0) {
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return -1;
    }

    if (!ctx->transitioning &&
            (ctx->primary != primary || ctx->replication != replication)) {
        colod_error_set(&local_errp, "qemu status mismatch: (%s, %s)"
                        " Expected: (%s, %s)",
                        bool_to_json(primary), bool_to_json(replication),
                        bool_to_json(ctx->primary),
                        bool_to_json(ctx->replication));
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return -1;
    }

    return 0;
}

int colod_start_migration(ColodContext *ctx) {
    if (ctx->pending_action || ctx->replication) {
        return -1;
    }

    colod_event_queue(ctx, EVENT_START_MIGRATION, "client request");
    return 0;
}

static void colod_watchdog_inc_inhibit(ColodWatchdog *state);
void colod_autoquit(ColodContext *ctx) {
    colod_watchdog_inc_inhibit(ctx->watchdog);
    colod_event_queue(ctx, EVENT_AUTOQUIT, "client request");
}

void colod_qemu_failed(ColodContext *ctx) {
    colod_event_queue(ctx, EVENT_FAILED, "?");
}

static gboolean colod_hup_cb(G_GNUC_UNUSED GIOChannel *channel,
                             G_GNUC_UNUSED GIOCondition revents,
                             gpointer data) {
    ColodContext *ctx = data;

    log_error("qemu quit");
    ctx->qemu_quit = TRUE;
    colod_event_queue(ctx, EVENT_QEMU_QUIT, "qmp hup");
    return G_SOURCE_REMOVE;
}

static Coroutine *colod_raise_timeout_coroutine(ColodContext *ctx);
static void colod_qmp_event_cb(gpointer data, ColodQmpResult *result) {
    ColodContext *ctx = data;
    const gchar *event;

    event = get_member_str(result->json_root, "event");

    if (!strcmp(event, "QUORUM_REPORT_BAD")) {
        const gchar *node, *type;
        node = get_member_member_str(result->json_root, "data", "node-name");
        type = get_member_member_str(result->json_root, "data", "type");

        if (!strcmp(node, "nbd0")) {
            if (!!strcmp(type, "read")) {
                colod_event_queue(ctx, EVENT_FAILOVER_SYNC,
                                  "nbd write/flush error");
            }
        } else {
            if (!!strcmp(type, "read")) {
                colod_event_queue(ctx, EVENT_YELLOW,
                                  "local disk write/flush error");
            }
        }
    } else if (!strcmp(event, "COLO_EXIT")) {
        const gchar *reason;
        reason = get_member_member_str(result->json_root, "data", "reason");

        if (!strcmp(reason, "error")) {
            colod_event_queue(ctx, EVENT_FAILOVER_SYNC, "COLO_EXIT");
        }
    } else if (!strcmp(event, "RESET")) {
        colod_raise_timeout_coroutine(ctx);
    }
}

typedef struct ColodWatchdog {
    Coroutine coroutine;
    ColodContext *ctx;
    guint interval;
    guint timer_id;
    guint inhibit;
    gboolean quit;
} ColodWatchdog;

static void colod_watchdog_refresh(ColodWatchdog *state) {
    if (state->timer_id) {
        g_source_remove(state->timer_id);
        state->timer_id = g_timeout_add_full(G_PRIORITY_LOW,
                                             state->interval,
                                             state->coroutine.cb.plain,
                                             &state->coroutine, NULL);
    }
}

static void colod_watchdog_inc_inhibit(ColodWatchdog *state) {
    state->inhibit++;
}

static void colod_watchdog_dec_inhibit(ColodWatchdog *state) {
    assert(state->inhibit != 0);

    state->inhibit--;
}

static void colod_watchdog_event_cb(gpointer data,
                                    G_GNUC_UNUSED ColodQmpResult *result) {
    ColodWatchdog *state = data;
    colod_watchdog_refresh(state);
}

static gboolean _colod_watchdog_co(Coroutine *coroutine);
static gboolean colod_watchdog_co(gpointer data) {
    ColodWatchdog *state = data;
    Coroutine *coroutine = &state->coroutine;
    gboolean ret;

    co_enter(ret, coroutine, _colod_watchdog_co);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    g_source_remove_by_user_data(coroutine);
    assert(!g_source_remove_by_user_data(coroutine));
    return ret;
}

static gboolean colod_watchdog_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return colod_watchdog_co(data);
}

static gboolean _colod_watchdog_co(Coroutine *coroutine) {
    ColodWatchdog *state = (ColodWatchdog *) coroutine;
    int ret;
    GError *local_errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    while (!state->quit) {
        state->timer_id = g_timeout_add_full(G_PRIORITY_LOW,
                                             state->interval,
                                             coroutine->cb.plain,
                                             coroutine, NULL);
        co_yield_int(G_SOURCE_REMOVE);
        if (state->quit) {
            break;
        }
        state->timer_id = 0;

        if (state->inhibit || state->ctx->failed) {
            continue;
        }

        colod_check_health_co(ret, state->ctx, &local_errp);
        if (ret < 0) {
            log_error_fmt("colod check health: %s", local_errp->message);
            g_error_free(local_errp);
            local_errp = NULL;
            return G_SOURCE_REMOVE;
        }
    }

    co_end;

    return G_SOURCE_REMOVE;
}

static void colo_watchdog_free(ColodWatchdog *state) {

    if (!state->interval) {
        g_free(state);
        return;
    }

    state->quit = TRUE;

    qmp_del_notify_event(state->ctx->qmp, colod_watchdog_event_cb, state);

    if (state->timer_id) {
        g_source_remove(state->timer_id);
        state->timer_id = 0;
        g_idle_add(colod_watchdog_co, &state->coroutine);
    }

    while (!state->coroutine.quit) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }

    g_free(state);
}

static ColodWatchdog *colod_watchdog_new(ColodContext *ctx) {
    ColodWatchdog *state;
    Coroutine *coroutine;

    state = g_new0(ColodWatchdog, 1);
    coroutine = &state->coroutine;
    coroutine->cb.plain = colod_watchdog_co;
    coroutine->cb.iofunc = colod_watchdog_co_wrap;
    state->ctx = ctx;
    state->interval = ctx->watchdog_interval;

    if (state->interval) {
        g_idle_add(colod_watchdog_co, coroutine);
        qmp_add_notify_event(ctx->qmp, colod_watchdog_event_cb, state);
    }
    return state;
}

typedef struct ColodRaiseCoroutine {
    Coroutine coroutine;
    ColodContext *ctx;
} ColodRaiseCoroutine;

static gboolean _colod_raise_timeout_co(Coroutine *coroutine,
                                        ColodContext *ctx);
static gboolean colod_raise_timeout_co(gpointer data) {
    ColodRaiseCoroutine *raiseco = data;
    Coroutine *coroutine = &raiseco->coroutine;
    ColodContext *ctx = raiseco->ctx;
    gboolean ret;

    co_enter(ret, coroutine, _colod_raise_timeout_co, ctx);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    qmp_set_timeout(ctx->qmp, ctx->qmp_timeout_low);

    g_source_remove_by_user_data(coroutine);
    assert(!g_source_remove_by_user_data(coroutine));
    g_free(ctx->raise_timeout_coroutine);
    ctx->raise_timeout_coroutine = NULL;
    return ret;
}

static gboolean colod_raise_timeout_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return colod_raise_timeout_co(data);
}

static gboolean _colod_raise_timeout_co(Coroutine *coroutine,
                                        ColodContext *ctx) {
    int ret;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    qmp_wait_event_co(ret, ctx->qmp, 0, "{'event': 'STOP'}", NULL);
    if (ret < 0) {
        return G_SOURCE_REMOVE;
    }

    qmp_wait_event_co(ret, ctx->qmp, 0, "{'event': 'RESUME'}", NULL);
    if (ret < 0) {
        return G_SOURCE_REMOVE;
    }

    co_end;

    return G_SOURCE_REMOVE;
}

static void colod_raise_timeout_coroutine_free(ColodContext *ctx) {
    if (!ctx->raise_timeout_coroutine) {
        return;
    }

    g_idle_add(colod_raise_timeout_co, ctx->raise_timeout_coroutine);

    while (ctx->raise_timeout_coroutine) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }
}

static Coroutine *colod_raise_timeout_coroutine(ColodContext *ctx) {
    ColodRaiseCoroutine *raiseco;
    Coroutine *coroutine;

    if (ctx->raise_timeout_coroutine) {
        return NULL;
    }

    qmp_set_timeout(ctx->qmp, ctx->qmp_timeout_high);

    raiseco = g_new0(ColodRaiseCoroutine, 1);
    coroutine = &raiseco->coroutine;
    coroutine->cb.plain = colod_raise_timeout_co;
    coroutine->cb.iofunc = colod_raise_timeout_co_wrap;
    raiseco->ctx = ctx;
    ctx->raise_timeout_coroutine = coroutine;

    g_idle_add(colod_raise_timeout_co, raiseco);
    return coroutine;
}

typedef enum ColodMessage {
    MESSAGE_FAILOVER,
    MESSAGE_FAILED
} ColodMessage;

static void colod_cpg_deliver(cpg_handle_t handle,
                              G_GNUC_UNUSED const struct cpg_name *group_name,
                              uint32_t nodeid,
                              G_GNUC_UNUSED uint32_t pid,
                              void *msg,
                              size_t msg_len) {
    ColodContext *ctx;
    uint32_t conv;
    uint32_t myid;

    cpg_context_get(handle, (void**) &ctx);
    cpg_local_get(handle, &myid);

    if (msg_len != sizeof(conv)) {
        log_error_fmt("Got message of invalid length %zu", msg_len);
        return;
    }
    conv = ntohl((*(uint32_t*)msg));

    switch (conv) {
        case MESSAGE_FAILOVER:
            if (nodeid == myid) {
                colod_event_queue(ctx, EVENT_FAILOVER_WIN, "");
            } else {
                colod_event_queue(ctx, EVENT_PEER_FAILOVER, "");
            }
        break;

        case MESSAGE_FAILED:
            if (nodeid != myid) {
                log_error("Peer failed");
                ctx->peer_failed = TRUE;
                colod_event_queue(ctx, EVENT_PEER_FAILED, "got MESSAGE_FAILED");
            }
        break;
    }
}

static void colod_cpg_confchg(cpg_handle_t handle,
    G_GNUC_UNUSED const struct cpg_name *group_name,
    G_GNUC_UNUSED const struct cpg_address *member_list,
    G_GNUC_UNUSED size_t member_list_entries,
    G_GNUC_UNUSED const struct cpg_address *left_list,
    size_t left_list_entries,
    G_GNUC_UNUSED const struct cpg_address *joined_list,
    G_GNUC_UNUSED size_t joined_list_entries) {
    ColodContext *ctx;

    cpg_context_get(handle, (void**) &ctx);

    if (left_list_entries) {
        log_error("Peer failed");
        ctx->peer_failed = TRUE;
        colod_event_queue(ctx, EVENT_PEER_FAILED, "peer left cpg group");
    }
}

static void colod_cpg_totem_confchg(G_GNUC_UNUSED cpg_handle_t handle,
                                    G_GNUC_UNUSED struct cpg_ring_id ring_id,
                                    G_GNUC_UNUSED uint32_t member_list_entries,
                                    G_GNUC_UNUSED const uint32_t *member_list) {

}

static gboolean colod_cpg_readable(G_GNUC_UNUSED gint fd,
                                   G_GNUC_UNUSED GIOCondition events,
                                   gpointer data) {
    ColodContext *ctx = data;
    cpg_dispatch(ctx->cpg_handle, CS_DISPATCH_ALL);
    return G_SOURCE_CONTINUE;
}

static void colod_cpg_send(ColodContext *ctx, uint32_t message) {
    struct iovec vec;
    uint32_t conv = htonl(message);

    if (ctx->disable_cpg) {
        if (message == MESSAGE_FAILOVER) {
            colod_event_queue(ctx, EVENT_FAILOVER_WIN,
                              "running without corosync");
        }
        return;
    }

    vec.iov_len = sizeof(conv);
    vec.iov_base = &conv;
    cpg_mcast_joined(ctx->cpg_handle, CPG_TYPE_AGREED, &vec, 1);
}

#define colod_stop_co(result, ctx, errp) \
    co_call_co((result), _colod_stop_co, ctx, errp)
static int _colod_stop_co(Coroutine *coroutine, ColodContext *ctx,
                          GError **errp) {
    ColodQmpResult *result;

    result = _colod_execute_co(coroutine, ctx, errp,
                               "{'execute': 'stop'}\n");
    if (coroutine->yield) {
        return -1;
    }
    if (!result) {
        return -1;
    }
    qmp_result_free(result);

    return 0;
}

#define colod_failover_co(result, ctx) \
    co_call_co((result), _colod_failover_co, ctx)
static ColodEvent _colod_failover_co(Coroutine *coroutine, ColodContext *ctx) {
    ColodCo *co = co_stack(colodco);
    int ret;
    GError *local_errp = NULL;

    co_begin(ColodEvent, EVENT_FAILED);

    qmp_yank_co(ret, ctx->qmp, &local_errp);
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return EVENT_FAILED;
    }

    if (ctx->primary) {
        CO commands = ctx->failover_primary_commands;
    } else {
        CO commands = ctx->failover_secondary_commands;
    }
    ctx->transitioning = TRUE;
    colod_execute_array(ret, ctx, CO commands, TRUE, &local_errp);
    ctx->transitioning = FALSE;
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return EVENT_FAILED;
    }

    co_end;

    return EVENT_DID_FAILOVER;
}

#define colod_failover_sync_co(result, ctx) \
    co_call_co((result), _colod_failover_sync_co, ctx)
static ColodEvent _colod_failover_sync_co(Coroutine *coroutine,
                                          ColodContext *ctx) {

    co_begin(ColodEvent, EVENT_FAILED);

    colod_cpg_send(ctx, MESSAGE_FAILOVER);

    while (TRUE) {
        ColodEvent event;
        colod_event_wait(event, ctx);
        if (event == EVENT_FAILOVER_WIN) {
            break;
        } else if (event == EVENT_PEER_FAILED) {
            break;
        } else if (event_critical(event) && event_escalate(event)) {
            return event;
        }
    }

    ColodEvent event;
    colod_failover_co(event, ctx);
    return event;

    co_end;

    return EVENT_FAILED;
}

#define colod_start_migration_co(result, ctx) \
    co_call_co((result), _colod_start_migration_co, (ctx));
static ColodEvent _colod_start_migration_co(Coroutine *coroutine,
                                            ColodContext *ctx) {
    ColodCo *co = co_stack(colodco);
    ColodQmpState *qmp = ctx->qmp;
    ColodQmpResult *qmp_result;
    ColodEvent result;
    int ret;
    GError *local_errp = NULL;

    co_begin(ColodEvent, EVENT_FAILED);
    colod_execute_co(qmp_result, ctx, &local_errp,
                     "{'execute': 'migrate-set-capabilities',"
                     "'arguments': {'capabilities': ["
                        "{'capability': 'events', 'state': true },"
                        "{'capability': 'pause-before-switchover', 'state': true}]}}\n");
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
        goto qmp_error;
    } else if (!qmp_result) {
        goto qemu_failed;
    }
    qmp_result_free(qmp_result);
    if (colod_critical_pending(ctx)) {
        goto handle_event;
    }

    colod_qmp_event_wait_co(ret, ctx, 5*60*1000,
                            "{'event': 'MIGRATION',"
                            " 'data': {'status': 'pre-switchover'}}",
                            &local_errp);
    if (ret < 0) {
        goto qmp_error;
    }

    colod_execute_array(ret, ctx, ctx->migration_commands, FALSE,
                        &local_errp);
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
        goto qmp_error;
    } else if (ret < 0) {
        goto qemu_failed;
    }
    if (colod_critical_pending(ctx)) {
        goto handle_event;
    }

    colod_raise_timeout_coroutine(ctx);

    colod_execute_co(qmp_result, ctx, &local_errp,
                     "{'execute': 'migrate-continue',"
                     "'arguments': {'state': 'pre-switchover'}}\n");
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto qmp_error;
    } else if (!qmp_result) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto qemu_failed;
    }
    qmp_result_free(qmp_result);
    if (colod_critical_pending(ctx)) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto handle_event;
    }

    ctx->transitioning = TRUE;
    colod_qmp_event_wait_co(ret, ctx, 10000,
                            "{'event': 'MIGRATION',"
                            " 'data': {'status': 'colo'}}",
                            &local_errp);
    ctx->transitioning = FALSE;
    if (ret < 0) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto qmp_error;
    }

    return EVENT_NONE;

qmp_error:
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
        g_error_free(local_errp);
        local_errp = NULL;
        assert(colod_critical_pending(ctx));
        colod_event_wait(CO event, ctx);
        if (event_failover(CO event)) {
            goto failover;
        } else {
            return CO event;
        }
    } else {
        log_error(local_errp->message);
        g_error_free(local_errp);
    }
    CO event = EVENT_PEER_FAILED;
    goto failover;

qemu_failed:
    log_error(local_errp->message);
    g_error_free(local_errp);
    return EVENT_FAILED;

handle_event:
    assert(colod_critical_pending(ctx));
    colod_event_wait(CO event, ctx);
    if (event_failover(CO event)) {
        goto failover;
    } else {
        return CO event;
    }

failover:
    colod_execute_co(qmp_result, ctx, &local_errp,
                     "{'execute': 'migrate_cancel'}\n");
    if (!qmp_result) {
        goto qemu_failed;
    }
    qmp_result_free(qmp_result);
    assert(event_failover(CO event));
    if (CO event == EVENT_FAILOVER_SYNC) {
        colod_failover_sync_co(result, ctx);
    } else {
        colod_failover_co(result, ctx);
    }
    return result;

    co_end;

    return EVENT_FAILED;
}

#define colod_replication_wait_co(result, ctx) \
    co_call_co((result), _colod_replication_wait_co, (ctx))
static ColodEvent _colod_replication_wait_co(Coroutine *coroutine,
                                             ColodContext *ctx) {
    ColodEvent event;
    int ret;
    ColodQmpResult *qmp_result;
    GError *local_errp = NULL;

    co_begin(ColodEvent, EVENT_FAILED);

    colod_execute_co(qmp_result, ctx, &local_errp,
                         "{'execute': 'migrate-set-capabilities',"
                         "'arguments': {'capabilities': ["
                            "{'capability': 'events', 'state': true }]}}\n");
    if (!qmp_result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return EVENT_FAILED;
    }
    qmp_result_free(qmp_result);

    while (TRUE) {
        ctx->transitioning = TRUE;
        colod_qmp_event_wait_co(ret, ctx, 0, "{'event': 'RESUME'}",
                                &local_errp);
        ctx->transitioning = FALSE;
        if (ret < 0) {
            g_error_free(local_errp);
            assert(colod_event_pending(ctx));
            colod_event_wait(event, ctx);
            if (event_critical(event) && event_escalate(event)) {
                return event;
            }
            continue;
        }
        break;
    }

    colod_raise_timeout_coroutine(ctx);

    co_end;

    return EVENT_NONE;
}

#define colod_replication_running_co(result, ctx) \
    co_call_co((result), _colod_replication_running_co, (ctx))
static ColodEvent _colod_replication_running_co(Coroutine *coroutine,
                                                ColodContext *ctx) {
    co_begin(ColodEvent, EVENT_FAILED);

    while (TRUE) {
        ColodEvent event;
        colod_event_wait(event, ctx);
        if (event == EVENT_FAILOVER_SYNC) {
            colod_failover_sync_co(event, ctx);
            return event;
        } else if (event == EVENT_PEER_FAILED) {
            colod_failover_co(event, ctx);
            return event;
        } else if (event_critical(event) && event_escalate(event)) {
            return event;
        }
    }

    co_end;

    return EVENT_FAILED;
}

void colod_quit(ColodContext *ctx) {
    g_main_loop_quit(ctx->mainloop);
}

void do_autoquit(ColodContext *ctx) {
    client_listener_free(ctx->listener);
    exit(EXIT_SUCCESS);
}

typedef struct ColodMainCoroutine {
    Coroutine coroutine;
    ColodContext *ctx;
} ColodMainCoroutine;

static gboolean _colod_main_co(Coroutine *coroutine, ColodContext *ctx);
static gboolean colod_main_co(gpointer data) {
    ColodMainCoroutine *mainco = data;
    Coroutine *coroutine = data;
    ColodContext *ctx = mainco->ctx;
    gboolean ret;

    co_enter(ret, coroutine, _colod_main_co, ctx);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    g_source_remove_by_user_data(coroutine);
    assert(!g_source_remove_by_user_data(coroutine));
    ctx->main_coroutine = NULL;
    g_free(coroutine);
    return ret;
}

static gboolean colod_main_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return colod_main_co(data);
}

static gboolean _colod_main_co(Coroutine *coroutine, ColodContext *ctx) {
    ColodEvent event = EVENT_NONE;
    int ret;
    GError *local_errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    if (!ctx->primary) {
        colod_syslog(LOG_INFO, "starting in secondary mode");
        while (TRUE) {
            colod_replication_wait_co(event, ctx);
            assert(event_escalate(event));
            if (event_failed(event)) {
                goto failed;
            } else if (event == EVENT_QUIT) {
                return G_SOURCE_REMOVE;
            } else if (event == EVENT_AUTOQUIT) {
                goto autoquit;
            } else if (event == EVENT_DID_FAILOVER) {
                break;
            }
            ctx->replication = TRUE;

            colod_replication_running_co(event, ctx);
            assert(event_escalate(event));
            assert(event != EVENT_NONE);
            if (event_failed(event)) {
                goto failed;
            } else if (event == EVENT_QUIT) {
                return G_SOURCE_REMOVE;
            } else if (event == EVENT_AUTOQUIT) {
                goto autoquit;
            } else if (event == EVENT_DID_FAILOVER) {
                break;
            } else {
                abort();
            }
        }
    } else {
        colod_syslog(LOG_INFO, "starting in primary mode");
    }

    // Now running primary standalone
    ctx->primary = TRUE;
    ctx->replication = FALSE;

    while (TRUE) {
        colod_event_wait(event, ctx);
        if (event == EVENT_START_MIGRATION) {
            ctx->pending_action = TRUE;
            colod_start_migration_co(event, ctx);
            assert(event_escalate(event));
            ctx->pending_action = FALSE;
            if (event_failed(event)) {
                goto failed;
            } else if (event == EVENT_QUIT) {
                return G_SOURCE_REMOVE;
            } else if (event == EVENT_AUTOQUIT) {
                goto autoquit;
            } else if (event == EVENT_DID_FAILOVER) {
                continue;
            }
            ctx->replication = TRUE;

            colod_replication_running_co(event, ctx);
            assert(event_escalate(event));
            assert(event != EVENT_NONE);
            if (event_failed(event)) {
                 goto failed;
             } else if (event == EVENT_QUIT) {
                 return G_SOURCE_REMOVE;
             } else if (event == EVENT_AUTOQUIT) {
                 goto autoquit;
             } else if (event == EVENT_DID_FAILOVER) {
                ctx->replication = FALSE;
                continue;
             } else {
                 abort();
             }
        } else if (event_failed(event)) {
            if (event != EVENT_PEER_FAILOVER) {
                goto failed;
            }
        } else if (event == EVENT_QUIT) {
            return G_SOURCE_REMOVE;
        } else if (event == EVENT_AUTOQUIT) {
            goto autoquit;
        }
    }

failed:
    qmp_set_timeout(ctx->qmp, ctx->qmp_timeout_low);
    ret = qmp_get_error(ctx->qmp, &local_errp);
    if (ret < 0) {
        log_error_fmt("qemu failed: %s", local_errp->message);
        g_error_free(local_errp);
        local_errp = NULL;
    }

    ctx->failed = TRUE;
    colod_cpg_send(ctx, MESSAGE_FAILED);

    if (event == EVENT_NONE) {
        log_error("Failed with EVENT_NONE");
    }
    if (event == EVENT_PEER_FAILOVER) {
        ctx->peer_failover = TRUE;
    }
    if (event != EVENT_QEMU_QUIT) {
        colod_stop_co(ret, ctx, &local_errp);
        if (ret < 0) {
            if (event == EVENT_PEER_FAILOVER) {
                log_error_fmt("Failed to stop qemu in response to "
                              "peer failover: %s", local_errp->message);
            }
            g_error_free(local_errp);
            local_errp = NULL;
        }
    }

    while (TRUE) {
        ColodEvent event;
        colod_event_wait(event, ctx);
        if (event == EVENT_PEER_FAILOVER) {
            ctx->peer_failover = TRUE;
        } else if (event == EVENT_QUIT) {
            return G_SOURCE_REMOVE;
        } else if (event == EVENT_AUTOQUIT) {
            if (ctx->qemu_quit) {
                do_autoquit(ctx);
            } else {
                goto autoquit;
            }
        }
    }

autoquit:
    ctx->failed = TRUE;
    colod_cpg_send(ctx, MESSAGE_FAILED);

    while (TRUE) {
        ColodEvent event;
        colod_event_wait(event, ctx);
        if (event == EVENT_PEER_FAILOVER) {
            ctx->peer_failover = TRUE;
        } else if (event == EVENT_QUIT) {
            return G_SOURCE_REMOVE;
        } else if (event == EVENT_QEMU_QUIT) {
            do_autoquit(ctx);
        }
    }

    co_end;

    return G_SOURCE_REMOVE;
}

static Coroutine *colod_main_coroutine(ColodContext *ctx) {
    ColodMainCoroutine *mainco;
    Coroutine *coroutine;

    assert(!ctx->main_coroutine);

    mainco = g_new0(ColodMainCoroutine, 1);
    coroutine = &mainco->coroutine;
    coroutine->cb.plain = colod_main_co;
    coroutine->cb.iofunc = colod_main_co_wrap;
    mainco->ctx = ctx;
    ctx->main_coroutine = coroutine;

    g_idle_add(colod_main_co, mainco);
    return coroutine;
}

static void colod_main_free(ColodContext *ctx) {
    colod_event_queue(ctx, EVENT_QUIT, "teardown");

    while (ctx->main_coroutine) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }
}

static void colod_mainloop(ColodContext *ctx) {
    GError *local_errp = NULL;

    // g_main_context_default creates the global context on demand
    ctx->mainctx = g_main_context_default();
    ctx->mainloop = g_main_loop_new(ctx->mainctx, FALSE);

    ctx->qmp = qmp_new(ctx->qmp1_fd, ctx->qmp2_fd, ctx->qmp_timeout_low,
                       &local_errp);
    if (local_errp) {
        colod_syslog(LOG_ERR, "Failed to initialize qmp: %s",
                     local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    ctx->listener = client_listener_new(ctx->mngmt_listen_fd, ctx);
    ctx->watchdog = colod_watchdog_new(ctx);
    colod_main_coroutine(ctx);
    qmp_add_notify_event(ctx->qmp, colod_qmp_event_cb, ctx);
    qmp_hup_source(ctx->qmp, colod_hup_cb, ctx);
    if (!ctx->disable_cpg) {
        ctx->cpg_source_id = g_unix_fd_add(ctx->cpg_fd, G_IO_IN | G_IO_HUP,
                                           colod_cpg_readable, ctx);
    }

    g_main_loop_run(ctx->mainloop);
    g_main_loop_unref(ctx->mainloop);
    ctx->mainloop = NULL;

    if (ctx->cpg_source_id) {
        g_source_remove(ctx->cpg_source_id);
    }
    qmp_del_notify_event(ctx->qmp, colod_qmp_event_cb, ctx);
    colod_raise_timeout_coroutine_free(ctx);
    colod_main_free(ctx);
    colo_watchdog_free(ctx->watchdog);
    client_listener_free(ctx->listener);
    qmp_free(ctx->qmp);

    g_main_context_unref(ctx->mainctx);
}

cpg_model_v1_data_t cpg_data = {
    CPG_MODEL_V1,
    colod_cpg_deliver,
    colod_cpg_confchg,
    colod_cpg_totem_confchg,
    0
};

static int colod_open_cpg(ColodContext *ctx, GError **errp) {
    cs_error_t ret;
    int fd;
    struct cpg_name name;

    if (strlen(ctx->instance_name) >= sizeof(name.value)) {
        colod_error_set(errp, "Instance name too long");
        return -1;
    }
    strcpy(name.value, ctx->instance_name);
    name.length = strlen(name.value);

    ret = cpg_model_initialize(&ctx->cpg_handle, CPG_MODEL_V1,
                               (cpg_model_data_t*) &cpg_data, ctx);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to initialize cpg: %s", cs_strerror(ret));
        return -1;
    }

    ret = cpg_join(ctx->cpg_handle, &name);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to join cpg group: %s", cs_strerror(ret));
        goto err;
    }

    ret = cpg_fd_get(ctx->cpg_handle, &fd);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to get cpg file descriptor: %s",
                        cs_strerror(ret));
        goto err_joined;
    }

    ctx->cpg_fd = fd;
    return 0;

err_joined:
    cpg_leave(ctx->cpg_handle, &name);
err:
    cpg_finalize(ctx->cpg_handle);
    return -1;
}

static int colod_open_mngmt(ColodContext *ctx, GError **errp) {
    int sockfd, ret;
    struct sockaddr_un address = { 0 };
    g_autofree char *path = NULL;

    path = g_strconcat(ctx->base_dir, "/colod.sock", NULL);
    if (strlen(path) >= sizeof(address.sun_path)) {
        colod_error_set(errp, "Management unix path too long");
        return -1;
    }
    strcpy(address.sun_path, path);
    address.sun_family = AF_UNIX;

    ret = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ret < 0) {
        colod_error_set(errp, "Failed to create management socket: %s",
                        g_strerror(errno));
        return -1;
    }
    sockfd = ret;

    unlink(path);
    ret = bind(sockfd, (const struct sockaddr *) &address, sizeof(address));
    if (ret < 0) {
        colod_error_set(errp, "Failed to bind management socket: %s",
                        g_strerror(errno));
        goto err;
    }

    ret = listen(sockfd, 2);
    if (ret < 0) {
        colod_error_set(errp, "Failed to listen management socket: %s",
                        g_strerror(errno));
        goto err;
    }

    ret = colod_fd_set_blocking(sockfd, FALSE, errp);
    if (ret < 0) {
        goto err;
    }

    ctx->mngmt_listen_fd = sockfd;
    return 0;

err:
    close(sockfd);
    return -1;
}

static int colod_open_qmp(ColodContext *ctx, GError **errp) {
    int ret;

    ret = colod_unix_connect(ctx->qmp_path, errp);
    if (ret < 0) {
        return -1;
    }
    ctx->qmp1_fd = ret;

    ret = colod_unix_connect(ctx->qmp_yank_path, errp);
    if (ret < 0) {
        close(ctx->qmp1_fd);
        ctx->qmp1_fd = 0;
        return -1;
    }
    ctx->qmp2_fd = ret;

    return 0;
}

static int colod_daemonize(ColodContext *ctx) {
    GError *local_errp = NULL;
    gchar *path;
    int logfd, pipefd, ret;

    pipefd = os_daemonize();

    path = g_strconcat(ctx->base_dir, "/colod.log", NULL);
    logfd = open(path, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR);
    g_free(path);

    if (logfd < 0) {
        openlog("colod", LOG_PID, LOG_DAEMON);
        syslog(LOG_ERR, "Fatal: Unable to open log file");
        exit(EXIT_FAILURE);
    }

    assert(logfd == 0);
    assert(dup(0) == 1);
    assert(dup(0) == 2);

    openlog("colod", LOG_PID, LOG_DAEMON);

    if (ctx->do_trace) {
        path = g_strconcat(ctx->base_dir, "/trace.log", NULL);
        trace = fopen(path, "a");
        g_free(path);
    }

    path = g_strconcat(ctx->base_dir, "/colod.pid", NULL);
    ret = colod_write_pidfile(path, &local_errp);
    g_free(path);
    if (!ret) {
        syslog(LOG_ERR, "Fatal: %s", local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    return pipefd;
}

static int colod_parse_options(ColodContext *ctx, int *argc, char ***argv,
                               GError **errp) {
    gboolean ret;
    GOptionContext *context;
    GOptionEntry entries[] =
    {
        {"daemonize", 'd', 0, G_OPTION_ARG_NONE, &ctx->daemonize, "Daemonize", NULL},
        {"syslog", 's', 0, G_OPTION_ARG_NONE, &do_syslog, "Log to syslog", NULL},
        {"disable_cpg", 0, 0, G_OPTION_ARG_NONE, &ctx->disable_cpg, "Disable corosync communication", NULL},
        {"instance_name", 'i', 0, G_OPTION_ARG_STRING, &ctx->instance_name, "The CPG group name for corosync communication", NULL},
        {"node_name", 'n', 0, G_OPTION_ARG_STRING, &ctx->node_name, "The node hostname", NULL},
        {"base_directory", 'b', 0, G_OPTION_ARG_FILENAME, &ctx->base_dir, "The base directory to store logs and sockets", NULL},
        {"qmp_path", 'q', 0, G_OPTION_ARG_FILENAME, &ctx->qmp_path, "The path to the qmp socket", NULL},
        {"qmp_yank_path", 'y', 0, G_OPTION_ARG_FILENAME, &ctx->qmp_yank_path, "The path to the qmp socket used for yank", NULL},
        {"timeout_low", 'l', 0, G_OPTION_ARG_INT, &ctx->qmp_timeout_low, "Low qmp timeout", NULL},
        {"timeout_high", 't', 0, G_OPTION_ARG_INT, &ctx->qmp_timeout_high, "High qmp timeout", NULL},
        {"watchdog_interval", 'a', 0, G_OPTION_ARG_INT, &ctx->watchdog_interval, "Watchdog interval (0 to disable)", NULL},
        {"primary", 'p', 0, G_OPTION_ARG_NONE, &ctx->primary, "Startup in primary mode", NULL},
        {"trace", 0, 0, G_OPTION_ARG_NONE, &ctx->do_trace, "Enable tracing", NULL},
        {0}
    };

    ctx->qmp_timeout_low = 600;
    ctx->qmp_timeout_high = 10000;

    context = g_option_context_new("- qemu colo heartbeat daemon");
    g_option_context_set_help_enabled(context, TRUE);
    g_option_context_add_main_entries(context, entries, 0);

    ret = g_option_context_parse(context, argc, argv, errp);
    g_option_context_free(context);
    if (!ret) {
        return -1;
    }

    if (!ctx->node_name || !ctx->instance_name || !ctx->base_dir ||
            !ctx->qmp_path) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                    "--instance_name, --node_name, --base_directory and --qmp_path need to be given.");
        return -1;
    }

    return 0;
}

int main(int argc, char **argv) {
    GError *errp = NULL;
    ColodContext ctx_struct = { 0 };
    ColodContext *ctx = &ctx_struct;
    int ret;
    int pipefd = 0;

    ret = colod_parse_options(ctx, &argc, &argv, &errp);
    if (ret < 0) {
        fprintf(stderr, "%s\n", errp->message);
        g_error_free(errp);
        exit(EXIT_FAILURE);
    }

    if (ctx->daemonize) {
        pipefd = colod_daemonize(ctx);
    }
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
    prctl(PR_SET_DUMPABLE, 1);

    signal(SIGPIPE, SIG_IGN); // TODO: Handle this properly

    ret = colod_open_qmp(ctx, &errp);
    if (ret < 0) {
        goto err;
    }

    ret = colod_open_mngmt(ctx, &errp);
    if (ret < 0) {
        goto err;
    }

    if (!ctx->disable_cpg) {
        ret = colod_open_cpg(ctx, &errp);
        if (ret < 0) {
            goto err;
        }
    }

    if (ctx->daemonize) {
        ret = os_daemonize_post_init(pipefd, &errp);
        if (ret < 0) {
            goto err;
        }
    }

    colod_mainloop(ctx);

    // cleanup pidfile, cpg, qmp and mgmt connection

    return EXIT_SUCCESS;

err:
    if (errp) {
        colod_syslog(LOG_ERR, "Fatal: %s", errp->message);
        g_error_free(errp);
    }
    exit(EXIT_FAILURE);
}
