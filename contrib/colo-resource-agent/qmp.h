/*
 * COLO background daemon qmp
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef QMP_H
#define QMP_H

#include <glib-2.0/glib.h>

#include <json-glib-1.0/json-glib/json-glib.h>

#include "coroutine.h"
#include "coutil.h"
#include "base_types.h"

typedef struct ColodWaitState ColodWaitState;

typedef struct ColodQmpResult {
    JsonNode *json_root;
    gchar *line;
    gsize len;
} ColodQmpResult;

typedef struct ColodQmpCo {
    gchar *line;
    union {
        gsize len;
        guint timeout_source_id;
    };
    union {
        gchar *command;
        ColodWaitState *wait_state;
    };
    ColodQmpResult *result;
} ColodQmpCo;

typedef void (*QmpYankCallback)(gpointer user_data);
typedef void (*QmpEventCallback)(gpointer user_data, ColodQmpResult *event);

void qmp_result_free(ColodQmpResult *result);
ColodQmpResult *qmp_parse_result(gchar *line, gsize len, GError **errp);

ColodQmpState *qmp_new(int fd1, int fd2, guint timeout, GError **errp);
void qmp_free(ColodQmpState *state);

#define qmp_execute_co(ret, state, errp, command) \
    co_call_co((ret), _qmp_execute_co, (state), (errp), (command))
ColodQmpResult *_qmp_execute_co(Coroutine *coroutine,
                                ColodQmpState *state,
                                GError **errp,
                                const gchar *command);

#define qmp_execute_nocheck_co(ret, state, errp, command) \
    co_call_co((ret), _qmp_execute_nocheck_co, (state), (errp), (command))
ColodQmpResult *_qmp_execute_nocheck_co(Coroutine *coroutine,
                                        ColodQmpState *state,
                                        GError **errp,
                                        const gchar *command);

#define qmp_yank_co(ret, state, errp) \
    co_call_co((ret), _qmp_yank_co, (state), (errp))
int _qmp_yank_co(Coroutine *coroutine, ColodQmpState *state,
                 GError **errp);

void qmp_add_notify_event(ColodQmpState *state, QmpEventCallback _func,
                          gpointer user_data);
void qmp_add_notify_yank(ColodQmpState *state, QmpYankCallback _func,
                         gpointer user_data);
void qmp_del_notify_event(ColodQmpState *state, QmpEventCallback _func,
                          gpointer user_data);
void qmp_del_notify_yank(ColodQmpState *state, QmpYankCallback _func,
                         gpointer user_data);

#define qmp_wait_event_co(ret, state, timeout, match, errp) \
    co_call_co((ret), _qmp_wait_event_co, (state), (timeout), (match), (errp))
int _qmp_wait_event_co(Coroutine *coroutine, ColodQmpState *state,
                       guint timeout, const gchar *match, GError **errp);

int qmp_get_error(ColodQmpState *state, GError **errp);
gboolean qmp_get_yank(ColodQmpState *state);
void qmp_clear_yank(ColodQmpState *state);

guint qmp_hup_source(ColodQmpState *state, GIOFunc func, gpointer data);
void qmp_set_yank_instances(ColodQmpState *state, JsonNode *instances);
void qmp_set_timeout(ColodQmpState *state, guint timeout);

#endif // QMP_H
