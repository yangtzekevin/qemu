/*
 * COLO background daemon coroutine utils
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include "coutil.h"
#include "util.h"

#include <glib-2.0/glib.h>

#include "coroutine_stack.h"

GIOStatus _colod_channel_read_line_timeout_co(Coroutine *coroutine,
                                              GIOChannel *channel,
                                              gchar **line,
                                              gsize *len,
                                              guint timeout,
                                              GError **errp) {
    CoroutineUtilCo *co = co_stack(utilco);
    GIOStatus ret;

    co_begin(GIOStatus, 0);

    if (timeout) {
        CO timeout_source_id = g_timeout_add(timeout, coroutine->cb.plain,
                                             coroutine);
    }

    while (TRUE) {
        ret = g_io_channel_read_line(channel, line, len, NULL, errp);
        if ((ret == G_IO_STATUS_NORMAL && *len == 0) ||
                ret == G_IO_STATUS_AGAIN) {
            CO io_source_id = g_io_add_watch(channel,
                                             G_IO_IN | G_IO_HUP,
                                             coroutine->cb.iofunc,
                                             coroutine);
            co_yield_int(G_SOURCE_REMOVE);

            if (timeout && g_source_get_id(g_main_current_source())
                    == CO timeout_source_id) {
                g_source_remove(CO io_source_id);
                g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                            "Channel read timed out");
                ret = G_IO_STATUS_ERROR;
                break;
            }
        } else {
            break;
        }
    }

    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }
    co_end;

    return ret;
}

GIOStatus _colod_channel_read_line_co(Coroutine *coroutine,
                                      GIOChannel *channel, gchar **line,
                                      gsize *len, GError **errp) {
    return _colod_channel_read_line_timeout_co(coroutine, channel, line,
                                               len, 0, errp);
}

GIOStatus _colod_channel_write_timeout_co(Coroutine *coroutine,
                                          GIOChannel *channel,
                                          const gchar *buf,
                                          gsize len,
                                          guint timeout,
                                          GError **errp) {
    CoroutineUtilCo *co = co_stack(utilco);
    GIOStatus ret;
    gsize write_len;
    CO offset = 0;

    co_begin(GIOStatus, 0);

    if (timeout) {
        CO timeout_source_id = g_timeout_add(timeout, coroutine->cb.plain,
                                             coroutine);
    }

    while (CO offset < len) {
        ret = g_io_channel_write_chars(channel,
                                       buf + CO offset,
                                       len - CO offset,
                                       &write_len, errp);
        CO offset += write_len;
        if (ret == G_IO_STATUS_NORMAL || ret == G_IO_STATUS_AGAIN) {
            if (write_len == 0) {
                CO io_source_id = g_io_add_watch(channel, G_IO_OUT | G_IO_HUP,
                                                 coroutine->cb.iofunc,
                                                 coroutine);
                co_yield_int(G_SOURCE_REMOVE);

                if (timeout && g_source_get_id(g_main_current_source())
                        == CO timeout_source_id) {
                    g_source_remove(CO io_source_id);
                    g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                                "Channel write timed out");
                    ret = G_IO_STATUS_ERROR;
                    break;
                }
            }
        } else {
            break;
        }
    }

    if (ret != G_IO_STATUS_NORMAL) {
        return ret;
    }

    ret = g_io_channel_flush(channel, errp);
    while(ret == G_IO_STATUS_AGAIN) {
        CO io_source_id = g_io_add_watch(channel, G_IO_OUT | G_IO_HUP,
                                         coroutine->cb.iofunc,
                                         coroutine);
        co_yield_int(G_SOURCE_REMOVE);

        if (timeout && g_source_get_id(g_main_current_source())
                == CO timeout_source_id) {
            g_source_remove(CO io_source_id);
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                        "Channel flush timed out");
            ret = G_IO_STATUS_ERROR;
            break;
        }

        ret = g_io_channel_flush(channel, errp);
    }

    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }

    co_end;

    return ret;
}

GIOStatus _colod_channel_write_co(Coroutine *coroutine,
                                  GIOChannel *channel, const gchar *buf,
                                  gsize len, GError **errp) {
    return _colod_channel_write_timeout_co(coroutine, channel, buf, len, 0,
                                           errp);
}
