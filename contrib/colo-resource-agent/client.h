/*
 * COLO background daemon client handling
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef CLIENT_H
#define CLIENT_H

#include <glib-2.0/glib.h>

#include "base_types.h"
#include "daemon.h"

typedef struct ColodClientCo {
    gchar *line;
    gsize len;
    union {
        ColodQmpResult *result;
        ColodQmpResult *qemu_status;
    };
    union {
        ColodQmpResult *request;
        ColodQmpResult *colo_status;
    };
} ColodClientCo;

void client_listener_free(ColodClientListener *listener);
ColodClientListener *client_listener_new(int socket, ColodContext *ctx);

#endif // CLIENT_H
