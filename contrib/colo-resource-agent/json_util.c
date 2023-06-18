/*
 * COLO background daemon json utilities
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <assert.h>

#include <glib-2.0/glib.h>
#include <json-glib-1.0/json-glib/json-glib.h>

#include "util.h"

const gchar *bool_to_json(gboolean bool) {
    if (bool) {
        return "true";
    } else {
        return "false";
    }
}

gboolean has_member(JsonNode *node, const gchar *member) {
    JsonObject *object;

    assert(JSON_NODE_HOLDS_OBJECT(node));
    object = json_node_get_object(node);
    return json_object_has_member(object, member);
}

const gchar *get_member_str(JsonNode *node, const gchar *member) {
    JsonObject *object;

    assert(JSON_NODE_HOLDS_OBJECT(node));
    object = json_node_get_object(node);
    return json_object_get_string_member(object, member);
}

JsonNode *get_member_node(JsonNode *node, const gchar *member) {
    JsonObject *object;

    assert(JSON_NODE_HOLDS_OBJECT(node));
    object = json_node_get_object(node);
    return json_object_get_member(object, member);
}

const gchar *get_member_member_str(JsonNode *node, const gchar *member1,
                                   const gchar *member2) {
    JsonObject *object;

    assert(JSON_NODE_HOLDS_OBJECT(node));
    object = json_node_get_object(node);
    object = json_object_get_object_member(object, member1);
    return json_object_get_string_member(object, member2);
}

gboolean object_matches(JsonNode *node, JsonNode *match) {
    JsonObject *object, *match_object;
    JsonObjectIter iter;
    const gchar *match_member;
    JsonNode *match_node;

    assert(JSON_NODE_HOLDS_OBJECT(match));

    if (!JSON_NODE_HOLDS_OBJECT(node)) {
        return FALSE;
    }

    object = json_node_get_object(node);
    match_object = json_node_get_object(match);

    json_object_iter_init(&iter, match_object);
    while (json_object_iter_next(&iter, &match_member, &match_node))
    {
        if (!json_object_has_member(object, match_member)) {
            return FALSE;
        }

        JsonNode *member_node = json_object_get_member(object, match_member);
        if (!json_node_equal(member_node, match_node)) {
            return FALSE;
        }
    }

    return TRUE;
}

gboolean object_matches_json(JsonNode *node, const gchar *match) {
    JsonNode *match_node = json_from_string(match, NULL);
    assert(match_node);

    gboolean ret = object_matches(node, match_node);
    json_node_unref(match_node);
    return ret;
}

gboolean object_matches_match_array(JsonNode *node, JsonNode *match_array) {
    JsonArray *array;

    assert(JSON_NODE_HOLDS_ARRAY(match_array));

    array = json_node_get_array(match_array);
    guint count = json_array_get_length(array);
    for (guint i = 0; i < count; i++) {
        JsonNode *match = json_array_get_element(array, i);
        assert(match);

        if (object_matches(node, match)) {
            return TRUE;
        }
    }

    return FALSE;
}
