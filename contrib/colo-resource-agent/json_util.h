#ifndef JSON_UTIL_H
#define JSON_UTIL_H

#include <glib-2.0/glib.h>
#include <json-glib-1.0/json-glib/json-glib.h>

const gchar *bool_to_json(gboolean bool);

gboolean has_member(JsonNode *node, const gchar *member);
const gchar *get_member_str(JsonNode *node, const gchar *member);
JsonNode *get_member_node(JsonNode *node, const gchar *member);
const gchar *get_member_member_str(JsonNode *node, const gchar *member1,
                                   const gchar *member2);
gboolean object_matches(JsonNode *node, JsonNode *match);
gboolean object_matches_json(JsonNode *node, const gchar *match);
gboolean object_matches_match_array(JsonNode *node, JsonNode *match_array);

#endif // JSON_UTIL_H
