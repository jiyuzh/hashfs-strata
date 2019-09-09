#include "cache_stats.h"

int cache_access_fd = 0;
int cache_misses_fd = 0;
int l1_cache_access_fd = 0;
int l1_cache_misses_fd = 0;
int li_cache_misses_fd = 0;

bool enable_cache_stats = false;
bool cache_stats_done_init = false;

void add_cache_stats_to_json(json_object *root, const char *object_name, cache_stats_t *cs) {
    if (!enable_cache_stats) return;

    double l1_hits = cs->l1_accesses - cs->l1_misses;
    double l2_hits = cs->l2_accesses - cs->l2_misses;
    double l3_hits = cs->l3_accesses - cs->l3_misses;

    json_object *obj = json_object_new_object(); {
        json_object *jl1 = json_object_new_object(); {
            js_add_double(jl1, "accesses", cs->l1_accesses);
            js_add_double(jl1, "hits", l1_hits);
            js_add_double(jl1, "misses", cs->l1_misses);
            json_object_object_add(obj, "l1", jl1);
        }
        json_object *jl2 = json_object_new_object(); {
            js_add_double(jl2, "accesses", cs->l2_accesses);
            js_add_double(jl2, "hits", l2_hits);
            js_add_double(jl2, "misses", cs->l2_misses);
            json_object_object_add(obj, "l2", jl2);
        }
        json_object *jl3 = json_object_new_object(); {
            js_add_double(jl3, "accesses", cs->l3_accesses);
            js_add_double(jl3, "hits", l3_hits);
            js_add_double(jl3, "misses", cs->l3_misses);
            json_object_object_add(obj, "l3", jl3);
        }
        json_object_object_add(root, object_name, obj);
    }
}
