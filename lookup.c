/**
 * This file contains functionaility
 * for looking up tables, columns 
 * and results.
 **/
#define _XOPEN_SOURCE
#define _BSD_SOURCE

#include "cs165_api.h"

#define LOOKUP_TABLE_SIZE 100

/**
 * Simple hash function.
 **/
size_t hash(const char* str) {
    size_t len = strlen(str);

    size_t hash = 0;
    for(size_t i=0; i < len; i++) {
        char c = str[i];
        int a = c - '0';
        hash = (hash * 10) + a;     
    } 

    return hash % LOOKUP_TABLE_SIZE;
}


/**
 * Given lookup table, object name, object ptr,
 * and object type, put new node into lookup table
 **/
void insert_object(LookupTable* lookup_table, const char* object_name, void* object, LookupType type) {
    size_t bin = hash(object_name);

    // go to end of bin
    LookupNode* last = NULL;
    LookupNode* next = lookup_table->object_table[bin];
    while (next != NULL) {
        // check if duplicate, if so just replace object and return
        if (strcmp(next->object_name, object_name) == 0 && next->type == type) {
            free(next->object);
            next->object = object;
            return;
        }

        last = next;
        next = next->next;
    }

    // create new node
    LookupNode* new_node = calloc(1, sizeof(LookupNode));
    strcpy(new_node->object_name, object_name);
    new_node->type = type;
    new_node->object = object;

    // if bin empty put at head, else put at end
    if (next == lookup_table->object_table[bin]) {
        lookup_table->object_table[bin] = new_node;
    } else {
        last->next = new_node;
    }
}


/**
 * Given lookup table, object name and object type,
 * lookup node in table, then return object ptr if
 * it exists, else return NULL ptr
 **/
void* lookup_object(LookupTable* lookup_table, const char* object_name, LookupType type) {
    size_t bin = hash(object_name);

    LookupNode* node = lookup_table->object_table[bin];
    while (node != NULL && (strcmp(object_name, node->object_name) != 0 || node->type != type)) {
        node = node->next;
    }

    if (node != NULL) {
        return node->object;
    }
    return NULL;
}


/**
 * Initialize new lookup table
 **/
LookupTable* init_lookup_table() {
    LookupTable* lookup_table = malloc(sizeof(LookupTable));
    lookup_table->object_table = malloc(LOOKUP_TABLE_SIZE * sizeof(LookupNode*));
    lookup_table->size = LOOKUP_TABLE_SIZE;

    for (int i=0; i < LOOKUP_TABLE_SIZE; i++) {
        lookup_table->object_table[i] = NULL;
    }

    return lookup_table;
}
