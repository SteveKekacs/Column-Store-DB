#include "cs165_api.h"

/**
 * Simple structs for thread function paramaters
 **/
typedef struct sharedSelectParams {
    DbOperator** queries;
    size_t num_queries;
    Status* status;
} sharedSelectParams;

typedef struct selectParams {
    DbOperator* query;
    Status* status;
} selectParams;

typedef struct chunkedParams {
    Comparator* comparators;
    int num_items;
    int* data;
    int num_thread;
    long* min;
    long* max;
} chunkedParams;


void execute_insert(Table* table, int* values, Status* status);
void execute_db_operator(DbOperator* query, Status* status);
void db_operator_free(DbOperator* query);
