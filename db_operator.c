#define _XOPEN_SOURCE
#define _BSD_SOURCE

#include <pthread.h>
#include <string.h>
#include "db_operator.h"
#include "index.h"
#include <limits.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

// for batching queries
int batching;
int num_batched_queries;
DbOperator** batched_queries = NULL;
int*** all_results = NULL;
int** all_results_counts = NULL;

/**
 * Frees all memory allocated for DbOperator
 **/
void db_operator_free(DbOperator* query) {
    switch(query->type) {
        case INSERT:
            free(query->operator_fields.insert_operator.values);
            break;
        case PRINT:
            free(query->operator_fields.print_operator.fields);
            break;
        default:
            break;
    }
    free(query);
}


/**
 * Executes min and max operators given
 * a DbOperator* query.
 **/
void execute_min_max_operator(DbOperator* query, Status* status) {
    AggregateOperator operator = query->operator_fields.aggregate_operator;

    if (!query->num_handles) {
        status->code = INCORRECT_FORMAT;
        return;
    }

    int num_rows;
    int* data = NULL;
    int* indices = NULL;

    // get data array
    if (operator.chandle_1->type == COLUMN) {
        num_rows = (int) operator.chandle_1->pointer.column->col_size;
        data = operator.chandle_1->pointer.column->data;
    } else {
        num_rows = (int) operator.chandle_1->pointer.result->num_tuples;
        data = (int*) operator.chandle_1->pointer.result->payload;
    }

    // check for indices array
    if (operator.chandle_2 != NULL) {
        if (query->num_handles != 2) {
            status->code = INCORRECT_FORMAT;
            return;
        }

        indices = data;
        if (operator.chandle_2->type == COLUMN) {
            if (num_rows != (int) operator.chandle_2->pointer.column->col_size) {
                status->code = QUERY_UNSUPPORTED;
                return;
            }
            data = operator.chandle_2->pointer.column->data;
        } else {
            if (num_rows != (int) operator.chandle_2->pointer.result->num_tuples) {
                status->code = QUERY_UNSUPPORTED;
                return;
            }
            data = (int*) operator.chandle_2->pointer.result->payload;
        }
    }

    // init new Result
    Result* result = malloc(sizeof(Result));
    result->num_tuples = 1;
    result->data_type = INT;
    int* payload = malloc(sizeof(int));

    // if vector of indices pass store in result
    Result* result_indices = NULL;

    if (num_rows) {
        if (operator.type == MIN) {
            int min = data[0];

            // if no position vector
            if (indices == NULL) {
                for (int i=1; i < num_rows; i++) {
                    min = data[i] < min ? data[i] : min;
                }
                *payload = min;
            } else {
                result_indices = malloc(sizeof(Result));
                result_indices->data_type = INT;

                // var to hold min
                int min = data[indices[0]];
                // var to keep track of number of indices min is at
                int num_min_indices = 1;
                // array to hold indices of min
                int* index_payload = malloc(sizeof(int) * num_rows);

                for (int i=1; i < num_rows; i++) {
                    // if data at index == min
                    // append to return indices
                    if (data[indices[i]] == min) {
                        index_payload[num_min_indices++] = indices[i];
                    // else if less than min
                    // set num_min_indices = 1
                    // and set return indices[0] to curr index
                    } else if (data[indices[i]] < min) {
                        num_min_indices = 1;
                        index_payload[0] = indices[i];
                        min = data[indices[i]];
                    }
                }
                *payload = min;

                // realloc to size of num_min_indices
                index_payload = realloc(index_payload, sizeof(int) * num_min_indices);

                // store in result
                result_indices->payload = (void*) index_payload;
                result_indices->num_tuples = num_min_indices;
            }
        } else {
            int max = data[0];

            // if no position vector
            if (indices == NULL) {
                for (int i=1; i < num_rows; i++) {
                    max = data[i] > max ? data[i] : max;
                }
                *payload = max;
            } else {
                result_indices = malloc(sizeof(Result));
                result_indices->data_type = INT;

                // var to hold max
                int max = data[indices[0]];
                // var to keep track of number of indices max is at
                int num_max_indices = 1;
                // array to hold indices of max
                int* index_payload = malloc(sizeof(int) * num_rows);

                for (int i=1; i < num_rows; i++) {
                    // if data at index == max
                    // append to return indices
                    if (data[indices[i]] == max) {
                        index_payload[num_max_indices++] = indices[i];
                    // else if less than max
                    // set num_max_indices = 1
                    // and set return indices[0] to curr index
                    } else if (data[indices[i]] < max) {
                        num_max_indices = 1;
                        index_payload[0] = indices[i];
                        max = data[indices[i]];
                    }
                }
                *payload = max;

                // realloc to size of num_max_indices
                index_payload = realloc(index_payload, sizeof(int) * num_max_indices);

                // store in result
                result_indices->payload = (void*) index_payload;
                result_indices->num_tuples = num_max_indices;
            }
        }
    }
    result->payload = (void*) payload;

    // create CHandle to store result val/indices in
    CHandle* res_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);

    if (result_indices != NULL) {
        res_chandle->pointer.result = result_indices;

        // create CHandle to store result val in
        CHandle* res_chandle_2 = lookup_object(query->client_lookup_table, query->handle_names[1], RESULT);
        res_chandle_2->pointer.result = result;
    } else {
        // else just val, set result
        res_chandle->pointer.result = result;
    }
    status->code = OK_DONE;
}

/*
 * Executes sum and avg operators given
 * a DbOperator* query
 */
void execute_sum_avg_operator(DbOperator* query, Status* status) {
    AggregateOperator operator = query->operator_fields.aggregate_operator;

    if (!query->num_handles) {
        status->code = INCORRECT_FORMAT;
        return;
    }

    int num_rows;
    int* data = NULL;

    // get data array
    if (operator.chandle_1->type == COLUMN) {
        num_rows = (int) operator.chandle_1->pointer.column->col_size;
        data = operator.chandle_1->pointer.column->data;
    } else {
        num_rows = (int) operator.chandle_1->pointer.result->num_tuples;
        data = (int*) operator.chandle_1->pointer.result->payload;
    }

    // init new Result
    Result* result = malloc(sizeof(Result));

    // if data
    if (num_rows) {
        // sum all vals
        long sum = 0;
        for (int i=0; i < num_rows; i++) {
            sum += data[i];
        }

        // set result fields
        if (operator.type == AVG) {
            result->data_type = FLOAT;
            double* payload = malloc(sizeof(double));
            *payload = (double) sum / (double) num_rows;
            result->payload = (void*) payload;
        } else {
            result->data_type = LONG;
            long* payload = malloc(sizeof(long));
            *payload = sum;
            result->payload = (void*) payload;
        }
        result->num_tuples = 1;
    } else {
        result->num_tuples = 0;
        result->payload = NULL;
    }

    // create CHandle to store result in
    CHandle* res_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);
    res_chandle->pointer.result = result;

    status->code = OK_DONE;
}

/*
 * Executes add and sub operators given
 * a DbOperator* query.
 */
void execute_add_sub_operator(DbOperator* query, Status* status) {
    AggregateOperator operator = query->operator_fields.aggregate_operator;

    if (!query->num_handles) {
        status->code = INCORRECT_FORMAT;
        return;
    }

    int num_rows;
    int* data_1 = NULL;
    int* data_2 = NULL;

    // get first data array
    if (operator.chandle_1->type == COLUMN) {
        num_rows = (int) operator.chandle_1->pointer.column->col_size;
        data_1 = operator.chandle_1->pointer.column->data;
    } else {
        num_rows = (int) operator.chandle_1->pointer.result->num_tuples;
        data_1 = (int*) operator.chandle_1->pointer.result->payload;
    }

    // get second data array
    if (operator.chandle_2->type == COLUMN) {
        if (num_rows != (int) operator.chandle_2->pointer.column->col_size) {
            status->code = QUERY_UNSUPPORTED;
            return;
        }
        data_2 = operator.chandle_2->pointer.column->data;
    } else {
        if (num_rows != (int) operator.chandle_2->pointer.result->num_tuples) {
            status->code = QUERY_UNSUPPORTED;
            return;
        }
        data_2 = (int*) operator.chandle_2->pointer.result->payload;
    }

    // add/sub data
    int* payload = malloc(sizeof(int) * num_rows);
    if (operator.type == ADD) {
        for (int i=0; i < num_rows; i++) {
            payload[i] = data_1[i] + data_2[i];
        }
    } else {
        for (int i=0; i < num_rows; i++) {
            payload[i] = data_1[i] - data_2[i];
        }
    }

    // init new Result
    Result* result = malloc(sizeof(Result));
    result->num_tuples = num_rows;
    result->data_type = INT;
    result->payload = (void*) payload;

    // create CHandle to store result in
    CHandle* res_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);
    res_chandle->pointer.result = result;

    status->code = OK_DONE;
}

/*
 * Executes an aggregation operation:
 *     min, max, sum, avg, add, sub
 */
void execute_aggregate_operator(DbOperator *query, Status* status) {
    switch (query->operator_fields.aggregate_operator.type) {
        case MIN:
            execute_min_max_operator(query, status);
            break;
        case MAX:
            execute_min_max_operator(query, status);
            break;
        case AVG:
            execute_sum_avg_operator(query, status);
            break;
        case SUM:
            execute_sum_avg_operator(query, status);
            break;
        case ADD:
            execute_add_sub_operator(query, status);
            break;
        case SUB:
            execute_add_sub_operator(query, status);
            break;
        default:
            status->code = QUERY_UNSUPPORTED;
    }
}


void execute_print_operator(DbOperator *query, Status* status) {
    Column** cols = NULL;
    Result** results = NULL;

    // get fields
    char** fields = query->operator_fields.print_operator.fields;
    int num_fields = query->operator_fields.print_operator.num_fields;

    // lookup first chandle
    LookupType chandle_type = COLUMN;
    CHandle* chandle = (CHandle*) lookup_object(db_catalog, fields[0], chandle_type);
    int num_results, num_results_set = 0;

    // if NULL, then must be RESULT
    if (chandle == NULL) {
        chandle_type = RESULT;
        results = malloc(sizeof(Result*) * num_fields);

        for (int i=0; i < num_fields; i++) {
            chandle = (CHandle*) lookup_object(query->client_lookup_table, fields[i], chandle_type);
            if (chandle == NULL) {
                free(results);
                status->code = OBJECT_DOES_NOT_EXIST;
                return;
            }

            results[i] = chandle->pointer.result;

            if (num_results_set && num_results != (int) results[i]->num_tuples) {
                free(results);
                status->code = QUERY_UNSUPPORTED;
                return;
            } else if (!num_results_set) {
                num_results = (int) results[i]->num_tuples;
                num_results_set = 1;
            }
        }
    // else its columns
    } else {
        cols = malloc(sizeof(Column*) * num_fields);
        cols[0] = chandle->pointer.column;
        num_results = cols[0]->col_size;

        for (int i=1; i < num_fields; i++) {
              chandle = (CHandle*) lookup_object(db_catalog, fields[i], chandle_type);
              if (chandle == NULL) {
                  free(cols);
                  status->code = OBJECT_DOES_NOT_EXIST;
                  return;
              }

              cols[i] = chandle->pointer.column;

              if (cols[i]->col_size != (size_t) num_results) {
                  free(cols);
                  status->code = QUERY_UNSUPPORTED;
                  return;
              }
          }
    }

    PrintPayload* print_payload = malloc(sizeof(PrintPayload));
    print_payload->num_results = num_results;
    print_payload->num_cols = num_fields;

    Status message_status;
    message* send_message = malloc(sizeof(message));
    send_message->status = message_status;
    send_message->length = 1;
    send_message->print_payload = 1;

    // send print metadata to client
    send(query->client_fd, send_message, sizeof(message), 0);
    send(query->client_fd, print_payload, sizeof(PrintPayload), 0);

    free(send_message);
    free(print_payload);

    if (num_results) {
        if (cols != NULL) {
            int data_type = 0;
            for (int i=0; i < num_fields; i++) {
                // send data type
                send(query->client_fd, &data_type, sizeof(int), 0);

                int* data = cols[i]->data;

                int total_num_bytes = sizeof(int) * num_results;
                int results_sent = 0;
                while(results_sent < total_num_bytes) {
                    int send_size = total_num_bytes - results_sent;
                    // send to client
                    results_sent += send(query->client_fd, &data[results_sent / sizeof(int)], send_size, 0);
                }
            }
        } else {
            int data_type = 0;
            for (int i=0; i < num_fields; i++) {
                if (results[i]->data_type == LONG) {
                    // send data type
                    data_type = 1;
                    send(query->client_fd, &data_type, sizeof(int), 0);

                    long* data = (long*) results[i]->payload;

                    int total_num_bytes = sizeof(long) * num_results;
                    int results_sent = 0;
                    while(results_sent < total_num_bytes) {
                        int send_size = total_num_bytes - results_sent;

                        // send to client
                        results_sent += send(query->client_fd, &data[results_sent / sizeof(long)], send_size, 0);
                    }
                } else if (results[i]->data_type == FLOAT) {
                    // send data type
                    data_type = 2;
                    send(query->client_fd, &data_type, sizeof(int), 0);

                    double* data = (double*) results[i]->payload;

                    int total_num_bytes = sizeof(double) * num_results;
                    int results_sent = 0;
                    while(results_sent < total_num_bytes) {
                        int send_size = total_num_bytes - results_sent;

                        // send to client
                        results_sent += send(query->client_fd, &data[results_sent / sizeof(double)], send_size, 0);
                    }
                } else {
                    // send data type
                    data_type = 0;
                    send(query->client_fd, &data_type, sizeof(int), 0);

                    int* data = (int*) results[i]->payload;

                    int total_num_bytes = sizeof(int) * num_results;
                    int results_sent = 0;
                    while(results_sent < total_num_bytes) {
                        int send_size = total_num_bytes - results_sent;

                        // send to client
                        results_sent += send(query->client_fd, &data[results_sent / sizeof(int)], send_size, 0);
                    }
                }
            }
        }
    }

    status->code = OK_DONE;
}

/**
 * Executes fetch operator.
 **/
void execute_fetch_operator(DbOperator* query, Status* status) {
    // make sure there's a chandle name
    // to store result
    if (!query->num_handles) {
        status->code = INCORRECT_FORMAT;
        return;
    }

    // get column to fetch from and result indices to fetch
    Column* column = query->operator_fields.fetch_operator.column;
    Result* result_indices = query->operator_fields.fetch_operator.result;

    // create new Result obj
    Result* result = malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = 0;

    if (result_indices->num_tuples) {
        // get indices to fetch
        int* indices = (int*) result_indices->payload;
        int* results = calloc(result_indices->num_tuples, sizeof(int));

        size_t i;
        // fetch results
        for (i=0; i < result_indices->num_tuples - 1; i+=2) {
            results[i] = column->data[indices[i]];
            results[i + 1] = column->data[indices[i + 1]];
        }

        if (i < result_indices->num_tuples) {
            results[i] = column->data[indices[i]];
        }
        result->num_tuples = result_indices->num_tuples;
        result->payload = (void*) results;
    } else {
        result->num_tuples = 0;
        result->payload = NULL;
    }

    // create CHandle to store result in
    CHandle* res_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);
    res_chandle->pointer.result = result;

    status->code = OK_DONE;
}


int* execute_scan(Comparator* comparator, int* data, int* indices, Result* pos_result, void* index, IndexType index_type) {
    int size = (int) pos_result->num_tuples;

    int* ret_indices = calloc(size, sizeof(int));
    int num_results = 0;

    if (indices == NULL) {
        if (index_type) {
            // set pos_min to 0 and pos_max to max position (size - 1)
            int pos_min = 0;
            int pos_max = size - 1;
            switch (index_type) {
                // if sorted index use binary search to find
                // endpoints, if unclustered need to use copy of data (index),
                // if clustered just use data
                case SORTED_UNCLUSTERED: {
                    UnclusteredIndex* unclustered_index = (UnclusteredIndex*) index;

                    int* values = unclustered_index->values;
                    int* positions = unclustered_index->positions;

                    // if lower bound binary search for that pos
                    if (comparator->type1) {
                        pos_min = binary_search(values, size, comparator->p_low);
                    }

                    // if upper bound binary search for that pos
                    if (comparator->type2) {
                        pos_max = binary_search(values, size, comparator->p_high);
                    }

                    num_results = pos_max - pos_min;

                    // now copy over those positions into ret_indices
                    memcpy(ret_indices, &positions[pos_min], sizeof(int) * num_results);
                    break;
                } case SORTED_CLUSTERED: {
                    pos_min = 0;
                    pos_max = size - 1;

                    // if lower bound binary search for that pos
                    if (comparator->type1) {
                        pos_min = binary_search(data, size, comparator->p_low);
                    }

                    // if upper bound binary search for that pos
                    if (comparator->type2) {
                        pos_max = binary_search(data, size, comparator->p_high);
                    }

                    // get number of items in range
                    num_results = pos_max - pos_min;
                    // put indices into results
                    for (int i=0; i <= num_results; i++) {
                        ret_indices[i] = pos_min + i;
                    }

                    break;
                } case BTREE_CLUSTERED: {
                    pos_min = 0;
                    pos_max = size - 1;

                    // if lower bound binary search for that pos
                    if (comparator->type1) {
                        pos_min = find_pos((BPTreeNode*) index, comparator->p_low, 1);
                    }

                    // if upper bound binary search for that pos
                    if (comparator->type2) {
                        pos_max = find_pos((BPTreeNode*) index, comparator->p_high, 0);
                    }

                    // get number of items in range
                    num_results = pos_max - pos_min;
                    // put indices into results
                    for (int i=0; i <= num_results; i++) {
                        ret_indices[i] = pos_min + i;
                    }
                    break;
                } case BTREE_UNCLUSTERED: {
                    int* min_val = NULL;
                    int* max_val = NULL;

                    // get min val
                    if (comparator->p_low) {
                        min_val = (int*) &comparator->p_low;
                    }

                    // get max val
                    if (comparator->p_high) {
                        max_val = (int*) &comparator->p_high;
                    }

                    // get resulting positions
                    find_pos_range((BPTreeNode*) index, &num_results, &ret_indices, min_val, max_val);
                } default: ;
            }
        } else {
            for (int i=0; i < size; i++) {
                ret_indices[num_results] = i;
                num_results += (comparator->type1 == NO_COMPARISON || comparator->p_low <= data[i]) && (comparator->type2 == NO_COMPARISON || comparator->p_high > data[i]);
            }
        }
    } else {
        for (int i=0; i < size; i++) {
            ret_indices[num_results] = indices[i];
            num_results += (comparator->type1 == NO_COMPARISON || comparator->p_low <= data[i]) && (comparator->type2 == NO_COMPARISON || comparator->p_high > data[i]);
        }
    }
    ret_indices = realloc(ret_indices, sizeof(int) * num_results);
    pos_result->num_tuples = num_results;

    return ret_indices;
}


void execute_select_operator(DbOperator* query, Status* status) {
    // make sure there's a chandle name
    // to store result
    if (!query->num_handles) {
        status->code = INCORRECT_FORMAT;
        return;
    }

    // get fields for select
    CHandle* chandle_1 = query->operator_fields.select_operator.chandle_1;
    CHandle* chandle_2 = query->operator_fields.select_operator.chandle_2;
    Comparator select_comperator = query->operator_fields.select_operator.comparator;

    // get data to scan
    int* data = NULL;
    int* indices = NULL;
    int num_tuples = 0;

    // to check for index
    IndexType index_type = NONE;
    void* index = NULL;

    if (chandle_1->type == COLUMN) {
        // get col data
        num_tuples =  chandle_1->pointer.column->col_size;
        data = chandle_1->pointer.column->data;

        // get index info
        index = chandle_1->pointer.column->index;
        index_type = chandle_1->pointer.column->index_type;
    } else {
        // set data and indices
        indices = (int*) chandle_1->pointer.result->payload;
        data = (int*) chandle_2->pointer.result->payload;
        num_tuples = chandle_2->pointer.result->num_tuples;
    }
    // init new Result to hold qualifying indices
    Result* pos_result = malloc(sizeof(Result));
    pos_result->data_type = INT;
    pos_result->num_tuples = num_tuples;

    // check to make sure comparisons are being made
    if (select_comperator.type1 || select_comperator.type2) {
        pos_result->payload = (void*) execute_scan(&select_comperator, data, indices, pos_result, index, index_type);
    } else {
        // no comparison being made so just
        // create array of all indices
        if (indices == NULL) {
            indices = malloc(sizeof(int) * num_tuples);
            for (int i=0; i < num_tuples; i++) {
                indices[i] = i;
            }
        }
        pos_result->payload = (void*) indices;
    }

    // create CHandle to store result in
    CHandle* res_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);
    res_chandle->pointer.result = pos_result;

    status->code = OK_DONE;
}


/**
 * Simple wrapper around shared select operator for use in threads.
 **/
void* execute_select_operator_wrapper(void* context) {
    selectParams* params = (selectParams*) context;
    execute_select_operator(params->query, params->status);
    return NULL;
}


int** execute_shared_scan(Comparator* comparators, int* data, int* indices, Result** pos_results, size_t num_queries) {
    // get initial size from first query
    int size = (int) pos_results[0]->num_tuples;

    // get min and max
    long* min = NULL;
    long* max = NULL;
    for (size_t i = 0; i < num_queries; i++) {
        if (comparators[i].type1 != NO_COMPARISON && (min == NULL || *min > comparators[i].p_low))
            min = &comparators[i].p_low;
        if (comparators[i].type2 != NO_COMPARISON && (max == NULL || *max < comparators[i].p_high))
            max = &comparators[i].p_high;
    }

    if (min == NULL) {
        *min = INT_MIN;
    }

    if (max == NULL) {
        *max = INT_MAX;
    }

    // init num_results_array and ret_indices_array
    int* num_results_array = calloc(num_queries, sizeof(int));
    int** ret_indices_array = calloc(num_queries, sizeof(int*));
    for (size_t i=0; i < num_queries; i++) {
        ret_indices_array[i] = malloc(sizeof(int) * size);
    }

    if (indices == NULL) {
        int val;
        size_t num_query;
        for (int i=0; i < size; i++) {
            val = data[i];

            if (val < *min || val > *max) {
                continue;
            }

            for (num_query=0; num_query < num_queries; num_query++) {
                ret_indices_array[num_query][num_results_array[num_query]] = i;
                num_results_array[num_query] += (comparators[num_query].type1 == NO_COMPARISON || comparators[num_query].p_low <= val) && (comparators[num_query].type2 == NO_COMPARISON || comparators[num_query].p_high > val);
            }
        }
    } else {
        int val;
        size_t num_query;
        for (int i=0; i < size; i++) {
            val = data[i];

            if (val < *min || val > *max) {
                continue;
            }

            for (num_query=0; num_query < num_queries; num_query++) {
                ret_indices_array[num_query][num_results_array[num_query]] = indices[i];
                num_results_array[num_query] += (comparators[num_query].type1 == NO_COMPARISON || comparators[num_query].p_low <= val) && (comparators[num_query].type2 == NO_COMPARISON || comparators[num_query].p_high > val);
            }
        }
    }

    for (size_t num_result=0; num_result < num_queries; num_result++) {
        ret_indices_array[num_result] = realloc(ret_indices_array[num_result], sizeof(int) * num_results_array[num_result]);
        pos_results[num_result]->num_tuples = num_results_array[num_result];
    }

    free(num_results_array);

    return ret_indices_array;
}


void execute_shared_select_operator(DbOperator** queries, size_t num_queries, Status* status) {
    // to hold data to scan
    int* data = NULL;
    int* indices = NULL;
    int num_tuples;

    // to hold each selects comparator
    Comparator* comparators = malloc(sizeof(Comparator) * num_queries);

    // to hold results and chandles for each query
    Result** results = malloc(sizeof(Result*) * num_queries);
    CHandle** chandles = malloc(sizeof(CHandle*) * num_queries);

    // loop through each query and get appropriate info
    for (size_t num_query=0; num_query < num_queries; num_query++) {
        // get query
        DbOperator* query = queries[num_query];

        // get info
        CHandle* chandle_1 = query->operator_fields.select_operator.chandle_1;
        CHandle* chandle_2 = query->operator_fields.select_operator.chandle_2;
        Comparator select_comperator = query->operator_fields.select_operator.comparator;

        // add comparator to comparators array
        comparators[num_query] = select_comperator;

        // if data not yet set, get
        if (!data) {
            if (chandle_1->type == COLUMN) {
                // get col data
                num_tuples =  chandle_1->pointer.column->col_size;
                data = chandle_1->pointer.column->data;
            } else {
                // set data and indices
                data = (int*) chandle_1->pointer.result->payload;
                indices = (int*) chandle_2->pointer.result->payload;
                num_tuples = chandle_2->pointer.result->num_tuples;
            }
        }

        // init new Result to hold qualifying indices
        Result* pos_result = malloc(sizeof(Result));
        pos_result->data_type = INT;
        pos_result->num_tuples = num_tuples;
        results[num_query] = pos_result;

        CHandle* res_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);
        chandles[num_query] = res_chandle;
    }

    // execute shared scan
    int** all_results = execute_shared_scan(comparators, data, indices, results, num_queries);

    // set results and chandles
    for (size_t num_result=0; num_result < num_queries; num_result++) {
        results[num_result]->payload = (void*) all_results[num_result];
        chandles[num_result]->pointer.result = results[num_result];
    }

    // free memory
    free(results);
    free(chandles);
    free(all_results);
    free(comparators);

    status->code = OK_DONE;
}


/**
 * Simple wrapper around shared select operator for use in threads.
 **/
void* execute_shared_select_operator_wrapper(void* context) {
    sharedSelectParams* params = (sharedSelectParams*) context;
    execute_shared_select_operator(params->queries, params->num_queries, params->status);
    return NULL;
}


/**
 * Wrapper around shared select operator for use in threads.
 **/
void* shared_select_chunk_wrapper(void* context) {
    chunkedParams* params = (chunkedParams*) context;
    int* data = params->data;
    int size = params->num_items;
    int num_thread = params->num_thread;
    long* min = params->min;
    long* max = params->max;
    Comparator* comparators = params->comparators;

    // init num_results_array and ret_indices_array
    int* num_results_array = calloc(num_batched_queries, sizeof(int));
    int** ret_indices_array = calloc(num_batched_queries, sizeof(int*));
    for (int i=0; i < num_batched_queries; i++) {
        ret_indices_array[i] = malloc(sizeof(int) * size);
    }

    int val, val2, i;
    for (i=0; i < size - 1; i+=2) {
        val = data[i];
        val2 = data[i + 1];

        if ((val < *min || val > *max) && (val2 < *min || val2 > *max))
            continue;

        for (int num_query=0; num_query < num_batched_queries; num_query++) {
            ret_indices_array[num_query][num_results_array[num_query]] = i;
            num_results_array[num_query] += (comparators[num_query].type1 == NO_COMPARISON || comparators[num_query].p_low <= val) && (comparators[num_query].type2 == NO_COMPARISON || comparators[num_query].p_high > val);
            ret_indices_array[num_query][num_results_array[num_query]] = i + 1;
            num_results_array[num_query] += (comparators[num_query].type1 == NO_COMPARISON || comparators[num_query].p_low <= val2) && (comparators[num_query].type2 == NO_COMPARISON || comparators[num_query].p_high > val2);
        }
    }

    if (i < size) {
        for (int num_query=0; num_query < num_batched_queries; num_query++) {
            ret_indices_array[num_query][num_results_array[num_query]] = i;
            num_results_array[num_query] += (comparators[num_query].type1 == NO_COMPARISON || comparators[num_query].p_low <= val) && (comparators[num_query].type2 == NO_COMPARISON || comparators[num_query].p_high > val);
        }
    }

    for (int num_result=0; num_result < num_batched_queries; num_result++) {
        ret_indices_array[num_result] = realloc(ret_indices_array[num_result], sizeof(int) * num_results_array[num_result]);
    }

    all_results[num_thread] = ret_indices_array;
    all_results_counts[num_thread] = num_results_array;

    return NULL;
}


/*
 * Given a table and values, inserts values into table.
 */
void execute_insert(Table* table, int* values, Status* status) {
    Column* columns = table->columns;

    // check if columns need more space
    if (table->table_length == (table->table_length_capacity - 1)) {
        table->table_length_capacity *= 2;
        for (size_t i=0; i < table->col_count; i++) {
            columns[i].data = realloc(columns[i].data, sizeof(int) * table->table_length_capacity);

            // check if index needs more memory
            if (columns[i].index_type == SORTED_UNCLUSTERED) {
                UnclusteredIndex* index = (UnclusteredIndex*) columns[i].index;
                index->values = realloc(index->values, sizeof(int) * table->table_length_capacity);
                index->positions = realloc(index->positions, sizeof(int) * table->table_length_capacity);
            }
        }
    }

    int* insert_pos = NULL;
    // check if first column has clustered index
    if (columns[0].index_type == SORTED_CLUSTERED || columns[0].index_type == BTREE_CLUSTERED) {
        // get insert position
        int res = binary_search(columns[0].data, columns[0].col_size, values[0]);
        insert_pos = &res;
    }

    // add data to columns
    for (size_t idx=0; idx < table->col_count; idx++) {
        // if no clustered index just add to data
        if (insert_pos == NULL) {
            columns[idx].data[table->table_length] = values[idx];
        // else add at position
        } else {
            insert_at_pos(columns[idx].data, columns[idx].col_size, *insert_pos, values[idx]);
        }

        // check for unclustered index if applicable
        if (columns[idx].index_type) {
            if (insert_pos == NULL) {
                index_value(&columns[idx], values[idx], table->table_length, 0);
            } else {
                index_value(&columns[idx], values[idx], *insert_pos, 0);
            }
        }

        // increase col size
        columns[idx].col_size++;
    }
    table->table_length++;

    status->code = OK_DONE;
}


/*
 * execute_insert wrapper, reads query and executes insert.
 */
void execute_insert_operator(DbOperator* query, Status* status) {
    InsertOperator insert_operator = query->operator_fields.insert_operator;
    execute_insert(insert_operator.table, insert_operator.values, status);
}


/**
 * Executes a CREATE db operator.
 **/
void execute_create_operator(DbOperator* query, Status* status) {
    status->code = OK_DONE;

    CreateOperator operator = query->operator_fields.create_operator;
    switch (operator.type) {
        case CREATE_DB:
            create_db(operator.db_name, status);
            break;
        case CREATE_TBL:
            create_table(operator.table_name, operator.db_name, operator.col_capacity, status);
            break;
        case CREATE_COL:
            create_column(operator.col_name, operator.table_name, status);
            break;
        case CREATE_IDX:
            create_idx(operator.col_name, operator.index_type, status);
            break;
        default:
            status->code = ERROR;
    }

}


/**
 * Given smaller vals, positions and count and
 * bigger vals, positions and count and two
 * result array pointers and num results pointer,
 * execute nested loop join.
 **/
void nested_loop_join(
        int* smaller_vals, int* smaller_positions, int smaller_num_vals,
        int* bigger_vals, int* bigger_positions, int bigger_num_vals,
        int* smaller_result, int* bigger_result, int* num_results
    ) {

    // optimized nested loop join
    // get number of ints that will fit on a page
    int chunk_size = 4096 / sizeof(int);

    for (int bigger_chunk_pos = 0; 
            bigger_chunk_pos < bigger_num_vals; 
            bigger_chunk_pos += chunk_size
        ) {

        for (int smaller_chunk_pos = 0; 
                smaller_chunk_pos < smaller_num_vals;
                smaller_chunk_pos += chunk_size
            ) {
            
            for (int bigger_pos = bigger_chunk_pos; 
                    bigger_pos < bigger_chunk_pos + chunk_size && bigger_pos < bigger_num_vals; 
                    bigger_pos++
                ) {
                
                for (int smaller_pos = smaller_chunk_pos; 
                        smaller_pos < smaller_chunk_pos + chunk_size && smaller_pos < smaller_num_vals;
                        smaller_pos++
                    ) {
                    
                    if (bigger_vals[bigger_pos] == smaller_vals[smaller_pos]) {
                        bigger_result[*num_results] = bigger_positions[bigger_pos];
                        smaller_result[*num_results] = smaller_positions[smaller_pos];
                        (*num_results)++;
                    }
                }
            }
        }
    }
}


/**
 * Given smaller vals, positions and count and
 * bigger vals, positions and count and two
 * result array pointers and num results pointer,
 * execute one pass hash join.
 **/
void hash_join(
        int* smaller_vals, int* smaller_positions, int smaller_num_vals,
        int* bigger_vals, int* bigger_positions, int bigger_num_vals,
        int* smaller_result, int* bigger_result, int* num_results
    ) {

    // build hash table on smaller
    HashTable* hash_table = init_hashtable();

    // buildhash table on smaller vals
    for (int i = 0; i < smaller_num_vals; i++) {
        hash_insert(hash_table, smaller_vals[i], smaller_positions[i]);
    }

    // probe table on bigger vals
    int num_left_results = *num_results;
    int* num_probe_results = calloc(1, sizeof(int));
    for (int i = 0; i < bigger_num_vals; i++) {
        // probe hash table
        int* results = hash_probe(hash_table, bigger_vals[i], num_probe_results);

        // if results found
        if (*num_probe_results > 0) {
            // add bigger pos

            // add smaller positions
            for (int num_r = 0; num_r < *num_probe_results; num_r++) {
                smaller_result[num_left_results + num_r] = results[num_r];
                bigger_result[*num_results] = bigger_positions[i];
                (*num_results)++;
            }

            // free results
            free(results);

            num_left_results += *num_probe_results;
            *num_probe_results = 0;
        }
    }
}


// A utility function to swap two elements
void swap(int* a, int* b)
{
    int t = *a;
    *a = *b;
    *b = t;
}


int partition(int* array_1, int* array_2, int low, int high) {
    int pivot = array_1[high];
    int low_idx = (low - 1);

    for (int curr_idx = low; curr_idx <= high - 1; curr_idx++) {
        if (array_1[curr_idx] <= pivot) {
            low_idx++;
            swap(&array_1[low_idx], &array_1[curr_idx]);
            swap(&array_2[low_idx], &array_2[curr_idx]);
        }
    }
    swap(&array_1[low_idx + 1], &array_1[high]);
    swap(&array_2[low_idx + 1], &array_2[high]);
    return (low_idx + 1);
}


void quick_sort_double(int* array_1, int* array_2, int low, int high)
{
    if (low < high) {
        int partition_idx = partition(array_1, array_2, low, high);

        quick_sort_double(array_1, array_2, low, partition_idx - 1);
        quick_sort_double(array_1, array_2, partition_idx + 1, high);
    }
}


/**
 * Given val and num partitions,
 * uses hash_function to partition val into
 * one of num_partitions and returns partition.
 **/
unsigned int hash_partition(int val, int num_partitions) {
    return (unsigned int) val % num_partitions;
}


/**
 * Given smaller vals, positions and count and
 * bigger vals, positions and count and two
 * result array pointers and num results pointer,
 * execute one pass hash join.
 **/
void grace_hash_join(
        int* left_vals, int* left_positions, int left_num_vals,
        int* right_vals, int* right_positions, int right_num_vals,
        int* left_result, int* right_result, int* num_results
    ) {
    // TODO: Multiple cores

    // partition all vals and positions
    int num_partitions = 256;

    // create arrays to hold partitions
    int** left_val_partitions = malloc(num_partitions * sizeof(int*));
    int** left_pos_partitions = malloc(num_partitions * sizeof(int*));
    int** right_val_partitions = malloc(num_partitions * sizeof(int*));
    int** right_pos_partitions = malloc(num_partitions * sizeof(int*));
    int* left_partition_sizes = malloc(num_partitions * sizeof(int));
    int* right_partition_sizes = malloc(num_partitions * sizeof(int));
    for (int i = 0; i < num_partitions; i++) {
        left_partition_sizes[i] = 0;
        right_partition_sizes[i] = 0;

        left_val_partitions[i] = calloc(left_num_vals, sizeof(int));
        left_pos_partitions[i] = calloc(left_num_vals, sizeof(int));

        right_val_partitions[i] = calloc(right_num_vals, sizeof(int));
        right_pos_partitions[i] = calloc(right_num_vals, sizeof(int));
    }

    // partition values
    int num_partition = 0;
    for (int i = 0; i < left_num_vals; i++) {
        num_partition = hash_partition(left_vals[i], num_partitions);
        left_val_partitions[num_partition][left_partition_sizes[num_partition]] = left_vals[i];
        left_pos_partitions[num_partition][left_partition_sizes[num_partition]] = left_positions[i];
        left_partition_sizes[num_partition]++;
    }

    for (int i = 0; i < right_num_vals; i++) {
        num_partition = hash_partition(right_vals[i], num_partitions);
        right_val_partitions[num_partition][right_partition_sizes[num_partition]] = right_vals[i];
        right_pos_partitions[num_partition][right_partition_sizes[num_partition]] = right_positions[i];
        right_partition_sizes[num_partition]++;
    }

    // int* left_position_indices = calloc(left_num_vals, sizeof(int));
    // int* right_position_indices = calloc(right_num_vals, sizeof(int));

    // execute hash_join on each partition
    for (num_partition = 0; num_partition < num_partitions; num_partition++) {
        // create hashtable on smaller partition
        if (left_partition_sizes[num_partition] < right_partition_sizes[num_partition]) {
            hash_join(
                left_val_partitions[num_partition], left_pos_partitions[num_partition], left_partition_sizes[num_partition],
                right_val_partitions[num_partition], right_pos_partitions[num_partition], right_partition_sizes[num_partition],
                left_result, right_result, num_results
            );
        } else {
            hash_join(
                right_val_partitions[num_partition], right_pos_partitions[num_partition], right_partition_sizes[num_partition],
                left_val_partitions[num_partition], left_pos_partitions[num_partition], left_partition_sizes[num_partition],
                right_result, left_result, num_results
            );
        }

        // free memory
        free(left_val_partitions[num_partition]);
        free(left_pos_partitions[num_partition]);
        free(right_val_partitions[num_partition]);
        free(right_pos_partitions[num_partition]);
    }

    /* // now need to get actual positions from position indices */
    /* // sort position indices */
    /* quick_sort_double(left_position_indices, right_position_indices, 0, *num_results - 1); */

    /* // add all actual positions to results */
    /* for (int i = 0; i < *num_results; i++) { */
    /*     // printf("%d,%d\n", left_positions[left_position_indices[i]], right_positions[right_position_indices[i]]); */
    /*     left_result[i] = left_positions[left_position_indices[i]]; */
    /*     right_result[i] = right_positions[right_position_indices[i]]; */
    /* } */

    // free memory
    free(left_val_partitions);
    free(left_pos_partitions);
    free(left_partition_sizes);
    // free(left_position_indices);
    free(right_val_partitions);
    free(right_pos_partitions);
    free(right_partition_sizes);
    // free(right_position_indices);
}


/**
 * Executes a JOIN db operator.
 */
void exeucte_join_operator(DbOperator* query, Status* status) {
    // make sure two handles to store results in
    if (query->num_handles != 2) {
        status->code = INCORRECT_FORMAT;
        return;
    }

    JoinOperator operator = query->operator_fields.join_operator;

    // get all result objects
    Result* pos_1 = operator.pos_1;
    Result* val_1 = operator.val_1;
    Result* pos_2 = operator.pos_2;
    Result* val_2 = operator.val_2;

    // get indice and data arrs
    int* left_vals = (int*) val_1->payload;
    int left_num_vals = val_1->num_tuples;
    int* left_positions = (int*) pos_1->payload;

    // same for right
    int* right_vals = (int*) val_2->payload;
    int right_num_vals = val_2->num_tuples;
    int* right_positions = (int*) pos_2->payload;

    // init results arr
    int* left_result_pos = NULL;
    int* right_result_pos = NULL;
    int* num_results = calloc(1, sizeof(int));

    int right_smaller = 0;
    if (left_num_vals > right_num_vals) {
        left_result_pos = calloc(right_num_vals, sizeof(int));
        right_result_pos = calloc(right_num_vals, sizeof(int));
        right_smaller = 1;
    } else {
        left_result_pos = calloc(left_num_vals, sizeof(int));
        right_result_pos = calloc(left_num_vals, sizeof(int));
    }

    // check which type of join
    if (operator.type == NESTED_LOOP) {
        // if left is smaller use left as outer else use right as outer
        if (right_smaller) {
            nested_loop_join(
                right_vals, right_positions, right_num_vals,
                left_vals, left_positions, left_num_vals,
                right_result_pos, left_result_pos, num_results
            );
        } else {
            nested_loop_join(
                left_vals, left_positions, left_num_vals,
                right_vals, right_positions, right_num_vals,
                left_result_pos, right_result_pos, num_results
            );
        }
    } else {
        // one pass hash join
        if (0) {
            // if left is smaller use left as outer else use right as outer
            if (right_smaller) {
                hash_join(
                    right_vals, right_positions, right_num_vals,
                    left_vals, left_positions, left_num_vals,
                    right_result_pos, left_result_pos, num_results
                );
            } else {
                hash_join(
                    left_vals, left_positions, left_num_vals,
                    right_vals, right_positions, right_num_vals,
                    left_result_pos, right_result_pos, num_results
                );
            }
        // grace hash join
        } else {
            grace_hash_join(
                left_vals, left_positions, left_num_vals,
                right_vals, right_positions, right_num_vals,
                left_result_pos, right_result_pos, num_results
            );
        }
    }

    // realloc results
    left_result_pos = realloc(left_result_pos, sizeof(int) * *num_results);
    right_result_pos = realloc(right_result_pos, sizeof(int) * *num_results);

    // create new Result objects and store in chandles for results
    Result* left_result = malloc(sizeof(Result));
    Result* right_result = malloc(sizeof(Result));

    left_result->data_type = INT;
    left_result->num_tuples = *num_results;
    left_result->payload = (void*) left_result_pos;

    right_result->data_type = INT;
    right_result->num_tuples = *num_results;
    right_result->payload = (void*) right_result_pos;

    // store in chandles
    CHandle* left_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);
    CHandle* right_chandle = lookup_object(query->client_lookup_table, query->handle_names[1], RESULT);

    left_chandle->pointer.result = left_result;
    right_chandle->pointer.result = right_result;

    status->code = OK_DONE;
}

/**
 * Given table, positions and number of positions
 * remove rows from table.
 **/
void execute_delete(Table* table, int* positions, int num_positions) {
    // loop through cols in table, deleting vals and updating indexes
    for (size_t col_num = 0; col_num < table->col_count; col_num++) {
        Column* col = &table->columns[col_num];

        UnclusteredIndex* unclustered_index = NULL;
        BPTreeNode* btree_index = NULL;

        if (col->index_type == SORTED_UNCLUSTERED) {
            unclustered_index = (UnclusteredIndex*) col->index;
        } else if (col->index_type != SORTED_CLUSTERED) {
            btree_index = (BPTreeNode*) col->index;
        }

        // move everything in main copy of data greater than each pos over
        for (int pos_i = 0; pos_i < num_positions; pos_i++) {
            int pos = positions[pos_i];
            int val = col->data[pos];
            for (size_t i = pos; i < col->col_size; i++) {
                col->data[i] = col->data[i + 1];
            }

            // check index
            if (unclustered_index != NULL) {
                // for sorted unclustered remove position from array
                for (size_t i = 0; i < col->col_size; i++) {
                    if (unclustered_index->positions[i] == pos) {
                        // update index
                        remove_pos_and_update(unclustered_index->values, unclustered_index->positions, col->col_size, pos);
                        break;
                    }
                }
            } else if (btree_index != NULL) {
                // need to remove value from btree and update positions
                bplus_remove(btree_index, val, pos);
            }

            // subtract one from size
            col->col_size -= 1;
        }
    }

    // subtract from table length
    table->table_length -= num_positions;
}


/**
 * Executes a DELETE db operator.
 */
void exeucte_delete_operator(DbOperator* query, Status* status) {
    // get fields
    Table* table = query->operator_fields.delete_operator.table;
    Result* pos_result = query->operator_fields.delete_operator.positions;

    int* positions = (int*) pos_result->payload;
    execute_delete(table, positions, pos_result->num_tuples);

    status->code = OK_DONE;
}


/**
 * Executes an UPDATE db operator.
 */
void exeucte_update_operator(DbOperator* query, Status* status) {
    // get fields
    Table* table = query->operator_fields.update_operator.table;
    Column* update_column = query->operator_fields.update_operator.column;
    Result* pos_result = query->operator_fields.update_operator.positions;
    int update_val = query->operator_fields.update_operator.update_val;

    // get all current vals
    int** all_vals = calloc(pos_result->num_tuples, sizeof(int*));

    int* positions = (int*) pos_result->payload;
    for (size_t pos_i = 0; pos_i < pos_result->num_tuples; pos_i++) {
        int* vals = calloc(table->col_count, sizeof(int));

        int pos = positions[pos_i];
        for (size_t col_num = 0; col_num < table->col_count; col_num++) {
            Column* column = &table->columns[col_num];

            // if on col being updated, insert update val
            if (column == update_column) {
                vals[col_num] = update_val;
            // else put current val
            } else {
                vals[col_num] = column->data[pos];                
            }
        }

        all_vals[pos_i] = vals;
    }
    
    // execute delete
    execute_delete(table, positions, pos_result->num_tuples);

    // execute insert for each row
    for (size_t pos_i = 0; pos_i < pos_result->num_tuples; pos_i++) {
        execute_insert(table, all_vals[pos_i], status);
    }

    status->code = OK_DONE;
}


/**
 * Simple query handler.
 **/
void handle_db_operator(DbOperator* query, Status* status) {
    switch (query->type) {
        case CREATE:
            execute_create_operator(query, status);
            break;
        case INSERT:
            execute_insert_operator(query, status);
            break;
        case SELECT:
            execute_select_operator(query, status);
            break;
        case FETCH:
            execute_fetch_operator(query, status);
            break;
        case PRINT:
            execute_print_operator(query, status);
            break;
        case AGGREGATE:
            execute_aggregate_operator(query, status);
            break;
        case JOIN:
            exeucte_join_operator(query, status);
            break;
        case UPDATE:
            exeucte_update_operator(query, status);
            break;
        case DELETE:
            exeucte_delete_operator(query, status);
            break;
        case SHUTDOWN:
            shutdown_server(status);
            break;
        case BATCH_QUERIES:
            batching = 1;
        default:
            break;
    }
}


/**
 * Loops through batched queries calling db_operator_free for each.
 **/
void free_batched_queries() {
    for (int i=0; i < num_batched_queries; i++) {
        DbOperator* dbo = batched_queries[i];
        db_operator_free(dbo);
    }
    free(batched_queries);
    batched_queries = NULL;
}


/**
 * Executes batched queries. Implements shared scans for selects on
 * the same data and executes queries on multiple threads to
 * parallelize work.
 **/
void execute_batched_queries(Status* status) {
    if (num_batched_queries) {
        // if just 1 query, just execute
        if (num_batched_queries == 1) {
            handle_db_operator(batched_queries[0], status);
        // if > 20 queries, simply execute shared scan
        } else if (1 || num_batched_queries > 20) {
            execute_shared_select_operator(batched_queries, num_batched_queries, status);
        // else parallelize queries
        // this splits the queries in groups of 4 and parallelizes their processing
        } else if (0) {
            int query_chunk_size = 6;

            // allocatx space for threads
            size_t num_threads = num_batched_queries / query_chunk_size;
            if (num_batched_queries % query_chunk_size != 0) {
                num_threads++;
            }
            pthread_t* threads = calloc(num_threads, sizeof(pthread_t));
            sharedSelectParams** params = calloc(num_threads, sizeof(sharedSelectParams*));

            for (int idx = 0; idx < num_batched_queries; idx += query_chunk_size) {
                // create params instance
                sharedSelectParams* select_params = malloc(sizeof(sharedSelectParams));
                select_params->queries = &batched_queries[idx];
                select_params->status = status;

                if (idx > num_batched_queries - query_chunk_size) {
                    select_params->num_queries = num_batched_queries - idx;
                } else {
                    select_params->num_queries = query_chunk_size;
                }

                // create thread to run shared scan for 2 queries
                pthread_create(&threads[idx / query_chunk_size], NULL, execute_shared_select_operator_wrapper, select_params);
                params[idx / query_chunk_size] = select_params;
            }

            // join threads
            for (size_t idx = 0; idx < num_threads; idx++) {
                pthread_join(threads[idx], NULL);
            }

            // free memory
            for (size_t idx = 0; idx < num_threads; idx++) {
                free(params[idx]);
            }
            free(threads);
            free(params);

        // this splits the data into chunks, then has all queries execute
        // a shared scan on each chunk, then reconstructs tuples at end
        } else {
            // get data
            int* data = batched_queries[0]->operator_fields.select_operator.chandle_1->pointer.column->data;
            int num_items = batched_queries[0]->operator_fields.select_operator.chandle_1->pointer.column->col_size;

            // split among threads
            int num_threads = 4;

            int chunk_size = num_items / num_threads;
            pthread_t* threads = calloc(num_threads, sizeof(pthread_t));
            chunkedParams** params = calloc(num_threads, sizeof(chunkedParams*));

            // get all comparators and min and max
            Comparator* comparators = malloc(sizeof(Comparator) * num_batched_queries);

            long* min = NULL;
            long* max = NULL;

            for (int num_q = 0; num_q < num_batched_queries; num_q++) {
                comparators[num_q] = batched_queries[num_q]->operator_fields.select_operator.comparator;

                if (comparators[num_q].type1 != NO_COMPARISON && (min == NULL || *min > comparators[num_q].p_low))
                    min = &comparators[num_q].p_low;

                if (comparators[num_q].type2 != NO_COMPARISON && (max == NULL || *max < comparators[num_q].p_high))
                    max = &comparators[num_q].p_high;
            }

            // check for null min/max
            if (min == NULL) {
                *min = INT_MIN;
            }
            if (max == NULL) {
                *max = INT_MAX;
            }

            // initialize list to hold results
            all_results = calloc(num_threads, sizeof(int*));
            all_results_counts = calloc(num_threads, sizeof(int*));
            for (int idx = 0; idx < num_threads; idx++) {
                chunkedParams* chunk_params = malloc(sizeof(chunkedParams));
                chunk_params->comparators = comparators;
                chunk_params->num_items = chunk_size;
                chunk_params->data = &data[idx * chunk_size];
                chunk_params->min = min;
                chunk_params->max = max;

                chunk_params->num_thread = idx;
                if (idx == num_threads - 1 && num_items % num_threads != 0) {
                    chunk_params->num_items = num_items - (idx * num_items);
                }
                // create thread to run shared scan for 2 queries
                pthread_create(&threads[idx], NULL, shared_select_chunk_wrapper, chunk_params);
                params[idx] = chunk_params;
            }

            // join threads
            for (int idx = 0; idx < num_threads; idx++) {
                pthread_join(threads[idx], NULL);
            }

            Result** results = malloc(sizeof(Result*) * num_batched_queries);
            for (int num_q = 0; num_q < num_batched_queries; num_q++) {
                results[num_q] = malloc(sizeof(Result));
                results[num_q]->data_type = INT;
                results[num_q]->num_tuples = 0;

                for (int num_thread=0; num_thread < num_threads; num_thread++) {
                    results[num_q]->num_tuples += all_results_counts[num_thread][num_q];
                }
                results[num_q]->payload = calloc(results[num_q]->num_tuples, sizeof(int));
            }

            // reconstruct tuples
            int* curr_counts = calloc(num_batched_queries, sizeof(int));
            for (int num_thread=0; num_thread < num_threads; num_thread++) {
                for (int num_q = 0; num_q < num_batched_queries; num_q++) {
                    int thread_num_tuples = all_results_counts[num_thread][num_q];
                    if (thread_num_tuples) {
                        void* ptr = ((int*) results[num_q]->payload) + curr_counts[num_q];
                        memcpy(ptr, (void*) all_results[num_thread][num_q], thread_num_tuples * sizeof(int));
                        free(all_results[num_thread][num_q]);
                        curr_counts[num_q] += thread_num_tuples;
                    }
                }
                free(all_results[num_thread]);
                free(all_results_counts[num_thread]);
            }

            // free memory
            free(all_results);
            free(all_results_counts);

            // set results
            for (int num_q = 0; num_q < num_batched_queries; num_q++) {
                DbOperator* query = batched_queries[num_q];
                CHandle* res_chandle = lookup_object(query->client_lookup_table, query->handle_names[0], RESULT);
                res_chandle->pointer.result = results[num_q];
            }

            // free memory
            for (int idx = 0; idx < num_threads; idx++) {
                free(params[idx]);
            }
            free(threads);
            free(params);
        }
    }
}


/** execute_db_operator takes as input the DbOperator and executes the query.
 **/
void execute_db_operator(DbOperator* query, Status* status) {
    // create CHandle objects
    if (query->num_handles) {
        for (unsigned int i=0; i < query->num_handles; i++) {
            // malloc space for new CHandle
            CHandle* chandle = malloc(sizeof(CHandle));
            strcpy(chandle->name, query->handle_names[i]);
            chandle->type = RESULT;

            // insert into lookup table
            insert_object(query->client_lookup_table, chandle->name, (void*) chandle, RESULT);
        }
    }

    if (status->code == OK_WAIT_FOR_RESPONSE) {
        if (!batching) {
            // temp to time how long this takes
            clock_t start, end;
            double cpu_time_used;
            start = clock();

            handle_db_operator(query, status);

            if (query->type == JOIN) {
                end = clock();
                cpu_time_used = (double) (end - start);
                printf("operation time used: %f\n", cpu_time_used);
            }

            db_operator_free(query);
        } else {
            switch (query->type) {
                case SHUTDOWN: {
                    db_operator_free(query);
                    free_batched_queries();
                    shutdown_server(status);
                    break;
                } case BATCH_EXECUTE: {
                    execute_batched_queries(status);
                    db_operator_free(query);

                    free_batched_queries();
                    num_batched_queries = 0;
                    batching = 0;

                    break;
                } default: {
                    batched_queries = realloc(batched_queries, sizeof(DbOperator*) * ++num_batched_queries);
                    batched_queries[num_batched_queries - 1] = query;
                    break;
                }
            }
        }
    }
}
