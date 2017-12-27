/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */
#define _XOPEN_SOURCE
#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, Status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        status->code = INCORRECT_FORMAT;
    }
    return token;
}


/**
 * parse_delete reads arguments for an delete query, then validates
 * those args and creates a DbOperator to be executed
 */
DbOperator* parse_delete(char* delete_arguments, LookupTable* client_lookup_table, Status* status) {
    // strip join_arguments of parens
    delete_arguments = trim_parenthesis(delete_arguments);

    // get args
    char positions_name[MAX_SIZE_NAME];
    char tbl_name[MAX_SIZE_NAME * 2];

    unsigned int num_args = sscanf(delete_arguments, "%[^,],%[^,]", tbl_name, positions_name);

    if (num_args != 2) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    // look up all objects
    CHandle* positions_chandle = (CHandle*) lookup_object(client_lookup_table, positions_name, RESULT);
    Table* tbl = (Table*) lookup_object(db_catalog, tbl_name, TABLE);

    if (positions_chandle == NULL || tbl == NULL) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return NULL;
    }

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = DELETE;
    dbo->operator_fields.delete_operator.positions = positions_chandle->pointer.result;
    dbo->operator_fields.delete_operator.table = tbl;

    return dbo;
}


/**
 * parse_update reads arguments for an update query, then validates
 * those args and creates a DbOperator to be executed
 */
DbOperator* parse_update(char* update_arguments, LookupTable* client_lookup_table, Status* status) {
    // strip join_arguments of parens
    update_arguments = trim_parenthesis(update_arguments);

    // get args
    char positions_name[MAX_SIZE_NAME];
    char col_name[MAX_SIZE_NAME * 3];
    int update_val;

    unsigned int num_args = sscanf(update_arguments, "%[^,],%[^,],%d", col_name, positions_name, &update_val);

    if (num_args != 3) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    // look up all objects
    CHandle* positions_chandle = (CHandle*) lookup_object(client_lookup_table, positions_name, RESULT);
    CHandle* col_chandle = (CHandle*) lookup_object(db_catalog, col_name, COLUMN);

    // get table name from col name
    char tbl_name[MAX_SIZE_NAME], db_name[MAX_SIZE_NAME];
    sscanf(col_name, "%[^.].%[^.]", db_name, tbl_name);

    char full_table_name[strlen(tbl_name) + strlen(db_name) + 2];
    strcpy(full_table_name, db_name);
    strcat(full_table_name, ".");
    strcat(full_table_name, tbl_name);    

    Table* table = (Table*) lookup_object(db_catalog, full_table_name, TABLE);

    if (positions_chandle == NULL || col_chandle == NULL || table == NULL) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return NULL;
    }

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = UPDATE;
    dbo->operator_fields.update_operator.table = table;
    dbo->operator_fields.update_operator.positions = positions_chandle->pointer.result;
    dbo->operator_fields.update_operator.column = col_chandle->pointer.column;
    dbo->operator_fields.update_operator.update_val = update_val;

    return dbo;
}


/**
 * parse_join reads arguments for a join query, then validates
 * those args and creates a DbOperator to be executed
 */
DbOperator* parse_join(char* join_arguments, LookupTable* client_lookup_table, Status* status) {
    // strip join_arguments of parens
    join_arguments = trim_parenthesis(join_arguments);

    // get required 5 args
    char pos_1_name[MAX_SIZE_NAME];
    char pos_2_name[MAX_SIZE_NAME];
    char val_1_name[MAX_SIZE_NAME];
    char val_2_name[MAX_SIZE_NAME];
    char join_type_name[20];

    unsigned int num_args = sscanf(join_arguments, "%[^,],%[^,],%[^,],%[^,],%[^,]", val_1_name, pos_1_name, val_2_name, pos_2_name, join_type_name);

    if (num_args != 5) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    // look up all objects
    CHandle* pos_1_chandle = (CHandle*) lookup_object(client_lookup_table, pos_1_name, RESULT);
    CHandle* val_1_chandle = (CHandle*) lookup_object(client_lookup_table, val_1_name, RESULT);
    CHandle* pos_2_chandle = (CHandle*) lookup_object(client_lookup_table, pos_2_name, RESULT);
    CHandle* val_2_chandle = (CHandle*) lookup_object(client_lookup_table, val_2_name, RESULT);

    if (pos_1_chandle == NULL || val_1_chandle == NULL || pos_2_chandle == NULL || val_2_chandle == NULL) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return NULL;
    }

    JoinType join_type = 0;
    // check type
    if (strcmp(join_type_name, "hash") == 0) {
        join_type = HASH;
    } else if (strcmp(join_type_name, "nested-loop") == 0) {
        join_type = NESTED_LOOP;
    } else {
        status->code = UNKNOWN_COMMAND;
        return NULL;
    }

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = JOIN;
    dbo->operator_fields.join_operator.pos_1 = pos_1_chandle->pointer.result;
    dbo->operator_fields.join_operator.val_1 = val_1_chandle->pointer.result;
    dbo->operator_fields.join_operator.pos_2 = pos_2_chandle->pointer.result;
    dbo->operator_fields.join_operator.val_2 = val_2_chandle->pointer.result;
    dbo->operator_fields.join_operator.type = join_type;

    return dbo;
}


/**
 * parse_aggregate reads arguments for an aggregate query, then validates
 * those args and creates a DbOperator to be executed.
 */
DbOperator* parse_aggregate(char* aggregate_arguments, LookupTable* client_lookup_table, AggregateType type, Status* status) {
    // strip aggregate_arguments of parens
    aggregate_arguments = trim_parenthesis(aggregate_arguments);

    // get two args
    char arg1[MAX_SIZE_NAME];
    char arg2[MAX_SIZE_NAME];

    unsigned int num_args = sscanf(aggregate_arguments, "%[^,],%[^,]", arg1, arg2);

    if (num_args == 0 || ((type == ADD || type == SUB) && num_args == 1)) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    CHandle* chandle_1 = NULL;
    CHandle* chandle_2 = NULL;
    LookupType chandle_type_1 = COLUMN;
    LookupType chandle_type_2 = COLUMN;

    // get first chandle
    chandle_1 = lookup_object(db_catalog, arg1, chandle_type_1);
    // if NULL must be a result column
    if (chandle_1 == NULL) {
        chandle_type_1 = RESULT;
        chandle_1 = lookup_object(client_lookup_table, arg1, chandle_type_1);

        // if NULL err
        if (chandle_1 == NULL) {
            status->code = OBJECT_DOES_NOT_EXIST;
            return NULL;
        }
    }

    // if two args get second chandle
    if (num_args == 2) {
        chandle_2 = lookup_object(db_catalog, arg2, chandle_type_2);
        // if NULL must be a result column
        if (chandle_2 == NULL) {
            chandle_type_2 = RESULT;
            chandle_2 = lookup_object(client_lookup_table, arg2, chandle_type_2);

            // if NULL err
            if (chandle_2 == NULL) {
                status->code = OBJECT_DOES_NOT_EXIST;
                return NULL;
            }
        }
    }

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = AGGREGATE;
    dbo->operator_fields.aggregate_operator.type = type;
    dbo->operator_fields.aggregate_operator.chandle_1 = chandle_1;
    dbo->operator_fields.aggregate_operator.chandle_2 = chandle_2;

    return dbo;
}


/**
 * parse_print reads arguments to print, then creates
 * DbOperator to execute print.
 **/
DbOperator* parse_print(char* print_arguments, Status* status) {
    // strip print_arguments of parens
    print_arguments = trim_parenthesis(print_arguments);
    
    unsigned int num_fields = 0;
    char* token = NULL;
    DbOperator* dbo = NULL;

    char** command_index = &print_arguments;

    // make print operator. 
    dbo = calloc(1, sizeof(DbOperator));
    dbo->type = PRINT;

    // init fields to null ptr
    dbo->operator_fields.print_operator.fields = NULL;

    // parse inputs until we reach the end.. 
    while ((token = strsep(command_index, ",")) != NULL) {
        // allocate more space for field name             
        dbo->operator_fields.print_operator.fields = realloc(dbo->operator_fields.print_operator.fields, sizeof(char*) * (num_fields + 1));
        dbo->operator_fields.print_operator.fields[num_fields] = token;
        num_fields++;
    }

    if (!num_fields) {
        free(dbo);
        status->code = OK_DONE;
        return NULL;
    }

    // set num_fields
    dbo->operator_fields.print_operator.num_fields = num_fields;
    return dbo;
}

/**
 * parse_fetch reads arguments for a fetch query, then validates
 * those args and creates a DbOperator to be executed
 */
DbOperator* parse_fetch(char* fetch_arguments, LookupTable* client_lookup_table, Status* status) {
    // strip fetch_arguments of parens
    fetch_arguments = trim_parenthesis(fetch_arguments);

    // get two args
    char col_name[MAX_SIZE_NAME];
    char res_name[MAX_SIZE_NAME];

    unsigned int num_args = sscanf(fetch_arguments, "%[^,],%[^,]", col_name, res_name);

    if (num_args != 2) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    CHandle* col_chandle = (CHandle*) lookup_object(db_catalog, col_name, COLUMN);
    CHandle* res_chandle = (CHandle*) lookup_object(client_lookup_table, res_name, RESULT);

    // if either is null return
    if (col_chandle == NULL || res_chandle == NULL) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return NULL;
    }

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = FETCH;
    dbo->operator_fields.fetch_operator.column = col_chandle->pointer.column;
    dbo->operator_fields.fetch_operator.result = res_chandle->pointer.result;

    return dbo;
}


/**
 * parse_select reads arguments for a select query, then validates
 * those args and creates a DbOperator to be executed
 */
DbOperator* parse_select(char* select_arguments, LookupTable* client_lookup_table, Status* status) {
    // strip select_arguments of parens
    select_arguments = trim_parenthesis(select_arguments);

    // read args
    char arg1[MAX_SIZE_NAME], arg2[MAX_SIZE_NAME], arg3[MAX_SIZE_NAME], arg4[MAX_SIZE_NAME];
    int num_args = sscanf(select_arguments, "%[^,],%[^,],%[^,],%[^,]", arg1, arg2, arg3, arg4);

    if (num_args < 3) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    CHandle* chandle_1 = NULL;
    CHandle* chandle_2 = NULL;

    // fields for Comparator
    long int p_low = 0;
    long int p_high = 0;
    ComparatorType type1 = NO_COMPARISON;
    ComparatorType type2 = NO_COMPARISON;

    // if only 3 args, chandle should be col
    if (num_args == 3) {
        chandle_1 = (CHandle*) lookup_object(db_catalog, arg1, COLUMN);

        if (chandle_1 == NULL) {
            status->code = OBJECT_DOES_NOT_EXIST;
            return NULL;
        }

        if (strcmp(arg2, "null") != 0) {
            type1 = GREATER_THAN_OR_EQUAL;            
            p_low = atoi(arg2);
        }

        if (strcmp(arg3, "null") != 0) {
            type2 = LESS_THAN;
            p_high = atoi(arg3);
        }
    // else should be two chandles for result cols
    } else {
        chandle_1 = (CHandle*) lookup_object(client_lookup_table, arg1, RESULT);
        chandle_2 = (CHandle*) lookup_object(client_lookup_table, arg2, RESULT);

        if (chandle_1 == NULL || chandle_2 == NULL) {
            status->code = OBJECT_DOES_NOT_EXIST;
            return NULL;
        }

        if (strcmp(arg3, "null") != 0) {
            type1 = GREATER_THAN_OR_EQUAL;
            p_low = atoi(arg3);
        }

        if (strcmp(arg4, "null") != 0) {
            type2 = LESS_THAN;
            p_high = atoi(arg4);
        }
    }

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = SELECT;
    dbo->operator_fields.select_operator.chandle_1 = chandle_1;
    dbo->operator_fields.select_operator.chandle_2 = chandle_2;
    dbo->operator_fields.select_operator.comparator.p_low = p_low;
    dbo->operator_fields.select_operator.comparator.p_high = p_high;
    dbo->operator_fields.select_operator.comparator.type1 = type1;
    dbo->operator_fields.select_operator.comparator.type2 = type2;
    return dbo;   
}

/**
 * parse_insert reads in the arguments for a insert statement and 
 * then created a DbOperator to execute the insert later.
 **/
DbOperator* parse_insert(char* insert_arguments, Status* status) {
    // strip insert_arguments of parens
    insert_arguments = trim_parenthesis(insert_arguments);
    char** command_index = &insert_arguments;

    // parse table input
    char* table_name = next_token(command_index, status);
    if (status->code == INCORRECT_FORMAT) {
        return NULL;
    }

    // lookup table
    Table* table = (Table*) lookup_object(db_catalog, table_name, TABLE);
    if (table == NULL) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return NULL;
    }

    // make sure table is full
    if (table->col_capacity != table->col_count) {
        status->code = QUERY_UNSUPPORTED;
        return NULL;
    }
    // make insert operator. 
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = INSERT;
    dbo->operator_fields.insert_operator.table = table;
    dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * table->col_capacity);
    
    unsigned int columns_inserted = 0;
    char* token = NULL;

    // parse inputs until we reach the end. Turn each given string into an integer. 
    while ((token = strsep(command_index, ",")) != NULL) {
        int insert_val = atoi(token);
        dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
        columns_inserted++;
    }

    // check that we received the correct number of input values
    if (columns_inserted != table->col_capacity) {
        status->code = INCORRECT_FORMAT;
    }

    return dbo;
}


/**
 * Parses args for creating indexes. 
 **/
DbOperator* parse_create_idx(char* create_arguments, Status* status) {
    // args needed 
    char col_name[MAX_SIZE_NAME], index[strlen("sorted") + 1], clustered_arg[strlen("unclustered") + 1];
    int num_args = sscanf(create_arguments, "%[^,],%[^,],%[^,]", col_name, index, clustered_arg);

    // if didnt get all args
    if (num_args != 3) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    IndexType index_type = NONE;

    // get index type 
    if (strcmp(index, "sorted") == 0) {
        if (strcmp(clustered_arg, "clustered") == 0) {
            index_type = SORTED_CLUSTERED;
        } else if (strcmp(clustered_arg, "unclustered") == 0) {
            index_type = SORTED_UNCLUSTERED;
        }
    } else if (strcmp(index, "btree") == 0) {
        if (strcmp(clustered_arg, "clustered") == 0) {
            index_type = BTREE_CLUSTERED;
        } else if (strcmp(clustered_arg, "unclustered") == 0) {
            index_type = BTREE_UNCLUSTERED;
        }
    }

    // if no index type, args are invalid
    if (index_type == NONE) {
        status->code = UNKNOWN_COMMAND;
        return NULL;
    }

    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = CREATE;
    strcpy(dbo->operator_fields.create_operator.col_name, col_name);  
    dbo->operator_fields.create_operator.index_type = index_type;
    dbo->operator_fields.create_operator.type = CREATE_IDX;

    return dbo;
}


/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates 
 * a DbOperator to create a column.
 **/
DbOperator* parse_create_col(char* create_arguments, Status* status) {
    // args needed 
    char table_name[2 * MAX_SIZE_NAME], col_name[MAX_SIZE_NAME];
    int num_args = sscanf(create_arguments, "%*[\"]%[^\"]\",%[^,]", col_name, table_name);

    // if didnt get all args
    if (num_args != 2) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = CREATE;
    strcpy(dbo->operator_fields.create_operator.col_name, col_name);  
    strcpy(dbo->operator_fields.create_operator.table_name, table_name);  
    dbo->operator_fields.create_operator.type = CREATE_COL;

    return dbo;
}


/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a 
 * DbOperator that will execute table creation.
 **/

DbOperator* parse_create_tbl(char* create_arguments, Status* status) {
    // args needed 
    char table_name[MAX_SIZE_NAME], db_name[MAX_SIZE_NAME];
    int col_capacity;
    int num_args = sscanf(create_arguments, "%*[\"]%[^\"]\",%[^,],%d", table_name, db_name, &col_capacity);

    // if didnt get all args
    if (num_args != 3) {
        status->code = INCORRECT_FORMAT;
        return NULL;
    }

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = CREATE;
    strcpy(dbo->operator_fields.create_operator.db_name, db_name);  
    strcpy(dbo->operator_fields.create_operator.table_name, table_name);  
    dbo->operator_fields.create_operator.type = CREATE_TBL;
    dbo->operator_fields.create_operator.col_capacity = col_capacity;

    return dbo;
}


/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a DbOperator
 * that will be executed.
 **/
DbOperator* parse_create_db(char* db_name, Status* status) {
    // not enough arguments if db_name is NULL
    if (db_name == NULL) {
        status->code = INCORRECT_FORMAT;
        return NULL;                    
    }

    // trim quotes and check for finishing parenthesis.
    db_name = trim_quotes(db_name);

    // create DbOperator
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = CREATE;
    strcpy(dbo->operator_fields.create_operator.db_name, db_name);  
    dbo->operator_fields.create_operator.type = CREATE_DB;

    return dbo;
}


/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments, Status* status) {
    // strip create_arguments of parens
    create_arguments = trim_parenthesis(create_arguments);

    // Since strsep destroys input, we create a copy of our input. 
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    strcpy(tokenizer_copy, create_arguments);
    
    // token stores first argument. Tokenizer copy now points to just past first ","
    char *token;
    token = next_token(&tokenizer_copy, status);

    DbOperator* dbo = NULL;
    if (status->code == INCORRECT_FORMAT) {
        return NULL;
    } else {
        // pass off to next parse function. 
        if (strcmp(token, "db") == 0) {
            dbo = parse_create_db(tokenizer_copy, status);
        } else if (strcmp(token, "tbl") == 0) {
            dbo = parse_create_tbl(tokenizer_copy, status);
        } else if (strcmp(token, "col") == 0) {
            dbo = parse_create_col(tokenizer_copy, status);
        } else if (strcmp(token, "idx") == 0) {
            dbo = parse_create_idx(tokenizer_copy, status);
        } else {
            status->code = UNKNOWN_COMMAND;
        }
    }

    free(to_free);
    return dbo;
}


/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(char* query_command, Status* status, LookupTable* client_lookup_table, int client_socket) {
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator)); // calloc?

    if (strncmp(query_command, "--", 2) == 0) {
        // The -- signifies a comment line, no operator needed.  
        status->code = OK_DONE;
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    unsigned int num_handles = 0;
    char handle_names[2][64];

    if (equals_pointer != NULL) {
        *equals_pointer = '\0';

        // handle exists, store here. 
        char* handle_name = next_token(&handle, status);
        while (handle_name != NULL) {
            num_handles++;
            strcpy(handle_names[num_handles - 1], handle_name);
            handle_name = next_token(&handle, status);            
        }
        query_command = ++equals_pointer;

    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    status->code = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command, status);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, status);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, client_lookup_table, status);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, client_lookup_table, status);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, status);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, client_lookup_table, MIN, status);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, client_lookup_table, MAX, status);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, client_lookup_table, SUM, status);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, client_lookup_table, AVG, status);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, client_lookup_table, ADD, status);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, client_lookup_table, SUB, status);
    } else if (strncmp(query_command, "join", 4) == 0) {
        query_command += 4;
        dbo = parse_join(query_command, client_lookup_table, status);
    } else if (strncmp(query_command, "relational_delete", 17) == 0) {
        query_command += 17;
        dbo = parse_delete(query_command, client_lookup_table, status);
    } else if (strncmp(query_command, "relational_update", 17) == 0) {
        query_command += 17;
        dbo = parse_update(query_command, client_lookup_table, status);
    } else if (strcmp(query_command, "shutdown") == 0) {
        dbo = calloc(1, sizeof(DbOperator));
        dbo->type = SHUTDOWN;
    } else if (strcmp(query_command, "batch_queries()") == 0) {
        dbo = calloc(1, sizeof(DbOperator));
        dbo->type = BATCH_QUERIES;
    } else if (strcmp(query_command, "batch_execute()") == 0) {
        dbo = calloc(1, sizeof(DbOperator));
        dbo->type = BATCH_EXECUTE;
    } else {
        status->code = UNKNOWN_COMMAND;
    }

    if (dbo == NULL) {
        return dbo;
    }

    dbo->client_lookup_table = client_lookup_table;
    dbo->client_fd = client_socket;
    dbo->num_handles = num_handles;
    for (unsigned int i=0; i < num_handles; i++) {
        strcpy(dbo->handle_names[i], handle_names[i]);
    }

    return dbo;
}
