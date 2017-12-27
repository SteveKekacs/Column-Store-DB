#define _XOPEN_SOURCE
#define _BSD_SOURCE
#define _GNU_SOURCE

#include <string.h>
#include <sys/socket.h>

#include "db_operator.h"
#include "cs165_api.h"
#include "utils.h"
#include "index.h"

// In this class, there will always be only one active database at a time
Db* current_db;
LookupTable* db_catalog;


const size_t TABLE_CAPACITY = 10;
const size_t INITIAL_TABLE_LENGTH_CAPACITY = 100000;


/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_line_token(char** tokenizer, Status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        status->code = INCORRECT_FORMAT;
    }
    return token;
}


/* 
 * This method creates a database object.
 * It checks to make sure no other db exists,
 * then allocates space for obj and sets
 * current_db.
 */
void create_db(const char* db_name, Status* status) {
    // first make sure db doesn't exist
    if (current_db != NULL) {
        status->code = OBJECT_ALREADY_EXISTS;
        return;
    }

    // allocate memory for new db
    current_db = calloc(1, sizeof(Db));

    // set initial vars
    strcpy(current_db->name, db_name);

    current_db->tables_capacity = TABLE_CAPACITY;
    current_db->tables_size = 0;

    // allocate space for tables
    current_db->tables = calloc(TABLE_CAPACITY, sizeof(Table));

    // now create catalog
    db_catalog = init_lookup_table();

    status->code = OK_DONE;
}


/** 
 * This method creates a table.
 * It checks to make sure all arguments are valid,
 * then adds the table to the Db.
 * 
 * Returns created table object.
 **/
Table* create_table(const char* name, const char* db_name, unsigned int col_capacity, Status* status) {
    // make sure current_db
    if (current_db == NULL || strcmp(current_db->name, db_name) != 0) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return NULL;
    }

    // check if name is not already used
    char lookup_name[strlen(db_name) + strlen(name) + 2];
    strcpy(lookup_name, db_name);
    strcat(lookup_name, ".");
    strcat(lookup_name, name);
    lookup_name[strlen(db_name) + strlen(name) + 1] = '\0';
    if (lookup_object(db_catalog, lookup_name, TABLE) != NULL) {
        status->code = OBJECT_ALREADY_EXISTS;
        return NULL;
    }

    // need to check if tables is at capcity
    if (current_db->tables_size == current_db->tables_capacity) {
        // increase memory allocation for tables
        current_db->tables = realloc(current_db->tables, sizeof(Table) * current_db->tables_size * 2);
        current_db->tables_capacity *= 2;
    }

    // get pointer for new table
    Table *new_table = &(current_db->tables[current_db->tables_size]);

    // set initial table attributes
    strcpy(new_table->name, name);
    new_table->col_count = 0;
    new_table->table_length = 0;
    new_table->col_capacity = col_capacity;
    new_table->table_length_capacity = INITIAL_TABLE_LENGTH_CAPACITY;

    // allocate space for columns
    new_table->columns = calloc(col_capacity, sizeof(Column));

    // add one to db tables_size
    current_db->tables_size++;

    // insert table into db_catalog
    insert_object(db_catalog, lookup_name, (void*) new_table, TABLE);

    status->code=OK_DONE;

    return new_table;
}

/**
 * This method creates a column. It validates
 * all arguments, then adds the column to 
 * the appropriate table.
 **/
Column* create_column(const char* name, const char* table_name, Status* status) {
    // check if name is not already used
    char lookup_name[strlen(table_name) + strlen(name) + 2];
    strcpy(lookup_name, table_name);
    strcat(lookup_name, ".");
    strcat(lookup_name, name);
    
    if (lookup_object(db_catalog, lookup_name, COLUMN) != NULL) {
        status->code = OBJECT_ALREADY_EXISTS;
        return NULL;
    }

    // lookup table
    Table* table = (Table*) lookup_object(db_catalog, table_name, TABLE);
    if (table == NULL) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return NULL;
    }

    // check if table has room
    if (table->col_capacity == table->col_count) {
        status->code = TABLE_AT_CAPACITY;
        return NULL;
    }

    // get space for new column
    Column *new_column = &(table->columns[table->col_count]);

    // set initial vars
    strcpy(new_column->name, name);

    // allocate memory for data 
    new_column->data = malloc(sizeof(int) * INITIAL_TABLE_LENGTH_CAPACITY);

    // increase table col_count
    table->col_count++;

    // add CHandle for col to db_catalog
    CHandle* col_handle = calloc(1, sizeof(CHandle));
    strcpy(col_handle->name, lookup_name);
    col_handle->type = COLUMN;
    col_handle->pointer.column = new_column;

    insert_object(db_catalog, lookup_name, (void*) col_handle, COLUMN);

    status->code = OK_DONE;

    return new_column;
}


/**
 * Creates index on given column.
 **/
void create_idx(const char* col_name, IndexType index_type, Status* status) {
    // get column
    CHandle* column_handle = (CHandle*) lookup_object(db_catalog, col_name, COLUMN);
    
    // err if doesn't exist
    if (column_handle == NULL) {
        status->code = OBJECT_DOES_NOT_EXIST;
        return;
    }
    Column* column = column_handle->pointer.column;

    // set column index type
    column->index_type = index_type;

    if (index_type == SORTED_UNCLUSTERED) {
        UnclusteredIndex* index = malloc(sizeof(UnclusteredIndex));

        // lookup table to get table capacity
        char db_name[MAX_SIZE_NAME];
        char table_name[MAX_SIZE_NAME];
        sscanf(col_name, "%[^.].%[^.]", db_name, table_name);

        char full_name[strlen(db_name) + strlen(table_name) + 1];
        strcpy(full_name, db_name);
        strcat(full_name, ".");
        strcat(full_name, table_name);

        Table* table = lookup_object(db_catalog, full_name, TABLE);

        index->values = malloc(sizeof(int) * table->table_length_capacity);
        index->positions = malloc(sizeof(int) * table->table_length_capacity);
        
        column->index = (void*) index;
    } else {
        column->index = NULL;
    }

    // if a clustered index, mark all columns in table as clustered
    if (index_type == BTREE_CLUSTERED || index_type == SORTED_CLUSTERED) {
        // lookup table to get table capacity
        char db_name[MAX_SIZE_NAME];
        char table_name[MAX_SIZE_NAME];
        sscanf(col_name, "%[^.].%[^.]", db_name, table_name);

        char full_name[strlen(db_name) + strlen(table_name) + 1];
        strcpy(full_name, db_name);
        strcat(full_name, ".");
        strcat(full_name, table_name);

        Table* table = lookup_object(db_catalog, full_name, TABLE);

        for (size_t num_col = 0; num_col < table->col_count; num_col++) {
            table->columns[num_col].clustered = 1;
        }
    }
}


typedef struct temp {
    int val;
    int pos;
} temp;


int compare( const void* a, const void* b) {
    temp* a_tmp = (temp*) a;
    temp* b_tmp = (temp*) b;

    if ( a_tmp->val == b_tmp->val ) return 0;
    else if ( a_tmp->val < b_tmp->val ) return -1;
    else return 1;
}


/**
 * Handles loading file into db.
 **/
void handle_db_load(int client_socket, char* tbl_name, int num_cols) {
    Status* status = malloc(sizeof(Status));

    // get table
    Table* table = lookup_object(db_catalog, tbl_name, TABLE);

    // get columns
    Column* columns = table->columns;

    // read num rows
    int num_rows = 0;
    recv(client_socket, &num_rows, sizeof(int), 0);

    size_t initial_table_length_capacity = table->table_length_capacity;
    // make sure enough capacity
    while (num_rows > (int) table->table_length_capacity) {
        table->table_length_capacity *= 2;
    }

    if (initial_table_length_capacity != table->table_length_capacity) {
        for (int i = 0; i < num_cols; i++) {
            columns[i].data = realloc(columns[i].data, sizeof(int) * table->table_length_capacity);

            // check if index needs more memory
            if (columns[i].index_type == SORTED_UNCLUSTERED) {
                UnclusteredIndex* index = (UnclusteredIndex*) columns[i].index;
                index->values = realloc(index->values, sizeof(int) * table->table_length_capacity);
                index->positions = realloc(index->positions, sizeof(int) * table->table_length_capacity);
            }
        }
    }

    int* data[num_cols];
    for (int i = 0; i < num_cols; i++) {
        data[i] = malloc(sizeof(int) * num_rows);
    }

    // read data
    for (int i = 0; i < num_rows; i++) {
        int vals[num_cols];
        recv(client_socket, vals, sizeof(int) * num_cols, 0);

        for (int j = 0; j < num_cols; j++) {
            data[j][i] = vals[j];
        }
    }

    int* primary_index_col = NULL;
    for (int i = 0; i < num_cols; i++) {
        if (columns[i].index_type == SORTED_CLUSTERED || columns[i].index_type == SORTED_UNCLUSTERED) {
            primary_index_col = malloc(sizeof(int));
            *primary_index_col = i;
        }
    }

    // if primary index sort on that column
    if (primary_index_col != NULL) {
        temp* temps = malloc(sizeof(temp) * num_rows);
        for (int i = 0; i < num_rows; i++) {
            temps[i].val = data[*primary_index_col][i];
            temps[i].pos = i;
        }

        qsort(temps, num_rows, sizeof(temp), compare);

        // quick_sort_double(data[*primary_index_col], positions, 0, num_rows - 1);
        // qsort_arrays(data, 0, num_rows - 1, *primary_index_col, num_cols);
        int* positions = malloc(sizeof(int) * num_rows);
        for (int i = 0; i < num_rows; i++) {
            data[*primary_index_col][temps[i].pos] = temps[i].val;
            positions[i] = temps[i].pos;
        }

        memcpy(columns[*primary_index_col].data, data[*primary_index_col], sizeof(int) * num_rows);

        for (int i = 0; i < num_rows; i++) {
            for (int j = 0; j < num_cols; j++) {
                if (j != *primary_index_col) {
                    columns[j].data[i] = data[j][positions[i]];
                }
            }
        }

        free(positions);
        free(temps);
    }

    // set columns data
    for (int i = 0; i < num_cols; i++) {
        if (primary_index_col == NULL) {
            memcpy(columns[i].data, data[i], sizeof(int) * num_rows);
        }

        columns[i].col_size = num_rows;

        // check index
        if (columns[i].index_type == BTREE_CLUSTERED || columns[i].index_type == BTREE_UNCLUSTERED) {
            for (int j = 0; j < num_rows; j++) {
                index_value(&columns[i], columns[i].data[j], j, 1);
            }
        } else if (columns[i].index_type == SORTED_UNCLUSTERED) {
            temp* temps = malloc(sizeof(temp) * num_rows);

            for (int row = 0; row < num_rows; row++) {
                temps[row].val = columns[i].data[row];
                temps[row].pos = row;
            }

            qsort(temps, num_rows, sizeof(temp), compare);

            UnclusteredIndex* index = (UnclusteredIndex*) columns[i].index;
            // add to index
            for (int row = 0; row < num_rows; row++) {
                index->values[row] = temps[row].val;
                index->positions[row] = temps[row].pos;
            }

            free(temps);
        }
        free(data[i]);
    }

    table->table_length = num_rows;
    free(status);
    free(primary_index_col);

}


/*
 * Load server data -- load db and all of
 * its content from file:
 *     - all tables
 *     - all columns and their data/indexes
 */
Status load_server_data() {
    Status status;
    status.code = OK_DONE;

    FILE* fd = fopen("dbdump.bin", "rb");

    // if no dbdump file just return NULL
    if (!fd) {
        return status;
    }

    // allocate space for current_db
    current_db = malloc(sizeof(Db));

    // init db catalog
    db_catalog = init_lookup_table();

    // load db
    fread(current_db, sizeof(Db), 1, fd);

    // malloc space for db's tables
    current_db->tables = calloc(current_db->tables_capacity, sizeof(Table));

    // load db's tables
    for (size_t num_table = 0; num_table < current_db->tables_size; num_table++) {
        // read table
        Table* table = &current_db->tables[num_table];
        fread(table, sizeof(Table), 1, fd);

        // add table to db_catalog
        char table_lookup_name[strlen(current_db->name) + strlen(table->name) + 2];
        strcpy(table_lookup_name, current_db->name);
        strcat(table_lookup_name, ".");
        strcat(table_lookup_name, table->name);
        table_lookup_name[strlen(current_db->name) + strlen(table->name) + 1] = '\0';
        insert_object(db_catalog, table_lookup_name, (void*) table, TABLE);

        // malloc space for table's columns
        table->columns = calloc(table->col_capacity, sizeof(Column));

        // read table's columns
        for (size_t num_col = 0; num_col < table->col_count; num_col++) {
            // read column
            Column* col = &table->columns[num_col];
            fread(col, sizeof(Column), 1, fd);

            // read columns' data
            col->data = calloc(table->table_length_capacity, sizeof(int));
            fread(col->data, sizeof(int), col->col_size, fd);

            // read index
            // for sorted unclustered just read array
            if (col->index_type == SORTED_UNCLUSTERED) {
                UnclusteredIndex* index = malloc(sizeof(UnclusteredIndex));
                index->values = calloc(table->table_length_capacity, sizeof(int));
                index->positions = calloc(table->table_length_capacity, sizeof(int));
                fread(index->values, sizeof(int), col->col_size, fd);
                fread(index->positions, sizeof(int), col->col_size, fd);
                col->index = (void*) index;
            // else if btree need to read all nodes
            } else if (col->index_type == BTREE_CLUSTERED || col->index_type == BTREE_UNCLUSTERED) {
                col->index = load_bptree(fd, col->data);
            }

            // get column lookup name
            char col_lookup_name[strlen(table_lookup_name) + strlen(col->name) + 2];
            strcpy(col_lookup_name, table_lookup_name);
            strcat(col_lookup_name, ".");
            strcat(col_lookup_name, col->name);
            col_lookup_name[strlen(table_lookup_name) + strlen(col->name) + 1] = '\0';

            // add column to db_catalog
            CHandle* col_handle = malloc(sizeof(CHandle));
            strcpy(col_handle->name, col_lookup_name);
            col_handle->type = COLUMN;
            col_handle->pointer.column = col;

            insert_object(db_catalog, col_lookup_name, (void*) col_handle, COLUMN);
        }
    }

    // flush and close file
    fflush(fd);
    fclose(fd);

    // return sttus
    return status;
}


/**
 * Dump server data: dump current_db and all of 
 * its content to file and free memory:
 *     - all tables
 *     - all columns and their data and indexes
 **/
void dump_server_data(Db* db, Status* status) {
    // open file for writing
    FILE* fd = fopen("dbdump.bin", "wb");

    // dump db metadata
    fwrite(db, sizeof(Db), 1, fd);

    // dump db's tables
    for (size_t num_table = 0; num_table < db->tables_size; num_table++) {
        Table* table = &db->tables[num_table];
        
        // dump table metadata
        fwrite(table, sizeof(Table), 1, fd);

        // dump table's columns
        for (size_t num_col = 0; num_col < table->col_count; num_col++) {
            Column* col = &table->columns[num_col];

            // dump col metadata
            fwrite(col, sizeof(Column), 1, fd);
    
            // dump col's data
            fwrite(col->data, sizeof(int), col->col_size, fd);

            // dump index
            // for sorted unclustered just dump array
            if (col->index_type == SORTED_UNCLUSTERED) {
                UnclusteredIndex* index = (UnclusteredIndex*) col->index;
                // write to file
                fwrite(index->values, sizeof(int), col->col_size, fd);
                fwrite(index->positions, sizeof(int), col->col_size, fd);
                // free memory
                free(index->values);
                free(index->positions);
                free(index);

            // else if btree need to dump all nodes
            } else if (col->index_type == BTREE_CLUSTERED || col->index_type == BTREE_UNCLUSTERED) {
                // dump bplus tree node by node
                BPTreeNode* root = (BPTreeNode*) col->index;
                dump_bptree(fd, root, col->data);
            }

            // free col's data
            free(col->data);
        }

        // free table's columns
        free(table->columns);
    }    

    // free db's memory
    free(db->tables);
    free(db);

    // flush and close file
    fflush(fd);
    fclose(fd);

    (void) status;
}


/*
 * Shutdown server -- free all memory and write
 * current statue of Dbs to file to persist data.
 */
void shutdown_server(Status* status) {
    // if dbs exists, dump to file
    // and shutdown
    if (current_db != NULL) {
        dump_server_data(current_db, status);
    }
    shutdown_lookup_table(db_catalog);
    status->result = "shutdown complete";
}


/**
 * Frees all memory allocated for lookup table
 **/
void shutdown_lookup_table(LookupTable* lookup_table) {
    if (lookup_table != NULL) {
        // free all nodes
        for (size_t i=0; i < lookup_table->size; i++) {
            LookupNode* node = lookup_table->object_table[i];
            LookupNode* to_free = NULL;

            while (node != NULL) {
                to_free = node;
                node = node->next;

                // if chandle free
                if (to_free->type != TABLE) {
                    CHandle* chandle = (CHandle*) to_free->object;

                    if (to_free->type == RESULT) {
                        Result* result = chandle->pointer.result;
                        free(result->payload);
                        free(result);
                    }

                    free(chandle);
                }

                // free node
                free(to_free);
            }
        }
        // free object_table
        free(lookup_table->object_table);
        // free lookup table
        free(lookup_table);
    }
}

