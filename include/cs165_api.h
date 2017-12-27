

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
#define _GNU_SOURCE

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include "message.h"

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64

// define fanout of btree so that each node fits on one page (4096 bytes)
#define FANOUT 340
#define LEAF_SIZE 508

// define bucket size so each fits on one page
#define BUCKET_SIZE 511


/************************************************************/
/* Following structs are for looking up tables/cols/results */
/**
 * Type of object at nodein Lookup Table.
 **/
typedef enum LookupType {
    TABLE,
    COLUMN,
    RESULT
} LookupType;

/**
 * LookupNode holds pointer to object,
 * also contains name and type.
 **/
typedef struct LookupNode {
    void* object;
    char object_name[64];
    LookupType type;

    struct LookupNode* next;
} LookupNode;

/**
 * LookupTable has pointer to bins
 * of nodes, used to access
 * tables, columns, and results quickly.
 **/
typedef struct LookupTable {
    LookupNode** object_table;
    size_t size;
} LookupTable;
/************************************************************/


/************************************************************/
/* Following structs are for indexing. */

// typedef union BPTreeNodeItems {
//     void* pointers[FANOUT];
//     int positions[LEAF_SIZE];
// } BPTreeNodeItems;

// typedef union BPTreeNodeKeys {
//     int vals[FANOUT - 1];
//     int leaf_vals[LEAF_SIZE];
// } BPTreeNodeKeys;

// typedef struct BPTreeNode {
//     int is_leaf;                  // bool flag for if leaf node
    
//     BPTreeNodeItems items;         // array of pointers to other nodes
//     BPTreeNodeKeys keys;          // array of keys 

//     int num_vals;                 // number of currently stored keys

//     struct BPTreeNode* parent;    // pointer to parent node
//     struct BPTreeNode* next;      // for leaf node, pointer to next leaf
//     struct BPTreeNode* prev;      // for leaf node, pointer to prev leaf
// } BPTreeNode;


// TODO: convert btree to following structure

typedef struct BPTreeNode BPTreeNode;

typedef struct BPTreeInternalNode {
    struct BPTreeNode* pointers[FANOUT];    // array of pointers to other nodes
    int vals[FANOUT - 1];                   // array of keys 
} BPTreeInternalNode;


typedef struct BPTreeLeafNode {
    int vals[LEAF_SIZE];         // array of values 
    int positions[LEAF_SIZE];    // array of corresponding positions in base data
    
    struct BPTreeNode* next;     // pointer to next leaf
    struct BPTreeNode* prev;     // pointer to previous leaf
} BPTreeLeafNode;


typedef union BPTreeNodeType {
    BPTreeInternalNode internal_node;
    BPTreeLeafNode leaf_node;
} BPTreeNodeType;


struct BPTreeNode {
    int is_leaf;                  // bool for leaf
    int num_vals;                 // number of vals stored
    BPTreeNodeType type;          // leaf or internal

    struct BPTreeNode* parent;    // pointer to parent node
};


typedef struct UnclusteredIndex {
    int* values; 
    int* positions;
} UnclusteredIndex;


/**
 * Simple helper struct for finding val pos in leaf node.
 **/
typedef struct LeafIndexRes {
    BPTreeNode* leaf_node;
    int index;
} LeafIndexRes;

/************************************************************/



/**************************************************************/
/* Following structs are used for hash table (used in joins). */

/**
 * HashTable bucket -- holds key value pairs. Make sure bucket
 * is "fat" -- is ~ size of one page.
 */
typedef struct Bucket {
    int size;                 // num items in bucket
    int num_ptrs;             // number of ptrs to bucket 

    int keys[BUCKET_SIZE];    // keys in bucket
    int vals[BUCKET_SIZE];    // values in bucket
} Bucket;

/**
 * Extendible hash table struct -- acts as directory for 
 # pointing hash values to correct buckets.
 **/
typedef struct HashTable {
    int num_bits;        // number of bits being used currently
    int num_buckets;     // number of buckets currently allocated

    Bucket** buckets;    // array of pointers to buckets 
} HashTable;


/**************************************************************/


/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     LONG,
     FLOAT
} DataType;


/**
 * Types of indexes.
 **/
typedef enum IndexType {
    NONE,
    BTREE_CLUSTERED,
    BTREE_UNCLUSTERED,
    SORTED_CLUSTERED,
    SORTED_UNCLUSTERED
} IndexType;


typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    int* data;
    size_t col_size;

    IndexType index_type;
    void* index;
    int clustered;
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - columns, this is the pointer to an array of columns contained in the table.
 * - col_count, the current number of columns in the table
 * - col_capacity, the number of columns the table can currently hold.
 * - table_length, the number of rows in the table.
 * - table_length_capacity - the number of rows the table can currently hold.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;

    size_t col_count;
    size_t col_capacity;

    size_t table_length;
    size_t table_length_capacity;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;


/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;


/*
 * a union type holding either a column or a result struct
 */
typedef union HandlePointer {
    Result* result;
    Column* column;
} HandlePointer;


/*
 * Used to refer to a client named variable (handle).
 * Uses LookupType, but only COLUMN & RESULT vals
 */
typedef struct CHandle {
    char name[HANDLE_MAX_SIZE];
    LookupType type;
    HandlePointer pointer;
} CHandle;


/*
 * tells the database what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    SELECT,
    FETCH,
    PRINT,
    AGGREGATE,
    SHUTDOWN,
    BATCH_QUERIES,
    BATCH_EXECUTE,
    JOIN,
    UPDATE,
    DELETE
} OperatorType;


/*
 * necessary fields for deleting
 */
typedef struct DeleteOperator {
    Table* table;         // table to perform operator on
    Result* positions;    // positions to delete 
} DeleteOperator;


/*
 * necessary fields for updating
 */
typedef struct UpdateOperator {
    Table* table;            // table update being performed on
    Column* column;          // column to perform operator on
    Result* positions;       // positions to update 
    int update_val;          // value to set rows to
} UpdateOperator;


/**
 * Two types of joins.
 **/
typedef enum JoinType {
    HASH,
    NESTED_LOOP
} JoinType;


/*
 * necessary fields for joining
 */
typedef struct JoinOperator {
    Result* pos_1;
    Result* val_1;
    Result* pos_2;
    Result* val_2;

    JoinType type;
} JoinOperator;


/*
 * types for aggregate operator
 */
typedef enum AggregateType {
    MIN,
    MAX,
    SUM,
    AVG,
    ADD,
    SUB
} AggregateType;

/*
 * necessary fields for printing
 */
typedef struct AggregateOperator {
    CHandle* chandle_1;
    CHandle* chandle_2;
    AggregateType type;
} AggregateOperator;


/*
 * necessary fields for printing
 */
typedef struct PrintOperator {
    unsigned int num_fields;
    char** fields;
} PrintOperator;


/*
 * necessary fields for fetching
 */
typedef struct FetchOperator {
    Column* column;    // column being fetched from
    Result* result;    // vector of positions to fetch
} FetchOperator;



// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN_OR_EQUAL = 2
} ComparatorType;


/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low;     // used in equality and ranges.
    long int p_high;    // used in range compares. 
    ComparatorType type1;
    ComparatorType type2;
} Comparator;


/*
 * necessary fields for selection
 */
typedef struct SelectOperator {
    CHandle* chandle_1;
    CHandle* chandle_2;
    Comparator comparator;
} SelectOperator;


/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;


/**
 * types of objects that can be created
 **/
typedef enum CreateType {
    CREATE_DB,
    CREATE_TBL,
    CREATE_COL,
    CREATE_IDX
} CreateType;


/*
 * necessary fields for creating
 */
typedef struct CreateOperator {
    CreateType type;    // type of object being created

    char db_name[HANDLE_MAX_SIZE];       // stored db_name arg (used diff based on type)
    char table_name[HANDLE_MAX_SIZE];    // stored table_name arg (used diff based on type)
    char col_name[HANDLE_MAX_SIZE];      // stored col_name arg (used diff based on type)

    unsigned int col_capacity;    // for table creation, num cols being created

    IndexType index_type;         // for index creation, what type of index being created
} CreateOperator;


/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AggregateOperator aggregate_operator;
    JoinOperator join_operator;
    UpdateOperator update_operator;
    DeleteOperator delete_operator;
} OperatorFields;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;

    // for client context
    int client_fd;
    LookupTable* client_lookup_table;
 
    char handle_names[2][MAX_SIZE_NAME];
    unsigned int num_handles;
} DbOperator;

extern Db* current_db;
extern LookupTable* db_catalog;


/**********************************************************/
/* Functions for hash table */
/**
 * Initializes a new hash table.
 **/
HashTable* init_hashtable();


/**
 * Given hash table, key and value, inserts into table.
 **/
void hash_insert(HashTable* hash_table, int key, int val);


/**
 * Given a hash table and key, looks up value for given key.
 * Returns NULL if not found, else returns int pointer to value.
 **/
int* hash_probe(HashTable* hash_table, int key, int* num_results);
/**********************************************************/


/***********************************************************/
/* Functions for dumping and loading database to/from disk */
void* load_bptree(FILE* fd, int* base_data);
void dump_bptree(FILE* fd, BPTreeNode* root, int* base_data); 
void free_node(BPTreeNode* node);

/**
 * Saves the current status of the database to disk.
 **/
void sync_db_new(Db* db, Status* status);

/**
 * Loads the database from disk and returns db_catalog.
 **/
LookupTable* load_db_new(Db* db, Status* status);

void shutdown_server(Status* status);
void shutdown_database(Db* db, Status* status);

Status load_server_data();

/***********************************************************/

void handle_db_load(int client_socket, char* tbl_name, int num_cols);
void create_db(const char* db_name, Status* status);
Table* create_table(const char* name, const char* db_name, unsigned int col_capacity, Status* status);
Column* create_column(const char* name, const char* table_name, Status* status);
void create_idx(const char* col_name, IndexType index_type, Status* status);


void insert_object(LookupTable* lookup_table, const char* object_name, void* object, LookupType type);
void* lookup_object(LookupTable* lookup_table, const char* object_name, LookupType type);
LookupTable* init_lookup_table();
void shutdown_lookup_table(LookupTable* lookup_table);

#endif /* CS165_H */

