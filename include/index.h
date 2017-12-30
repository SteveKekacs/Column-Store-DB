/**
 * Contains function definitions for indexing.
 **/

#include "cs165_api.h"
#include "bplus.h"

int binary_search(int* sorted_data, int num_items, int val);

void insert_at_pos(int* data, int num_items, int pos, int val);
void remove_pos_and_update(int* values, int* positions, int num_items, int pos);

void sorted_insert(UnclusteredIndex* index, int num_items, int val, int pos, int clustered);
void index_value(Column* column, int val, int pos, int dont_update);
