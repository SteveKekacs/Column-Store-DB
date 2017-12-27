/**
 * Contains all functionality
 * for indexing;
 **/

#include "index.h"


/**
 * Given array of sorted data, number of items
 * in array, and a val to find, lookup correct pos for val.
 **/
int binary_search(int* sorted_data, int num_items, int val) {    
    if (!num_items) {
        return 0;
    }

    int first = 0;
    int last = num_items - 1;
    int middle = last / 2;

    while (first <= last) {
        if (sorted_data[middle] < val) {
            first = middle + 1;
        } else if (sorted_data[middle] == val) {
            // make sure at first instance of item
            while (sorted_data[middle - 1] == val && middle > 0) {
                middle--;
            }
            return middle;
        } else {
            last = middle - 1;
        }

        middle = (first + last) / 2;
    }

    if (sorted_data[middle] < val) {
        return middle + 1;
    }
    return middle;
}


/**
 * Given array of data, number of items
 * in array, a val to insert, and a position to
 * insert at, insert at that position;
 **/
void insert_at_pos(int* data, int num_items, int pos, int val) {
    if (num_items) {
        // move everything past pos over
        for (int i = num_items + 1; i > pos; i--) {
            data[i] = data[i - 1];
        }
    }

    // insert new val at pos
    data[pos] = val;
}


/**
 * Given array of data and corresponding positions, number of items
 * in array, a position to remove,
 * remove that position and shift remaining data over, and 
 * update all positions > pos minus 1;
 **/
void remove_pos_and_update(int* values, int* positions, int num_items, int pos) {
    if (num_items) {
        // move everything greater than pos over
        for (int i = pos; i < num_items; i++) {
            values[i] = values[i + 1];
            positions[i] = positions[i + 1];

            // check position
            if (positions[i] > pos) {
                positions[i] -= 1;
            }
        }

        // now check positions of everything before
        for (int i = 0; i < pos; i++) {
            if (positions[i] > pos) {
                positions[i] -= 1;
            }
        }
    }
}


/**
 * Given UnclusteredIndex of sorted data and associated positions, 
 * number of items, a val to insert, and the pos of that val, 
 * insert at correct position.
 **/
void sorted_insert(UnclusteredIndex* index, int num_items, int val, int pos, int clustered) {
    // get position where value goes
    int insert_pos = binary_search(index->values, num_items, val);

    // insert value into index values
    insert_at_pos(index->values, num_items, insert_pos, val);
    // insert pos into index positions
    insert_at_pos(index->positions, num_items, insert_pos, pos);

    // increment following positions b/c shifted over
    if (clustered) {
        for (int i = insert_pos + 1; i < num_items; i++) {
            index->positions[i] += 1;
        }
    }
}


/**
 * Given val and pos and a column,
 * insert's into index if applicable.
 **/
void index_value(Column* column, int val, int pos, int dont_update) {
    int update_vals = 0;

    switch (column->index_type) {
        case BTREE_UNCLUSTERED:
            update_vals = !dont_update && column->clustered && (unsigned int) pos != column->col_size;        
        case BTREE_CLUSTERED: {
            // insert into btree
            column->index = bplus_insert((BPTreeNode*) column->index, val, pos, update_vals);
            break;
        } case SORTED_UNCLUSTERED: {
            sorted_insert((UnclusteredIndex*) column->index, column->col_size, val, pos, column->clustered && !dont_update);
            break;
        } default:
            break;
    }
}
