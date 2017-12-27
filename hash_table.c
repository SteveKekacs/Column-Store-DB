/**
 * Implements an extendible hash table that is used in 
 * hash joins.
 **/
#include "hash_table.h"

#define INITIAL_NUM_BITS 2


/**
 * Return 2^power.
 **/
int two_power(int power) {
	return (1 << power); 
}


/**
 * Uses sdbm hash algorithm (http://www.cse.yorku.ca/~oz/hash.html).
 **/
unsigned long hash_function(int key) {
	unsigned char* str = (unsigned char*) &key;

    unsigned long hash = 0;
    int c;

    while ((c = *str++)) {
        hash = c + (hash << 6) + (hash << 16) - hash;
    }

    return hash;
}



/**
 * Returns value of last num bits of hash_val
 **/
int get_bucket_num(int hash_val, int num_bits) {
    // get mask
    unsigned  mask;
    mask = (1 << num_bits) - 1;
    
    // return val of last num bits
    return hash_val & mask;
}


/**
 * Increase num bits used by hash table.
 *
 * 1. Increase num_bits in hash table
 * 2. Increase bucket pointers array memory
 * 3. Set newly allocated pointers as NULL
 **/
void increase_num_bits(HashTable* hash_table) {
	// get old and new bucket counts
	int old_num_buckets = two_power(hash_table->num_bits);
	hash_table->num_buckets = 2 * old_num_buckets;

	// reallocate bucket pointer array
	hash_table->buckets = realloc(hash_table->buckets, hash_table->num_buckets * sizeof(Bucket*));

	// set new pointers
	for (int num_bucket = old_num_buckets; num_bucket < hash_table->num_buckets; num_bucket++) {
		int old_num_bucket = num_bucket - two_power(hash_table->num_bits);
		hash_table->buckets[num_bucket] = hash_table->buckets[old_num_bucket];
		hash_table->buckets[num_bucket]->num_ptrs++;
	}

	// add one to num bits
	hash_table->num_bits++;
}


/**
 * Initialize new hash table.
 *
 * 1. Allocate memory for table
 * 2. Set initial num_bits
 * 3. Allocate initial bucket array
 * 4. Allocate bucket objs
 **/
HashTable* init_hashtable() {
	HashTable* hash_table = malloc(sizeof(HashTable));

	// set num bits (4 initial buckets)
	hash_table->num_bits = INITIAL_NUM_BITS;

	hash_table->num_buckets = two_power(hash_table->num_bits);

	// allocate bucket pointers
	hash_table->buckets = calloc(hash_table->num_buckets, sizeof(Bucket*));

	// allocate space for buckets
	for (int num_bucket = 0; num_bucket < hash_table->num_buckets; num_bucket++) {
		hash_table->buckets[num_bucket] = calloc(1, sizeof(Bucket));
		hash_table->buckets[num_bucket]->num_ptrs = 1;
	}

	return hash_table;
}


void split_bucket(HashTable* hash_table, int num_bucket) {
	// get bucket
	Bucket* bucket = hash_table->buckets[num_bucket];

	// check if directory needs to be increased
	if (bucket->num_ptrs == 1) {
		increase_num_bits(hash_table);
	}

	// get num buckets, allocating new ones if multiple ptrs to same bucket
	int all_keys[BUCKET_SIZE];
	int all_vals[BUCKET_SIZE];

	int num_bucket_ptrs = 0;
	for (int bucket_num = 0; bucket_num < hash_table->num_buckets; bucket_num++) {
		if (hash_table->buckets[bucket_num] == bucket) {
			num_bucket_ptrs++;

			// if not first, allocate new bucket
			if (num_bucket_ptrs > 1) {
				hash_table->buckets[bucket_num] = calloc(1, sizeof(Bucket));
				hash_table->buckets[bucket_num]->num_ptrs = 1;
			}
		}
	}

	// reset bucket
	bucket->size = 0;
	bucket->num_ptrs = 1;

	// copy over keys and vals
	memcpy(&all_keys, &bucket->keys, sizeof(int) * BUCKET_SIZE);
	memcpy(&all_vals, &bucket->vals, sizeof(int) * BUCKET_SIZE);

	// reassign items in bucket
	for (int i = 0; i < BUCKET_SIZE; i++) {
		bucket = hash_table->buckets[get_bucket_num(hash_function(all_keys[i]), hash_table->num_bits)];
		
		// add key and val
		bucket->keys[bucket->size] = all_keys[i];
		bucket->vals[bucket->size++] = all_vals[i];
	}
}


/**
 * Get bucket in hash table for given key.
 *
 * 1. Get num bucket from last n bits of hash val.
 * 2. If bucket at num bucket is not NULL return bucket.
 * 3. Else original bucket hasn't been split so get bucket from n - 1 bits.
 **/
Bucket* get_bucket(HashTable* hash_table, int key, int inserting) {
	// get hash val for key
	int hash_val = hash_function(key);

	// get num bucket
	int num_bucket = get_bucket_num(hash_val, hash_table->num_bits);

	// get bucket
	Bucket* bucket = hash_table->buckets[num_bucket];

	// if not inserting or not full return bucket
	if (!inserting || bucket->size != BUCKET_SIZE) {
		return bucket;
	}

	// else need to split
	if (bucket->size == BUCKET_SIZE) {
		// split bucket
		split_bucket(hash_table, num_bucket);

		// return bucket
		int new_num_bucket = get_bucket_num(hash_val, hash_table->num_bits);
		return hash_table->buckets[new_num_bucket];
	}

	return bucket;
}


void hash_insert(HashTable* hash_table, int key, int val) {
	// get bucket 
	Bucket* bucket = get_bucket(hash_table, key, 1);

	// add val key and val to bucket
	bucket->keys[bucket->size] = key;
	bucket->vals[bucket->size++] = val;
}


int* hash_probe(HashTable* hash_table, int key, int* num_results) {
	// get bucket 
	Bucket* bucket = get_bucket(hash_table, key, 0);
	
	int* return_vals = NULL;
	// search for key in bucket
	for (int i = 0; i < bucket->size; i++) {
		if (bucket->keys[i] == key) {
			return_vals = realloc(return_vals, sizeof(int) * (*num_results + 1));
			return_vals[*num_results] = bucket->vals[i];
			(*num_results)++;
		}
	}

	return return_vals;
}

