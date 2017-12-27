/**
 * Defines functions used in implementing 
 * extendible hash table.
 **/
#include "cs165_api.h"

/**
 * Return two to the power passed.
 **/
int two_power(int power);


/**
 * Hashes key to hash value.
 **/
unsigned long hash_function(int key);


/**
 * Given hash value and number of bits to use,
 * returns decimal value of those last bits.
 * 
 * Used to get pointer to correct bucket.
 **/
int get_bucket_num(int hash, int num_bits);


/**
 * Given hash table and key, finds correct bucket in table.
 * Also takes in inserting flag to determine if new buckets should
 * be created if full.
 **/
Bucket* get_bucket(HashTable* hash_table, int key, int inserting);


/**
 * Given a full bucket, splits into two and returns new bucket.
 **/
void split_bucket(HashTable* hash_table, int num_bucket);


/**
 * Given a hash table, increases num bits used 
 * for buckets, thus increasing size.
 **/
void increase_num_bits(HashTable* hash_table);


