#ifndef PARSE_H__
#define PARSE_H__
#include "cs165_api.h"

DbOperator* parse_command(char* query_command, Status* status, LookupTable* client_lookup_table, int client);

#endif
