/**
 * Contains functionality for 
 * loading data from file.
 **/
#define _XOPEN_SOURCE
#define _BSD_SOURCE
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <limits.h>
#include "load.h"
#include "message.h"
#include "utils.h"


int numPlaces (int n) {
    int r = 1;
    if (n < 0) n = (n == INT_MIN) ? INT_MAX: -n;
    while (n > 9) {
        n /= 10;
        r++;
    }
    return r;
}


/**
 * Given a file name, loads data from file into appropriate
 * table by sending batches of relational_insert commands
 * to server.
 **/
void load_file(char* file_name, int client_socket, Status* status) {
    message* send_message = calloc(1, sizeof(message));
    message* recv_message = calloc(1, sizeof(message));

    // load file
    FILE* fd = fopen(file_name, "r");

    if (fd == 0) {
        status->code = FILE_NOT_FOUND;
        printf("File not found.\n");
        return;
    }

    // vars for reading line
    char line[1000];    
    
    // get table name from first line
    fgets(line, 1000, fd);

    char db_name[64], table_name[64];
    sscanf(line, "%[^.].%[^.]", db_name, table_name);

    // compile full table name
    char full_table_name[strlen(db_name) + strlen(table_name) + 2];
    strcpy(full_table_name, db_name);
    strcat(full_table_name, ".");
    strcat(full_table_name, table_name);

    // compile array of all data
    int num_cols = 1;
    for (int i = 0; line[i]; i++) {
        num_cols += (line[i] == ',');
    }

    // send load call
    char command[1000];
    sprintf(command, "load(%s,%d)\n", full_table_name, num_cols);
    send_message->payload = command;
    send_message->length = strlen(command);

    // send command to server
    if (send(client_socket, send_message, sizeof(message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
    }

    // Send the payload (query) to server
    if (send(client_socket, send_message->payload, send_message->length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
    }

    int num_rows = 0;
    int data_capacity = 10000;
    int** data = malloc(sizeof(int*) * data_capacity);
    // loop through lines sending relational_insert commands to server
    while (fgets(line, 1000, fd) != NULL) {
        char* temp = malloc(strlen(line) + 1);
        char* to_free = temp;
        strncpy(temp, line, strlen(line));
        temp[strlen(line)] = '\0';

        int* vals = malloc(sizeof(int) * num_cols);
        int val = 0;
        for (int i = 0; i < num_cols; i++) {
            sscanf(temp, "%d", &val);
            temp += numPlaces(val) + 1;
            vals[i] = val;
        }
        data[num_rows] = vals;
        num_rows += 1;

        if (num_rows > data_capacity) {
            data_capacity *= 2;
            data = realloc(data, sizeof(int*) * data_capacity);
        }

        free(to_free);
    }

    send(client_socket, &num_rows, sizeof(int), 0);

    for (int i = 0; i < num_rows; i++) {
        send(client_socket, data[i], sizeof(int) * num_cols, 0);
    }

    free(send_message);
    free(recv_message);
}
