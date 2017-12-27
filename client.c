/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE
#define _BSD_SOURCE

/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"
#include "load.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024


void handle_print_payload(int client_socket, PrintPayload* print_payload) {
    int num_results = print_payload->num_results;
    int num_cols = print_payload->num_cols;

    if (num_results) {
        void** all_data = malloc(sizeof(void*) * num_cols);
        int data_types[num_cols];

        // collect all results
        for (int num_col = 0; num_col < num_cols; num_col++) {
            // recv data type of col
            recv(client_socket, &data_types[num_col], sizeof(int), 0);

            // array to hold all results
            if (data_types[num_col] == 0) {
                int total_num_bytes = sizeof(int) * num_results;
                int* data = malloc(total_num_bytes);

                int results_recv = 0;
                while (results_recv < total_num_bytes) {
                    int recv_size = total_num_bytes - results_recv;
                    results_recv += recv(client_socket, &data[results_recv / sizeof(int)], recv_size, 0);
                }

                all_data[num_col] = (void*) data;
            } else if (data_types[num_col] == 1) {
                int total_num_bytes = sizeof(long) * num_results;
                long* data = malloc(total_num_bytes);

                int results_recv = 0;
                while (results_recv < total_num_bytes) {
                    int recv_size = total_num_bytes - results_recv;
                    results_recv += recv(client_socket, &data[results_recv / sizeof(long)], recv_size, 0);
                }

                all_data[num_col] = (void*) data;
            } else {
                int total_num_bytes = sizeof(double) * num_results;
                double* data = malloc(total_num_bytes);

                int results_recv = 0;
                while (results_recv < total_num_bytes) {
                    int recv_size = total_num_bytes - results_recv;
                    results_recv += recv(client_socket, &data[results_recv / sizeof(double)], recv_size, 0);
                }

                all_data[num_col] = (void*) data;        
            }

        }

        // now print data
        // if just one col, act differently to speed up
        if (num_cols == 1) {
            if (data_types[0] == 0) {
                int* data = (int*) all_data[0];
                for (int data_i = 0; data_i < num_results; data_i++) {
                    printf("%d\n", data[data_i]);
                }
            } else if (data_types[0] == 1) {
                long* data = (long*) all_data[0];
                for (int data_i = 0; data_i < num_results; data_i++) {
                    printf("%ld\n", data[data_i]);
                }
            } else {
                double* data = (double*) all_data[0];
                for (int data_i = 0; data_i < num_results; data_i++) {
                    printf("%.2f\n", data[data_i]);
                }
            }
        } else {
            for (int data_i = 0; data_i < num_results; data_i++) {
                for (int col_i = 0; col_i < num_cols - 1; col_i++) {
                    switch (data_types[col_i]) {
                        case 0:
                            printf("%d,", ((int*) all_data[col_i])[data_i]);
                            break;
                        case 1:
                            printf("%ld,", ((long*) all_data[col_i])[data_i]);
                            break;
                        case 2:
                            printf("%.2f,", ((double*) all_data[col_i])[data_i]);
                            break;
                    }
                }
                switch (data_types[num_cols - 1]) {
                    case 0:
                        printf("%d\n", ((int*) all_data[num_cols - 1])[data_i]);
                        break;
                    case 1:
                        printf("%ld\n", ((long*) all_data[num_cols - 1])[data_i]);
                        break;
                    case 2:
                        printf("%.2f\n", ((double*) all_data[num_cols - 1])[data_i]);
                        break;
                }
            }
        }

        // free memory
        for (int num_col = 0; num_col < num_cols; num_col++) {
            free(all_data[num_col]);
        }
        free(all_data);

    // if no results were found just print new line
    } else {
        printf("\n");
    }

    free(print_payload);
}


/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message* send_message = calloc(1, sizeof(message));
    message* recv_message = calloc(1, sizeof(message));
    
    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message->payload = read_buffer;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {

        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // check for load function
        if (strncmp(read_buffer, "load", 4) == 0) {
            // get file name 
            char file_name[200];
            sscanf(read_buffer, "%*[load(\"]%[^\"]\"", file_name);
            load_file(file_name, client_socket, &send_message->status);
            continue;
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message->length = strlen(read_buffer);
        if (send_message->length > 1) {
            // Send the message_header, which tells server payload size
            if (send(client_socket, send_message, sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message->payload, send_message->length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, recv_message, sizeof(message), 0)) > 0) {
                
                // check if data from print command
                if (recv_message->print_payload) {
                    PrintPayload* print_payload = malloc(sizeof(PrintPayload));
                    recv(client_socket, print_payload, sizeof(PrintPayload), 0);
                    handle_print_payload(client_socket, print_payload);
                    recv_message->print_payload = 0;
                // else, if payload to be received just receive and print
                } else if (recv_message->length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message->length;
                    char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) >= 0) {
                        payload[num_bytes] = '\0';

                        // check for shutdown
                        if (strcmp(payload, "shutdown complete") == 0) {
                            exit(0);
                        }
                        printf("%s\n", payload);
                    }
                }
            }
            else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                exit(1);
            }
        }
    }
    close(client_socket);    

    free(send_message);
    free(recv_message);

    return 0;
}
