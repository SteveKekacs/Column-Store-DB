/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#define _XOPEN_SOURCE
#define _BSD_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "utils.h"
#include "db_operator.h"
#include "index.h"


/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
int handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message* send_message = calloc(1, sizeof(message));
    message* recv_message = calloc(1, sizeof(message));

    // init new client context lookup table
    LookupTable* client_lookup_table = init_lookup_table();

    int shutdown = 0;
    int dont_send = 0;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            Status status = send_message->status;
            status.result = NULL;

            char recv_buffer[recv_message->length + 1];
            length = recv(client_socket, recv_buffer, recv_message->length, 0);
            recv_message->payload = recv_buffer;
            recv_message->payload[recv_message->length] = '\0';

            // check if load command
            if (strncmp(recv_message->payload, "load", 4) == 0) {
                recv_message->payload += 5;
                // get args
                char table_name[MAX_SIZE_NAME];
                int num_cols;

                sscanf(recv_message->payload, "%[^,],%d", table_name, &num_cols);
                handle_db_load(client_socket, table_name, num_cols);
                continue;
            }

            char* result = "";

            // 1. Parse command
            DbOperator* query = parse_command(recv_message->payload, &status, client_lookup_table, client_socket);

            // 2. Handle request if valid
            if (query != NULL) {
                // if print mark dont send b/c handled in db_operator
                if (query->type == PRINT) {
                    dont_send = 1;
                } else if (query->type == SHUTDOWN) {
                    shutdown = 1;
                }

                execute_db_operator(query, &status);
            }

            if (dont_send && status.code == OK_DONE) {
                dont_send = 0;
                continue;
            }
            dont_send = 0;

            // TODO map status err code to result str
            // set result if status has message
            if (status.result != NULL) {
                result = status.result;
            }

            // set send message status
            send_message->status = status;

            size_t total_len = strlen(result);

            // send to client
            send_message->length = total_len;
            char send_buffer[send_message->length + 1];
            strncpy(send_buffer, result, total_len);
            send_buffer[total_len] = '\0';
            send_message->payload = send_buffer;

            // Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, send_message, sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // if payload to be sent, send
            if (total_len) {
                // Send response of request
                if (send(client_socket, send_buffer, send_message->length, 0) == -1) {
                    log_err("Failed to send message.");
                    exit(1);
                }
            }
        }
    } while (!done);

    free(send_message);
    free(recv_message);

    shutdown_lookup_table(client_lookup_table);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);

    return shutdown;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);


    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }


    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

void test_b_plus_tree() {
    int vals[6] = {1,1,3,3,1,5};
    BPTreeNode* root = NULL;
    int num_test = 6;
    for (int pos = 0; pos < num_test; pos++) {
        int val = vals[pos];
        printf("%d: %d\n", val, pos);
        root = bplus_insert(root, val, pos, 0);
    }

    // remove 3 s
    bplus_remove(root, 3, 2);
    bplus_remove(root, 3, 3);

    int new_vals[4] = {1,1,1,5};
    printf("=========================\n");

    for (int i = 0; i < 4; i++) {
        int val = new_vals[i];
        int pos = find_pos(root, val, 0);
        printf("%d: %d\n", val, pos);
    }

    // print_tree(root);
}

void test_binary_search() {
    int vals[10] = {1,3,4,4,5,15,20,22,29,40};
    int find_val = 0;
    int found_pos = binary_search(vals, 10, find_val);
    printf("FOUND POS: %d\n", found_pos);
}

void test_bptree_dump() {
    // int num_test = 10;

    // // create bptree
    // int vals[num_test];
    // for (int i = 0; i < num_test; i++) {
    //     vals[i] = i;
    // }

    // // create bptree
    // BPTreeNode* root = NULL;
    // for (int pos = 0; pos < num_test; pos++) {
    //     int val = vals[pos];
    //     root = bplus_insert(root, val, &pos, 0);
    // }

    // // dump to file
    // FILE* fd_write = fopen("test_dump.bin", "wb");
    // dump_bptree(fd_write, root);
    // fflush(fd_write);
    // fclose(fd_write);
    // printf("\n\n==================\n\n");

    // // load from file
    // FILE* fd_read = fopen("test_dump.bin", "rb");
    // BPTreeNode* new_root = (BPTreeNode*) load_bptree(fd_read, &vals);

    // printf("\n\n==================\n\n");
    // for (int i = 0; i < num_test; i++) {
    //     int val = vals[i];
    //     int pos = find_pos(new_root, val);
    //     printf("%d: %d\n", val, pos, 0);
    // }

    // printf("\n\n==================\n\n");
    // // get leaf
    // BPTreeNode* curr = new_root;
    // while (!curr->is_leaf) {
    //     if (!curr->num_vals)
    //         return;

    //     curr = (BPTreeNode*) curr->pointers[0];
    // }

    // while (curr != NULL) {
    //     for (int i = 0; i < curr->num_vals; i++) {
    //         PosPtr* temp = curr->pointers[i];
    //         printf("%d: %d\n", curr->vals[i], temp->pos);
    //     }
    //     curr = curr->next;
    // }
}


// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void) {
    // test_binary_search();
    // test_b_plus_tree();
    // test_bptree_dump();
    // return 1;
    load_server_data();

    int shutdown = 0;
    while(!shutdown) {
        int server_socket = setup_server();
        if (server_socket < 0) {
            exit(1);
        }

        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        shutdown = handle_client(client_socket);
    }

    return 0;
}
