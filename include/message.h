#ifndef MESSAGE_H__
#define MESSAGE_H__

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK_DONE,
  /* Intermediate statuses */
  OK_WAIT_FOR_RESPONSE,
  /* Different types of errors */
  ERROR,
  UNKNOWN_COMMAND,
  OBJECT_DOES_NOT_EXIST,
  OBJECT_ALREADY_EXISTS,
  TABLE_AT_CAPACITY,
  QUERY_UNSUPPORTED,
  INCORRECT_FORMAT,
  FILE_NOT_FOUND
} StatusCode;


// status declares a code and associated message
typedef struct Status {
    StatusCode code;
    char* result;
} Status;


/**
 * Contains info about incoming data to print on 
 * client side. 
 **/
typedef struct PrintPayload {
    int num_results;    // how many rows to print
    int num_cols;       // how many different cols/results to print
} PrintPayload;


// message is a single packet of information sent between client/server.
// Status: defines the status of the message.
// length: defines the length of the string message to be sent.
// payload: defines the payload of the message.
// print_payload: indicates if print payload is coming
typedef struct message {
    Status status;
    int length;
    char* payload;
    int print_payload;
} message;


#endif
