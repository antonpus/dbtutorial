#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct InputBuffer_t {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

enum PrepareStatementResult_t {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareStatementResult_t PrepareStatementResult;

enum StatementType_t {
    INSERT,
    SELECT
};
typedef enum StatementType_t StatementType;

struct Statement_t {
    StatementType type;
};
typedef struct Statement_t Statement;

PrepareStatementResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strcmp(input_buffer->buffer, "insert") == 0) {
        statement->type = INSERT;
        return PREPARE_SUCCESS;
    }

    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement) {
    switch(statement->type) {
        case INSERT:
            printf("Inserted!\n");
            break;
        case SELECT:
            printf("Selected!\n");
            break;
    }
}

InputBuffer *new_input_buffer() {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt() {
    printf("db >");
}

void read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    // Skip tailing newLine
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;

    if (input_buffer->input_length <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
}

bool is_meta_command_in_buffer(InputBuffer *input_buffer) {
    return input_buffer->buffer[0] == '.';
}

void do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        printf("Unrecognized command '%s'.\n", input_buffer->buffer);
    }
}

void do_sql_command(InputBuffer *input_buffer) {
    Statement statement;
    PrepareStatementResult prepare_result = prepare_statement(input_buffer, &statement);

    if (prepare_result == PREPARE_SUCCESS) {
        execute_statement(&statement);
    } else if (prepare_result == PREPARE_UNRECOGNIZED_STATEMENT) {
        printf("Unrecognized statement found:'%s'\n", input_buffer->buffer);
    }
}

int main(int argc, char *argv[]) {
    InputBuffer *input_buffer = new_input_buffer();

    while(true) {
        print_prompt();
        read_input(input_buffer);

        if (is_meta_command_in_buffer(input_buffer)) {
            do_meta_command(input_buffer);
        } else {
            do_sql_command(input_buffer);
        }
    }
}