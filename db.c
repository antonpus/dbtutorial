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

enum ExecuteResult_t {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL 
};
typedef enum ExecuteResult_t ExecuteResult;

enum PrepareStatementResult_t {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareStatementResult_t PrepareStatementResult;

enum StatementType_t {
    INSERT,
    SELECT
};
typedef enum StatementType_t StatementType;

const int COLUMN_NAME_SIZE = 32;
const int COLUMN_EMAIL_SIZE = 255;

struct Row_t {
    uint32_t id;
    char username[COLUMN_NAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
};
typedef struct Row_t Row;

struct Statement_t {
    StatementType type;
    //check with pointer as Row *row_to_insert
    Row row_to_insert;
};
typedef struct Statement_t Statement;

const uint32_t TABLE_MAX_PAGES = 100;

struct Table_t {
    void *pages[TABLE_MAX_PAGES];
    uint32_t num_rows;
};
typedef struct Table_t Table;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row *source, void *destination) {
    memcpy(destination, source, ROW_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(destination, source, ROW_SIZE);
}

void *row_slot(Table *table, int row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if (!page) {
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    serialize_row(&(statement->row_to_insert), row_slot(table, table->num_rows));
    table->num_rows++;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table) {
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

PrepareStatementResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = INSERT;
        int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
                                statement->row_to_insert.username, statement->row_to_insert.email);
        return PREPARE_SUCCESS;
    }

    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
    switch(statement->type) {
        case INSERT:
            return execute_insert(statement, table);
        case SELECT:
            return execute_select(statement, table);
    }
}

Table *new_table() {
    Table *table = malloc(sizeof(Table));
    table->num_rows = 0;
    return table;
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

void do_sql_command(InputBuffer *input_buffer, Table *table) {
    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
        case (PREPARE_SUCCESS):
            switch (execute_statement(&statement, table)) {
                case (EXECUTE_SUCCESS):
                    printf("Executed.\n");
                    break;
                case (EXECUTE_TABLE_FULL):
                    printf("Error: Table full.\n");
                    break;
            }
            break;

        case (PREPARE_SYNTAX_ERROR):
            printf("Syntax error. Could not parse statement.\n");
            break;
        case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
            break;
    }
}

int main(int argc, char *argv[]) {
    Table *table = new_table();
    InputBuffer *input_buffer = new_input_buffer();

    while(true) {
        print_prompt();
        read_input(input_buffer);

        if (is_meta_command_in_buffer(input_buffer)) {
            do_meta_command(input_buffer);
        } else {
            do_sql_command(input_buffer, table);
        }
    }
}