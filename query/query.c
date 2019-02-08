#include "query.h"

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

struct Statement_t {
    StatementType type;
    Row row_to_insert;
};
typedef struct Statement_t Statement;

void print_prompt() {
    printf("db >");
}

void print_leaf_node(void* node) {
    uint32_t num_cells = *get_node_cells_number(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *((uint32_t*) leaf_node_key(node, i));
        printf("  - %d : %d\n", i, key);
    }
}

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row *source, void *destination) {
    memcpy(destination, source, ROW_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(destination, source, ROW_SIZE);
}

InputBuffer *new_input_buffer() {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
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

void do_meta_command(InputBuffer *input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager, 0));
   } else {
        printf("Unrecognized command '%s'.\n", input_buffer->buffer);
    }
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    void* node = get_page(table->pager, table->root_page_num);
    if ((*get_node_cells_number(node) >= LEAF_NODE_MAX_CELLS)) {
        return EXECUTE_TABLE_FULL;
    }

    Cursor *cursor = table_end(table);

    leaf_node_insert(cursor, statement->row_to_insert.id, &(statement->row_to_insert));

    print_row(&(statement->row_to_insert));
    printf("\nInserted.\n");

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table) {
    Row row;
    Cursor *cursor = table_start(table);
    while (!(cursor->end_of_file)) {
        deserialize_row(cursor_current_value(cursor), &row);
        cursor_advance(cursor);

        print_row(&row);
    }

    free(cursor);
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

void do_sql_command(InputBuffer *input_buffer, Table *table) {
    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
        case (PREPARE_SUCCESS):
            switch (execute_statement(&statement, table)) {
                case (EXECUTE_SUCCESS):
                    printf("Executed successfully.\n");
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