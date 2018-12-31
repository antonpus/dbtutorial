#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

struct Pager_t {
    int file_descriptor;
    uint32_t file_lenght;
    void *pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
    Pager *pager;
    uint32_t num_rows;
};
typedef struct Table_t Table;

struct Cursor_t {
    Table *table;
    uint32_t row_num;
    bool end_of_file;
};
typedef struct Cursor_t Cursor;

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

void *get_page(Pager *pager, uint32_t page_num) {

    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);

        uint32_t pages_num_in_file = pager->file_lenght / PAGE_SIZE;
        // We might save a partial page at the end of the file
        if (pager->file_lenght % PAGE_SIZE) {
              pages_num_in_file++;
        }

        if (page_num <= pages_num_in_file) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
        
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

Cursor *table_start(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_file = (table->num_rows == 0);
    return cursor;
}

Cursor *table_end(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_file = true;
    return cursor;
}

void *cursor_current_value(Cursor *cursor) {
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;

    void *page = get_page(cursor->table->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

void cursor_advance(Cursor *cursor) {
    cursor->row_num++;
    if (cursor->row_num == cursor->table->num_rows) {
        cursor->end_of_file = true;
    }
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Cursor *cursor = table_end(table);

    serialize_row(&(statement->row_to_insert), cursor_current_value(cursor));
    table->num_rows++;

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

    printf("%d rows selected.\n", table->num_rows);

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

Pager *pager_open(const char *file_name) {
    int file_descr = open(file_name,
                    O_RDWR |      // Read/Write mode
                    O_CREAT,  // Create file if it does not exist
                    S_IWUSR |     // User write permission
                    S_IRUSR   // User read permission
                 );

    if (file_descr == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_lenght = lseek(file_descr, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = file_descr;
    pager->file_lenght = file_lenght;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Table *db_open(const char *file_name) {
    Pager *pager = pager_open(file_name);
    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = pager->file_lenght / ROW_SIZE;
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

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size) {

    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* table) {
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    for (int i = 0; i < num_full_pages; i++) {
        void *page = pager->pages[i];
        if (page != NULL) {
            pager_flush(pager, i, PAGE_SIZE);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }

    // There may be a partial page to write to the end of the file
    // This should not be needed after we switch to a B-tree
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        if (pager->pages[num_full_pages] != NULL) {
            pager_flush(pager, num_full_pages, num_additional_rows * ROW_SIZE);
            free(pager->pages[num_full_pages]);
            pager->pages[num_full_pages] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    free(pager);
}

void do_meta_command(InputBuffer *input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
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

int main(int argc, char *argv[]) {

    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char *db_filename = argv[1];
    Table *table = db_open(db_filename);

    InputBuffer *input_buffer = new_input_buffer();

    while(true) {
        print_prompt();
        read_input(input_buffer);

        if (is_meta_command_in_buffer(input_buffer)) {
            do_meta_command(input_buffer, table);
        } else {
            do_sql_command(input_buffer, table);
        }
    }
}