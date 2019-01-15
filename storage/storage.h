#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static const uint32_t TABLE_MAX_PAGES = 100;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

static const int COLUMN_NAME_SIZE = 32;
static const int COLUMN_EMAIL_SIZE = 255;

struct Row_t {
    uint32_t id;
    char username[COLUMN_NAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
};
typedef struct Row_t Row;

static const uint32_t ID_SIZE = size_of_attribute(Row, id);
static const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
static const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
static const uint32_t ID_OFFSET = 0;
static const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
static const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
static const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

static const uint32_t PAGE_SIZE = 4096;
static const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
static const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

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

Table *db_open(const char *file_name);

void db_close(Table* table);

Cursor *table_end(Table *table);

Cursor *table_start(Table *table);

void *cursor_current_value(Cursor *cursor);

void cursor_advance(Cursor *cursor);
