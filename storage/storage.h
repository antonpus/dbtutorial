#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

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
    uint32_t total_pages_num;
    void *pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
    Pager *pager;
    uint32_t root_page_num;
};
typedef struct Table_t Table;

struct Cursor_t {
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_file;
};
typedef struct Cursor_t Cursor;

enum NodeType_t {NODE_INTERNAL, NODE_LEAF};
typedef enum NodeType_t NodeType;

/*
 * Common Node Header memory layout
 */
static const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
static const uint32_t NODE_TYPE_OFFSET = 0;
static const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
static const uint32_t IS_ROOT_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
static const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
static const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
static const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header memory layout
 */
static const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
static const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
static const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/*
 * Leaf Node Body memory layout
 */
static const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
static const uint32_t LEAF_NODE_KEY_OFFSET = 0;
static const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
static const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
static const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
static const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
static const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

Table *db_open(const char *file_name);

void db_close(Table* table);

Cursor *table_find(Table *table, uint32_t key);

Cursor *table_start(Table *table);

void *cursor_current_value(Cursor *cursor);

void cursor_advance(Cursor *cursor);

uint32_t *get_node_cells_number(void *node);

void *leaf_node_key(void *node, uint32_t cell_num);

void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value);

void *get_page(Pager *pager, uint32_t page_num);
