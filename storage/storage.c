#include "storage.h"

uint32_t *get_node_cells_number(void *node) {
    return (uint32_t*) ((char*) node + LEAF_NODE_NUM_CELLS_OFFSET);
}

void *leaf_node_cell(void *node, uint32_t cell_num) {
    return (char*) node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

void *leaf_node_key(void *node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

void *leaf_node_value(void *node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

NodeType get_node_type(void *node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    *get_node_cells_number(node) = 0;
}

void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value) {
    void *node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t total_cells_number = *get_node_cells_number(node);

    if (total_cells_number >= LEAF_NODE_MAX_CELLS) {
        printf("Need to implement splitting of a leaf node.\n");
        exit(EXIT_FAILURE);
    }

    //shift array to insert a cell_
    if (cursor->cell_num < total_cells_number) {
        for (uint32_t i = total_cells_number; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(get_node_cells_number(node))+=1;

    printf("cursor->cell_num=%d\n",cursor->cell_num);

    *((uint32_t*) leaf_node_key(node, cursor->cell_num)) = key;
    memcpy(leaf_node_value(node, cursor->cell_num), value, LEAF_NODE_VALUE_SIZE);
}

uint32_t find_cell_num_to_insert_into(void *node, uint32_t start, uint32_t end, uint32_t key_to_insert) {
    printf("===============\n");
    if (start == end) {
        uint32_t current_key = *((uint32_t*) leaf_node_key(node, start));
        if (key_to_insert < current_key) {
            printf("return=%d\n", start);
            return start;
        } else {
            printf("return=%d\n", start +1);

            return start + 1;
        }
    } else {
        uint32_t mid = start + (end - start)/2;
        uint32_t mid_key = *((uint32_t*) leaf_node_key(node, mid));

                printf("mid=%d\n", mid);
                printf("mid_key=%d\n", mid_key);



        if (key_to_insert == mid_key) {
                        printf("return=%d\n", mid + 1);

            return mid + 1;
        } else if (key_to_insert < mid_key) {
            return find_cell_num_to_insert_into(node, start, mid, key_to_insert);
        } else if (key_to_insert > mid_key) {
            return find_cell_num_to_insert_into(node, mid + 1, end, key_to_insert);
        }
    }
}

Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key) {
    void *node = get_page(table->pager, page_num);
    uint32_t total_cells_num = *get_node_cells_number(node);

    uint32_t cell_num_to_insert;
    bool end_of_file;
    if (total_cells_num == 0) {
        cell_num_to_insert = 0;
        end_of_file = true;
    } else {
        cell_num_to_insert = find_cell_num_to_insert_into(node, 0, total_cells_num - 1, key);
        end_of_file = cell_num_to_insert == total_cells_num;
    }

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;
    cursor->cell_num = cell_num_to_insert;
    cursor->end_of_file = end_of_file;
    return cursor;
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
        
        if (page_num >= pager->total_pages_num) {
            pager->total_pages_num = page_num + 1;
        }
    }
    return pager->pages[page_num];
}

Cursor *table_start(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void *root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *get_node_cells_number(root_node);
    cursor->end_of_file = (num_cells == 0);
    return cursor;
}

Cursor *table_find(Table *table, uint32_t key) {
    void *root_node = get_page(table->pager, table->root_page_num);
    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, table->root_page_num, key);
    } else {
        printf("Need to implement searching through internal nodes\n");
        exit(EXIT_FAILURE);
    }
}

void *cursor_current_value(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    void *page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    void *node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*get_node_cells_number(node))) {
        cursor->end_of_file = true;
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
    pager->total_pages_num = file_lenght / PAGE_SIZE;

    if (file_lenght % PAGE_SIZE != 0) {
        printf("Db file has not a whole number of pages. Corrupted file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Table *db_open(const char *file_name) {
    Pager *pager = pager_open(file_name);
    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->total_pages_num == 0) {
        void *root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }
    return table;
}

void pager_flush(Pager *pager, uint32_t page_num) {

    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* table) {
    Pager *pager = table->pager;

    for (int i = 0; i < pager->total_pages_num; i++) {
        void *page = pager->pages[i];
        if (page != NULL) {
            pager_flush(pager, i);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    free(pager);
}