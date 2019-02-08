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

void initialize_leaf_node(void *node) {
    *get_node_cells_number(node) = 0;
}

void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value) {
    void *node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t cells_number = *get_node_cells_number(node);

    if (cells_number >= LEAF_NODE_MAX_CELLS) {
        printf("Need to implement splitting of a leaf node.\n");
        exit(EXIT_FAILURE);
    }

    //why do we need this? maybe just insert into next page's cell?
    if (cursor->cell_num < cells_number) {
        for (uint32_t i = cells_number; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
                 LEAF_NODE_CELL_SIZE);
        }
    }

    *(get_node_cells_number(node))+=1;
    *((uint32_t*) leaf_node_key(node, cursor->cell_num)) = key;

    memcpy(leaf_node_value(node, cursor->cell_num), value, LEAF_NODE_CELL_SIZE);
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
        
        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
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

Cursor *table_end(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;

    void *root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *get_node_cells_number(root_node);
    cursor->cell_num = num_cells;
    cursor->end_of_file = true;
    return cursor;
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
    pager->num_pages = file_lenght / PAGE_SIZE;

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

    if (pager->num_pages == 0) {
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

    for (int i = 0; i < pager->num_pages; i++) {
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