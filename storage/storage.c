#include "storage.h"

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