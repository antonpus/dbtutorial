#include <string.h>
#include "../storage/storage.h"

struct InputBuffer_t {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

void print_prompt();

InputBuffer *new_input_buffer();

void read_input(InputBuffer *input_buffer);

bool is_meta_command_in_buffer(InputBuffer *input_buffer);

void do_meta_command(InputBuffer *input_buffer, Table* table);

void do_sql_command(InputBuffer *input_buffer, Table *table);