#include "query/query.h"

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