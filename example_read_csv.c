#include <stdio.h>
#include <stdlib.h>

#include "csv.h"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        printf("usage: example_read_csv file.csv\n");
        exit(EXIT_FAILURE);
    }

    FILE *file = fopen(argv[1], "r");
    if (file == NULL) {
        perror("fopen:");
        exit(EXIT_FAILURE);
    }

    csv_error_t *err = NULL;
    csv_parser_t *parser = csv_parser_new(file, &err);
    if (parser == NULL) {
        fprintf(stderr,
                "create csv parser failed: code=%d, message=%s\n", err->error_code, err->message);

        fclose(file);
        csv_error_free(err);
        exit(EXIT_FAILURE);
    }

    csv_row_t *row = NULL;
    int row_count = 0;
    while ((row = csv_parse_next_row(parser, &err)) != NULL) {
        row_count++;
        printf("row=%d,field_count=%d: ", row_count, csv_row_field_count(row));

        int i;
        for (i = 0; i < csv_row_field_count(row); i++) {
            printf("%s", csv_row_field_get(row, i));
            if (i < csv_row_field_count(row) - 1) {
                printf(",\t");
            }
        }
        printf("<NL>\n");
        csv_row_free(row);
    }

    if (err != NULL) {
        fprintf(stderr, "parse csv failed: code=%d, message=%s\n", err->error_code, err->message);
        csv_error_free(err);
        csv_parser_free(parser);
        fclose(file);
        exit(EXIT_FAILURE);
    } else {
        printf("\n==============================\n");
        printf("parse succeeded!\nrow_count=%d\n", row_count);
    }

    csv_parser_free(parser);
    fclose(file);
    return 0;
}
