#include <stdio.h>

/*
 * A CSV parser which treats csv file as a list of rows, and a row as a list of fields.
 * Each file contains zero or more rows, and each row contains zero or more fields.
 *
 *
 * The CSV format supported by this library:
 * rows are separated by `\r` or `\n` or `\r\n`.
 * fields are separated by field_delimiter character, which defaults to `,`, and can be specified
   manually(i.e. `\t`).
   But field_delimiter should not be `\r` or `\n` or `"`.
 * fields can be quoted by `"`.
 * fields without special characters can be optionally quoted or not.
 * fields with special characters akka `\r` and `\n` and field_delimiter should be quoted
   (notice that `\r` and `\n` in quoted field are treated as ordinary chars, not row separators.)
 * quote character `"` in field should be escaped as `""` and containing field should be quoted too.
 *
 *
 * Some special cases need to be noticed when parsing:
 * an empty line will be parsed as a row with zero field.
 * a line with `""` will be parsed as a row with one field whose content is empty string.
 * line with `""""` will be parse as a row with one field whose content is `""`.
 *
 * When write to a csv file, the opposite will be done, see `csv_write_row`.
 *
 *
 * This parser is a bit strict about quoting and escaping, and it returns an error if rules are
 * violated.
 * (In comparison, the parser in Python standard library is more tolerant.)
 */


/* error_code */
enum {
    CSV_ENOMEMORY = 1,            /* oom */
    CSV_EINVALID_FIELD_DELIMITER, /* invalid field delimiter, i.e. `"` */
    CSV_EIO,                      /* error when read/write */
    CSV_EINVALID_FORMAT,          /* invalid format, i.e. half quoted field */

    CSV_EINVALID_QUOTE_STYLE,     /* invalid quote style. should be QUOTE_ALL or QUOTE_MINIMAL */
    CSV_EINVALID_LINEBREAK,       /* invalid line break. should be LINEBREAK_LF, LINEBREAK_CRLF, LINEBREAK_CRLF */
};

/* error struct */
typedef struct {
    int error_code;
    char *message;
} csv_error_t;

/* create an error.
 * if oom happens when dynamiclly alloc memory for the new error, or the new
 * error we want to create is oom, just return the global one. */
csv_error_t *csv_error_new(int error_code, const char *message);
/* free the error */
void csv_error_free(csv_error_t *err);


typedef struct csv_parser_t csv_parser_t;
/* create a parser.
 * return the parser if succeeds. otherwise return NULL and err will be set
 * parser will not take ownership of `file`, you have to close the file yourself after parsing finished.
 * this may be inconvenient, but so you can parse input from stdin. */
csv_parser_t *csv_parser_new(FILE *file, csv_error_t **err);
/* create a parser with specified field_delimiter.
 * return the parser if succeeds. otherwise return NULL and err will be set */
csv_parser_t *csv_parser_new_with_field_delimiter(FILE *file, char field_delimiter, csv_error_t **err);
/* destroy parser */
void csv_parser_free(csv_parser_t *parser);

typedef struct csv_row_t csv_row_t;
/* create a empty csv row.
 * return the new row if succeeds. otherwise return NULL and err will be set */
csv_row_t *csv_row_new(csv_error_t **err);
/* destroy csv row */
void csv_row_free(csv_row_t *row);
/* reset csv row, but do not release resources it holds.
 * after reset, the row can be treated as a new empty row and reused.
 * call `csv_row_free` if the row is not needed any more. */
void csv_row_reset(csv_row_t *row);

/* append a new field to the row.
 * returns 0 when succeeds, -1 when fails */
int csv_row_append_field(csv_row_t *row, const char *field, size_t len, csv_error_t **err);
/* get field count */
int csv_row_field_count(const csv_row_t *row);
/* get field with specified index */
char *csv_row_field_get(const csv_row_t *row, int idx);

/* do the actual parse work.
 * returns the parsed row if succeeds.
 * return NULL if error occurred, and err is set.
 * return NULL too if parsing finished without error, but err will not be set.
 * (set err to NULL before call this function.)
 *
 * usually you will call this function in a loop until it returns NULL.
 * remember to call csv_row_free when you are done with the row.
 */
csv_row_t *csv_parse_next_row(csv_parser_t *parser, csv_error_t **err);


/* quote style */
enum {
    QUOTE_ALL,     /* all fields are quoted */
    QUOTE_MINIMAL, /* minimal quote, only fields containing special characters are quoted */
};

/* line break */
enum {
    LINEBREAK_LF = 0, /* \n   */
    LINEBREAK_CRLF,   /* \r\n */
    LINEBREAK_CR,     /* \r   */
};

typedef struct csv_writer_t csv_writer_t;
/* create a csv writer, which can be used to write csv rows into a file.
 * return the new writer if succeeds. otherwise return NULL and err will be set */
csv_writer_t *csv_writer_new(FILE *file, char field_delimiter, int quote_style, int line_break, csv_error_t **err);
/* create a csv writer with default parameters:
 * field_delimiter = ,
 * quote_style     = QUOTE_MINIMAL
 * line_break      = LINEBREAK_LF
 */
csv_writer_t *csv_writer_default(FILE *file, csv_error_t **err);
/* destroy csv writer */
void csv_writer_free(csv_writer_t *writer);

/* write one row into the file
 * returns 0 when succeeds, -1 when fails
 *
 * generally, if quote_style is QUOTE_ALL, every field will be quoted. if quote_style is
 * QUOTE_MINIMAL, only fields containing special characters will be quoted.
 *
 * however, some special cases will be handled to keep consistency with the parser, no matter which
 * quote_style:
 * a row with zero field, will be written as an empty line.
 * a row with one field which is empty string will be written as `""`.
 *
 * each row is separated by line break according to line_break.
 */
int csv_write_row(csv_writer_t *writer, const csv_row_t *row, csv_error_t **err);
