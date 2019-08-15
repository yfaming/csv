#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "csv.h"

/* the global oom error.
 * suppose got OOM when generate an error with user specified error code,
 * we need to return an error under this situation.
 */
static csv_error_t GLOBAL_OOM = {CSV_ENOMEMORY, "out of memory"};

/* return the global oom error */
static csv_error_t *csv_error_oom()
{
    return &GLOBAL_OOM;
}

/* malloc wrapper. when oom, return the global oom error */
static void *xmalloc(size_t len, csv_error_t **err)
{
    void *p = malloc(len);
    if (p == NULL) {
        *err = csv_error_oom();
        return NULL;
    }
    return p;
}

csv_error_t *csv_error_new(int error_code, const char *message)
{
    if (error_code == CSV_ENOMEMORY)
        return csv_error_oom();

    csv_error_t *err = (csv_error_t *)malloc(sizeof(csv_error_t));
    if (err == NULL)
        return csv_error_oom();

    err->error_code = error_code;
    err->message = (char *)malloc(strlen(message) + 1);
    if (err->message == NULL) {
        free(err);
        return csv_error_oom();
    }
    strcpy(err->message, message);

    return err;
}

/* free the error. if the error is the global oom err, do nothing */
void csv_error_free(csv_error_t *err)
{
    if (err->error_code == CSV_ENOMEMORY)
        return;
    free(err->message);
    free(err);
}

enum {
    SUCCEED = 0,
    FAIL = -1,
};

static const char QUOTE_CHAR = '"';
static const char COMMA_CHAR = ',';
static const char CR_CHAR = '\r';
static const char LF_CHAR = '\n';

static const char *CR_STR = "\r";
static const char *LF_STR = "\n";
static const char *CRLF_STR = "\r\n";
static const char *DOUBLE_QUOTES_STR = "\"\"";

/* check if c is valid field_delimiter */
static int is_valid_field_delimiter(char c)
{
    const char *INVALID_FILED_DELIMITER_STR = "\r\n\"";
    const char *p;
    for (p = INVALID_FILED_DELIMITER_STR; *p != '\0'; p++) {
        if (*p == c)
            return 0;
    }
    return 1;
}

/* parser */
struct csv_parser_t {
    FILE *file;
    char field_delimiter;
};

csv_parser_t *csv_parser_new(FILE *file, csv_error_t **err)
{
    csv_parser_t *parser = xmalloc(sizeof(csv_parser_t), err);
    if (parser == NULL)
        return NULL;

    parser->file = file;
    parser->field_delimiter = COMMA_CHAR;
    return parser;
}

csv_parser_t *csv_parser_new_with_field_delimiter(FILE *file, char field_delimiter, csv_error_t **err)
{
    if (!is_valid_field_delimiter(field_delimiter)) {
        *err = csv_error_new(CSV_EINVALID_FIELD_DELIMITER, "invalid field delimiter");
        return NULL;
    }

    csv_parser_t *parser = csv_parser_new(file, err);
    if (parser != NULL)
        parser->field_delimiter = field_delimiter;
    return parser;
}

void csv_parser_free(csv_parser_t *parser)
{
    free(parser);
}

/* csv row */
struct csv_row_t {
    int len;
    int capacity;
	char **fields;
};

csv_row_t *csv_row_new(csv_error_t **err)
{
    csv_row_t *row = xmalloc(sizeof(csv_row_t), err);
    if (row == NULL)
        return NULL;

    row->len = 0;
    row->capacity = 32; /* keey it small for dev */
    row->fields = xmalloc(row->capacity * sizeof(char *), err);
    if (row->fields == NULL) {
        free(row);
        return NULL;
    }

    int i;
    for (i = 0; i < row->capacity; i++)
        row->fields[i] = NULL;
    return row;
}

void csv_row_free(csv_row_t *row)
{
    int i;
    for (i = 0; i < row->capacity; i++)
        free(row->fields[i]);
    free(row->fields);
    free(row);
}

/* TODO: more aggressive reuse of dynamically allocated memory */
void csv_row_reset(csv_row_t *row)
{
    int i;
    for (i = 0; i < row->len; i++) {
        free(row->fields[i]);
        row->fields[i] = NULL;
    }
    row->len = 0;
}

/* expand the fields of the row.
 * returns 0 when succeeds, -1 when fails */
static int csv_row_expand_fields(csv_row_t *row, csv_error_t **err)
{
    int new_cap = row->capacity * 2;
    char **new_fields = xmalloc(new_cap * sizeof(char *), err);
    if (new_fields == NULL) {
        return FAIL;
    }

    int i;
    for (i = 0; i < row->len; i++)
        new_fields[i] = row->fields[i];
    for (i = row->len; i < new_cap; i++)
        new_fields[i] = NULL;

    free(row->fields);
    row->capacity = new_cap;
    row->fields = new_fields;
    return SUCCEED;
}

int csv_row_field_count(const csv_row_t *row)
{
    return row->len;
}

char *csv_row_field_get(const csv_row_t *row, int idx)
{
    return row->fields[idx];
}

int csv_row_append_field(csv_row_t *row, const char *field, size_t len, csv_error_t **err)
{
    if (row->len >= row->capacity && csv_row_expand_fields(row, err) == FAIL)
        return FAIL;

    char *s = xmalloc(len + 1, err);
    if (s == NULL)
        return FAIL;

    size_t i;
    for (i = 0; i < len; i++)
        s[i] = field[i];
    s[i] = '\0';

    row->fields[row->len++] = s;
    return SUCCEED;
}

/* the buffer used when parse
 * TODO: maybe make parser own the buffer to simplify the code
 */
typedef struct {
    int len;
    int capacity;
	char *p;
} buffer_t;

/* create a buffer
 * return the new buffer if succeeds. otherwise return NULL and err will be set */
static buffer_t *buffer_new(csv_error_t **err)
{
    buffer_t *buffer = xmalloc(sizeof(buffer_t), err);
    if (buffer == NULL)
        return NULL;

    buffer->len = 0;
    buffer->capacity = 256;
    buffer->p = xmalloc(buffer->capacity, err);
    if (buffer->p == NULL) {
        free(buffer);
        return NULL;
    }

    return buffer;
}

/* destroy the buffer */
static void buffer_free(buffer_t *buffer)
{
    free(buffer->p);
    free(buffer);
}

/* reset the buffer, so it can be used like an fresh new one */
static void buffer_reset(buffer_t *buffer)
{
    buffer->len = 0;
}

/* put a char to the end of the buffer.
 * will automatically expand to accommodate it.
 * returns 0 when succeeds, -1 when fails */
static int buffer_putc(buffer_t *buffer, char c, csv_error_t **err)
{
    if (buffer->len >= buffer->capacity) {
        /* expand */
        int new_cap = buffer->capacity * 2;
        char *new_p = xmalloc(new_cap, err);
        if (new_p == NULL)
            return FAIL;


        int i;
        for (i = 0; i < buffer->capacity; i++)
            new_p[i] = buffer->p[i];
        buffer->capacity = new_cap;
        free(buffer->p);
        buffer->p = new_p;
    }

    buffer->p[buffer->len++] = c;
    return SUCCEED;
}

/* add a new field to the end of the row with buffer's content
 * returns 0 when succeeds, -1 when fails */
static int csv_row_append_field_with_buffer(csv_row_t *row, const buffer_t *buffer, csv_error_t **err)
{
    return csv_row_append_field(row, buffer->p, buffer->len, err);
}


enum PARSE_STATE {
    ST_START,   /* before parse a field */
    ST_INFIELD, /* parsing an field */
};


/* lines are separated by `\r` or `\n` or `\r\n`.
 * when `\r` encountered, consume possible `\n` */
static void consume_end_of_line(int c, FILE *file)
{
    if (c == CR_CHAR) {
        int lookahead = fgetc(file);
        if (lookahead != LF_CHAR && lookahead != EOF)
            ungetc(lookahead, file);
    }
}

csv_row_t *csv_parse_next_row(csv_parser_t *parser, csv_error_t **err)
{
    buffer_t *buffer = buffer_new(err);
    if (buffer == NULL)
        return NULL;

    csv_row_t *row = csv_row_new(err);
    if (row == NULL)
        return NULL;

    int state = ST_START;
    int quoted = 0;

    int c;
    int lookahead;
    while (1) {
        c = fgetc(parser->file);
        switch (state) {
            case ST_START:
                if (c == QUOTE_CHAR) {
                    /* this means, when state is ST_START, quoted is always 0 */
                    quoted = 1;
                    state = ST_INFIELD;
                } else if (c == EOF) {
                    if (ferror(parser->file)) { /* io error happend*/
                        *err = csv_error_new(CSV_EIO, strerror(errno));
                        goto FAILURE_RETURN;
                    }

                    /* if this row has previous field(s), append an empty string field,
                     * since field_delimiter has been encountered previously.
                     * otherwise, this line is empty, return NULL to indicate parsing finished.
                     */
                    if (row->len > 0) {
                        if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                            goto FAILURE_RETURN;
                        }
                        buffer_free(buffer);
                        return row;
                    } else {
                        csv_row_free(row);
                        buffer_free(buffer);
                        return NULL;
                    }
                } else if (c == CR_CHAR || c == LF_CHAR) {
                    consume_end_of_line(c, parser->file);

                    /* if this row has previous field(s), append an empty string field,
                     * since field_delimiter has been encountered previously.
                     * otherwise, this line is empty, we have a row with zero fields
                     */
                    if (row->len > 0) {
                        if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                            goto FAILURE_RETURN;
                        }
                    }
                    buffer_free(buffer);
                    return row;
                } else if (c == parser->field_delimiter) {
                    /* empty string field */
                    if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                        goto FAILURE_RETURN;
                    }
                    /* get ready for next row */
                    buffer_reset(buffer);
                    state = ST_START;
                    quoted = 0;
                } else { /* normal char */
                    if (buffer_putc(buffer, c, err) == FAIL) {
                        goto FAILURE_RETURN;
                    }
                    state = ST_INFIELD;
                }
                break;

            case ST_INFIELD:
                if (c == QUOTE_CHAR) {
                    if (!quoted) {
                        *err = csv_error_new(CSV_EINVALID_FORMAT, "quote(\") should be quoted");
                        goto FAILURE_RETURN;
                    }

                    /* when quoted, look ahead to check if c indicates escape or field end or row end or illegal format */
                    lookahead = fgetc(parser->file);
                    if (lookahead == QUOTE_CHAR) { /* escape */
                        if (buffer_putc(buffer, QUOTE_CHAR, err) == FAIL)
                            goto FAILURE_RETURN;
                    } else if (lookahead == parser->field_delimiter) { /* end of field */
                        if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                            goto FAILURE_RETURN;
                        }
                        buffer_reset(buffer);
                        state = ST_START;
                        quoted = 0;
                    } else if (lookahead == CR_CHAR || lookahead == LF_CHAR) { /* end of row */
                        consume_end_of_line(c, parser->file);
                        if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                            goto FAILURE_RETURN;
                        }
                        buffer_free(buffer);
                        return row;
                    } else { /* otherwise, illegal format */
                        *err = csv_error_new(CSV_EINVALID_FORMAT, "closing quote can only followed by `\r\n` or field_delimiter");
                        goto FAILURE_RETURN;
                    }
                } else if (c == EOF) {
                    if (ferror(parser->file)) { /* io error happend*/
                        *err = csv_error_new(CSV_EIO, strerror(errno));
                        goto FAILURE_RETURN;
                    }
                    if (quoted) {
                        *err = csv_error_new(CSV_EINVALID_FORMAT, "unclosed quote");
                        goto FAILURE_RETURN;
                    } else {
                        /* treat like end of row.
                         * next invoke of csv_parse_next_row will return NULL to indicate parsing finished */
                        if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                            goto FAILURE_RETURN;
                        }
                        buffer_free(buffer);
                        return row;
                    }
                } else if (c == CR_CHAR || c == LF_CHAR) {
                    /* when quoted, \r and \n are just like normal characters
                     * otherwise, end of row */
                    if (quoted) {
                        if (buffer_putc(buffer, c, err) == FAIL)
                            goto FAILURE_RETURN;
                    } else {
                        consume_end_of_line(c, parser->file);
                        if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                            goto FAILURE_RETURN;
                        }
                        buffer_free(buffer);
                        return row;
                    }
                } else if (c == parser->field_delimiter) {
                    /* when quoted, field_delimiter is just like normal characters
                     * otherwise, end of field */
                    if (quoted) {
                        if (buffer_putc(buffer, c, err) == FAIL)
                            goto FAILURE_RETURN;
                    } else {
                        if (csv_row_append_field_with_buffer(row, buffer, err) == FAIL) {
                            goto FAILURE_RETURN;
                        }
                        buffer_reset(buffer);
                        state = ST_START;
                        quoted = 0;
                    }
                }
                else { /* normal char */
                    if (buffer_putc(buffer, c, err) == FAIL)
                        goto FAILURE_RETURN;
                }
                break;

        }
    }

FAILURE_RETURN:
    csv_row_free(row);
    buffer_free(buffer);
    return NULL;
}


/* writer */
struct csv_writer_t {
    FILE *file;
    char field_delimiter;
    int quote_style;
    int line_break;
};


csv_writer_t *csv_writer_new(FILE *file, char field_delimiter, int quote_style, int line_break, csv_error_t **err)
{
    if (!is_valid_field_delimiter(field_delimiter)) {
        *err = csv_error_new(CSV_EINVALID_FIELD_DELIMITER, "invalid field delimiter");
        return NULL;
    }
    if (quote_style != QUOTE_ALL && quote_style != QUOTE_MINIMAL) {
        *err = csv_error_new(CSV_EINVALID_QUOTE_STYLE, "invalid quote style");
        return NULL;
    }
    if (line_break != LINEBREAK_LF && line_break != LINEBREAK_CRLF && line_break != LINEBREAK_CR) {
        *err = csv_error_new(CSV_EINVALID_LINEBREAK, "invalid line break");
        return NULL;
    }

    csv_writer_t *writer = xmalloc(sizeof(csv_writer_t), err);
    if (writer == NULL)
        return NULL;

    writer->file = file;
    writer->field_delimiter = field_delimiter;
    writer->quote_style = quote_style;
    writer->line_break = line_break;
    return writer;
}

csv_writer_t *csv_writer_default(FILE *file, csv_error_t **err)
{
    return csv_writer_new(file, COMMA_CHAR, QUOTE_MINIMAL, LINEBREAK_LF, err);
}

void csv_writer_free(csv_writer_t *writer)
{
    free(writer);
}


/* write str
 * returns 0 when succeeds, -1 when fails */
static int csv_write_str(csv_writer_t *writer, const char *str, csv_error_t **err)
{
    if (fputs(str, writer->file) < 0) {
        *err = csv_error_new(CSV_EIO, strerror(errno));
        return FAIL;
    }
    return SUCCEED;
}

static int csv_write_char(csv_writer_t *writer, char c, csv_error_t **err)
{
    char str[2] = {c, '\0'};
    return csv_write_str(writer, str, err);
}

static int csv_write_newline(csv_writer_t *writer, csv_error_t **err)
{
    const char *newline;
    if (writer->line_break == LINEBREAK_LF)
        newline = LF_STR;
    else if (writer->line_break == LINEBREAK_CRLF)
        newline = CRLF_STR;
    else /* LINEBREAK_CR */
        newline = CR_STR;
    return csv_write_str(writer, newline, err);
}

static int csv_write_field(csv_writer_t *writer, const char *field, int field_idx, int field_count, csv_error_t **err)
{
    const char* p;
    int need_quote = 0;
    if (writer->quote_style == QUOTE_ALL)
        need_quote = 1;
    else { /* QUOTE_MINIMAL */
        /* check each character in field to determine whether quote is needed */
        for (p = field; *p != '\0'; p++) {
            if (*p == CR_CHAR || *p == LF_CHAR || *p == QUOTE_CHAR || *p == writer->field_delimiter) {
                need_quote = 1;
                break;
            }
        }
    }

    /* begining quote */
    if (need_quote && csv_write_char(writer, QUOTE_CHAR, err) == FAIL)
        return FAIL;

    /* content */
    int res = SUCCEED;
    for (p = field; *p != '\0'; p++) {
        if (*p == QUOTE_CHAR) /* escape */
            res = csv_write_str(writer, DOUBLE_QUOTES_STR, err);
        else
            res = csv_write_char(writer, *p, err);

        if (res == FAIL)
            return FAIL;
    }

    /* ending quote */
    if (need_quote && csv_write_char(writer, QUOTE_CHAR, err) == FAIL)
        return FAIL;

    /* field_delimiter. only needed when there are next field(s) */
    if (field_idx < field_count - 1) {
        return csv_write_char(writer, writer->field_delimiter, err);
    }
    return SUCCEED; /* succeed */
}


int csv_write_row(csv_writer_t *writer, const csv_row_t *row, csv_error_t **err)
{
    const int field_count = csv_row_field_count(row);
    if (field_count == 0)
        /* special case: 1) a row with zero field, will be written as an empty line. */
        /* do nothing */;
    else if (field_count == 1 && csv_row_field_get(row, 0)[0] == '\0') {
        /* special case: 2) a row with one field which is empty string will be written as `""` */
        if (csv_write_str(writer, DOUBLE_QUOTES_STR, err) == FAIL)
            return FAIL;
    } else {
        /* normal case */
        int i;
        for (i = 0; i < field_count; i++) {
            if (csv_write_field(writer, csv_row_field_get(row, i), i, field_count, err) == FAIL)
                return FAIL;
        }
    }

    return csv_write_newline(writer, err);
}
