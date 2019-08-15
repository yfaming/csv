#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mysql/mysql.h>
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

#include "csv.h"

/* build:
 * apt install libmysqlclient-dev
 * gcc -c `pkg-config --cflags mysqlclient` example_dump_mysql_db.c csv.c
 * gcc -o example_dump_mysql_db example_dump_mysql_db.o `pkg-config --libs mysqlclient`
 */


/* print usage and exit */
void usage(const char *progname) {
    const char *TMPL = ""
    "%s [OPTIONS] DB_NAME TABLE_NAME                                                          \n"
    "OPTIONS:                                                                                 \n"
    "-h     host. optional, defaults to localhost                                             \n"
    "-u     user name. optional, defaults to current user                                     \n"
    "-p     password. optional.                                                               \n"
    "       when specified, will prompt for secure input, do not specify as option argument.  \n"
    "       when not specified, assumes empty password.                                       \n"
    "-P     port. optional, defaults to 3306                                                  \n"
    "--help print usage info and exit                                                         \n";
    printf(TMPL, progname);
    exit(EXIT_FAILURE);
}

typedef struct {
    char *host;
    unsigned int port;
    char user[1024];
    char password[1024];
    char *db;
    char *table;
} options;

options parse_options(int argc, char *argv[]);
MYSQL *connect_db(const char *host, unsigned int port, const char *user, const char *password, const char *db);
int dump_table_to_csv(MYSQL *conn, const char *table);

int main(int argc, char *argv[])
{
    options opt = parse_options(argc, argv);

    if (mysql_library_init(0, NULL, NULL)) {
        fprintf(stderr, "could not initialize MySQL client library\n");
        exit(EXIT_FAILURE);
    }

    MYSQL *conn = connect_db(opt.host, opt.port, opt.user, opt.password, opt.db);
    if (conn == NULL) {
        mysql_library_end();
        exit(EXIT_FAILURE);
    }

    /* dump to csv */
    dump_table_to_csv(conn, opt.table);

    mysql_close(conn);
    mysql_library_end();
    return 0;
}


options parse_options(int argc, char *argv[])
{
    if (argc == 2 && strcmp(argv[1], "--help") == 0)
        usage(argv[0]);

    options options = {};
    options.user[0] = '\0';
    options.password[0] = '\0';
    int opt;
    long port;
    char *endptr;
    const char *optstring = ":h:u:pP:";
    int prompt_password = 0;
    while ((opt = getopt(argc, argv, optstring)) != -1) {
        switch (opt) {
            case 'h':
                options.host = optarg;
                break;
            case 'u':
                snprintf(options.user, 1024, "%s", optarg);
                break;
            case 'P':
                port = strtol(optarg, &endptr, 10);
                if (port <= 0 || port > 65535) {
                    fprintf(stderr, "invalid port, should be between 1 and 65535\n");
                    exit(EXIT_FAILURE);
                } else if (*endptr != '\0') {
                    fprintf(stderr, "port should be a number\n");
                    exit(EXIT_FAILURE);
                }
                options.port = (unsigned int)port;
                break;
            case 'p':
                prompt_password = 1;
                break;
            case ':':
                fprintf(stderr, "option `%c` requires an argument\n", optopt);
                exit(EXIT_FAILURE);
            case '?':
                fprintf(stderr, "unknow option: `%c`\n", optopt);
                exit(EXIT_FAILURE);
            default:
                fprintf(stderr, "unreachable! There must be a bug!\n");
                exit(EXIT_FAILURE);
        }
    }

    if (argc - optind != 2) {
        fprintf(stderr, "There should be 2 arguments for DB_NAME and TABLE_NAME\n\n");
        usage(argv[0]);
    } else {
        options.db = argv[optind];
        options.table = argv[optind + 1];
    }

    /* defaults for options */
    if (options.host == NULL)
        options.host = "localhost";
    if (options.port == 0)
        options.port = 3306;
    if (options.user[0] == '\0') {
        errno = 0;
        struct passwd *pwentry = getpwuid(getuid());
        if (pwentry == NULL) {
            if (errno)
                perror("getpwuid");
            else
                fprintf(stderr, "failed to get curent user\n");
            exit(EXIT_FAILURE);
        } else {
            snprintf(options.user, 1024, "%s", pwentry->pw_name);
        }
    }

    if (prompt_password) {
        snprintf(options.password, 1024, "%s", getpass("Enter password:"));
    } else {
        options.password[0] = '\0';
    }

    return options;
}


MYSQL *connect_db(const char *host, unsigned int port, const char *user, const char *password, const char *db)
{
    MYSQL *conn = mysql_init(NULL);
    if (conn == NULL) {
        fprintf(stderr, "error: mysql_init failed\n");
        return NULL;
    }
    mysql_options(conn, MYSQL_SET_CHARSET_NAME, MYSQL_AUTODETECT_CHARSET_NAME);

    if (mysql_real_connect(conn, host, user, password, db, port, NULL, 0) == NULL) {
        fprintf(stderr, "error: %s\n", mysql_error(conn));
        return NULL;
    }
    return conn;
}

int dump_table_to_csv(MYSQL *conn, const char *table)
{
    FILE *file = NULL;
    MYSQL_RES *result = NULL;
    csv_error_t *err = NULL;
    csv_writer_t *csv_writer = NULL;
    csv_row_t *csv_row = NULL;

    char filename[256];
    sprintf(filename, "%s.csv", table);
    file = fopen(filename, "w");
    if (file == NULL) {
        fprintf(stderr, "open file %s failed: %s", filename, strerror(errno));
        goto FAILURE_RETURN;
    }

    char sql[1024];
    sprintf(sql, "SELECT * FROM `%s`", table);
    if (mysql_query(conn, sql) != 0) {
        fprintf(stderr, "MySQL error: %s\n", mysql_error(conn));
        goto FAILURE_RETURN;
    }

    result = mysql_use_result(conn);
    if (result == NULL) {
        fprintf(stderr, "MySQL error: %s\n", mysql_error(conn));
        goto FAILURE_RETURN;
    }

    csv_writer = csv_writer_default(file, &err);
    if (csv_writer == NULL) {
        fprintf(stderr, "create csv writer failed: %s\n", err->message);
        goto FAILURE_RETURN;
    }

    csv_row = csv_row_new(&err);
    if (csv_row == NULL) {
        fprintf(stderr, "create csv row failed: %s\n", err->message);
        goto FAILURE_RETURN;
    }

    /* write column names to csv file*/
    MYSQL_FIELD *field = NULL;
    while ((field = mysql_fetch_field(result)) != NULL) {
        if (csv_row_append_field(csv_row, field->name, field->name_length, &err) == -1) {
            fprintf(stderr, "append field to csv row failed: %s\n", err->message);
            goto FAILURE_RETURN;
        }
    }
    if (csv_write_row(csv_writer, csv_row, &err) == -1) {
        fprintf(stderr, "write csv row to file failed: %s\n", err->message);
        goto FAILURE_RETURN;
    }
    /* for reuse */
    csv_row_reset(csv_row);

    /* write each row to csv */
    int i = 0;
    int n_fields = mysql_num_fields(result);
    MYSQL_ROW row = NULL;
    while ((row = mysql_fetch_row(result)) != NULL) {
        for (i = 0; i < n_fields; i++) {
            if (csv_row_append_field(csv_row, row[i], strlen(row[i]), &err) == -1) {
                fprintf(stderr, "append field to csv row failed: %s\n", err->message);
                goto FAILURE_RETURN;
            }
        }

        if (csv_write_row(csv_writer, csv_row, &err) == -1) {
            fprintf(stderr, "write csv row to file failed: %s\n", err->message);
            goto FAILURE_RETURN;
        }
        /* for reuse */
        csv_row_reset(csv_row);
    }

    csv_row_free(csv_row);
    csv_writer_free(csv_writer);
    mysql_free_result(result);
    fclose(file);
    return 0; /* succeed */

FAILURE_RETURN:
    if (csv_row != NULL)
        csv_row_free(csv_row);
    if (csv_writer != NULL)
        csv_writer_free(csv_writer);
    if (err != NULL)
        csv_error_free(err);
    if (result != NULL)
        mysql_free_result(result);
    if (file != NULL)
        fclose(file);
    return -1; /* FAIL; */
}
