// Built off starter code for "Asgn 2: A simple HTTP server."
// Skeleton By:     Eugene Chou
//                  Andrew Quinn
//                  Brian Zhao

#include "asgn2_helper_funcs.h"
#include "connection.h"
#include "response.h"
#include "request.h"
#include "queue.h"

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>

#include <sys/stat.h>
#include <sys/file.h>

void *start_worker(void *queue); // Worker thread function

void handle_connection(int);

void handle_get(conn_t *);
void handle_put(conn_t *);
void handle_unsupported(conn_t *);

pthread_mutex_t q_lock; // Mutex lock used in cond var for waiting threads
pthread_cond_t q_cond; // Cond var for waiting threads
pthread_mutex_t lock; // General mutex lock

int main(int argc, char **argv) {
    if (argc < 2 || argc > 4) {
        warnx("wrong arguments: %s port_num", argv[0]);
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int opt = 0;
    int num_threads = 4; // Number of worker threads, default 4
    size_t port;
    char *endptr = NULL;

    opt = getopt(argc, argv, "t:");

    switch (opt) {
    case 't':
        num_threads = atoi(optarg);

        // Checking if num threads inputted is valid
        if (snprintf(NULL, 0, "%d", num_threads) != (int) strlen(optarg)) {
            warnx("Invalid Number of Threads");
            return EXIT_FAILURE;
        }

        port = (size_t) strtoull(argv[3], &endptr, 10);

        if (endptr && *endptr != '\0') {
            warnx("invalid port number: %s", argv[3]);
            return EXIT_FAILURE;
        }

        break;

    default:
        port = (size_t) strtoull(argv[1], &endptr, 10);

        if (endptr && *endptr != '\0') {
            warnx("invalid port number: %s", argv[1]);
            return EXIT_FAILURE;
        }

        break;
    }

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket sock;
    listener_init(&sock, port);

    pthread_t workers[num_threads];
    queue_t *q = queue_new(num_threads);

    if (pthread_mutex_init(&q_lock, NULL) != 0 || pthread_mutex_init(&lock, NULL) != 0) {
        fprintf(stderr, "Mutex Lock Failed to Initialize\n");
        return EXIT_FAILURE;
    }

    if (pthread_cond_init(&q_cond, NULL) != 0) {
        fprintf(stderr, "Conditional Variable Failed to Initialize\n");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&workers[i], NULL, &start_worker, (void *) q) != 0) {
            fprintf(stderr, "Failed to Create Thread\n");
            return EXIT_FAILURE;
        }
    }

    // Loop for dispatcher thread, continuously accepts connections and signals
    // worker threads
    while (true) {
        uintptr_t connfd = listener_accept(&sock);
        queue_push(q, (void *) connfd);
        pthread_cond_signal(&q_cond);
    }

    return EXIT_SUCCESS;
}

void *start_worker(void *queue) {
    queue_t *q = (queue_t *) queue;
    uintptr_t connfd;

    // Loop for worker threads, continuously waits for new requests
    while (true) {
        pthread_mutex_lock(&q_lock);

        while (!queue_pop(q, (void **) &connfd)) {
            pthread_cond_wait(&q_cond, &q_lock);
        }

        pthread_mutex_unlock(&q_lock);

        handle_connection((int) connfd);
        close((int) connfd);
    }

    return 0;
}

void handle_connection(int connfd) {
    conn_t *conn = conn_new(connfd);

    const Response_t *res = conn_parse(conn);

    if (res != NULL) {
        conn_send_response(conn, res);
    } else {
        const Request_t *req = conn_get_request(conn);
        if (req == &REQUEST_GET) {
            handle_get(conn);
        } else if (req == &REQUEST_PUT) {
            handle_put(conn);
        } else {
            handle_unsupported(conn);
        }
    }

    conn_delete(&conn);
}

void handle_get(conn_t *conn) {
    char *uri = conn_get_uri(conn);
    const Response_t *res = NULL;
    struct stat statbuf;
    char *req_id = "0";
    char *new_req_id = conn_get_header(conn, "Request-Id");

    if (new_req_id) {
        req_id = new_req_id;
    }

    pthread_mutex_lock(&lock);

    int fd = open(uri, O_RDWR, 0600);

    if (errno == EACCES || errno == EISDIR) {
        pthread_mutex_unlock(&lock);
        res = &RESPONSE_FORBIDDEN;
        conn_send_response(conn, res);
        fprintf(stderr, "GET,%s,403,%s\n", uri, req_id);
        return;
    }

    if (fd < 0) {
        pthread_mutex_unlock(&lock);
        res = &RESPONSE_NOT_FOUND;
        conn_send_response(conn, res);
        fprintf(stderr, "GET,%s,404,%s\n", uri, req_id);
        return;
    }

    if (flock(fd, LOCK_SH) != 0) {
        pthread_mutex_unlock(&lock);
        res = &RESPONSE_INTERNAL_SERVER_ERROR;
        conn_send_response(conn, res);
        fprintf(stderr, "GET,%s,500,%s\n", uri, req_id);
        close(fd);
        return;
    }

    pthread_mutex_unlock(&lock);

    if (res == NULL) {
        stat(uri, &statbuf);
        conn_send_file(conn, fd, statbuf.st_size);
        fprintf(stderr, "GET,%s,200,%s\n", uri, req_id);
        close(fd);
        return;
    }

    close(fd);
    return;
}

void handle_unsupported(conn_t *conn) {
    char *uri = conn_get_uri(conn);
    char *req_id = "0";
    char *new_req_id = conn_get_header(conn, "Request-Id");

    if (new_req_id) {
        req_id = new_req_id;
    }

    // send responses
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
    fprintf(stderr, "%s,%s,501,%s\n", request_get_str(&REQUEST_UNSUPPORTED), uri, req_id);
    return;
}

void handle_put(conn_t *conn) {
    char *uri = conn_get_uri(conn);
    const Response_t *res = NULL;
    char *req_id = "0";
    char *new_req_id = conn_get_header(conn, "Request-Id");

    if (new_req_id) {
        req_id = new_req_id;
    }

    pthread_mutex_lock(&lock);

    // Check if file already exists before opening it.
    bool existed = access(uri, F_OK) == 0;

    // Open the file..
    int fd = open(uri, O_WRONLY, 0600);

    if (fd < 0) {
        if (errno == ENOENT) {
            fd = open(uri, O_WRONLY | O_CREAT, 0600);
        } else if (errno == EACCES || errno == EISDIR) {
            pthread_mutex_unlock(&lock);
            res = &RESPONSE_FORBIDDEN;
            conn_send_response(conn, res);
            fprintf(stderr, "PUT,%s,403,%s\n", uri, req_id);
            return;
        }

        if (fd < 0) {
            pthread_mutex_unlock(&lock);
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
            conn_send_response(conn, res);
            fprintf(stderr, "PUT,%s,500,%s\n", uri, req_id);
            return;
        }
    }

    if (flock(fd, LOCK_EX) != 0) {
        pthread_mutex_unlock(&lock);
        res = &RESPONSE_INTERNAL_SERVER_ERROR;
        conn_send_response(conn, res);
        fprintf(stderr, "PUT,%s,500,%s\n", uri, req_id);
        close(fd);
        return;
    }

    pthread_mutex_unlock(&lock);

    if (ftruncate(fd, 0) != 0) {
        res = &RESPONSE_INTERNAL_SERVER_ERROR;
        conn_send_response(conn, res);
        fprintf(stderr, "PUT,%s,500,%s\n", uri, req_id);
        close(fd);
        return;
    }

    res = conn_recv_file(conn, fd);
    int http_code = 0;

    if (res == NULL && existed) {
        res = &RESPONSE_OK;
        http_code = 200;
    } else if (res == NULL && !existed) {
        res = &RESPONSE_CREATED;
        http_code = 201;
    }

    conn_send_response(conn, res);
    fprintf(stderr, "PUT,%s,%d,%s\n", uri, http_code, req_id);
    close(fd);
    return;
}
