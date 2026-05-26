#define _GNU_SOURCE
#include <tcp_lib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <sched.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <protocol_binary.h>

#include "iokvs.h"

#define MAX_EVENTS 128
#define TCP_TX_BATCH 32

struct conn {
    int fd;
    uint32_t id;
    uint8_t want_out;
    uint8_t *rx_buf;
    uint8_t *tx_buf;
    size_t rx_len;
    size_t tx_len;
    size_t tx_off;
    size_t tx_cap;
};

struct core {
    int listen_fd;
    uint16_t id;
    uint32_t nconns;
    struct tcp_context_lib *ctx;
    struct tcp_wait *wait;
    struct tcp_wait_event *events;
    struct conn *conns;
    struct item_allocator ia;
    pthread_t pthread;
    uint64_t rx_malformed;
};

enum request_parse_result {
    REQ_PARSE_ERR = -1,
    REQ_PARSE_NEED_MORE = 0,
    REQ_PARSE_OK = 1,
};

static struct core *cores;
static struct item_allocator **iallocs;
static volatile size_t n_ready = 0;

static void conn_update_events(struct core *c, struct conn *con, uint8_t want_out)
{
    __u32 events;

    if (con->want_out == want_out) {
        return;
    }

    con->want_out = want_out;
    events = TCP_WAIT_IN;
    if (want_out) {
        events |= TCP_WAIT_OUT;
    }

    if (tcp_wait_mod(c->wait, con->fd, events, (__u64) con) != 0) {
        fprintf(stderr, "[%u] tcp_wait_mod failed\n", c->id);
        abort();
    }
}

static int core_listen_socket(struct core *c)
{
    int fd;
    int ret;
    struct sockaddr_in si;

    fd = tcp_socket(c->ctx);
    if (fd < 0) {
        fprintf(stderr, "[%u] socket failed\n", c->id);
        return -1;
    }

    ret = tcp_setsockopt(c->ctx, fd, SO_REUSEPORT);
    if (ret != 0) {
        fprintf(stderr, "[%u] set_reuseport failed\n", c->id);
        return -1;
    }

    memset(&si, 0, sizeof(si));
    si.sin_family = AF_INET;
    si.sin_addr.s_addr = settings.bindip;
    si.sin_port = htons(settings.port);

    if (tcp_bind(c->ctx, fd, (struct sockaddr *) &si, sizeof(si)) != 0) {
        perror("bind failed");
        fprintf(stderr, "[%u] bind failed\n", c->id);
        return -1;
    }
    if (tcp_listen(c->ctx, fd, settings.max_conns) != 0) {
        perror("listen failed");
        return -1;
    }

    return fd;
}

static int parse_one_request(const uint8_t *buf, size_t len, size_t *reqlen)
{
    const protocol_binary_request_header *req;

    if (len < sizeof(*req)) {
        return REQ_PARSE_NEED_MORE;
    }

    req = (const protocol_binary_request_header *) buf;
    if (req->request.magic != PROTOCOL_BINARY_REQ) {
        return REQ_PARSE_ERR;
    }

    *reqlen = sizeof(*req) + ntohl(req->request.bodylen);
    if (*reqlen > settings.max_msg_size) {
        return REQ_PARSE_ERR;
    }
    if (len < *reqlen) {
        return REQ_PARSE_NEED_MORE;
    }

    return REQ_PARSE_OK;
}

static size_t process_request(struct core *c, const uint8_t *req_buf,
    size_t rx_len, uint8_t *tx_buf)
{
    protocol_binary_request_header *req;
    protocol_binary_response_header *res;
    protocol_binary_response_get *gres;
    uint16_t kl;
    uint8_t extlen;
    uint8_t op;
    uint32_t bl;
    uint32_t vl;
    uint32_t h;
    size_t total_len;
    size_t tx_len;
    void *k;
    void *v;
    struct item *it;

    req = (protocol_binary_request_header *) req_buf;
    bl = ntohl(req->request.bodylen);
    total_len = sizeof(*req) + bl;
    if (total_len != rx_len) {
        return 0;
    }
    if (req->request.magic != PROTOCOL_BINARY_REQ) {
        return 0;
    }

    kl = ntohs(req->request.keylen);
    extlen = req->request.extlen;
    if ((uint32_t) extlen + kl > bl) {
        return 0;
    }

    op = req->request.opcode;
    res = (protocol_binary_response_header *) tx_buf;
    gres = (protocol_binary_response_get *) tx_buf;

    if (op == PROTOCOL_BINARY_CMD_GET) {
        k = (uint8_t *) (req + 1) + extlen;
        h = jenkins_hash(k, kl);
        it = hasht_get(k, kl, h);

        tx_len = sizeof(*gres);
        if (it != NULL) {
            tx_len += it->vallen;
        }
        if (tx_len > settings.max_msg_size) {
            if (it != NULL) {
                item_unref(it);
            }
            return 0;
        }

        res->response.magic = PROTOCOL_BINARY_RES;
        res->response.opcode = PROTOCOL_BINARY_CMD_GET;
        res->response.keylen = htons(0);
        res->response.extlen = 4;
        res->response.datatype = 0;
        if (it != NULL) {
            res->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
            res->response.bodylen = htonl(4 + it->vallen);
        } else {
            res->response.status = htons(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
            res->response.bodylen = htonl(4);
        }
        res->response.opaque = req->request.opaque;
        res->response.cas = 0;
        gres->message.body.flags = htonl(0);

        if (it != NULL) {
            memcpy(gres + 1, item_value(it), it->vallen);
            item_unref(it);
        }

        return tx_len;
    }

    if (op == PROTOCOL_BINARY_CMD_SET) {
        k = (uint8_t *) (req + 1) + extlen;
        v = (uint8_t *) k + kl;
        vl = bl - kl - extlen;

        it = ialloc_alloc(&c->ia, sizeof(struct item) + kl + vl, false);
        if (it != NULL) {
            h = jenkins_hash(k, kl);

            it->hv = h;
            it->vallen = vl;
            it->keylen = kl;
            memcpy(item_key(it), k, kl);
            memcpy(item_value(it), v, vl);

            hasht_put(it, NULL);
            item_unref(it);
        }

        tx_len = sizeof(protocol_binary_response_set);
        if (tx_len > settings.max_msg_size) {
            return 0;
        }

        res->response.magic = PROTOCOL_BINARY_RES;
        res->response.opcode = PROTOCOL_BINARY_CMD_SET;
        res->response.keylen = htons(0);
        res->response.extlen = 0;
        res->response.datatype = 0;
        res->response.status = htons(it != NULL ?
            PROTOCOL_BINARY_RESPONSE_SUCCESS : PROTOCOL_BINARY_RESPONSE_ENOMEM);
        res->response.bodylen = htonl(0);
        res->response.opaque = req->request.opaque;
        res->response.cas = 0;
        return tx_len;
    }

    return 0;
}

static size_t clean_log(struct item_allocator *ia, bool idle)
{
    struct item *it;
    struct item *nit;
    size_t n;

    if (!idle) {
        ialloc_cleanup_nextrequest(ia);
    }

    n = 0;
    while ((it = ialloc_cleanup_item(ia, idle)) != NULL) {
        n++;
        if (it->refcount != 1) {
            nit = ialloc_alloc(ia, sizeof(*nit) + it->keylen + it->vallen, true);
            if (nit == NULL) {
                fprintf(stderr, "Warning: ialloc_alloc failed during cleanup\n");
                abort();
            }

            nit->hv = it->hv;
            nit->vallen = it->vallen;
            nit->keylen = it->keylen;
            memcpy(item_key(nit), item_key(it), it->keylen + it->vallen);
            hasht_put(nit, it);
            item_unref(nit);
        }
        item_unref(it);
    }

    return n;
}

static int conn_tx(struct core *c, struct conn *con)
{
    while (con->tx_off < con->tx_len) {
        ssize_t ret = tcp_sendto(c->ctx, con->fd, con->tx_buf + con->tx_off,
            con->tx_len - con->tx_off, NULL, 0);

        if (ret > 0) {
            con->tx_off += (size_t) ret;
            continue;
        }
        if (ret == 0 || (ret < 0 &&
                (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)))
        {
            conn_update_events(c, con, 1);
            return 0;
        }

        perror("send failed");
        return -1;
    }

    con->tx_len = 0;
    con->tx_off = 0;
    conn_update_events(c, con, 0);
    return 0;
}

static size_t conn_process_rx(struct core *c, struct conn *con)
{
    bool blocked_on_tx = false;
    size_t parsed = 0;

    while (parsed < con->rx_len) {
        size_t reqlen;
        size_t tx_len;
        int parse_ret;

        if (con->tx_cap - con->tx_len < settings.max_msg_size) {
            blocked_on_tx = true;
            break;
        }

        parse_ret = parse_one_request(con->rx_buf + parsed,
            con->rx_len - parsed, &reqlen);
        if (parse_ret == REQ_PARSE_NEED_MORE) {
            break;
        }
        if (parse_ret == REQ_PARSE_ERR) {
            c->rx_malformed++;
            if (c->rx_malformed <= 3) {
                fprintf(stderr,
                    "[%u] malformed request byte (conn=%u rx_len=%zu parsed=%zu byte=0x%02x)\n",
                    c->id, con->id, con->rx_len, parsed, con->rx_buf[parsed]);
            }
            parsed++;
            continue;
        }

        tx_len = process_request(c, con->rx_buf + parsed, reqlen,
            con->tx_buf + con->tx_len);
        if (tx_len != 0) {
            con->tx_len += tx_len;
            clean_log(&c->ia, false);
        }
        parsed += reqlen;
    }

    if (parsed > 0) {
        con->rx_len -= parsed;
        if (con->rx_len > 0) {
            memmove(con->rx_buf, con->rx_buf + parsed, con->rx_len);
        }
    }

    if (!blocked_on_tx && con->rx_len >= settings.max_msg_size) {
        fprintf(stderr, "[%u] incomplete request exceeds max message size\n",
            c->id);
        abort();
    }

    return parsed;
}

static int conn_rx(struct core *c, struct conn *con)
{
    while (con->rx_len < settings.max_msg_size) {
        ssize_t ret = tcp_recvfrom(c->ctx, con->fd, con->rx_buf + con->rx_len,
            settings.max_msg_size - con->rx_len, NULL, 0);

        if (ret > 0) {
            con->rx_len += (size_t) ret;
            continue;
        }
        if (ret == 0) {
            break;
        }
        if (ret < 0 &&
                (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
        {
            break;
        }

        perror("recv failed");
        return -1;
    }

    conn_process_rx(c, con);
    if (con->tx_len != con->tx_off) {
        return conn_tx(c, con);
    }
    return 0;
}

static void conn_init(struct conn *con, int fd, uint32_t id)
{
    con->fd = fd;
    con->id = id;
    con->want_out = 0;
    con->rx_len = 0;
    con->tx_len = 0;
    con->tx_off = 0;
    con->tx_cap = (size_t) settings.max_msg_size * TCP_TX_BATCH;

    con->rx_buf = malloc(settings.max_msg_size);
    con->tx_buf = malloc(con->tx_cap);
    if (con->rx_buf == NULL || con->tx_buf == NULL) {
        fprintf(stderr, "connection buffer allocation failed\n");
        abort();
    }
}

static void core_accept(struct core *c)
{
    while (c->nconns < (uint32_t) settings.max_conns) {
        struct conn *con;
        int fd;

        fd = tcp_accept(c->ctx, c->listen_fd, NULL, NULL, SOCK_NONBLOCK);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                return;
            }
            perror("accept failed");
            abort();
        }

        con = &c->conns[c->nconns];
        conn_init(con, fd, c->nconns);

        if (tcp_wait_add(c->wait, fd, TCP_WAIT_IN, (__u64) con) != 0) {
            fprintf(stderr, "[%u] tcp_wait_add connection failed\n", c->id);
            abort();
        }

        c->nconns++;
    }

    fprintf(stderr, "[%u] reached max connections\n", c->id);
}

static void *processing_thread(void *data)
{
    struct core *c = (struct core *) data;

    c->conns = calloc(settings.max_conns, sizeof(*c->conns));
    c->events = calloc(MAX_EVENTS, sizeof(*c->events));
    if (c->conns == NULL || c->events == NULL) {
        fprintf(stderr, "[%u] allocation failed\n", c->id);
        abort();
    }

    c->ctx = tcp_ctx_new();
    if (c->ctx == NULL) {
        fprintf(stderr, "[%u] tcp_ctx_new failed\n", c->id);
        abort();
    }

    c->wait = tcp_wait_new(c->ctx);
    if (c->wait == NULL) {
        fprintf(stderr, "[%u] tcp_wait_new failed\n", c->id);
        abort();
    }

    if ((c->listen_fd = core_listen_socket(c)) < 0) {
        abort();
    }
    if (tcp_wait_add(c->wait, c->listen_fd, TCP_WAIT_ACCEPT, 0) != 0) {
        fprintf(stderr, "[%u] tcp_wait_add listen failed\n", c->id);
        abort();
    }

    ialloc_init_allocator(&c->ia);
    iallocs[c->id] = &c->ia;
    __sync_fetch_and_add(&n_ready, 1);

    while (1) {
        int ret;
        bool idle = true;

        tcp_poll_fast(c->ctx);
        tcp_poll_slow(c->ctx);
        ret = tcp_wait(c->wait, c->events, MAX_EVENTS, TCP_WAIT_NONBLOCK);
        tcp_poll_fast(c->ctx);
        tcp_poll_slow(c->ctx);
        if (ret < 0) {
            fprintf(stderr, "[%u] tcp_wait failed\n", c->id);
            abort();
        }

        for (int i = 0; i < ret; i++) {
            struct conn *con = (struct conn *) c->events[i].data;
            __u32 events = c->events[i].events;

            idle = false;
            if (con == NULL) {
                if ((events & TCP_WAIT_ACCEPT) != 0) {
                    core_accept(c);
                }
                continue;
            }

            if ((events & TCP_WAIT_OUT) != 0) {
                if (conn_tx(c, con) != 0) {
                    abort();
                }
                while (con->tx_len == con->tx_off && con->rx_len != 0) {
                    if (conn_process_rx(c, con) == 0) {
                        break;
                    }
                    if (con->tx_len != con->tx_off && conn_tx(c, con) != 0) {
                        abort();
                    }
                }
            }
            if ((events & TCP_WAIT_IN) != 0) {
                if (conn_rx(c, con) != 0) {
                    abort();
                }
                if (con->tx_len != con->tx_off && conn_tx(c, con) != 0) {
                    abort();
                }
            }
        }

        clean_log(&c->ia, idle);
    }

    return NULL;
}

static void maintenance(void)
{
    size_t i;

    while (1) {
        for (i = 0; i < settings.numcores; i++) {
            ialloc_maintenance(iallocs[i]);
        }
        usleep(10);
    }
}

int main(int argc, char *argv[])
{
    int ret;
    unsigned i;
    char name[32];

    if (settings_init(argc, argv) != 0) {
        return EXIT_FAILURE;
    }

    ret = tcp_connect_slow();
    if (ret != 0) {
        fprintf(stderr, "failed to connect to Chamelio\n");
        return EXIT_FAILURE;
    }

    hasht_init();
    ialloc_init();

    cores = calloc(settings.numcores, sizeof(*cores));
    iallocs = calloc(settings.numcores, sizeof(*iallocs));
    if (cores == NULL || iallocs == NULL) {
        fprintf(stderr, "allocation failed\n");
        return EXIT_FAILURE;
    }

    for (i = 0; i < settings.numcores; i++) {
        cores[i].id = (uint16_t) i;
        if (pthread_create(&cores[i].pthread, NULL, processing_thread, &cores[i]) != 0) {
            fprintf(stderr, "pthread_create failed\n");
            return EXIT_FAILURE;
        }

        snprintf(name, sizeof(name), "tcpflexkvs-w%u", i);
        pthread_setname_np(cores[i].pthread, name);
    }

    while (n_ready < settings.numcores) {
        sched_yield();
    }

    printf("Starting Chamelio TCP KVS maintenance\n");
    fflush(stdout);
    maintenance();
    return 0;
}
