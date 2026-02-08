/*
 * Copyright 2019 University of Washington, Max Planck Institute for
 * Software Systems, and The University of Texas at Austin
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>
#include <assert.h>
#include <locale.h>
#include <inttypes.h>

#include <protocol_binary.h>

#include "benchmark.h"

#define MIN(a,b) ((b) < (a) ? (b) : (a))

#define CONN_DEBUG(c, co, x...) do { } while (0)
/*#define CONN_DEBUG(c, co, x...) \
    do { printf("%d.%d: ", (int) c->id, co->fd); \
         printf(x); } while (0)*/

#define PRINT_STATS
#ifdef PRINT_STATS
#   define STATS_ADD(c, f, n) c->f += n
#else
#   define STATS_ADD(c, f, n) do { } while (0)
#endif

#define HIST_START_US 0
#define HIST_BUCKET_US 1
#define HIST_BUCKETS 4096
#define BUFSIZE 2048


enum conn_state {
    CONN_CLOSED = 0,
    CONN_CONNECTING = 1,
    CONN_OPEN = 2,
};

enum benchmark_phase {
    BENCHMARK_INIT,
    BENCHMARK_PRELOAD,
    BENCHMARK_WARMUP,
    BENCHMARK_RUNNING,
    BENCHMARK_COOLDOWN,
    BENCHMARK_DONE,
};

struct settings settings;
static struct workload workload;
static volatile enum benchmark_phase phase;
static volatile uint16_t init_count = 0;
/*static uint32_t max_pending = 64;*/

struct connection {
    enum conn_state state;
    int fd;
    int ep_wr;
    uint32_t pending;
    uint32_t tx_len;
    uint32_t tx_off;
    uint32_t rx_len;
    void *rx_buf;
    void *tx_buf;
    struct connection *next;
#ifdef PRINT_STATS
    uint64_t cnt;
#endif
};

struct core {
    struct connection *conns;
#ifdef PRINT_STATS
    uint64_t tx_get;
    uint64_t tx_set;
    uint64_t rx_get;
    uint64_t rx_set;
    uint64_t rx_success;
    uint64_t rx_fail;
    uint64_t rx_calls;
    uint64_t rx_nanos;
    uint64_t tx_calls;
    uint64_t tx_nanos;
    uint64_t epupd;
    uint32_t *hist;
#endif
    int ep;
    struct workload_core wlc;
    uint64_t id;
    pthread_t pthread;
    uint16_t conn_pending;
    double tokens;
    uint64_t last_token_nanos;

    uint32_t msgs_pending;
    struct connection *q_first;
    struct connection *q_last;
} __attribute__((aligned(64)));

static inline uint64_t get_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t) ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec;
}

static inline void refill_tokens(struct core *c)
{
    uint64_t now;
    double elapsed_s;
    double max_tokens;

    if (settings.rate <= 0.0) {
        return;
    }

    now = get_nanos();
    if (c->last_token_nanos == 0) {
        c->last_token_nanos = now;
        return;
    }

    elapsed_s = (double) (now - c->last_token_nanos) / 1000000000.0;
    c->tokens += elapsed_s * settings.rate;

    max_tokens = settings.rate / 1000.0;
    if (max_tokens < 1.0) {
        max_tokens = 1.0;
    }
    if (c->tokens > max_tokens) {
        c->tokens = max_tokens;
    }

    c->last_token_nanos = now;
}

static inline void record_latency(struct core *c, uint64_t nanos)
{
    size_t bucket = ((nanos / 1000) - HIST_START_US) / HIST_BUCKET_US;
    if (bucket >= HIST_BUCKETS) {
        bucket = HIST_BUCKETS - 1;
    }
    c->hist[bucket]++;
    /*__sync_fetch_and_add(&c->hist[bucket], 1)*/;
}

#ifdef PRINT_STATS
static inline uint64_t read_cnt(uint64_t *p)
{
  uint64_t v = *p;
  __sync_fetch_and_sub(p, v);
  return v;
}
#endif

static inline int set_nonblock(int fd)
{
    int flag;

    if ((flag = fcntl(fd, F_GETFL, 0)) == -1) {
        return -1;
    }
    flag |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flag);
}

/* Open connection */
static inline void conn_connect(struct core *c, struct connection *co)
{
    int fd, cn, ret;
    struct epoll_event ev;
    struct sockaddr_in addr;

    cn = c->id;
    CONN_DEBUG(c, co, "Opening new connection\n");

    /* create socket */
    if ((fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP))
        < 0)
    {
        perror("creating socket failed");
        fprintf(stderr, "[%d] socket failed\n", cn);
        abort();
    }

    /* make socket non-blocking */
    if ((ret = set_nonblock(fd)) != 0) {
        fprintf(stderr, "[%d] set_nonblock failed: %d\n", cn, ret);
        abort();
    }

    /* add to epoll */
    ev.data.ptr = co;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR;
    if (epoll_ctl(c->ep, EPOLL_CTL_ADD, fd, &ev) < 0) {
        perror("epoll_ctl add failed");
        fprintf(stderr, "[%d] adding to epoll failed\n", cn);
        abort();
    }

    /* initialize address of socket */
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(settings.dstip);
    addr.sin_port = htons(settings.dstport);

    /* initiate non-blocking connect*/
    ret = connect(fd, (struct sockaddr *) &addr, sizeof(addr));
    if (ret == 0) {
        /* success */
        CONN_DEBUG(c, co, "Connection succeeded\n");
        co->state = CONN_OPEN;
    } else if (ret < 0 && errno == EINPROGRESS) {
        /* still going on */
        CONN_DEBUG(c, co, "Connection pending: %d\n", fd);
        co->state = CONN_CONNECTING;
    } else {
        /* opening connection failed */
        perror("connect failed");
        fprintf(stderr, "[%d] connect failed: %d errno=%d\n", cn, ret, errno);
        abort();
    }

    co->fd = fd;
    co->ep_wr = 1;
    co->pending = 0;
    co->rx_len = 0;
    co->tx_len = 0;
    co->tx_off = 0;
#ifdef PRINT_STATS
    co->cnt = 0;
#endif
}

static inline void conn_epupdate(struct core *c, struct connection *co, int wr)
{
    struct epoll_event ev;
    int cn = c->id;

    if (co->ep_wr == wr) {
        return;
    }
    /*printf("conn_epupdate(%p, %d)\n", co, wr);*/

    ev.data.ptr = co;
    ev.events = EPOLLHUP | EPOLLERR | (wr ? EPOLLOUT : 0) | EPOLLIN;
    if (epoll_ctl(c->ep, EPOLL_CTL_MOD, co->fd, &ev) != 0) {
        perror("epoll_ctl failed");
        fprintf(stderr, "[%d] epoll_ctl failed\n", cn);
        abort();
    }
    co->ep_wr = wr;

    STATS_ADD(c, epupd, 1);
}

/* receive response on connection */
static inline int conn_recv(struct core *c, struct connection *co,
        enum workload_op *op, uint16_t *err, uint32_t *opaque)
{
    protocol_binary_response_header *res =
        (protocol_binary_response_header *) co->rx_buf;
    int cn = c->id;
    size_t reqlen;
    ssize_t ret;

    ret = recv(co->fd, co->rx_buf, BUFSIZE, 0);
    if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return 1;
    } else if (ret <= 0) {
        fprintf(stderr, "[%d] recv failed: %d\n", cn, (int) ret);
        abort();
        return -1;
    }
    co->rx_len = ret;

    /* response is complete now */
    if (co->rx_len < sizeof(*res)) {
        fprintf(stderr, "[%d] short response: %u\n", cn, co->rx_len);
        return -1;
    }

    if (res->response.magic != PROTOCOL_BINARY_RES) {
        fprintf(stderr, "[%d] invalid magic on response: %x\n", cn,
                res->response.magic);
        abort();
    }

    reqlen = sizeof(*res) + ntohl(res->response.bodylen);
    if (co->rx_len < reqlen) {
        fprintf(stderr, "[%d] truncated response: got %u need %zu\n", cn,
                co->rx_len, reqlen);
        return -1;
    }

    if (res->response.opcode == PROTOCOL_BINARY_CMD_GET) {
        *op = WL_OP_GET;
    } else if (res->response.opcode == PROTOCOL_BINARY_CMD_SET) {
        *op = WL_OP_SET;
    } else {
        fprintf(stderr, "[%d] conn_recv: unknown opcode=%x\n", cn,
                res->response.opcode);
    }
    *err = ntohs(res->response.status);
    *opaque = res->response.opaque;

    co->rx_len = 0;
    return 0;
}

/* send out request on connection */
static inline int conn_send(struct core *c, struct connection *co)
{
    int cn;
    ssize_t ret;
    //uint64_t tsc;

    assert(co->tx_len > 0);
    assert(co->tx_off == 0);

    cn = c->id;

    //tsc = get_nanos();
    ret = send(co->fd, co->tx_buf, co->tx_len, 0);
    //STATS_ADD(c, tx_nanos, get_nanos() - tsc);
    //STATS_ADD(c, tx_calls, 1);
    if (ret == (ssize_t) co->tx_len) {
        co->tx_off = 0;
        co->tx_len = 0;
        return 0;
    } else if (ret > 0) {
        fprintf(stderr, "[%d] partial udp send: %zd of %u\n", cn, ret, co->tx_len);
        abort();
        return -1;
    } else if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        /* send failed */
        fprintf(stderr, "[%d] send failed: %d\n", cn, (int) ret);
        abort();
        return -1;
    } else if (ret < 0 &&
            (errno == EAGAIN || errno == EWOULDBLOCK))
    {
        /* send would block */
        return 1;
    }

    return -1;
}

static inline void set_request(struct core *c, struct connection *co,
        struct key *k, uint32_t opaque)
{
    protocol_binary_request_set *set =
        (protocol_binary_request_set *) co->tx_buf;
    protocol_binary_request_header *req = &set->message.header;

    assert(co->tx_off == 0);
    assert(co->tx_len == 0);
    assert(sizeof(*set) + k->keylen + settings.valuesize <= BUFSIZE);

    req->request.magic = PROTOCOL_BINARY_REQ;
    req->request.opcode = PROTOCOL_BINARY_CMD_SET;
    req->request.keylen = htons(k->keylen);
    req->request.extlen = 8;
    req->request.datatype = 0;
    req->request.reserved = 0;
    req->request.bodylen = htonl(8 + k->keylen + settings.valuesize);
    req->request.opaque = opaque;
    req->request.cas = 0;
    set->message.body.flags = 0;
    set->message.body.expiration = 0;

    memcpy(set + 1, k->key, k->keylen);

    co->tx_len = sizeof(*set) + k->keylen + settings.valuesize;
}

static inline void get_request(struct core *c, struct connection *co,
        struct key *k, uint32_t opaque)
{
    protocol_binary_request_get *get =
        (protocol_binary_request_get *) co->tx_buf;
    protocol_binary_request_header *req = &get->message.header;

    assert(co->tx_off == 0);
    assert(co->tx_len == 0);
    assert(sizeof(*get) + k->keylen <= BUFSIZE);

    req->request.magic = PROTOCOL_BINARY_REQ;
    req->request.opcode = PROTOCOL_BINARY_CMD_GET;
    req->request.keylen = htons(k->keylen);
    req->request.extlen = 0;
    req->request.datatype = 0;
    req->request.reserved = 0;
    req->request.bodylen = htonl(k->keylen);
    req->request.opaque = opaque;
    req->request.cas = 0;

    memcpy(get + 1, k->key, k->keylen);

    co->tx_len = sizeof(*get) + k->keylen;
}

static void poll_conns(struct core *c, int timeout)
{
    struct connection *co;
    int ret, j, cn, status;
    struct epoll_event evs[32];
    socklen_t slen;
    uint32_t events;

    cn = c->id;

    /* get events */
    if ((ret = epoll_wait(c->ep, evs, 32, timeout)) < 0) {
        fprintf(stderr, "[%d] wait_conns epoll_wait failed\n", cn);
        abort();
    }

    for (j = 0; j < ret; j++) {
        co = evs[j].data.ptr;
        events = evs[j].events;

        /* we only exepect events for connections that are currently
         * connecting. */
        if (co->state != CONN_CONNECTING) {
            continue;
        }

        /* check for errors on the connection */
        if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
            fprintf(stderr, "[%d] wait_conns error on conn\n", cn);
            abort();
        }

        /* check connection state */
        slen = sizeof(status);
        if (getsockopt(co->fd, SOL_SOCKET, SO_ERROR, &status, &slen) < 0)
        {
            fprintf(stderr, "[%d] wait_conns getsockopt failed\n", cn);
            abort();
        }
        if (status != 0) {
            fprintf(stderr, "[%d] wait_conns conn failed\n", cn);
            abort();
        }

        c->conn_pending--;
        co->state = CONN_OPEN;
        conn_epupdate(c, co, 0);

        co->next = NULL;
        if (c->q_last == NULL) {
          c->q_last = co;
          c->q_first = co;
        } else {
          c->q_last->next = co;
          c->q_last = co;
        }

    }
}

/* initialize core */
static void prepare_core(struct core *c)
{
    int cn = c->id;
    uint32_t i;
    uint8_t *buf;

    /* create epoll */
    if ((c->ep = epoll_create(4 * settings.conns)) < 0) {
        fprintf(stderr, "[%d] epoll_create failed\n", c->ep);
        abort();
    }

    /* Allocate histogram */
    if ((c->hist = calloc(HIST_BUCKETS, sizeof(*c->hist))) == NULL) {
        fprintf(stderr, "[%d] allocating histogram failed\n", cn);
        abort();
    }

    /* Allocate connection structs */
    if ((c->conns = calloc(settings.conns, sizeof(*c->conns))) == NULL) {
        fprintf(stderr, "[%d] allocating connection structs failed\n", cn);
        abort();
    }

    /* Initiate connections */
    c->conn_pending = 0;
    c->tokens = 0.0;
    c->last_token_nanos = 0;
    c->msgs_pending = 0;
    c->q_first = c->q_last = NULL;
    for (i = 0; i < settings.conns; i++) {
        if ((buf = malloc(BUFSIZE * 2)) == NULL) {
            fprintf(stderr, "[%d] allocating conn buffer failed\n", cn);
        }
        c->conns[i].rx_buf = buf;
        c->conns[i].tx_buf = buf + BUFSIZE;
        c->conns[i].state = CONN_CONNECTING;
        c->conns[i].fd = -1;
        conn_connect(c, &c->conns[i]);
        if (c->conns[i].state == CONN_CONNECTING) {
            c->conn_pending++;
        } else {
            conn_epupdate(c, &c->conns[i], 0);
            c->conns[i].next = NULL;
            if (c->q_last == NULL) {
                c->q_last = &c->conns[i];
                c->q_first = &c->conns[i];
            } else {
                c->q_last->next = &c->conns[i];
                c->q_last = &c->conns[i];
            }
        }

        while (c->conn_pending >= 16) {
            poll_conns(c, 0);
        }
    }

    while (c->conn_pending > 0) {
        poll_conns(c, 0);
    }
    fprintf(stderr, "core ready: %d\n", cn);

}

/* wait for all connections to be established */
static void wait_conns(struct core *c)
{
    uint32_t i;

    for (i = 0; i < settings.conns; i++) {
        while (c->conns[i].state == CONN_CONNECTING) {
            poll_conns(c, 0);
        }
    }
}

/* wait for all connections to be established */
static void load_keys(struct core *c)
{
    int cn, ret, l;
    size_t i, pending;
    struct key *k;
    struct connection *co;
    struct epoll_event evs[8];
    uint32_t events, opaque;
    uint16_t err;
    enum workload_op op;

    cn = c->id;

    /* set connections to trigger epoll OUT events */
    for (i = 0; i < MIN(32, settings.conns); i++) {
        conn_epupdate(c, &c->conns[i], 1);
    }

    i = cn;
    pending = 0;
    while (i < workload.keys_num || pending > 0) {
        if ((ret = epoll_wait(c->ep, evs, 8, -1)) < 0) {
            fprintf(stderr, "[%d] load_keys epoll_wait failed\n", cn);
            abort();
        }

        for (l = 0; l < ret; l++) {
            co = evs[l].data.ptr;
            events = evs[l].events;

            /* check for errors on the connection */
            if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
                fprintf(stderr, "[%d] load_keys error on conn\n", cn);
                abort();
            }

            if ((events & EPOLLIN) == EPOLLIN) {
                /* data ready to receive */
                if (conn_recv(c, co, &op, &err, &opaque) == 0) {
                    if (err != 0) {
                        fprintf(stderr, "[%d] load_keys set failed:%x\n", cn, err);
                    }
                    pending--;
                    co->pending--;
                    conn_epupdate(c, co, 1);
                }
            } else {
                /* ready to send */
                assert((events & EPOLLOUT) == EPOLLOUT);
                if (co->tx_len == 0 && i < workload.keys_num) {
                    /* send out new request */
                    /*printf("[%d:%p] i=%zu  keys_num=%zu\n", cn, co, i, workload.keys_num);*/
                    k = &workload.keys[i];
                    set_request(c, co, k, 0);
                    if (conn_send(c, co) == 0) {
                        /* fully sent out -> poll for RX */
                        conn_epupdate(c, co, 0);
                    }
                    i += settings.threads;
                    pending++;
                    co->pending++;
                } else if (co->tx_len == 0) {
                    /* no more keys to initialize -> poll for RX */
                    conn_epupdate(c, co, 0);
                } else {
                    if (conn_send(c, co) == 0) {
                        /* fully sent out -> poll for RX */
                        conn_epupdate(c, co, 0);
                    }
                }
            }

        }
    }

    /* set connections to trigger epoll OUT events */
    /*for (i = 0; i < settings.conns; i++) {
        conn_epupdate(c, &c->conns[i], 1);
    }*/
}


static inline void conn_events(struct core *c, struct connection *co,
        uint32_t events)
{
    int cn;
    uint32_t opaque;
    uint16_t err;
    enum workload_op op = 0;

    cn = c->id;

    /* check for errors on the connection */
    if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
        fprintf(stderr, "[%d] error on connection\n", cn);
        abort();
    }

    /* receive responses */
    while (co->pending > 0 && conn_recv(c, co, &op, &err, &opaque) == 0) {
        record_latency(c, (uint32_t) get_nanos() - opaque);

        STATS_ADD(co, cnt, 1);
        if (op == WL_OP_GET) {
            STATS_ADD(c, rx_get, 1);
        } else {
            STATS_ADD(c, rx_set, 1);
        }
        if (err == 0) {
            STATS_ADD(c, rx_success, 1);
        } else {
            STATS_ADD(c, rx_fail, 1);
            fprintf(stderr, "[%d] load_keys set failed:%x\n", cn, err);
        }
        co->pending--;
        c->msgs_pending--;

        if (co->pending == 0) {
          co->next = NULL;
          if (c->q_last == NULL) {
            c->q_last = co;
            c->q_first = co;
          } else {
            c->q_last->next = co;
            c->q_last = co;
          }
        }
    }

    /* try to send out any remaining tx buffer contents */
    if (co->tx_len != 0) {
        conn_send(c, co);
    }

    /* make sure we epoll for write iff we're actually blocked on writes */
    conn_epupdate(c, co, co->tx_len != 0);

}

static inline void send_pending(struct core *c)
{
    struct connection *co;
    uint32_t opaque;
    enum workload_op op = 0;
    struct key *k;

    refill_tokens(c);

    /* send out new requests */
    while (c->q_first != NULL) {
        if (settings.rate > 0.0 && c->tokens < 1.0) {
            break;
        }

        co = c->q_first;
        c->q_first = co->next;
        if (c->q_first == NULL)
          c->q_last = NULL;

        /* pick a key and operation */
        workload_op(&workload, &c->wlc, &k, &op);

        /* assign a time stamp */
        opaque = get_nanos();

        /* assemble request */
        if (op == WL_OP_GET) {
            get_request(c, co, k, opaque);
            STATS_ADD(c, tx_get, 1);
        } else {
            set_request(c, co, k, opaque);
            STATS_ADD(c, tx_set, 1);
        }

        /* send out request */
        conn_send(c, co);
        conn_epupdate(c, co, co->tx_len != 0);

        co->pending++;
        c->msgs_pending++;
        if (settings.rate > 0.0) {
            c->tokens -= 1.0;
        }
    }
}


static void *thread_run(void *arg)
{
    struct core *c = arg;
    int i, cn, ret, ep, num_evs;
    struct connection *co;
    struct epoll_event *evs;

    /* initiate core struct and connections */
    prepare_core(c);

    cn = c->id;
    ep = c->ep;

    printf("[%d] Waiting for connections...\n", cn);
    fflush(stdout);

    /* wait for connections to be established */
    wait_conns(c);

     printf("[%d] Waiting for connections done\n", cn);
    fflush(stdout);

   /* wait until we start running */
    while (phase < BENCHMARK_PRELOAD) {
        sched_yield();
    }

    printf("[%d] Preloading keys...\n", cn);
    fflush(stdout);
    /* pre-load keys */
    load_keys(c);
    printf("[%d] Preloaded keys...\n", cn);
    fflush(stdout);

    __sync_fetch_and_add(&init_count, 1);

   /* wait until we start running */
    while (phase < BENCHMARK_WARMUP) {
        sched_yield();
    }

    num_evs = 32;
    if ((evs = calloc(num_evs, sizeof(*evs))) == NULL) {
        fprintf(stderr, "[%d] malloc failed\n", cn);
        abort();
    }

    /* wait until we start running */
    while (phase < BENCHMARK_RUNNING) {
        sched_yield();
    }
    printf("[%d] Start running...\n", cn);
    fflush(stdout);

    send_pending(c);

    while (phase == BENCHMARK_RUNNING) {
        /* poll periodically when rate-limited so tokens can refill */
        if ((ret = epoll_wait(ep, evs, num_evs,
                settings.rate > 0.0 ? 1 : -1)) < 0)
        {
            fprintf(stderr, "[%d] epoll_wait failed\n", cn);
            abort();
        }

        for (i = 0; i < ret; i++) {
            co = evs[i].data.ptr;
            conn_events(c, co, evs[i].events);
        }

        send_pending(c);
    }

    return NULL;
}

static inline void hist_fract_buckets(uint32_t *hist, uint64_t total,
        double *fracs, size_t *idxs, size_t num)
{
    size_t i, j;
    uint64_t sum = 0, goals[num];
    for (j = 0; j < num; j++) {
        goals[j] = total * fracs[j];
    }
    for (i = 0, j = 0; i < HIST_BUCKETS && j < num; i++) {
        sum += hist[i];
        for (; j < num && sum >= goals[j]; j++) {
            idxs[j] = i;
        }
    }
}

static inline int hist_value(size_t i)
{
    if (i == HIST_BUCKETS - 1) {
        return -1;
    }

    return i * HIST_BUCKET_US + HIST_START_US;
}

int main(int argc, char *argv[])
{
    int i, j, num_threads;
    struct core *cs;
    uint64_t t_prev, t_cur, t_start, t_end;
    long double *ttp, tp, tp_total;
    uint32_t *hist, hx, sec_elapsed;
    uint64_t msg_total, cnt_success;
    uint64_t total_success = 0;
    uint64_t total_tx_get = 0, total_tx_set = 0;
    uint64_t total_rx_get = 0, total_rx_set = 0, total_rx_fail = 0;
    uint64_t total_tx, total_rx;
    double elapsed_s;
    double fracs[6] = { 0.5, 0.9, 0.95, 0.99, 0.999, 0.9999 };
    size_t fracs_pos[sizeof(fracs) / sizeof(fracs[0])];
    double summary_fracs[3] = { 0.5, 0.99, 0.999 };
    size_t summary_pos[sizeof(summary_fracs) / sizeof(summary_fracs[0])];

    setlocale(LC_NUMERIC, "");

    /* parse settings from command line */
    init_settings(&settings);
    if (parse_settings(argc, argv, &settings) != 0) {
        print_usage();
        return EXIT_FAILURE;
    }
    num_threads = settings.threads;

    /* initialize workload */
    workload_init(&workload);

    /* allocate core structs */
    assert(sizeof(*cs) % 64 == 0);
    cs = calloc(num_threads, sizeof(*cs));

    /* allocate instrumentation structs */
    ttp = calloc(num_threads, sizeof(*ttp));
    hist = calloc(HIST_BUCKETS, sizeof(*hist));

    if (cs == NULL || ttp == NULL || hist == NULL) {
        fprintf(stderr, "allocation failed failed\n");
        return EXIT_FAILURE;
    }

    phase = BENCHMARK_INIT;

    for (i = 0; i < num_threads; i++) {
        cs[i].id = i;
        workload_core_init(&workload, &cs[i].wlc);
        if (pthread_create(&cs[i].pthread, NULL, thread_run, cs + i)) {
            fprintf(stderr, "pthread_create failed\n");
            return EXIT_FAILURE;
        }
    }

    //sleep(10);
    phase = BENCHMARK_PRELOAD;
    //sleep(10);
    phase = BENCHMARK_WARMUP;

    while (init_count < num_threads) {
        sched_yield();
    }
    printf("Preloading completed\n");
    fflush(stdout);
    phase = BENCHMARK_RUNNING;

    t_start = get_nanos();
    t_prev = t_start;

    while (1) {
        sleep(1);
        t_cur = get_nanos();
        tp_total = 0;
        msg_total = 0;
        sec_elapsed = (uint32_t) ((t_cur - t_start) / 1000000000ULL);
        for (i = 0; i < num_threads; i++) {
            cnt_success = read_cnt(&cs[i].rx_success);
            total_success += cnt_success;
            tp = cnt_success;
            tp /= (double) (t_cur - t_prev) / 1000000000.;
            ttp[i] = tp;
            tp_total += tp;

            for (j = 0; j < HIST_BUCKETS; j++) {
                hx = cs[i].hist[j];
                msg_total += hx;
                hist[j] += hx;
            }
        }

        hist_fract_buckets(hist, msg_total, fracs, fracs_pos,
                sizeof(fracs) / sizeof(fracs[0]));


        printf("[ %3us ] tp=%'.0Lf rps | tp=%'.4Lf mops | "
                "p50=%d us  p90=%d us  p95=%d us  p99=%d us  "
                "p99.9=%d us  p99.99=%d us\n",
                sec_elapsed, tp_total, tp_total / 1000000.,
                hist_value(fracs_pos[0]), hist_value(fracs_pos[1]),
                hist_value(fracs_pos[2]), hist_value(fracs_pos[3]),
                hist_value(fracs_pos[4]), hist_value(fracs_pos[5]));

#if 0
#ifdef PRINT_PERCORE
        for (i = 0; i < num_threads; i++) {
            printf("core[%d]=%'.2Lf mbps  ", i,
                    ttp[i] * message_size * 8 / 1000000.);
        }
#endif
        printf("\n");
        printf("stats:\n");
        for (i = 0; i < num_threads; i++) {
            printf("    core %2d: (tg=%"PRIu64", ts=%"PRIu64", rg=%"PRIu64
                ", rs=%"PRIu64", rS=%"PRIu64", rF=%"PRIu64", rC=%"PRIu64
                ", rN=%"PRIu64", tC=%"PRIu64", tN=%"PRIu64" epu=%"PRIu64
                ")\n", i,
                read_cnt(&cs[i].tx_get), read_cnt(&cs[i].tx_set),
                read_cnt(&cs[i].rx_get), read_cnt(&cs[i].rx_set),
                read_cnt(&cs[i].rx_success), read_cnt(&cs[i].rx_fail),
                read_cnt(&cs[i].rx_calls), read_cnt(&cs[i].rx_nanos),
                read_cnt(&cs[i].tx_calls), read_cnt(&cs[i].tx_nanos),
                read_cnt(&cs[i].epupd));
        }
#endif
#if 0
#ifdef PRINT_STATS
        for (i = 0; i < num_threads; i++) {
            for (j = 0; j < settings.conns; j++) {
                printf("      t[%d].conns[%d]:  pend=%u  rx_l=%u  tx_l=%u  "
                        "tx_o=%u  cnt=%"PRIu64" fd=%d\n",
                        i, j, cs[i].conns[j].pending, cs[i].conns[j].rx_len,
                        cs[i].conns[j].tx_len, cs[i].conns[j].tx_len, cs[i].conns[j].cnt,
                        cs[i].conns[j].fd);
            }
        }
#endif
#endif

        fflush(stdout);
        memset(hist, 0, sizeof(*hist) * HIST_BUCKETS);

        if (settings.run_time > 0 &&
                t_cur - t_start >= (uint64_t) settings.run_time * 1000000000ULL)
        {
            break;
        }

        t_prev = t_cur;
    }

    phase = BENCHMARK_DONE;
    for (i = 0; i < num_threads; i++) {
        pthread_join(cs[i].pthread, NULL);
    }
    t_end = get_nanos();

    elapsed_s = (double) (t_end - t_start) / 1000000000.0;
    if (elapsed_s <= 0.0) {
        elapsed_s = 1.0;
    }

    for (i = 0; i < num_threads; i++) {
        total_success += read_cnt(&cs[i].rx_success);
        total_tx_get += read_cnt(&cs[i].tx_get);
        total_tx_set += read_cnt(&cs[i].tx_set);
        total_rx_get += read_cnt(&cs[i].rx_get);
        total_rx_set += read_cnt(&cs[i].rx_set);
        total_rx_fail += read_cnt(&cs[i].rx_fail);
    }

    memset(hist, 0, sizeof(*hist) * HIST_BUCKETS);
    msg_total = 0;
    for (i = 0; i < num_threads; i++) {
        for (j = 0; j < HIST_BUCKETS; j++) {
            hx = cs[i].hist[j];
            msg_total += hx;
            hist[j] += hx;
        }
    }
    if (msg_total > 0) {
        hist_fract_buckets(hist, msg_total, summary_fracs, summary_pos,
                sizeof(summary_fracs) / sizeof(summary_fracs[0]));
    } else {
        memset(summary_pos, 0, sizeof(summary_pos));
    }

    total_tx = total_tx_get + total_tx_set;
    total_rx = total_rx_get + total_rx_set;

    printf("Cores               : %d\n", num_threads);
    printf("Duration            : %.2f s\n", elapsed_s);
    printf("TX requests         : %" PRIu64 "\n", total_tx);
    printf("RX responses        : %" PRIu64 "\n", total_success);
    printf("RX decoded ops      : %" PRIu64 "\n", total_rx);
    printf("RX failures         : %" PRIu64 "\n", total_rx_fail);
    printf("Avg Load            : %.0f rps %.4f mops\n",
            (double) total_tx / elapsed_s, (double) total_tx / elapsed_s / 1000000.0);
    printf("Avg Throughput      : %.0f rps %.4f mops\n",
            (double) total_success / elapsed_s,
            (double) total_success / elapsed_s / 1000000.0);
    printf("Latency percentiles : p50=%d us  p99=%d us  p99.9=%d us\n",
            hist_value(summary_pos[0]), hist_value(summary_pos[1]),
            hist_value(summary_pos[2]));

    free(hist);
    free(ttp);
    free(cs);

    return 0;
}
