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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include <locale.h>
#include <inttypes.h>
#include <sched.h>
#include <time.h>

#include <protocol_binary.h>

#include "benchmark.h"

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

enum benchmark_phase {
    BENCHMARK_INIT,
    BENCHMARK_PRELOAD,
    BENCHMARK_RUNNING,
};

struct settings settings;
static struct workload workload;
static volatile enum benchmark_phase phase;
static volatile uint16_t init_count = 0;

struct core {
#ifdef PRINT_STATS
    uint64_t tx_get;
    uint64_t tx_set;
    uint64_t rx_get;
    uint64_t rx_set;
    uint64_t rx_success;
    uint64_t rx_fail;
    uint32_t *hist;
#endif
    int fd;
    uint8_t *rx_buf;
    uint8_t *tx_buf;
    size_t tx_len;
    struct workload_core wlc;
    uint64_t id;
    pthread_t pthread;
} __attribute__((aligned(64)));

static inline uint64_t get_nanos(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t) ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec;
}

static inline void record_latency(struct core *c, uint64_t nanos)
{
    size_t bucket = ((nanos / 1000) - HIST_START_US) / HIST_BUCKET_US;

    if (bucket >= HIST_BUCKETS) {
        bucket = HIST_BUCKETS - 1;
    }

    c->hist[bucket]++;
}

#ifdef PRINT_STATS
static inline uint64_t read_cnt(uint64_t *p)
{
    uint64_t v = *p;

    __sync_fetch_and_sub(p, v);
    return v;
}
#endif

static void prepare_core(struct core *c)
{
    struct sockaddr_in addr;

    c->rx_buf = malloc(BUFSIZE * 2);
    if (c->rx_buf == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating rx/tx buffer failed\n", c->id);
        abort();
    }
    c->tx_buf = c->rx_buf + BUFSIZE;

    c->hist = calloc(HIST_BUCKETS, sizeof(*c->hist));
    if (c->hist == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating histogram failed\n", c->id);
        abort();
    }

    c->fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (c->fd < 0) {
        perror("socket failed");
        fprintf(stderr, "[%"PRIu64"] socket failed\n", c->id);
        abort();
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(settings.dstip);
    addr.sin_port = htons(settings.dstport);

    if (connect(c->fd, (struct sockaddr *) &addr, sizeof(addr)) != 0) {
        perror("connect failed");
        fprintf(stderr, "[%"PRIu64"] connect failed\n", c->id);
        abort();
    }
}

static int core_tx(struct core *c)
{
    ssize_t ret;

    assert(c->tx_len > 0);

    while (1) {
        ret = send(c->fd, c->tx_buf, c->tx_len, 0);
        if (ret == (ssize_t) c->tx_len) {
            c->tx_len = 0;
            return 0;
        }

        if (ret < 0 && errno == EINTR) {
            continue;
        }

        if (ret >= 0) {
            fprintf(stderr, "[%"PRIu64"] short send: %zd of %zu\n", c->id, ret, c->tx_len);
        } else {
            perror("send failed");
        }
        return -1;
    }
}

static int core_rx(struct core *c, enum workload_op *op, uint16_t *err,
    uint32_t *opaque)
{
    protocol_binary_response_header *res;
    ssize_t ret;
    size_t reqlen;

    while (1) {
        ret = recv(c->fd, c->rx_buf, BUFSIZE, 0);
        if (ret < 0 && errno == EINTR) {
            continue;
        }
        break;
    }

    if (ret < 0) {
        perror("recv failed");
        return -1;
    }

    if (ret < (ssize_t) sizeof(*res)) {
        fprintf(stderr, "[%"PRIu64"] short response\n", c->id);
        return -1;
    }

    res = (protocol_binary_response_header *) c->rx_buf;
    if (res->response.magic != PROTOCOL_BINARY_RES) {
        fprintf(stderr, "[%"PRIu64"] invalid response magic: %x\n",
            c->id, res->response.magic);
        return -1;
    }

    reqlen = sizeof(*res) + ntohl(res->response.bodylen);
    if ((size_t) ret != reqlen) {
        fprintf(stderr, "[%"PRIu64"] malformed response length\n", c->id);
        return -1;
    }

    if (res->response.opcode == PROTOCOL_BINARY_CMD_GET) {
        *op = WL_OP_GET;
    } else if (res->response.opcode == PROTOCOL_BINARY_CMD_SET) {
        *op = WL_OP_SET;
    } else {
        fprintf(stderr, "[%"PRIu64"] unknown response opcode: %x\n",
            c->id, res->response.opcode);
        return -1;
    }

    *err = ntohs(res->response.status);
    *opaque = res->response.opaque;
    return 0;
}

static inline void set_request(struct core *c, struct key *k, uint32_t opaque)
{
    protocol_binary_request_set *set;
    protocol_binary_request_header *req;

    assert(sizeof(*set) + k->keylen + settings.valuesize <= BUFSIZE);

    set = (protocol_binary_request_set *) c->tx_buf;
    req = &set->message.header;

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
    memset((uint8_t *) (set + 1) + k->keylen, 0, settings.valuesize);

    c->tx_len = sizeof(*set) + k->keylen + settings.valuesize;
}

static inline void get_request(struct core *c, struct key *k, uint32_t opaque)
{
    protocol_binary_request_get *get;
    protocol_binary_request_header *req;

    assert(sizeof(*get) + k->keylen <= BUFSIZE);

    get = (protocol_binary_request_get *) c->tx_buf;
    req = &get->message.header;

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

    c->tx_len = sizeof(*get) + k->keylen;
}

static void preload_keys(struct core *c)
{
    enum workload_op op;
    uint16_t err;
    uint32_t opaque;
    size_t i;

    for (i = c->id; i < workload.keys_num; i += settings.threads) {
        set_request(c, &workload.keys[i], 0);
        if (core_tx(c) != 0) {
            abort();
        }

        if (core_rx(c, &op, &err, &opaque) != 0) {
            abort();
        }

        if (op != WL_OP_SET || err != 0) {
            fprintf(stderr, "[%"PRIu64"] preload set failed: op=%d err=%x\n",
                c->id, (int) op, err);
            abort();
        }
    }
}

static void *thread_run(void *arg)
{
    struct core *c = arg;

    prepare_core(c);

    while (phase < BENCHMARK_PRELOAD) {
        sched_yield();
    }

    preload_keys(c);
    __sync_fetch_and_add(&init_count, 1);

    while (phase < BENCHMARK_RUNNING) {
        sched_yield();
    }

    while (phase == BENCHMARK_RUNNING) {
        struct key *k;
        enum workload_op tx_op;
        enum workload_op rx_op;
        uint16_t err;
        uint32_t opaque;
        uint64_t now;

        workload_op(&workload, &c->wlc, &k, &tx_op);
        opaque = (uint32_t) get_nanos();

        if (tx_op == WL_OP_GET) {
            get_request(c, k, opaque);
            STATS_ADD(c, tx_get, 1);
        } else {
            set_request(c, k, opaque);
            STATS_ADD(c, tx_set, 1);
        }

        if (core_tx(c) != 0) {
            abort();
        }

        if (core_rx(c, &rx_op, &err, &opaque) != 0) {
            abort();
        }

        now = get_nanos();
        record_latency(c, now - opaque);

        if (rx_op == WL_OP_GET) {
            STATS_ADD(c, rx_get, 1);
        } else {
            STATS_ADD(c, rx_set, 1);
        }

        if (err == 0) {
            STATS_ADD(c, rx_success, 1);
        } else {
            STATS_ADD(c, rx_fail, 1);
        }
    }

    return NULL;
}

static inline void hist_fract_buckets(uint32_t *hist, uint64_t total,
        double *fracs, size_t *idxs, size_t num)
{
    size_t i;
    size_t j;
    uint64_t sum = 0;
    uint64_t goals[num];

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
    int i;
    int j;
    int num_threads;
    struct core *cs;
    uint64_t t_prev;
    uint64_t t_cur;
    long double *ttp;
    long double tp;
    long double tp_total;
    uint32_t *hist;
    uint32_t hx;
    uint64_t msg_total;
    double fracs[6] = { 0.5, 0.9, 0.95, 0.99, 0.999, 0.9999 };
    size_t fracs_pos[sizeof(fracs) / sizeof(fracs[0])];

    setlocale(LC_NUMERIC, "");

    init_settings(&settings);
    if (parse_settings(argc, argv, &settings) != 0) {
        print_usage();
        return EXIT_FAILURE;
    }

    num_threads = settings.threads;
    workload_init(&workload);

    assert(sizeof(*cs) % 64 == 0);
    cs = calloc(num_threads, sizeof(*cs));
    ttp = calloc(num_threads, sizeof(*ttp));
    hist = calloc(HIST_BUCKETS, sizeof(*hist));
    if (cs == NULL || ttp == NULL || hist == NULL) {
        fprintf(stderr, "allocation failed\n");
        return EXIT_FAILURE;
    }

    phase = BENCHMARK_INIT;
    for (i = 0; i < num_threads; i++) {
        cs[i].id = i;
        workload_core_init(&workload, &cs[i].wlc);
        if (pthread_create(&cs[i].pthread, NULL, thread_run, cs + i) != 0) {
            fprintf(stderr, "pthread_create failed\n");
            return EXIT_FAILURE;
        }
    }

    phase = BENCHMARK_PRELOAD;
    while (init_count < num_threads) {
        sched_yield();
    }

    printf("Preloading completed\n");
    fflush(stdout);

    phase = BENCHMARK_RUNNING;
    t_prev = get_nanos();
    while (1) {
        sleep(1);
        t_cur = get_nanos();
        tp_total = 0;
        msg_total = 0;

        for (i = 0; i < num_threads; i++) {
            tp = read_cnt(&cs[i].rx_success);
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

        printf("TP: total=%'.4Lf mops  50p=%d us  90p=%d us  95p=%d us  "
               "99p=%d us  99.9p=%d us  99.99p=%d us\n",
            tp_total / 1000000.,
            hist_value(fracs_pos[0]), hist_value(fracs_pos[1]),
            hist_value(fracs_pos[2]), hist_value(fracs_pos[3]),
            hist_value(fracs_pos[4]), hist_value(fracs_pos[5]));

        fflush(stdout);
        memset(hist, 0, sizeof(*hist) * HIST_BUCKETS);

        t_prev = t_cur;
    }
}
