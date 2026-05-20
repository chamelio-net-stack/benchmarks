#define _GNU_SOURCE
#include <tcp_lib.h>
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
#include "clock.h"

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
    BENCHMARK_DONE,
};

enum parse_result {
    PARSE_ERR = -1,
    PARSE_NEED_MORE = 0,
    PARSE_OK = 1,
};

struct settings settings;
static struct workload workload;
static volatile enum benchmark_phase phase;
static volatile uint16_t init_count = 0;

struct conn {
    int fd;
    uint8_t connected;
    uint8_t want_out;
    uint8_t *rx_buf;
    uint8_t *tx_buf;
    size_t rx_len;
    size_t tx_len;
    size_t tx_off;
    uint64_t last_tsc;
    double tokens;
    double reqs_per_us;
    double token_cap;
    uint32_t id;
};

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
    struct tcp_context_lib *ctx;
    struct tcp_wait *wait;
    struct tcp_wait_event *events;
    size_t events_num;
    struct sockaddr_in dst_addr;
    struct conn *conns;
    struct workload_core wlc;
    uint64_t rx_malformed;
    uint64_t id;
    pthread_t pthread;
} __attribute__((aligned(64)));

static double tsc_per_us;

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
        fprintf(stderr, "[%"PRIu64"] tcp_wait_mod failed\n", c->id);
        abort();
    }
}

static inline void refill_tokens(struct conn *con)
{
    uint64_t now_tsc;
    uint64_t delta_tsc;
    double elapsed_us;

    now_tsc = util_rdtsc();
    delta_tsc = now_tsc - con->last_tsc;
    con->last_tsc = now_tsc;

    elapsed_us = (double) delta_tsc / tsc_per_us;
    con->tokens += elapsed_us * con->reqs_per_us;
    if (con->tokens > con->token_cap) {
        con->tokens = con->token_cap;
    }
}

static void prepare_conn(struct core *c, struct conn *con, uint32_t id)
{
    double reqs_per_s;

    con->fd = tcp_socket(c->ctx);
    if (con->fd < 0) {
        perror("socket failed");
        abort();
    }

    if (settings.srcip != INADDR_ANY || settings.srcport != 0) {
        struct sockaddr_in bind_addr;
        int ret;

        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_addr.s_addr = htonl(settings.srcip);
        bind_addr.sin_port = htons(settings.srcport);
        ret = tcp_bind(c->ctx, con->fd, (struct sockaddr *) &bind_addr,
            sizeof(bind_addr));
        if (ret != 0) {
            perror("bind failed");
            fprintf(stderr, "[%"PRIu64"] bind failed\n", c->id);
            abort();
        }
    }

    con->rx_buf = malloc(BUFSIZE * 2);
    if (con->rx_buf == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating rx/tx buffer failed\n", c->id);
        abort();
    }
    con->tx_buf = con->rx_buf + BUFSIZE;
    con->rx_len = 0;
    con->tx_len = 0;
    con->tx_off = 0;
    con->connected = 0;
    con->want_out = 0;
    con->id = id;

    reqs_per_s = (double) settings.rate * 1000.0;
    con->reqs_per_us = reqs_per_s / 1000000.0;
    con->token_cap = reqs_per_s / 1000.0;
    if (con->token_cap < 1.0) {
        con->token_cap = 1.0;
    }
    con->tokens = 0.0;
    con->last_tsc = util_rdtsc();
}

static void prepare_core(struct core *c)
{
    size_t i;

    c->ctx = tcp_ctx_new();
    if (c->ctx == NULL) {
        fprintf(stderr, "[%"PRIu64"] tcp_ctx_new failed\n", c->id);
        abort();
    }

    c->wait = tcp_wait_new(c->ctx);
    if (c->wait == NULL) {
        fprintf(stderr, "[%"PRIu64"] tcp_wait_new failed\n", c->id);
        abort();
    }

    c->hist = calloc(HIST_BUCKETS, sizeof(*c->hist));
    if (c->hist == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating histogram failed\n", c->id);
        abort();
    }

    c->conns = calloc(settings.nconns, sizeof(*c->conns));
    if (c->conns == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating connections failed\n", c->id);
        abort();
    }

    c->events_num = settings.nconns * 2;
    if (c->events_num < 1) {
        c->events_num = 1;
    }
    c->events = calloc(c->events_num, sizeof(*c->events));
    if (c->events == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating tcp wait events failed\n", c->id);
        abort();
    }

    memset(&c->dst_addr, 0, sizeof(c->dst_addr));
    c->dst_addr.sin_family = AF_INET;
    c->dst_addr.sin_addr.s_addr = htonl(settings.dstip);
    c->dst_addr.sin_port = htons(settings.dstport);

    for (i = 0; i < settings.nconns; i++) {
        prepare_conn(c, &c->conns[i], (uint32_t) i);
    }
}

static void conn_add_connected(struct core *c, struct conn *con)
{
    con->connected = 1;
    con->want_out = 0;
    if (tcp_wait_add(c->wait, con->fd, TCP_WAIT_IN, (__u64) con) != 0) {
        fprintf(stderr, "[%"PRIu64"] tcp_wait_add failed\n", c->id);
        abort();
    }
}

static void open_connections(struct core *c)
{
    size_t next = 0;
    size_t pending = 0;
    size_t connected = 0;
    size_t pending_limit = settings.mpending;
    struct tcp_wait_event *evs;

    if (pending_limit < 1) {
        pending_limit = 1;
    }
    evs = calloc(pending_limit, sizeof(*evs));
    if (evs == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating connect events failed\n", c->id);
        abort();
    }

    while (connected < settings.nconns) {
        while (pending < pending_limit && next < settings.nconns) {
            struct conn *con = &c->conns[next++];
            int ret;

            ret = tcp_connect(c->ctx, con->fd, (struct sockaddr *) &c->dst_addr,
                sizeof(c->dst_addr), SOCK_NONBLOCK);
            if (ret == 0) {
                conn_add_connected(c, con);
                connected++;
                continue;
            }
            if (ret < 0 && errno != EINPROGRESS) {
                perror("connect failed");
                abort();
            }

            if (tcp_wait_add(c->wait, con->fd, TCP_WAIT_CONNECTED,
                    (__u64) con) != 0)
            {
                fprintf(stderr, "[%"PRIu64"] tcp_wait_add connect failed\n",
                    c->id);
                abort();
            }
            pending++;
        }

        if (pending == 0) {
            continue;
        }

        tcp_poll_slow(c->ctx);
        int ret = tcp_wait(c->wait, evs, (int) pending_limit, 0);
        if (ret < 0) {
            fprintf(stderr, "[%"PRIu64"] tcp_wait failed\n", c->id);
            abort();
        }

        for (int i = 0; i < ret; i++) {
            struct conn *con = (struct conn *) evs[i].data;

            if ((evs[i].events & TCP_WAIT_CONNECTED) == 0) {
                continue;
            }
            con->connected = 1;
            con->want_out = 1;
            conn_update_events(c, con, 0);
            pending--;
            connected++;
        }
    }

    free(evs);
}

static int conn_send_pending(struct core *c, struct conn *con)
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
            return 1;
        }

        perror("send failed");
        return -1;
    }

    con->tx_len = 0;
    con->tx_off = 0;
    conn_update_events(c, con, 0);
    return 0;
}

static inline void set_request(uint8_t *buf, size_t *len, struct key *k,
    uint32_t opaque)
{
    protocol_binary_request_set *set;
    protocol_binary_request_header *req;

    assert(sizeof(*set) + k->keylen + settings.valuesize <= BUFSIZE);

    set = (protocol_binary_request_set *) buf;
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

    *len = sizeof(*set) + k->keylen + settings.valuesize;
}

static inline void get_request(uint8_t *buf, size_t *len, struct key *k,
    uint32_t opaque)
{
    protocol_binary_request_get *get;
    protocol_binary_request_header *req;

    assert(sizeof(*get) + k->keylen <= BUFSIZE);

    get = (protocol_binary_request_get *) buf;
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

    *len = sizeof(*get) + k->keylen;
}

static int parse_one_response(const uint8_t *buf, size_t len,
    size_t *reqlen, enum workload_op *op, uint16_t *err, uint32_t *opaque)
{
    const protocol_binary_response_header *res;

    if (len < sizeof(*res)) {
        return PARSE_NEED_MORE;
    }

    res = (const protocol_binary_response_header *) buf;
    if (res->response.magic != PROTOCOL_BINARY_RES) {
        return PARSE_ERR;
    }

    *reqlen = sizeof(*res) + ntohl(res->response.bodylen);
    if (*reqlen > BUFSIZE) {
        return PARSE_ERR;
    }
    if (len < *reqlen) {
        return PARSE_NEED_MORE;
    }

    if (res->response.opcode == PROTOCOL_BINARY_CMD_GET) {
        *op = WL_OP_GET;
    } else if (res->response.opcode == PROTOCOL_BINARY_CMD_SET) {
        *op = WL_OP_SET;
    } else {
        return PARSE_ERR;
    }

    *err = ntohs(res->response.status);
    *opaque = res->response.opaque;
    return PARSE_OK;
}

static inline void account_response(struct core *c, enum workload_op op,
    uint16_t err, uint32_t opaque)
{
    uint32_t now_us;
    uint32_t delta_us;

    if (phase != BENCHMARK_RUNNING) {
        if (op != WL_OP_SET || err != 0) {
            fprintf(stderr, "[%"PRIu64"] preload response failed: op=%d err=%x\n",
                c->id, (int) op, err);
            abort();
        }
        return;
    }

    now_us = (uint32_t) (get_nanos() / 1000);
    delta_us = now_us - opaque;
    record_latency(c, (uint64_t) delta_us * 1000);

    if (op == WL_OP_GET) {
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

static int conn_rx(struct core *c, struct conn *con)
{
    int processed = 0;

    while (1) {
        size_t parsed = 0;

        while (parsed < con->rx_len) {
            enum workload_op op;
            uint16_t err;
            uint32_t opaque;
            size_t reqlen;
            int parse_ret;

            parse_ret = parse_one_response(con->rx_buf + parsed,
                con->rx_len - parsed, &reqlen, &op, &err, &opaque);
            if (parse_ret == PARSE_NEED_MORE) {
                break;
            }
            if (parse_ret == PARSE_ERR) {
                c->rx_malformed++;
                if (c->rx_malformed <= 3) {
                    fprintf(stderr,
                        "[%"PRIu64"] malformed response byte (conn=%u rx_len=%zu parsed=%zu byte=0x%02x)\n",
                        c->id, con->id, con->rx_len, parsed, con->rx_buf[parsed]);
                }
                parsed++;
                continue;
            }

            account_response(c, op, err, opaque);
            parsed += reqlen;
            processed++;
        }

        if (parsed > 0) {
            con->rx_len -= parsed;
            if (con->rx_len > 0) {
                memmove(con->rx_buf, con->rx_buf + parsed, con->rx_len);
            }
        }

        if (con->rx_len >= BUFSIZE) {
            fprintf(stderr, "[%"PRIu64"] incomplete response exceeds BUFSIZE\n",
                c->id);
            return -1;
        }

        ssize_t ret = tcp_recvfrom(c->ctx, con->fd, con->rx_buf + con->rx_len,
            BUFSIZE - con->rx_len, NULL, 0);
        if (ret > 0) {
            con->rx_len += (size_t) ret;
            continue;
        }
        if (ret == 0) {
            return processed;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return processed;
        }

        perror("recv failed");
        return -1;
    }
}

static int core_poll(struct core *c, int timeout)
{
    int processed = 0;
    int ret;
    int flags = timeout == 0 ? TCP_WAIT_NONBLOCK : 0;

    tcp_poll_fast(c->ctx);
    tcp_poll_slow(c->ctx);
    ret = tcp_wait(c->wait, c->events, (int) c->events_num, flags);
    tcp_poll_fast(c->ctx);
    tcp_poll_slow(c->ctx);
    if (ret < 0) {
        fprintf(stderr, "[%"PRIu64"] tcp_wait failed\n", c->id);
        abort();
    }

    for (int i = 0; i < ret; i++) {
        struct conn *con = (struct conn *) c->events[i].data;
        __u32 events = c->events[i].events;

        if ((events & TCP_WAIT_OUT) != 0) {
            if (conn_send_pending(c, con) < 0) {
                abort();
            }
        }
        if ((events & TCP_WAIT_IN) != 0) {
            int rx = conn_rx(c, con);
            if (rx < 0) {
                abort();
            }
            processed += rx;
        }
    }

    return processed;
}

static void preload_keys(struct core *c)
{
    size_t i;
    size_t expected = 0;
    size_t received = 0;

    for (i = c->id; i < workload.keys_num; i += settings.threads) {
        struct conn *con = &c->conns[i % settings.nconns];

        while (con->tx_len != 0) {
            if (conn_send_pending(c, con) < 0) {
                abort();
            }
            if (con->tx_len != 0) {
                core_poll(c, -1);
            }
        }

        set_request(con->tx_buf, &con->tx_len, &workload.keys[i], 0);
        con->tx_off = 0;
        expected++;

        while (con->tx_len != 0) {
            if (conn_send_pending(c, con) < 0) {
                abort();
            }
            if (con->tx_len != 0) {
                received += (size_t) core_poll(c, -1);
            }
        }
    }

    while (received < expected) {
        received += (size_t) core_poll(c, -1);
    }
}

static void core_tx(struct core *c)
{
    for (size_t i = 0; i < settings.nconns; i++) {
        struct conn *con = &c->conns[i];

        refill_tokens(con);
        if (con->tx_len != 0) {
            if (conn_send_pending(c, con) < 0) {
                abort();
            }
            if (con->tx_len != 0) {
                continue;
            }
        }

        while (con->tokens >= 1.0 && phase == BENCHMARK_RUNNING) {
            struct key *k;
            enum workload_op tx_op;
            uint32_t opaque;

            workload_op(&workload, &c->wlc, &k, &tx_op);
            opaque = (uint32_t) (get_nanos() / 1000);
            if (tx_op == WL_OP_GET) {
                get_request(con->tx_buf, &con->tx_len, k, opaque);
                STATS_ADD(c, tx_get, 1);
            } else {
                set_request(con->tx_buf, &con->tx_len, k, opaque);
                STATS_ADD(c, tx_set, 1);
            }
            con->tx_off = 0;
            con->tokens -= 1.0;

            if (conn_send_pending(c, con) < 0) {
                abort();
            }
            if (con->tx_len != 0) {
                break;
            }
        }
    }
}

static void *thread_run(void *arg)
{
    struct core *c = arg;

    prepare_core(c);
    open_connections(c);

    while (phase < BENCHMARK_PRELOAD) {
        sched_yield();
    }

    preload_keys(c);
    __sync_fetch_and_add(&init_count, 1);

    while (phase < BENCHMARK_RUNNING) {
        sched_yield();
    }

    for (size_t i = 0; i < settings.nconns; i++) {
        c->conns[i].tokens = 0.0;
        c->conns[i].last_tsc = util_rdtsc();
    }

    while (phase == BENCHMARK_RUNNING) {
        core_tx(c);
        core_poll(c, 0);
        sched_yield();
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
    int ret;
    int num_threads;
    struct core *cs;
    uint64_t t_prev;
    uint64_t t_cur;
    uint32_t *hist_cur;
    uint64_t gen_total = 0;
    uint64_t tp_total = 0;
    uint64_t gen_int = 0;
    uint64_t tp_int = 0;
    uint64_t sec;
    long double load_mops;
    long double tp_mops;
    long double dt_s;
    uint64_t msg_total;
    double fracs[6] = { 0.5, 0.9, 0.95, 0.99, 0.999, 0.9999 };
    size_t fracs_pos[sizeof(fracs) / sizeof(fracs[0])];

    setlocale(LC_NUMERIC, "");

    init_settings(&settings);
    if (parse_settings(argc, argv, &settings) != 0) {
        print_usage();
        return EXIT_FAILURE;
    }

    ret = tcp_connect_slow();
    if (ret != 0) {
        fprintf(stderr, "failed to connect to Chamelio\n");
        return EXIT_FAILURE;
    }

    tsc_per_us = clock_tsc_per_us();

    num_threads = settings.threads;
    workload_init(&workload);

    assert(sizeof(*cs) % 64 == 0);
    cs = calloc(num_threads, sizeof(*cs));
    hist_cur = calloc(HIST_BUCKETS, sizeof(*hist_cur));
    if (cs == NULL || hist_cur == NULL)
    {
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
    for (sec = 1; sec <= settings.run_time; sec++) {
        sleep(1);
        t_cur = get_nanos();
        dt_s = (long double) (t_cur - t_prev) / 1000000000.0L;
        if (dt_s <= 0.0L) {
            dt_s = 1.0L;
        }
        gen_int = 0;
        tp_int = 0;
        msg_total = 0;

        for (i = 0; i < num_threads; i++) {
            gen_int += read_cnt(&cs[i].tx_get) + read_cnt(&cs[i].tx_set);
            tp_int += read_cnt(&cs[i].rx_success);
        }

        gen_total += gen_int;
        tp_total += tp_int;

        for (j = 0; j < HIST_BUCKETS; j++) {
            uint64_t sum = 0;
            for (i = 0; i < num_threads; i++) {
                sum += cs[i].hist[j];
            }
            hist_cur[j] = (uint32_t) sum;
            msg_total += hist_cur[j];
        }

        if (msg_total > 0) {
            hist_fract_buckets(hist_cur, msg_total, fracs, fracs_pos,
                sizeof(fracs) / sizeof(fracs[0]));
        } else {
            for (i = 0; i < (int) (sizeof(fracs) / sizeof(fracs[0])); i++) {
                fracs_pos[i] = HIST_BUCKETS - 1;
            }
        }

        load_mops = (long double) gen_int / dt_s / 1000000.0L;
        tp_mops = (long double) tp_int / dt_s / 1000000.0L;

        printf("[ %3"PRIu64"s ] load=%'.4Lf mops  tp=%'.4Lf mops  50p=%d us  "
               "90p=%d us  95p=%d us  "
               "99p=%d us  99.9p=%d us  99.99p=%d us\n",
            sec,
            load_mops,
            tp_mops,
            hist_value(fracs_pos[0]), hist_value(fracs_pos[1]),
            hist_value(fracs_pos[2]), hist_value(fracs_pos[3]),
            hist_value(fracs_pos[4]), hist_value(fracs_pos[5]));

        fflush(stdout);
        t_prev = t_cur;
    }

    phase = BENCHMARK_DONE;
    for (i = 0; i < num_threads; i++) {
        pthread_join(cs[i].pthread, NULL);
    }

    for (i = 0; i < num_threads; i++) {
        gen_total += read_cnt(&cs[i].tx_get) + read_cnt(&cs[i].tx_set);
        tp_total += read_cnt(&cs[i].rx_success);
    }

    msg_total = 0;
    for (j = 0; j < HIST_BUCKETS; j++) {
        uint64_t sum = 0;
        for (i = 0; i < num_threads; i++) {
            sum += cs[i].hist[j];
        }
        hist_cur[j] = (uint32_t) sum;
        msg_total += hist_cur[j];
    }
    if (msg_total > 0) {
        hist_fract_buckets(hist_cur, msg_total, fracs, fracs_pos,
            sizeof(fracs) / sizeof(fracs[0]));
    } else {
        for (i = 0; i < (int) (sizeof(fracs) / sizeof(fracs[0])); i++) {
            fracs_pos[i] = HIST_BUCKETS - 1;
        }
    }

    printf("Summary:\n");
    printf("  Duration: %"PRIu32" s\n", settings.run_time);
    printf("  TCP Connections/Thread: %"PRIu32"\n", settings.nconns);
    printf("  Generated Requests: %"PRIu64"\n", gen_total);
    printf("  Completed Requests: %"PRIu64"\n", tp_total);
    printf("  Avg Generated Load: %'.4Lf mops\n",
        (long double) gen_total / (long double) settings.run_time / 1000000.0L);
    printf("  Avg Throughput:     %'.4Lf mops\n",
        (long double) tp_total / (long double) settings.run_time / 1000000.0L);
    printf("  Latency: 50p=%d us  90p=%d us  95p=%d us  99p=%d us  99.9p=%d us  99.99p=%d us\n",
        hist_value(fracs_pos[0]), hist_value(fracs_pos[1]),
        hist_value(fracs_pos[2]), hist_value(fracs_pos[3]),
        hist_value(fracs_pos[4]), hist_value(fracs_pos[5]));

    return 0;
}
