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

#include <demi/libos.h>
#include <demi/sga.h>
#include <demi/wait.h>

#include <protocol_binary.h>

#include "benchmark.h"
#include "clock.h"
#include "sock.h"

#define PRINT_STATS
#ifdef PRINT_STATS
#   define STATS_ADD(c, f, n) __atomic_fetch_add(&(c)->f, (n), __ATOMIC_RELAXED)
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
    uint64_t *hist;
#endif
    int fd;
    demi_qtoken_t rx_qt;
    struct sockaddr_in dst_addr;
    socklen_t dst_addr_len;
    bool rx_pending;
    uint8_t *rx_buf;
    uint8_t *tx_buf;
    size_t tx_len;
    uint64_t last_tsc;
    double tokens;
    double reqs_per_us;
    double token_cap;
    struct workload_core wlc;
    uint64_t id;
    pthread_t pthread;
} __attribute__((aligned(64)));

static double tsc_per_us;
static const struct timespec zero_timeout = { .tv_sec = 0, .tv_nsec = 0 };

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

    __atomic_fetch_add(&c->hist[bucket], 1, __ATOMIC_RELAXED);
}

#ifdef PRINT_STATS
static inline uint64_t read_cnt(uint64_t *p)
{
    return __atomic_exchange_n(p, 0, __ATOMIC_RELAXED);
}
#endif

static void prepare_core(struct core *c)
{
    struct sockaddr_in bind_addr;
    struct sockaddr_in addr;
    double reqs_per_s;
    int ret;

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

    ret = demi_socket(&c->fd, AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (ret != 0) {
        errno = ret;
        perror("demi_socket failed");
        fprintf(stderr, "[%"PRIu64"] socket failed\n", c->id);
        abort();
    }

    if (settings.srcport != -1) {
        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_addr.s_addr = htonl(settings.srcip);
        bind_addr.sin_port = htons((uint16_t) settings.srcport);
        ret = demi_bind(c->fd, (const struct sockaddr *) &bind_addr,
            sizeof(bind_addr));
        if (ret != 0) {
            errno = ret;
            perror("demi_bind failed");
            fprintf(stderr, "[%"PRIu64"] bind failed\n", c->id);
            abort();
        }
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(settings.dstip);
    addr.sin_port = htons(settings.dstport);
    c->dst_addr = addr;
    c->dst_addr_len = sizeof(c->dst_addr);

    c->rx_pending = false;
    c->rx_qt = 0;
    reqs_per_s = (double) settings.rate * 1000.0;
    c->reqs_per_us = reqs_per_s / 1000000.0;
    c->token_cap = reqs_per_s;
    if (c->token_cap < 1.0) {
        c->token_cap = 1.0;
    }
    c->tokens = 0.0;
    c->last_tsc = util_rdtsc();
}

static int core_tx(struct core *c)
{
    demi_qtoken_t tx_qt;
    demi_qresult_t qr;
    demi_sgarray_t sga;
    size_t tx_len;
    int ret;

    assert(c->tx_len > 0);
    tx_len = c->tx_len;
    if (tx_len > BUFSIZE) {
        fprintf(stderr, "[%"PRIu64"] tx_len too large: %zu\n", c->id, tx_len);
        return -1;
    }

    sga = demi_sgaalloc(tx_len);
    if (sga.sga_numsegs != 1 || sga.segments[0].data_len_bytes < tx_len) {
        fprintf(stderr, "[%"PRIu64"] demi_sgaalloc failed for tx_len=%zu\n",
            c->id, tx_len);
        return -1;
    }
    sga.sga_numsegs = 1;
    sga.segments[0].data_len_bytes = (uint32_t) tx_len;
    memcpy(sga.segments[0].data_buf_ptr, c->tx_buf, tx_len);

    ret = demi_pushto(&tx_qt, c->fd, &sga,
        (const struct sockaddr *) &c->dst_addr, c->dst_addr_len);
    if (ret != 0) {
        errno = ret;
        perror("demi_pushto failed");
        (void) demi_sgafree(&sga);
        return -1;
    }

    while (1) {
        ret = demi_wait(&qr, tx_qt, &zero_timeout);
        if (ret == ETIMEDOUT) {
            sched_yield();
            continue;
        }
        if (ret != 0) {
            errno = ret;
            perror("demi_wait push failed");
            (void) demi_sgafree(&sga);
            return -1;
        }
        if (qr.qr_opcode != DEMI_OPC_PUSH) {
            fprintf(stderr, "[%"PRIu64"] unexpected opcode on push: %d\n",
                c->id, (int) qr.qr_opcode);
            (void) demi_sgafree(&sga);
            return -1;
        }

        ret = demi_sgafree(&sga);
        if (ret != 0) {
            errno = ret;
            perror("demi_sgafree failed");
            return -1;
        }
        c->tx_len = 0;
        return 0;
    }
}

static int core_rx(struct core *c, enum workload_op *op, uint16_t *err,
    uint32_t *opaque)
{
    demi_qresult_t qr;
    demi_sgarray_t *sga;
    protocol_binary_response_header *res;
    ssize_t rx_len;
    int ret;
    size_t reqlen;

    if (!c->rx_pending) {
        ret = demi_pop(&c->rx_qt, c->fd);
        if (ret != 0) {
            errno = ret;
            perror("demi_pop failed");
            return -1;
        }
        c->rx_pending = true;
    }

    ret = demi_wait(&qr, c->rx_qt, &zero_timeout);
    if (ret == ETIMEDOUT) {
        return 0;
    }
    if (ret != 0) {
        errno = ret;
        perror("demi_wait pop failed");
        return -1;
    }
    c->rx_pending = false;

    if (qr.qr_opcode != DEMI_OPC_POP) {
        fprintf(stderr, "[%"PRIu64"] unexpected opcode on pop: %d\n",
            c->id, (int) qr.qr_opcode);
        return -1;
    }

    sga = &qr.qr_value.sga;
    if (sga->sga_numsegs != 1) {
        fprintf(stderr, "[%"PRIu64"] invalid pop segment count: %u\n",
            c->id, sga->sga_numsegs);
        demi_sgafree(sga);
        return -1;
    }

    rx_len = (ssize_t) sga->segments[0].data_len_bytes;
    if (rx_len < 0 || rx_len > BUFSIZE) {
        fprintf(stderr, "[%"PRIu64"] invalid pop length: %zd\n", c->id, rx_len);
        demi_sgafree(sga);
        return -1;
    }
    memcpy(c->rx_buf, sga->segments[0].data_buf_ptr, (size_t) rx_len);
    if (demi_sgafree(sga) != 0) {
        fprintf(stderr, "[%"PRIu64"] demi_sgafree failed\n", c->id);
        return -1;
    }

    if (rx_len < (ssize_t) sizeof(*res)) {
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
    if ((size_t) rx_len != reqlen) {
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
    return 1;
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
    int rx_ret;

    for (i = c->id; i < workload.keys_num; i += settings.threads) {
        set_request(c, &workload.keys[i], 0);
        if (core_tx(c) != 0) {
            abort();
        }

        while (1) {
            rx_ret = core_rx(c, &op, &err, &opaque);
            if (rx_ret < 0) {
                abort();
            }
            if (rx_ret > 0) {
                break;
            }
            sched_yield();
        }

        if (op != WL_OP_SET || err != 0) {
            fprintf(stderr, "[%"PRIu64"] preload set failed: op=%d err=%x\n",
                c->id, (int) op, err);
            abort();
        }
    }
}

static inline void refill_tokens(struct core *c)
{
    uint64_t now_tsc;
    uint64_t delta_tsc;
    double elapsed_us;

    now_tsc = util_rdtsc();
    delta_tsc = now_tsc - c->last_tsc;
    c->last_tsc = now_tsc;

    elapsed_us = (double) delta_tsc / tsc_per_us;
    c->tokens += elapsed_us * c->reqs_per_us;
    if (c->tokens > c->token_cap) {
        c->tokens = c->token_cap;
    }
}

static void *thread_run(void *arg)
{
    int ret;
    struct core *c = arg;
    const struct demi_args args = {
        .argc = 0,
        .argv = NULL,
        .callback = NULL,
    };

    ret = demi_init(&args);
    if (ret != 0) {
        errno = ret;
        perror("demi_init failed");
        abort();
    }

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
        uint32_t budget;
        uint32_t sent = 0;
        uint32_t received = 0;
        enum workload_op rx_op;
        uint16_t err;
        uint32_t opaque;
        uint32_t now_us;
        uint32_t delta_us;
        int rx_ret;

        refill_tokens(c);
        budget = (uint32_t) c->tokens;

        while (sent < budget && phase == BENCHMARK_RUNNING) {
            struct key *k;
            enum workload_op tx_op;

            workload_op(&workload, &c->wlc, &k, &tx_op);
            opaque = (uint32_t) (get_nanos() / 1000);

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

            sent++;
        }
        c->tokens -= (double) sent;

        while (1) {
            rx_ret = core_rx(c, &rx_op, &err, &opaque);
            if (rx_ret < 0) {
                abort();
            }
            if (rx_ret == 0) {
                break;
            }

            received++;

            now_us = (uint32_t) (get_nanos() / 1000);
            delta_us = now_us - opaque;
            record_latency(c, (uint64_t) delta_us * 1000);

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

        if (sent == 0 && received == 0) {
            sched_yield();
        }
    }

    return NULL;
}

static inline void hist_fract_buckets(uint64_t *hist, uint64_t total,
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
    uint64_t *hist_cur;
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

    tsc_per_us = clock_tsc_per_us();
    if (settings.threads != 1) {
        fprintf(stderr,
            "Demikernel benchmark is single-threaded, forcing threads=1\n");
        settings.threads = 1;
    }

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
                sum += __atomic_load_n(&cs[i].hist[j], __ATOMIC_RELAXED);
            }
            hist_cur[j] = sum;
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
            sum += __atomic_load_n(&cs[i].hist[j], __ATOMIC_RELAXED);
        }
        hist_cur[j] = sum;
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
