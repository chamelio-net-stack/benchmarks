#define _GNU_SOURCE
#include <chamelio/udp_lib.h>
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
#include "sock.h"

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

struct settings settings;
static struct workload workload;
static volatile enum benchmark_phase phase;
static volatile uint16_t init_count = 0;
static volatile uint16_t arp_ready = 0;

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
    uint64_t rx_malformed;
    int fd;
    size_t rx_len;
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
    struct udp_context_lib *ctx;
    struct sockaddr_in addr;
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
static inline uint64_t load_cnt(uint64_t *p)
{
    return __atomic_load_n(p, __ATOMIC_RELAXED);
}
#endif

static void prepare_core(struct core *c)
{
    struct udp_context_lib *udp_ctx;
    struct sockaddr_in *addr;
    double reqs_per_s;

    /* Each core has its own UDP context and socket */
    udp_ctx = udp_ctx_new();
    if (udp_ctx == NULL)
    {
        fprintf(stderr, "core %ld: failed to create UDP context\n", c->id);
        pthread_exit((void *) (intptr_t) -1);
    }
    c->ctx = udp_ctx;

    c->rx_buf = malloc(BUFSIZE * 2);
    if (c->rx_buf == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating rx/tx buffer failed\n", c->id);
        abort();
    }
    memset(c->rx_buf, 0, BUFSIZE * 2);
    c->tx_buf = c->rx_buf + BUFSIZE;
    c->rx_len = 0;

    c->hist = calloc(HIST_BUCKETS, sizeof(*c->hist));
    if (c->hist == NULL) {
        fprintf(stderr, "[%"PRIu64"] allocating histogram failed\n", c->id);
        abort();
    }

    c->fd = udp_socket(c->ctx);
    fprintf(stderr, "sock fd=%d\n", c->fd);
    if (c->fd < 0) {
        perror("socket failed");
        fprintf(stderr, "[%"PRIu64"] socket failed\n", c->id);
        abort();
    }

    addr = &c->addr;
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl(settings.dstip);
    addr->sin_port = htons(settings.dstport);

    /* Core 0 resolves ARP */
    int arp_size = 8;
    ssize_t arp_ret;
    if (c->id == 0)
    {
        udp_sendto(c->ctx, c->fd, c->tx_buf, arp_size, 
            (struct sockaddr *) &c->addr, sizeof(c->addr));
        sleep(1);
        udp_sendto(c->ctx, c->fd, c->tx_buf, arp_size, 
            (struct sockaddr *) &c->addr, sizeof(c->addr));
        sleep(1);
        while(udp_poll_fast(c->ctx) == 0);
        do {
            arp_ret = udp_recvfrom(c->ctx, c->fd, c->rx_buf,
                BUFSIZE, NULL, 0);
        } while (arp_ret > 0);
        if (arp_ret < 0) {
            fprintf(stderr, "[%"PRIu64"] ARP probe drain failed: ret=%zd errno=%d\n",
                c->id, arp_ret, errno);
            abort();
        }
        fprintf(stderr, "Resolved ARP\n");
        __sync_fetch_and_add(&arp_ready, 1);
    } else {
        while (__sync_fetch_and_add(&arp_ready, 0) == 0) {
            sched_yield();
        }
    }

    reqs_per_s = (double) settings.rate * 1000.0;
    c->reqs_per_us = reqs_per_s / 1000000.0;
    c->token_cap = reqs_per_s;
    if (c->token_cap < 1.0) {
        c->token_cap = 1.0;
    }
    c->tokens = 0.0;
    c->last_tsc = util_rdtsc();
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

static int core_tx(struct core *c)
{
    ssize_t ret;

    assert(c->tx_len > 0);

    while (1)
    {
        udp_poll_fast(c->ctx);
        ret = udp_sendto(c->ctx, c->fd, c->tx_buf, c->tx_len,
            (struct sockaddr *) &c->addr, sizeof(c->addr));
    
        if (ret == c->tx_len) {
            c->tx_len = 0;
            return 0;
        }
        
        if (ret < 0 && errno == EAGAIN)
            continue;
            
        if (ret < 0) {
            fprintf(stderr, "error sending in core_tx\n");
            abort();
        }
        
        if (ret < c->tx_len) {
            fprintf(stderr, "[%"PRIu64"] short send: %zd of %zu\n",
                c->id, ret, c->tx_len);
            return -1;
        }
    }
}

enum parse_result {
    PARSE_ERR = -1,
    PARSE_NEED_MORE = 0,
    PARSE_OK = 1,
};

static int parse_one_response(const uint8_t *buf, size_t len,
    size_t *reqlen, enum workload_op *op, uint16_t *err, uint32_t *opaque)
{
    const protocol_binary_response_header *res;

    if (len < sizeof(*res)) {
        return 0;
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

static int core_rx(struct core *c, enum workload_op *last_op,
    uint16_t *last_err)
{
    ssize_t ret;
    int processed = 0;

    udp_poll_fast(c->ctx);
    while (1) {
        size_t parsed = 0;

        while (parsed < c->rx_len) {
            enum workload_op op;
            uint16_t err;
            uint32_t opaque;
            size_t reqlen;
            int parse_ret;

            parse_ret = parse_one_response(c->rx_buf + parsed,
                c->rx_len - parsed, &reqlen, &op, &err, &opaque);
            if (parse_ret == PARSE_NEED_MORE) {
                break;
            }
            if (parse_ret == PARSE_ERR) {
                c->rx_malformed++;
                if (c->rx_malformed <= 3) {
                    fprintf(stderr,
                        "[%"PRIu64"] malformed response byte (rx_len=%zu parsed=%zu byte=0x%02x)\n",
                        c->id, c->rx_len, parsed, c->rx_buf[parsed]);
                }
                parsed++;
                continue;
            }

            if (phase == BENCHMARK_RUNNING) {
                account_response(c, op, err, opaque);
            }
            if (last_op != NULL) {
                *last_op = op;
            }
            if (last_err != NULL) {
                *last_err = err;
            }

            parsed += reqlen;
            processed++;
        }

        if (parsed > 0) {
            c->rx_len -= parsed;
            if (c->rx_len > 0) {
                memmove(c->rx_buf, c->rx_buf + parsed, c->rx_len);
            }
        }

        if (c->rx_len >= BUFSIZE) {
            fprintf(stderr, "[%"PRIu64"] incomplete response exceeds BUFSIZE\n",
                c->id);
            return -1;
        }

        ret = udp_recvfrom(c->ctx, c->fd, c->rx_buf + c->rx_len,
                BUFSIZE - c->rx_len, NULL, 0);
        if (ret > 0) {
            c->rx_len += (size_t) ret;
            continue;
        }
        if (ret < 0) {
            fprintf(stderr, "[%"PRIu64"] error when receiving: ret=%zd errno=%d\n",
                c->id, ret, errno);
            return -1;
        }

        return processed;
    }
}

static void preload_keys(struct core *c)
{
    size_t i;

    fprintf(stderr, "begino core=%d\n", c->id);
    for (i = c->id; i < workload.keys_num; i++) {
        set_request(c, &workload.keys[i], 0);
        if (core_tx(c) != 0) {
            fprintf(stderr, "error sending on preload\n");
            abort();
        }
    }
    fprintf(stderr, "fini core=%d\n", c->id);
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
        uint32_t opaque;

        refill_tokens(c);
        while (c->tokens >= 1.0 && phase == BENCHMARK_RUNNING) {
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
                fprintf(stderr, "error sending data loop\n");
                abort();
            }
            c->tokens -= 1.0;
        }

        ret = core_rx(c, NULL, NULL);
        if (ret < 0) {
            abort();
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
    int i, j, ret;
    int num_threads;
    struct core *cs;
    uint64_t t_prev;
    uint64_t t_cur;
    uint32_t *hist_cur;
    uint64_t *prev_gen_cur;
    uint64_t *prev_tp_cur;
    uint64_t gen_total = 0;
    uint64_t tp_total = 0;
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

    num_threads = settings.threads;
    workload_init(&workload);

    assert(sizeof(*cs) % 64 == 0);
    cs = calloc(num_threads, sizeof(*cs));
    hist_cur = calloc(HIST_BUCKETS, sizeof(*hist_cur));
    prev_gen_cur = calloc((size_t) num_threads, sizeof(*prev_gen_cur));
    prev_tp_cur = calloc((size_t) num_threads, sizeof(*prev_tp_cur));
    if (cs == NULL || hist_cur == NULL ||
        prev_gen_cur == NULL || prev_tp_cur == NULL)
    {
        fprintf(stderr, "allocation failed\n");
        return EXIT_FAILURE;
    }

    ret = udp_connect_slow();
    if (ret != 0)
    {
        fprintf(stderr, "failed to connect to Chamelio");
        exit(EXIT_FAILURE);
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
        uint64_t gen_int = 0;
        uint64_t tp_int = 0;

        sleep(1);
        t_cur = get_nanos();
        dt_s = (long double) (t_cur - t_prev) / 1000000000.0L;
        if (dt_s <= 0.0L) {
            dt_s = 1.0L;
        }
        msg_total = 0;

        for (i = 0; i < num_threads; i++) {
            uint64_t gen_cur = load_cnt(&cs[i].tx_get) + load_cnt(&cs[i].tx_set);
            uint64_t tp_cur = load_cnt(&cs[i].rx_success);

            if (gen_cur >= prev_gen_cur[i]) {
                gen_int += gen_cur - prev_gen_cur[i];
            } else {
                gen_int += gen_cur;
            }
            if (tp_cur >= prev_tp_cur[i]) {
                tp_int += tp_cur - prev_tp_cur[i];
            } else {
                tp_int += tp_cur;
            }

            prev_gen_cur[i] = gen_cur;
            prev_tp_cur[i] = tp_cur;
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

    gen_total = 0;
    tp_total = 0;
    for (i = 0; i < num_threads; i++) {
        gen_total += load_cnt(&cs[i].tx_get) + load_cnt(&cs[i].tx_set);
        tp_total += load_cnt(&cs[i].rx_success);
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
