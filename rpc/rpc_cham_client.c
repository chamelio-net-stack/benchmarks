/* Reports per-second throughput (Mbits/s and packets/s) and cumulative
 * latency percentiles (p50, p99, p99.9) in microseconds.
 *
 * Usage:
 *   ./rpc_cham_client <src_ip> <src_port> <rem_ip> <rem_port>
 *     [--msize N] [--duration S] [--rate R] [--ncores N]
 *
 *   --msize Total UDP payload bytes.
 *   --duration Duration in seconds to run benchmark
 *   --rate Per-core send rate in megabits per second
 *   --ncores Number of cores to use
 *   --cores Comma-separated core list (main, thread0, thread1, ...)
 */

#define _GNU_SOURCE
#include <chamelio/rpc_lib.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>
#include <stdio.h>
#include <time.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include "services.h"
#include <stdint.h>

/* Sentinel value meaning "this sent_rids slot is empty".
 * UINT32_MAX is safe because next_rid wraps at 2^32 and the library
 * uses __sync_fetch_and_add so it will never naturally produce UINT32_MAX
 * as the first value seen during a short benchmark run. */
#define RID_SLOT_EMPTY UINT32_MAX

/* next_rid is global across all cores, so RIDs assigned to a single core
 * are not consecutive — they can be any value in the global sequence.
 * RID_TABLE_SIZE must be large enough that two in-flight RIDs from the
 * same core never collide under % RID_TABLE_SIZE.
 * 65536 entries = 256 KB, far beyond any realistic in-flight window. */
#define RID_TABLE_SIZE 65536

struct payload_hdr
{
  __u64 tsc;
} __attribute__((packed));

/* Per-core context */
struct core
{
  int id;
  __u8 *txbuf;
  __u8 *rxbuf;
  struct rpc_client_lib *client;
  struct sockaddr_in dst_addr;
  __u64 txb_load_interval;
  __u64 txp_load_interval;
  __u64 txb_tp_interval;
  __u64 txp_tp_interval;
  __u64 total_tx_pkts;
  __u64 total_rx_pkts;
  __u64 rid_mismatches; /* responses whose RID did not match the expected sent RID */
  double rate;
  double tokens;
  __u64 last_tsc;
  __u64 prng_state; /* xorshift64 state for operand generation */

  /* Circular buffer of sent RIDs — indexed by (rid % RID_TABLE_SIZE).
   * Filled in core_tx immediately after rpc_call; checked in core_rx. */
  /* sent RIDs indexed by (rid % RID_TABLE_SIZE).
   * Filled in core_tx after rpc_call; checked and cleared in core_rx.
   * Empty slots hold RID_SLOT_EMPTY to distinguish from valid RID 0. */
  __u32 sent_rids[RID_TABLE_SIZE];
};

/* Default arg values */
size_t msize = 64;
const char *src_ip;
int src_port;
struct sockaddr_in src_addr;
socklen_t src_len;
int duration = 30;
double rate = 10.0;
int ncores = 1;
const char *rem_ip;
int rem_port;
int service = SERVICE_ECHO;

/* Global time and stats variables */
__u64 t_start_ns, t_end_ns;
__u64 t_last_report_ns, t_next_report_ns;
__u64 txb_load_interval_total, txp_load_interval_total;
__u64 txb_tp_interval_total, txp_tp_interval_total;

/* Cycles per microsecond */
static double tsc_per_us = 0.0;

/* Total received samples across all cores */
static __u64 total_rx = 0;
static __u64 *rtt_hist = NULL;

/* Clamp histogram at 1 second */
#define MAX_RTT_US 1000000u

static pthread_barrier_t start_barrier;
static pthread_t *threads = NULL;
static struct core *cores = NULL;

static inline __u64 now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return (__u64)ts.tv_sec * 1000000000ULL + (__u64)ts.tv_nsec;
}

static inline void hist_add(__u64 rtt_us)
{
  if (rtt_us > MAX_RTT_US)
    rtt_us = MAX_RTT_US;

  __sync_fetch_and_add(&rtt_hist[rtt_us], 1);
  __sync_fetch_and_add(&total_rx, 1);
}

static inline __u64 util_rdtsc(void)
{
  __u32 eax, edx;
  asm volatile("rdtsc" : "=a"(eax), "=d"(edx));
  return ((__u64)edx << 32) | eax;
}

static inline __u64 us_since_tsc(__u64 tsc_then)
{
  __u64 tsc_now = util_rdtsc();
  double delta = (double)(tsc_now - tsc_then);
  assert(delta >= 0);
  double us = delta / tsc_per_us;
  assert(us >= 0);

  return (__u64)us;
}

static inline void calibrate_tsc(void)
{
  struct timespec ts_before, ts_after;
  __u64 t0, t1;
  __u64 us_before, us_after;
  __u64 dt_cycles, dt_us;

  if (tsc_per_us != 0.0)
    return;

  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts_before) != 0)
  {
    perror("calibrate_tsc: clock_gettime before");
    exit(EXIT_FAILURE);
  }

  t0 = util_rdtsc();
  usleep(10000);
  t1 = util_rdtsc();

  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts_after) != 0)
  {
    perror("calibrate_tsc: clock_gettime after");
    exit(EXIT_FAILURE);
  }

  dt_cycles = t1 - t0;
  us_before = (__u64)ts_before.tv_sec * 1000000ULL + (__u64)(ts_before.tv_nsec / 1000ULL);
  us_after = (__u64)ts_after.tv_sec * 1000000ULL + (__u64)(ts_after.tv_nsec / 1000ULL);
  dt_us = us_after - us_before;

  if (dt_us == 0)
    dt_us = 1;

  tsc_per_us = (double)dt_cycles / (double)dt_us;
  if (tsc_per_us <= 0.0)
  {
    fprintf(stderr, "calibrate_tsc: invalid tsc_per_us: %f\n", tsc_per_us);
    exit(EXIT_FAILURE);
  }
}

static inline bool hist_percentiles(__u64 *p50, __u64 *p99, __u64 *p999)
{
  __u32 i;
  __u64 samples, N, c;
  __u64 t50, t99, t999;
  __u64 acc = 0;
  __u64 v50 = 0, v99 = 0, v999 = 0;
  __u8 got50 = 0, got99 = 0, got999 = 0;

  samples = __sync_fetch_and_add(&total_rx, 0);
  if (samples == 0)
    return 0;

  N = samples;
  t50 = (N * 50 + 100 - 1) / 100;
  t99 = (N * 99 + 100 - 1) / 100;
  t999 = (N * 999 + 1000 - 1) / 1000;

  for (i = 0; i <= MAX_RTT_US; i++)
  {
    c = __sync_fetch_and_add(&rtt_hist[i], 0);
    if (c == 0)
      continue;

    acc += c;

    if (!got50 && acc >= t50)
    {
      v50 = i;
      got50 = 1;
    }

    if (!got99 && acc >= t99)
    {
      v99 = i;
      got99 = 1;
    }

    if (!got999 && acc >= t999)
    {
      v999 = i;
      got999 = 1;
    }

    if (got50 && got99 && got999)
      break;
  }

  *p50 = v50;
  *p99 = v99;
  *p999 = v999;
  return 1;
}

static inline void print_stats(__u64 now)
{
  double interval_s, rps_load, mbps_load, rps_tp, mbps_tp;
  __u64 p50 = 0, p99 = 0, p999 = 0;
  __u64 bytes_load = 0, pkts_load = 0, bytes_tp = 0, pkts_tp = 0;

  interval_s = (double)(t_next_report_ns - t_last_report_ns) / 1e9;
  if (interval_s <= 0.0)
    interval_s = 1.0;

  /* Aggregate per-core and reset per-core intervals */
  for (int i = 0; i < ncores; i++)
  {
    bytes_load += __sync_fetch_and_add(&cores[i].txb_load_interval, 0);
    pkts_load += __sync_fetch_and_add(&cores[i].txp_load_interval, 0);
    bytes_tp += __sync_fetch_and_add(&cores[i].txb_tp_interval, 0);
    pkts_tp += __sync_fetch_and_add(&cores[i].txp_tp_interval, 0);

    /* Zero values safely */
    __sync_fetch_and_and(&cores[i].txb_load_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_load_interval, 0);
    __sync_fetch_and_and(&cores[i].txb_tp_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_tp_interval, 0);
  }
  txb_load_interval_total = bytes_load;
  txp_load_interval_total = pkts_load;
  txb_tp_interval_total = bytes_tp;
  txp_tp_interval_total = pkts_tp;

  rps_load = (double)txp_load_interval_total / interval_s;
  mbps_load = ((double)txb_load_interval_total * 8.0 / 1e6) / interval_s;
  rps_tp = (double)txp_tp_interval_total / interval_s;
  mbps_tp = ((double)txb_tp_interval_total * 8.0 / 1e6) / interval_s;

  hist_percentiles(&p50, &p99, &p999);

  printf("[ %3lus ] load=%lld rps | load=%.2f Mb/s | tp=%lld rps | tp=%.2f Mb/s | ",
         (unsigned long)((t_next_report_ns - t_start_ns) / 1000000000ULL),
         (__u64)rps_load, mbps_load, (__u64)rps_tp, mbps_tp);
  printf("p50=%llu us  p99=%llu us  p99.9=%llu us\n",
         (unsigned long long)p50,
         (unsigned long long)p99,
         (unsigned long long)p999);
  fflush(stdout);

  t_last_report_ns = t_next_report_ns;
  t_next_report_ns += 1000000000ULL;

  if (now > t_next_report_ns + 5000000000ULL)
  {
    t_next_report_ns = now + 1000000000ULL;
    t_last_report_ns = now;
  }
}

// TODO: add arg for cores to use (main, thread0, thread1, etc.)
static inline int parse_args(int argc, char **argv)
{
  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <src_ip> <src_port> <rem_ip> <rem_port> [--msize N]"
                    " [--duration S] [--rate R] [--ncores N]"
                    " [--service N (1=echo 2=compute)]\n",
            argv[0]);
    return -1;
  }

  src_ip = argv[1];
  src_port = atoi(argv[2]);
  if (src_port < 0 || src_port > 65535)
  {
    fprintf(stderr, "Invalid src port\n");
    return -1;
  }
  rem_ip = argv[3];
  rem_port = atoi(argv[4]);
  if (rem_port <= 0 || rem_port > 65535)
  {
    fprintf(stderr, "Invalid remote port\n");
    return -1;
  }

  for (int i = 5; i < argc; i++)
  {
    if ((strcmp(argv[i], "--msize") == 0) && i + 1 < argc)
    {
      msize = (size_t)strtoul(argv[++i], NULL, 10);
    }
    else if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc)
    {
      duration = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--rate") == 0 && i + 1 < argc)
    {
      rate = strtod(argv[++i], NULL);
    }
    else if ((strcmp(argv[i], "--ncores") == 0) && i + 1 < argc)
    {
      ncores = atoi(argv[++i]);
    }
    else if ((strcmp(argv[i], "--service") == 0) && i + 1 < argc)
    {
      service = atoi(argv[++i]);
      if (service != SERVICE_ECHO && service != SERVICE_COMPUTE)
      {
        fprintf(stderr, "Unknown service %d (valid: %d=echo  %d=compute)\n",
                service, SERVICE_ECHO, SERVICE_COMPUTE);
        return -1;
      }
    }
    else
    {
      fprintf(stderr, "Unknown arg: %s\n", argv[i]);
      return -1;
    }
  }
  if (rate <= 0.0)
  {
    fprintf(stderr, "rate must be > 0\n");
    return -1;
  }

  size_t min_msize = (service == SERVICE_COMPUTE)
                         ? COMPUTE_REQ_MIN_MSIZE
                         : sizeof(struct payload_hdr);
  if (msize < min_msize)
  {
    fprintf(stderr, "msize must be at least %zu bytes for service %d\n",
            min_msize, service);
    return -1;
  }

  if (ncores <= 0)
    ncores = 1;
  return 0;
}

static inline int init_hist(void)
{
  rtt_hist = (__u64 *)calloc((size_t)MAX_RTT_US + 1, sizeof(__u64));
  if (!rtt_hist)
  {
    perror("calloc rtt_hist");
    return -1;
  }
  return 0;
}

static inline void init_client()
{
  src_addr.sin_family = AF_INET;
  src_addr.sin_port = htons((__u16)src_port);
  if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
  {
    fprintf(stderr, "invalid bind_ip\n");
    abort();
  }

  /* Calibrate tsc */
  calibrate_tsc();

  /* Allocate cores and thread handles */
  cores = (struct core *)calloc((size_t)ncores,
                                sizeof(struct core));
  threads = (pthread_t *)calloc((size_t)ncores, sizeof(pthread_t));
  if (!cores || !threads)
  {
    perror("calloc cores/threads");
    exit(EXIT_FAILURE);
  }

  /* Barrier for all cores + main (to synchronize start of sending) */
  if (pthread_barrier_init(&start_barrier, NULL,
                           (unsigned)ncores + 1) != 0)
  {
    perror("pthread_barrier_init");
    exit(EXIT_FAILURE);
  }
}

static inline void prepare_cores()
{
  int i;

  for (i = 0; i < ncores; i++)
  {
    memset(&cores[i], 0, sizeof(struct core));
    cores[i].id = i;
    cores[i].rate = rate;
    cores[i].tokens = 0.0;
    cores[i].last_tsc = 0;

    memset(&cores[i].dst_addr, 0, sizeof(cores[i].dst_addr));
    cores[i].dst_addr.sin_family = AF_INET;
    cores[i].dst_addr.sin_port = htons((__u16)rem_port);
    if (inet_pton(AF_INET, rem_ip, &cores[i].dst_addr.sin_addr) != 1)
    {
      fprintf(stderr, "Invalid server_ip\n");
      abort();
    }
    // cores[i].dst_len = sizeof(cores[i].dst_addr);
  }
}

// random number generator for compute operands
static inline __u64 xorshift64(__u64 *state)
{
  __u64 x = *state;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  *state = x;
  return x;
}

static inline void core_tx(struct rpc_context_lib *rpc_ctx, struct core *c)
{
  int ntx;
  struct payload_hdr *ph;
  struct compute_req *creq;

  while (c->tokens >= (double)msize)
  {
    if (service == SERVICE_COMPUTE)
    {
      creq = (struct compute_req *)c->txbuf;
      creq->tsc = util_rdtsc();
      creq->service_id = SERVICE_COMPUTE;
      creq->op = (__u8)(xorshift64(&c->prng_state) % 3);
      creq->a = xorshift64(&c->prng_state);
      creq->b = xorshift64(&c->prng_state);
    }
    else
    {
      ph = (struct payload_hdr *)c->txbuf;
      ph->tsc = util_rdtsc();
    }

    ntx = rpc_call(c->client, ntohl(c->dst_addr.sin_addr.s_addr),
                   ntohs(c->dst_addr.sin_port), service, c->txbuf, msize);

    // assert(ntx == msize || ntx <= 0);
    // assert(c->tokens >= msize);

    if (ntx < 0)
      break;

    /* record sent RID indexed by RID value, not position */
    c->sent_rids[c->client->last_sent_rid % RID_TABLE_SIZE] = c->client->last_sent_rid;

    c->tokens -= msize;
    c->total_tx_pkts++;
    __sync_fetch_and_add(&c->txb_load_interval, msize);
    __sync_fetch_and_add(&c->txp_load_interval, 1);
  }
}

static inline void core_rx(struct rpc_context_lib *rpc_ctx, struct core *c)
{
  int nrx;
  __u64 rtt_us;
  struct payload_hdr *ph;

  while (1)
  {
    nrx = rpc_response(c->client, c->rxbuf, msize);
    if (nrx < 0)
      break;

    /* verify RID by looking up the slot the sent RID was stored in */
    __u32 recv_rid = c->client->last_recv_rid;
    if (c->sent_rids[recv_rid % RID_TABLE_SIZE] != recv_rid)
      c->rid_mismatches++;
    else
      c->sent_rids[recv_rid % RID_TABLE_SIZE] = RID_SLOT_EMPTY; /* clear matched slot */

    /* tsc is at offset 0 in every service payload */
    ph = (struct payload_hdr *)c->rxbuf;
    rtt_us = us_since_tsc(ph->tsc);
    c->total_rx_pkts += 1;
    __sync_fetch_and_add(&c->txp_tp_interval, 1);
    __sync_fetch_and_add(&c->txb_tp_interval, nrx);
    hist_add(rtt_us);
  }
}

static inline struct rpc_context_lib *core_init(struct core *c)
{
  int ret, nrx, attempt;
  __u64 start_ns;
  struct rpc_context_lib *rpc_ctx;
  struct payload_hdr *ph;

  /* Each core has its own RPC context and socket */
  rpc_ctx = rpc_ctx_new();
  if (rpc_ctx == NULL)
  {
    fprintf(stderr, "core %d: failed to create RPC context\n", c->id);
    pthread_exit((void *)(intptr_t)-1);
  }

  c->txbuf = (__u8 *)malloc(msize);
  c->rxbuf = (__u8 *)malloc(msize);
  if (!c->txbuf || !c->rxbuf)
  {
    perror("malloc buffers");
    pthread_exit((void *)(intptr_t)-1);
  }
  memset(c->txbuf, 0, msize);
  memset(c->rxbuf, 0, msize);

  c->client = rpc_new_client(rpc_ctx, ntohl(src_addr.sin_addr.s_addr), src_port);
  if (c->client == NULL)
  {
    fprintf(stderr, "core %d: failed to create RPC client\n", c->id);
    pthread_exit((void *)(intptr_t)-1);
  }
  fprintf(stderr, "client: bind_success=%d tx_len=%u rx_len=%u tx_buf=%p rx_buf=%p core=%u\n",
          c->client->bind_success,
          c->client->tx_len,
          c->client->rx_len,
          c->client->tx_buf,
          c->client->rx_buf,
          c->client->core);

  fprintf(stderr, "client core=%u rx_ip=%u rx_port=%u tx_ip=%u\n",
          c->client->core,
          c->client->rx_ip,
          c->client->rx_port,
          c->client->tx_ip);

  /* Resolve ARP before the timed run */
  if (c->id == 0)
  {
    for (attempt = 0; attempt < 2; attempt++)
    {
      ph = (struct payload_hdr *)c->txbuf;
      ph->tsc = util_rdtsc();
      ret = rpc_call(c->client, ntohl(c->dst_addr.sin_addr.s_addr),
                     ntohs(c->dst_addr.sin_port), 1, c->txbuf, msize);
      if (ret < 0)
      {
        fprintf(stderr, "failed to send RPC ARP warm-up packet\n");
        pthread_exit((void *)(intptr_t)-1);
      }

      sleep(1);

      start_ns = now_ns();
      while (now_ns() - start_ns < 1000000000ULL)
      {
        rpc_poll_calls(rpc_ctx);
        nrx = rpc_response(c->client, c->rxbuf, msize);
        if (nrx >= 0)
          break;
        if (errno != EAGAIN)
        {
          fprintf(stderr, "failed to receive RPC ARP warm-up reply\n");
          pthread_exit((void *)(intptr_t)-1);
        }
        usleep(1000);
      }

      if (nrx >= 0)
        break;
    }

    if (nrx < 0)
    {
      fprintf(stderr, "failed to complete RPC ARP warm-up\n");
      pthread_exit((void *)(intptr_t)-1);
    }

    fprintf(stderr, "Resolved ARP for RPC benchmark\n");

    /* drain any warmup responses still in the rx buffer so they don't
     * leak into the timed run and trigger false RID mismatches */
    while (rpc_response(c->client, c->rxbuf, msize) >= 0)
      ;
  }

  c->txb_load_interval = 0;
  c->txp_load_interval = 0;
  c->txb_tp_interval = 0;
  c->txp_tp_interval = 0;
  c->total_tx_pkts = 0;
  c->total_rx_pkts = 0;
  c->tokens = 0.0;
  /* Seed per-core pseudo random generator with core id so each core has an independent sequence */
  c->prng_state = 0xdeadbeef12345678ULL ^ ((__u64)c->id + 1);

  /* mark all slots empty — distinguishes "unsent" from valid RID 0 */
  for (int i = 0; i < RID_TABLE_SIZE; i++)
    c->sent_rids[i] = RID_SLOT_EMPTY;

  return rpc_ctx;
}

static inline void *core_thread(void *arg)
{
  struct core *c;
  struct rpc_context_lib *rpc_ctx;
  __u64 start_tsc, elapsed, tsc_now;
  double bytes_per_us, delta_cycles, delta_us, max_tokens;

  c = (struct core *)arg;
  rpc_ctx = core_init(c);

  /* Synchronize start with all other cores and main */
  pthread_barrier_wait(&start_barrier);

  // /* Drain any warmup responses that arrived during the barrier wait */
  // rpc_poll_calls(rpc_ctx);
  // while (1)
  // {
  //   if (rpc_response(c->client, c->rxbuf, msize) < 0)
  //     break;
  // }

  c->last_tsc = util_rdtsc();
  start_tsc = util_rdtsc();

  fprintf(stderr, "core %d: rpc_ctx=%p ncores=%d\n",
          c->id, rpc_ctx, rpc_ctx->ncores);

  while (1)
  {
    elapsed = us_since_tsc(start_tsc);
    if (duration > 0 && elapsed >= (duration * 1000000))
      break;

    /* Update token bucket using TSC */
    tsc_now = util_rdtsc();
    delta_cycles = (double)(tsc_now - c->last_tsc);
    delta_us = delta_cycles / tsc_per_us;
    assert(delta_us >= 0);
    bytes_per_us = (c->rate / 8.0);
    c->tokens += delta_us * bytes_per_us;

    /* Cap bucket to 1 ms worth of traffic to avoid unbounded bursts */
    max_tokens = bytes_per_us * 1000;
    if (c->tokens > max_tokens)
      c->tokens = max_tokens;

    c->last_tsc = tsc_now;

    /* Poll bump messages */
    rpc_poll_calls(rpc_ctx);
    /* Send as many packets as tokens allow */
    core_tx(rpc_ctx, c);
    /* Drain replies */
    rpc_poll_calls(rpc_ctx);
    core_rx(rpc_ctx, c);
  }

  pthread_exit((void *)(intptr_t)0);
}

static inline void start_cores()
{
  int i, rc;

  /* Spawn core threads */
  for (i = 0; i < ncores; i++)
  {
    rc = pthread_create(&threads[i], NULL, core_thread, &cores[i]);
    if (rc != 0)
    {
      errno = rc;
      perror("pthread_create");
      exit(EXIT_FAILURE);
    }
  }
}

int main(int argc, char **argv)
{
  int ret, i;
  __u64 now;
  // struct rpc_client_lib *rpc_client;
  // struct rpc_context_lib *rpc_ctx;

  ret = parse_args(argc, argv);
  if (ret != 0)
  {
    fprintf(stderr, "Failed to parse args\n");
    abort();
  }
  ret = init_hist();
  if (ret != 0)
  {
    fprintf(stderr, "Failed to init hist\n");
    abort();
  }

  ret = rpc_connect_slow();
  if (ret != 0)
  {
    fprintf(stderr, "Failed to connect to RPC server\n");
    exit(EXIT_FAILURE);
  }
  init_client();
  prepare_cores();
  start_cores();

  /* Wait for work threads before continuing */
  printf("Initialized %d core(s). Waiting for barrier\n", ncores);
  fflush(stdout);
  pthread_barrier_wait(&start_barrier);

  /* Establish timebase and reporting windows */
  t_start_ns = now_ns();
  t_last_report_ns = t_start_ns;
  t_next_report_ns = t_start_ns + 1000000000ULL;
  t_end_ns = t_start_ns + (__u64)duration * 1000000000ULL;

  /* Per-second reporting loop in main thread */
  while (1)
  {
    now = now_ns();
    if (duration > 0 && now >= t_end_ns)
      break;

    if (now >= t_next_report_ns)
      print_stats(now);

    /* Sleep a bit to avoid busy wait */
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 2000000};
    nanosleep(&ts, NULL);
  }

  /* Join all cores */
  for (int i = 0; i < ncores; i++)
    pthread_join(threads[i], NULL);

  /* One last print if we overshot the tick */
  print_stats(now_ns());

  /* Final summary */
  __u64 p50 = 0, p99 = 0, p999 = 0;
  hist_percentiles(&p50, &p99, &p999);

  /* Sum totals across cores */
  __u64 totalp_tx = 0, totalp_rx = 0, total_rid_mismatches = 0;
  for (int i = 0; i < ncores; i++)
  {
    totalp_tx += cores[i].total_tx_pkts;
    totalp_rx += cores[i].total_rx_pkts;
    total_rid_mismatches += cores[i].rid_mismatches;
  }

  printf("Cores               : %d\n", ncores);
  printf("TX packets          : %llu\n", (unsigned long long)totalp_tx);
  printf("RX packets          : %llu\n", (unsigned long long)totalp_rx);
  printf("RID mismatches      : %llu\n", (unsigned long long)total_rid_mismatches);
  printf("Avg Load            : %lld rps %.2f Mb/s\n",
         totalp_tx / duration,
         ((double)totalp_tx * msize * 8 / 1000000) / duration);
  printf("Avg Throughput      : %lld rps %.2f Mb/s\n",
         totalp_rx / duration,
         ((double)totalp_rx * msize * 8 / 1000000) / duration);
  printf("RTT percentiles (us): p50=%llu  p99=%llu  p99.9=%llu\n",
         (unsigned long long)p50,
         (unsigned long long)p99,
         (unsigned long long)p999);

  /* Cleanup */
  for (i = 0; i < ncores; i++)
  {
    if (cores[i].txbuf)
      free(cores[i].txbuf);
    if (cores[i].rxbuf)
      free(cores[i].rxbuf);
  }

  free(cores);
  free(threads);
  free(rtt_hist);
  pthread_barrier_destroy(&start_barrier);

  return 0;
}
