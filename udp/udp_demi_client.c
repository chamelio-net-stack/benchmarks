/* Reports per-second throughput (Mbits/s and packets/s) and cumulative
 * latency percentiles (p50, p99, p99.9) in microseconds.
 *
 * Usage:
 *   ./udp_demi_client <src_ip> <src_port> <rem_ip> <rem_port>
 *     [--msize N] [--duration S] [--rate R] [--ncores N]
 *
 *   --msize Total UDP payload bytes.
 *   --duration Duration in seconds to run benchmark
 *   --rate Per-core send rate in megabits per second
 *   --ncores Number of cores to use
 */

#define _GNU_SOURCE
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
#include <fcntl.h>
#include <linux/types.h>
#include "../utils/clock.h"
#include "../utils/hist.h"

#include <demi/libos.h>
#include <demi/sga.h>
#include <demi/wait.h>

struct payload_hdr {
  __u64 tsc;
}__attribute__((packed));

/* Per-core context */
struct core {
  int id;
  int fd;
  demi_qresult_t res;
  demi_qtoken_t txqt;
  demi_qtoken_t rxqt;
  demi_sgarray_t sga;
  struct sockaddr_in dst_addr;
  socklen_t dst_len;
  __u64 txb_load_interval;
  __u64 txp_load_interval;
  __u64 txb_tp_interval;
  __u64 txp_tp_interval;
  __u64 total_tx_pkts;
  __u64 total_rx_pkts;
  double rate;
  double tokens;
  __u64 last_tsc;
};

/* Default arg values */
size_t msize = 64;
int duration = 30;
double rate = 10.0;
int ncores = 1;

struct timespec timeout;
const char *rem_ip;
int rem_port;
int src_port;
const char *src_ip;
struct sockaddr_in src_addr;

/* Global time and stats variables */
__u64 t_start_ns, t_end_ns;
__u64 t_last_report_ns, t_next_report_ns;
__u64 txb_load_interval_total, txp_load_interval_total;
__u64 txb_tp_interval_total, txp_tp_interval_total;

/* Clamp histogram at 1 second */
#define MAX_RTT_US 1000000u

static pthread_barrier_t start_barrier;
static pthread_t *threads = NULL;
static struct core *cores = NULL;
static struct hist_state rtt_hist = { 0 };

static inline int parse_args(int argc, char **argv)
{
  if (argc < 5)
  {
    fprintf(stderr,
        "Usage: %s <src_ip> <src_port> <rem_ip> <rem_port> [--msize N]"
        " [--duration S] [--rate R] [--ncores N]\n",
        argv[0]);
    return -1;
  }

  src_ip = argv[1];
  src_port = atoi(argv[2]);
  if (src_port <= 0 || src_port > 65535)
  {
    fprintf(stderr, "invalid src port\n");
    return -1;
  }
  
  rem_ip = argv[3];
  rem_port = atoi(argv[4]);
  if (rem_port <= 0 || rem_port > 65535)
  {
    fprintf(stderr, "invalid rem port\n");
    return -1;
  }

  for (int i = 5; i < argc; i++)
  {
    if ((strcmp(argv[i], "--msize") == 0 ||
        strcmp(argv[i], "--size") == 0) &&
        i + 1 < argc)
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

  if (msize < sizeof(struct payload_hdr))
  {
    fprintf(stderr, "msize must be at least %zu bytes (header size)\n",
        sizeof(struct payload_hdr));
    return -1;
  }

  if (ncores <= 0)
    ncores = 1;

  return 0;
}

static inline void print_stats(__u64 now)
{
  double interval_s, rps_load, mbps_load, rps_tp, mbps_tp;
  __u64 p50 = 0, p99 = 0, p999 = 0;
  __u64 bytes_load = 0, pkts_load = 0, bytes_tp = 0, pkts_tp = 0;

  interval_s = (double) (t_next_report_ns - t_last_report_ns) / 1e9;
  if (interval_s <= 0.0) interval_s = 1.0;

  /* Aggregate per-core and reset per-core intervals */
  for (int i = 0; i < ncores; i++)
  {
    bytes_load += __sync_fetch_and_add(&cores[i].txb_load_interval, 0);
    pkts_load  += __sync_fetch_and_add(&cores[i].txp_load_interval, 0);
    bytes_tp += __sync_fetch_and_add(&cores[i].txb_tp_interval, 0);
    pkts_tp  += __sync_fetch_and_add(&cores[i].txp_tp_interval, 0);
    
    /* Zero values safely */
    __sync_fetch_and_and(&cores[i].txb_load_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_load_interval, 0);
    __sync_fetch_and_and(&cores[i].txb_tp_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_tp_interval, 0);
  }
  txb_load_interval_total = bytes_load;
  txp_load_interval_total  = pkts_load;
  txb_tp_interval_total = bytes_tp;
  txp_tp_interval_total  = pkts_tp;

  rps_load  = (double) txp_load_interval_total / interval_s;
  mbps_load = ((double) txb_load_interval_total * 8.0 / 1e6) / interval_s;
  rps_tp  = (double) txp_tp_interval_total / interval_s;
  mbps_tp = ((double) txb_tp_interval_total * 8.0 / 1e6) / interval_s;

  hist_percentiles(&rtt_hist, &p50, &p99, &p999);

  printf("[ %3lus ] load=%lld rps | load=%.2f Mb/s | tp=%lld rps | tp=%.2f Mb/s | ",
          (unsigned long)((t_next_report_ns - t_start_ns) / 1000000000ULL),
          (__u64) rps_load, mbps_load, (__u64) rps_tp, mbps_tp);
  printf("p50=%llu us  p99=%llu us  p99.9=%llu us\n",
          (unsigned long long) p50,
          (unsigned long long) p99,
          (unsigned long long) p999);
  fflush(stdout);

  t_last_report_ns  = t_next_report_ns;
  t_next_report_ns += 1000000000ULL;

  if (now > t_next_report_ns + 5000000000ULL)
  {
    t_next_report_ns = now + 1000000000ULL;
    t_last_report_ns = now;
  }
}

static inline void core_init(struct core *c)
{
  int fd, ret;
  
  ret = demi_socket(&fd, AF_INET, SOCK_DGRAM, 0);
  if (ret != 0) 
  {
    perror("socket");
    abort();
  }
  
  ret = demi_bind(fd, (struct sockaddr *)&src_addr, sizeof(src_addr));
  if (ret != 0)
  {
    perror("bind");
    abort();
  }
  
  c->sga = demi_sgaalloc(msize);
  if (c->sga.sga_numsegs == 0)
  {
    fprintf(stderr, "failed to allocate sga for tx\n");
    abort();
  }
  
  memset(&c->dst_addr, 0, sizeof(c->dst_addr));
  c->dst_addr.sin_family = AF_INET;
  c->dst_addr.sin_port   = htons((__u16) rem_port);
  if (inet_pton(AF_INET, rem_ip, &c->dst_addr.sin_addr) != 1)
  {
    fprintf(stderr, "invalid server_ip\n");
    abort();
  }
  c->dst_len = sizeof(c->dst_addr);
  
  c->fd = fd;
  c->txqt = -1;
  c->rxqt = -1;
  c->txb_load_interval = 0;
  c->txp_load_interval = 0;
  c->txb_tp_interval = 0;
  c->txp_tp_interval = 0;
  c->total_tx_pkts = 0;
  c->total_rx_pkts = 0;
  c->tokens  = 0.0;
}

static inline void core_tx(struct core *c)
{
  int ret;
  struct payload_hdr ph;
  
  if (c->tokens >= (double) msize && c->txqt == -1)
  {
    ph.tsc = util_rdtsc();
    c->sga.sga_numsegs = 1;
    c->sga.segments[0].data_len_bytes = msize;
    memcpy(c->sga.segments[0].data_buf_ptr, &ph, sizeof(ph));
    assert(demi_pushto(&c->txqt, c->fd, &c->sga,
        (const struct sockaddr *) &c->dst_addr, c->dst_len) == 0);
  }
  
  if (c->txqt != -1)
  {
    ret = demi_wait(&c->res, c->txqt, &timeout);
    assert(ret == 0 || ret == ETIMEDOUT);
    
    if (ret == 0)
    {
      assert(c->res.qr_opcode == DEMI_OPC_PUSH);
      c->tokens -= msize;
      c->total_tx_pkts++;
      __sync_fetch_and_add(&c->txb_load_interval, msize);
      __sync_fetch_and_add(&c->txp_load_interval, 1);
      c->txqt = -1;
    }
  }
}

static inline void core_rx(struct core *c)
{
  int ret;
  __u64 rtt_us;
  struct payload_hdr *ph;
  
  if (c->rxqt == -1)
    assert(demi_pop(&c->rxqt, c->fd) == 0);

  if (c->rxqt != -1)
  {
    ret = demi_wait(&c->res, c->rxqt, &timeout);
    assert(ret == 0 || ret == ETIMEDOUT);
    
    if (ret == 0)
    {
      assert(c->res.qr_opcode == DEMI_OPC_POP);
      assert(c->res.qr_value.sga.sga_numsegs == 1);
      assert(c->res.qr_value.sga.segments[0].data_len_bytes == msize);   
      ph = (struct payload_hdr *) c->res.qr_value.sga.segments[0].data_buf_ptr;
      rtt_us = us_since_tsc(ph->tsc);
      hist_add(&rtt_hist, rtt_us);
      __sync_fetch_and_add(&c->txb_tp_interval, msize);
      __sync_fetch_and_add(&c->txp_tp_interval, 1);
      c->total_rx_pkts++;
      c->rxqt = -1;
      demi_sgafree(&c->res.qr_value.sga);
    }
  }
}

static inline void * core_thread(void *arg)
{
  int ret;
  struct core *c;
  __u64 start_tsc, elapsed, tsc_now;
  double bytes_per_us, delta_cycles, delta_us, max_tokens;

  const struct demi_args args = {
    .argc = 0,
    .argv = NULL,
    .callback = NULL,
  };
  
  ret = demi_init(&args);
  if (ret != 0)
  {
    fprintf(stderr, "Failed to initialize the demikernel\n");
    abort();
  }
  
  c = (struct core *) arg;
  core_init(c);
  
  /* Synchronize start with all other cores and main */
  pthread_barrier_wait(&start_barrier);
  c->last_tsc = util_rdtsc();
  start_tsc = util_rdtsc();
  
  while(1)
  {
    elapsed = us_since_tsc(start_tsc);
    if (duration > 0 && elapsed >= (duration * 1000000))
      break;

    /* Update token bucket using TSC */
    tsc_now = util_rdtsc();
    delta_cycles = (double) (tsc_now - c->last_tsc);
    delta_us = delta_cycles / clock_tsc_per_us();
    assert(delta_us >= 0);
    bytes_per_us = (c->rate / 8.0);
    c->tokens += delta_us * bytes_per_us;
    
    /* Cap bucket to 1 ms worth of traffic to avoid unbounded bursts */
    max_tokens = bytes_per_us * 1000;
    if (c->tokens > max_tokens)
      c->tokens = max_tokens;

    c->last_tsc = tsc_now;

    /* Send as many packets as tokens allow */
    core_tx(c);
    /* Drain replies */
    core_rx(c);
  }

  pthread_exit((void *) (intptr_t) 0);
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
  }
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

static inline void init_client()
{
  src_addr.sin_family = AF_INET;
  src_addr.sin_port = htons((__u16) src_port);
  if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
  {
    fprintf(stderr, "invalid bind_ip\n");
    abort();
  }
  
  /* Calibrate tsc */
  calibrate_tsc();

  /* Allocate cores and thread handles */
  cores = (struct core *) calloc((size_t) ncores, 
      sizeof(struct core));
  threads = (pthread_t *) calloc((size_t) ncores, sizeof(pthread_t));
  if (!cores || !threads)
  {
    perror("calloc cores/threads");
    exit(EXIT_FAILURE);
  }
  
  /* Barrier for all cores + main (to synchronize start of sending) */
  if (pthread_barrier_init(&start_barrier, NULL, 
      (unsigned) ncores + 1) != 0)
  {
    perror("pthread_barrier_init");
    exit(EXIT_FAILURE);
  }

  timeout.tv_nsec = 0;
  timeout.tv_sec = 0;
}

int main(int argc, char **argv)
{
  int ret;
  __u64 now;

  ret = parse_args(argc, argv);
  if (ret != 0)
  {
    fprintf(stderr, "failed to parse args\n");
    return EXIT_FAILURE;
  }

  ret = init_hist(&rtt_hist, MAX_RTT_US);
  if (ret != 0)
  {
    fprintf(stderr, "failed to init hist\n");
    return EXIT_FAILURE;
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
  t_end_ns = t_start_ns + (__u64) duration * 1000000000ULL;
  
  /* Per-second reporting loop in main thread */
  while (1)
  {
    now = now_ns();
    if (duration > 0 && now >= t_end_ns)
      break;

    if (now >= t_next_report_ns)
      print_stats(now);

    /* Sleep a bit to avoid busy wait */
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 2000000 };
    nanosleep(&ts, NULL);
  }

  /* Join all cores */
  for (int i = 0; i < ncores; i++)
    pthread_join(threads[i], NULL);

  /* One last print if we overshot the tick */
  print_stats(now_ns());

  /* Final summary */
  __u64 p50 = 0, p99 = 0, p999 = 0;
  hist_percentiles(&rtt_hist, &p50, &p99, &p999);

  /* Sum total sent packets across cores */
  __u64 totalp_tx = 0, totalp_rx = 0;
  for (int i = 0; i < ncores; i++)
  {
    totalp_tx += cores[i].total_tx_pkts;
    totalp_rx += cores[i].total_rx_pkts;
  }

  printf("Cores               : %d\n", ncores);
  printf("TX packets          : %llu\n", (unsigned long long) totalp_tx);
  printf("RX packets          : %llu\n", (unsigned long long) totalp_rx);
  printf("Avg Load            : %lld rps %.2f Mb/s\n", 
      totalp_tx / duration, 
      ((double) totalp_tx * msize * 8 / 1000000) / duration);
  printf("Avg Throughput      : %lld rps %.2f Mb/s\n", 
      totalp_rx / duration, 
      ((double) totalp_rx * msize * 8 / 1000000) / duration);
  printf("RTT percentiles (us): p50=%llu  p99=%llu  p99.9=%llu\n",
      (unsigned long long) p50,
      (unsigned long long) p99,
      (unsigned long long) p999);

  free(cores);
  free(threads);
  hist_cleanup(&rtt_hist);
  pthread_barrier_destroy(&start_barrier);

  return 0;
}
