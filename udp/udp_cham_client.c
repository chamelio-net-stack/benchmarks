/* Reports per-second throughput (Mbits/s and packets/s) and cumulative
 * latency percentiles (p50, p99, p99.9) in microseconds.
 *
 * Usage:
 *   ./udp_cham_client <src_ip> <src_port> <rem_ip> <rem_port>
 *     [--msize N] [--duration S] [--rate R] [--ncores N] [--cores LIST]
 *
 *   --msize Total UDP payload bytes.
 *   --duration Duration in seconds to run benchmark
 *   --rate Per-core send rate in megabits per second
 *   --ncores Number of cores to use
 *   --cores Comma-separated core list (main, thread0, thread1, ...)
 */

#define _GNU_SOURCE
#include <chamelio/udp_lib.h>
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
#include <sched.h>
#include "../utils/clock.h"
#include "../utils/hist.h"
#include "../utils/core.h"

struct payload_hdr {
  __u64 tsc;
}__attribute__((packed));

/* Per-core context */
struct core {
  int id;
  int fd;
  struct udp_wait *wait;
  struct udp_wait_event event;
  __u32 wait_events;
  __u8 *txbuf;
  __u8 *rxbuf;
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
  __u8 tx_blocked;
};

/* Default arg values */
size_t msize = 64;
int duration = 30;
double rate = 10.0;
int ncores = 1;
static char *cores_arg = NULL;
static int *core_list = NULL;
static int core_list_len = 0;
const char *rem_ip;
int rem_port;
const char *src_ip;
int src_port;
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

static inline __u32 core_wait_events(struct core *c)
{
  return UDP_WAIT_IN | (c->tx_blocked ? UDP_WAIT_OUT : 0);
}

static inline void core_wait_update(struct core *c)
{
  __u32 events = core_wait_events(c);

  if (c->wait_events == events)
    return;

  if (udp_wait_mod(c->wait, c->fd, events, 0) != 0)
    abort();
  c->wait_events = events;
}

static inline int parse_core_list(const char *arg, int expected_len)
{
  int count = 1;
  const char *p = arg;
  char *tmp, *tok, *end;
  int idx = 0;

  if (arg == NULL || *arg == '\0')
    return -1;

  while (*p)
  {
    if (*p == ',')
      count++;
    p++;
  }

  if (expected_len > 0 && count != expected_len)
  {
    fprintf(stderr, "cores list must have %d entries (main + %d threads)\n",
        expected_len, expected_len - 1);
    return -1;
  }

  core_list = (int *) calloc((size_t) count, sizeof(int));
  if (!core_list)
  {
    perror("calloc core_list");
    return -1;
  }

  tmp = strdup(arg);
  if (!tmp)
  {
    perror("strdup cores");
    free(core_list);
    core_list = NULL;
    return -1;
  }

  for (tok = strtok(tmp, ","); tok != NULL; tok = strtok(NULL, ","))
  {
    while (*tok == ' ' || *tok == '\t')
      tok++;
    if (*tok == '\0')
    {
      fprintf(stderr, "empty core id in list\n");
      free(core_list);
      core_list = NULL;
      free(tmp);
      return -1;
    }
    errno = 0;
    core_list[idx] = (int) strtol(tok, &end, 10);
    if (errno != 0 || end == tok || (*end != '\0' && *end != ' ' && *end != '\t'))
    {
      fprintf(stderr, "invalid core id: %s\n", tok);
      free(core_list);
      core_list = NULL;
      free(tmp);
      return -1;
    }
    if (core_list[idx] < 0 || core_list[idx] >= CPU_SETSIZE)
    {
      fprintf(stderr, "core id out of range: %d\n", core_list[idx]);
      free(core_list);
      core_list = NULL;
      free(tmp);
      return -1;
    }
    idx++;
  }

  free(tmp);
  core_list_len = count;
  return 0;
}

static inline int parse_args(int argc, char **argv)
{
  if (argc < 3)
  {
    fprintf(stderr,
        "Usage: %s <src_ip> <src_port> <rem_ip> <rem_port> [--msize N]"
        " [--duration S] [--rate R] [--ncores N] [--cores LIST]\n",
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
    else if ((strcmp(argv[i], "--cores") == 0) && i + 1 < argc)
    {
      cores_arg = argv[++i];
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

  if (cores_arg != NULL)
  {
    if (parse_core_list(cores_arg, ncores + 1) != 0)
      return -1;
  }

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

static inline struct udp_context_lib * core_init(struct core *c)
{
  int ret;
  struct udp_context_lib *udp_ctx;

  /* Each core has its own UDP context and socket */
  udp_ctx = udp_ctx_new();
  if (udp_ctx == NULL)
  {
    fprintf(stderr, "core %d: failed to create UDP context\n", c->id);
    pthread_exit((void *) (intptr_t) -1);
  }

  c->fd = udp_socket(udp_ctx);
  if (c->fd < 0)
  {
    fprintf(stderr, "failed to create udp socket");
    pthread_exit((void *) (intptr_t) -1);
  }

  if (src_port != 0)
  {
    ret = udp_bind(udp_ctx, c->fd, 
        (struct sockaddr *) &src_addr, sizeof(src_addr));
    if (ret < 0)
    {
      fprintf(stderr, "failed to bind src ip and port\n");
      abort();
    } 
  }

  c->txbuf = (__u8 *) malloc(msize);
  c->rxbuf = (__u8 *) malloc(msize);
  if (!c->txbuf || !c->rxbuf)
  {
    perror("malloc buffers");
    pthread_exit((void *) (intptr_t) -1);
  }
  memset(c->txbuf, 0, msize);
  memset(c->rxbuf, 0, msize);

  /* Core 0 resolves ARP */
  if (c->id == 0)
  {
    udp_sendto(udp_ctx, c->fd, c->txbuf, msize, 
        (struct sockaddr *) &c->dst_addr, c->dst_len);
    sleep(1);
    udp_sendto(udp_ctx, c->fd, c->txbuf, msize, 
        (struct sockaddr *) &c->dst_addr, c->dst_len);
    sleep(1);
    while(udp_poll_fast(udp_ctx) == 0);
    while(udp_recvfrom(udp_ctx, c->fd, c->rxbuf, 
        msize, NULL, 0) == 0);
    fprintf(stderr, "Resolved ARP\n");
  }

  c->txb_load_interval = 0;
  c->txp_load_interval = 0;
  c->txb_tp_interval = 0;
  c->txp_tp_interval = 0;
  c->total_tx_pkts = 0;
  c->total_rx_pkts = 0;
  c->tokens  = 0.0;
  c->tx_blocked = 0;
  c->wait = udp_wait_new(udp_ctx);
  if (c->wait == NULL)
    abort();
  c->wait_events = core_wait_events(c);
  if (udp_wait_add(c->wait, c->fd, c->wait_events, 0) != 0)
    abort();
  return udp_ctx;
}

static inline void core_tx(struct udp_context_lib *udp_ctx, struct core *c)
{
  int ntx;
  struct payload_hdr *ph;

  if (c->tx_blocked)
    return;
  
  while (c->tokens >= (double) msize)
  {
    ph = (struct payload_hdr *) c->txbuf;
    ph->tsc = util_rdtsc();

    ntx = udp_sendto(udp_ctx, c->fd, c->txbuf, msize,
                  (struct sockaddr *) &c->dst_addr, c->dst_len);
    assert(ntx == msize || ntx <= 0);
    assert(c->tokens >= msize);
    
    if (ntx < 0)
    {
      c->tx_blocked = 1;
      core_wait_update(c);
      return;
    }
      
    c->tokens -= msize;
    c->total_tx_pkts++;
    __sync_fetch_and_add(&c->txb_load_interval, ntx);
    __sync_fetch_and_add(&c->txp_load_interval, 1);
  }
}

static inline void core_rx(struct udp_context_lib *udp_ctx, struct core *c)
{
  int nevents;
  int nrx;
  __u64 rtt_us;
  struct payload_hdr *ph;

  udp_poll_fast(udp_ctx);
  nevents = udp_wait_fast(c->wait, &c->event, 1, UDP_WAIT_NONBLOCK);
  if (nevents < 0)
    abort();
  if (nevents == 0)
    return;

  if ((c->event.events & UDP_WAIT_OUT) != 0 && c->tx_blocked)
  {
    c->tx_blocked = 0;
    core_wait_update(c);
  }

  if ((c->event.events & UDP_WAIT_IN) == 0)
    return;

  while (1)
  {
    nrx = udp_recvfrom(udp_ctx, c->fd, c->rxbuf, msize, NULL, 0);
    assert(nrx == msize || nrx <= 0);
    
    if (nrx <= 0)
      break;
    
    ph = (struct payload_hdr *) c->rxbuf;
    rtt_us = us_since_tsc(ph->tsc);
    c->total_rx_pkts++;
    __sync_fetch_and_add(&c->txb_tp_interval, nrx);
    __sync_fetch_and_add(&c->txp_tp_interval, 1);
    hist_add(&rtt_hist, rtt_us);
  }
}

static inline void * core_thread(void *arg)
{
  struct core *c;
  struct udp_context_lib *udp_ctx;
  __u64 start_tsc, elapsed, tsc_now;
  double bytes_per_us, delta_cycles, delta_us, max_tokens;

  c = (struct core *) arg;
  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[c->id + 1]) != 0)
      pthread_exit((void *) (intptr_t) -1);
  }
  udp_ctx = core_init(c);
  
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

    core_tx(udp_ctx, c);
    core_rx(udp_ctx, c);
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

    memset(&cores[i].dst_addr, 0, sizeof(cores[i].dst_addr));
    cores[i].dst_addr.sin_family = AF_INET;
    cores[i].dst_addr.sin_port   = htons((__u16) rem_port);
    if (inet_pton(AF_INET, rem_ip, &cores[i].dst_addr.sin_addr) != 1)
    {
      fprintf(stderr, "Invalid server_ip\n");
      abort();
    }
    cores[i].dst_len = sizeof(cores[i].dst_addr);
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
}

int main(int argc, char **argv)
{
  int i, ret;
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

  ret = udp_connect_slow();
  if (ret != 0)
  {
    fprintf(stderr, "failed to connect to Chamelio");
    exit(EXIT_FAILURE);
  }
  
  init_client();
  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[0]) != 0)
      return EXIT_FAILURE;
  }

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

  /* Cleanup */
  for (i = 0; i < ncores; i++)
  {
    if (cores[i].txbuf) free(cores[i].txbuf);
    if (cores[i].rxbuf) free(cores[i].rxbuf);
  }

  free(cores);
  free(threads);
  free(core_list);
  hist_cleanup(&rtt_hist);
  pthread_barrier_destroy(&start_barrier);

  return 0;
}
