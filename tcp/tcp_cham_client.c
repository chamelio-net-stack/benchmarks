/* Reports per-second throughput (Mbits/s and packets/s) and cumulative
 * latency percentiles (p50, p99, p99.9) in microseconds.
 *
 * Usage:
 *   ./tcp_linux_client <src_ip> <src_port> <rem_ip> <rem_port>
 *     [--msize N] [--duration S] [--rate R] [--ncores N]
 *
 *   --msize Total TCP payload bytes.
 *   --duration Duration in seconds to run benchmark
 *   --rate Per-core send rate in megabits per second
 *   --ncores Number of cores to use
 *   --burst Enable bursty workload
 *   --burst-dur-mean Mean burst duration in microseconds
 *   --burst-dur-std Burst duration standard deviation in microseconds
 *   --burst-per-mean Mean off period in microseconds
 *   --burst-per-std Off period standard deviation in microseconds
 */

#define _GNU_SOURCE
#include <tcp_lib.h>
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
#include "../utils/burst.h"

struct payload_hdr {
  __u64 tsc;
}__attribute__((packed));

/* Connection */
struct conn {
  int id;
  int fd;
  int ready;
  __u8 connected;
  int tx_remain;
  __u8 *txbuf;
  int rx_remain;
  __u8 *rxbuf;
  double tokens;
  __u64 last_tsc;
  struct burst_state burst;
  int next;
};

/* Per-core context */
struct core {
  int id;
  struct tcp_context_lib *tcp_ctx;
  struct tcp_wait *wait;
  int nrx_evs;
  struct tcp_wait_event *rx_evs;
  struct sockaddr_in dst_addr;
  socklen_t dst_len;
  __u64 txb_load_interval;
  __u64 txp_load_interval;
  __u64 txb_tp_interval;
  __u64 txp_tp_interval;
  __u64 total_tx_pkts;
  __u64 total_rx_pkts;
  struct conn *conns;
  int nready;
  int ready_head;
  int ready_tail;
  int *ready_conns;
  int nconnected;
  int npending;
};

/* Default arg values */
size_t msize = 64;
int duration = 30;
double rate = 10.0;
int ncores = 1;
int nconns = 1;
int mpending = 0;
int cwait = 0;
bool burst = false;
double burst_dur_mean = 1.0;
double burst_dur_std = 1000.0;
double burst_per_mean = 1000.0;
double burst_per_std = 1000.0;
const char *rem_ip;
int rem_port;
const char *src_ip;
int src_port;
struct sockaddr_in src_addr;
socklen_t src_len;

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
  if (argc < 3)
  {
    fprintf(stderr,
        "Usage: %s <src_ip> <src_port> <rem_ip> <rem_port> [--msize N]"
        " [--duration S] [--rate R] [--ncores N]"
        " [--nconns N] [--mpending N] [--cwait S]"
        " [--burst] [--burst-dur-mean US] [--burst-dur-std US]"
        " [--burst-per-mean US] [--burst-per-std US]\n",
        argv[0]);
    return -1;
  }

  src_ip = argv[1];
  src_port = atoi(argv[2]);
  if (src_port != -1 && (src_port < 0 || src_port > 65535))
  {
    fprintf(stderr, "Invalid src_port\n");
    return -1;
  }
  
  rem_ip = argv[3];
  rem_port = atoi(argv[4]);
  if (rem_port <= 0 || rem_port > 65535)
  {
    fprintf(stderr, "Invalid rem_port\n");
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
    else if (strcmp(argv[i], "--nconns") == 0 && i + 1 < argc)
    {
      nconns = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--mpending") == 0 && i + 1 < argc)
    {
      mpending = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--cwait") == 0 && i + 1 < argc)
    {
      cwait = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--burst") == 0)
    {
      burst = true;
    }
    else if (strcmp(argv[i], "--burst-dur-mean") == 0 && i + 1 < argc)
    {
      burst_dur_mean = strtod(argv[++i], NULL);
    }
    else if (strcmp(argv[i], "--burst-dur-std") == 0 && i + 1 < argc)
    {
      burst_dur_std = strtod(argv[++i], NULL);
    }
    else if (strcmp(argv[i], "--burst-per-mean") == 0 && i + 1 < argc)
    {
      burst_per_mean = strtod(argv[++i], NULL);
    }
    else if (strcmp(argv[i], "--burst-per-std") == 0 && i + 1 < argc)
    {
      burst_per_std = strtod(argv[++i], NULL);
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

  if (burst_dur_mean < 1.0)
    burst_dur_mean = 1.0;
  if (burst_per_mean < 1.0)
    burst_per_mean = 1.0;

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

static void conn_rearm(struct core *c, struct conn *con)
{
  struct conn *tail_con;
  
  /* Connection is already in ready */
  if (con->ready)
    return;
  
  con->next = -1;
  if (c->nready == 0)
  {
    c->ready_head = con->id;
  }
  else
  {
    tail_con = &c->conns[c->ready_tail];
    tail_con->next = con->id;
  }
  con->ready = 1;
  c->ready_tail = con->id;
  c->nready++;
}

static inline void core_init(struct core *c)
{
  int ret;
  int fd, i, nevs;
  struct conn *conns, *con;
  struct tcp_wait_event *evs;
  
  c->tcp_ctx = tcp_ctx_new();
  if (c->tcp_ctx == NULL)
  {
    fprintf(stderr, "core %d: failed to create TCP context\n", c->id);
    abort();
  }

  c->wait = tcp_wait_new(c->tcp_ctx);
  if (c->wait == NULL)
  {
    fprintf(stderr, "core %d: failed to create TCP wait handle\n", c->id);
    abort();
  }
  
  conns = calloc(nconns, sizeof(struct conn));
  if (conns == NULL)
  {
    fprintf(stderr, "error allocating connections\n");
    abort();
  }
  c->conns = conns;
  
  c->ready_conns = malloc(sizeof(int) * nconns);
  if (c->ready_conns == NULL)
  {
    fprintf(stderr, "error allocating ready conns array");
    abort();
  }
  c->nready = 0;
  c->ready_head = -1;
  c->ready_tail = -1;
  
  nevs = nconns * 4;
  evs = calloc(nevs, sizeof(struct tcp_wait_event));
  if (evs == NULL)
  {
    fprintf(stderr, "error allocating rx tcp wait events\n");
    abort();
  }
  c->rx_evs = evs;
  c->nrx_evs = nevs;
  
  for (i = 0; i < nconns; i++)
  {
    fd = tcp_socket(c->tcp_ctx);
    if (fd < 0)
    {
      fprintf(stderr, "failed to create socket\n");
      abort();
    }
    
    if (src_port != 0 || src_ip != 0)
    {
      ret = tcp_bind(c->tcp_ctx, fd, 
          (struct sockaddr *) &src_addr, sizeof(src_addr));
      if (ret < 0)
      {
        fprintf(stderr, "failed to bind src ip and port\n");
        abort();
      } 
    }
  
    con = &conns[i];
    con->id = i;
    con->txbuf = (__u8 *) malloc(msize);
    con->rxbuf = (__u8 *) malloc(msize);
    if (con->txbuf == NULL || con->rxbuf == NULL)
    {
      perror("malloc buffers");
      pthread_exit((void *) (intptr_t) -1);
    }
    memset(con->txbuf, 0, msize);
    memset(con->rxbuf, 0, msize);
    
    con->fd = fd;
    con->ready = 0;
    con->tokens  = 0.0;
    con->connected = 0;
    con->tx_remain = msize;
    con->rx_remain = msize;
    con->next = -1;
    con->last_tsc = util_rdtsc();
    con->burst.on = 0;
    con->burst.next_tsc = 0;
    con->burst.seed = (unsigned int) (con->last_tsc ^ (c->id + 1) *
        2654435761u ^ (i + 1) * 2246822519u);
    conn_rearm(c, con);
  }
  
  c->txb_load_interval = 0;
  c->txp_load_interval = 0;
  c->txb_tp_interval = 0;
  c->txp_tp_interval = 0;
  c->total_tx_pkts = 0;
  c->total_rx_pkts = 0;
  c->nconnected = 0;
  c->npending = 0;
}

static inline void conn_refill(struct conn *con, __u64 tsc_now)
{
    double bytes_per_us, delta_cycles, delta_us, max_tokens;

    if (burst && burst_expired(&con->burst, tsc_now))
      burst_toggle(&con->burst, tsc_now, burst_dur_mean, burst_dur_std,
          burst_per_mean, burst_per_std);

    if (burst && !con->burst.on)
    {
      con->tokens = 0.0;
      con->last_tsc = tsc_now;
      return;
    }
    
    delta_cycles = (double) (tsc_now - con->last_tsc);
    delta_us = delta_cycles / clock_tsc_per_us();
    assert(delta_us >= 0);
    bytes_per_us = (rate / 8.0);
    con->tokens += delta_us * bytes_per_us;
    
    max_tokens = bytes_per_us * (burst ? burst_dur_mean : 1000.0);
    if (con->tokens > max_tokens)
      con->tokens = max_tokens;

    con->last_tsc = tsc_now;
}

static inline int conn_tx(struct core *c, struct conn *con)
{
  int ntx, pos;
  struct payload_hdr *ph;
  
  while (con->tokens >= con->tx_remain)
  {
    assert(con->tx_remain != 0);
    
    if (con->tx_remain == msize) 
    {
      ph = (struct payload_hdr *) con->txbuf;
      ph->tsc = util_rdtsc();
    }
    
    pos = msize - con->tx_remain;
    ntx = tcp_sendto(c->tcp_ctx, con->fd, con->txbuf + pos, 
      con->tx_remain, NULL, 0);
    assert(ntx <= 0 || (ntx <= con->tx_remain && ntx <= msize));
    
    if (ntx <= 0)
      return -1;
      
    con->tx_remain -= ntx;
    con->tokens -= ntx;
    __sync_fetch_and_add(&c->txb_load_interval, ntx);
    if (con->tx_remain == 0)
    {
      con->tx_remain = msize;
      c->total_tx_pkts++;
      __sync_fetch_and_add(&c->txp_load_interval, 1);
    }
  }
  
  return 0;
}

static inline void core_tx(struct core *c)
{
  int i, cid, nready, ret, next;
  __u64 tsc_now;
  struct conn *con;
  
  nready = c->nready;
  for (i = 0; i < nready && c->ready_head != -1; i++)
  {
    cid = c->ready_head;
    con = &c->conns[cid];
    next = con->next;
  
    /* Remove connection from ready */
    con->next = -1;
    con->ready = 0;
    c->nready--;
    c->ready_head = next;
    if (next == -1)
      c->ready_tail = -1;
      
    tsc_now = util_rdtsc();
    conn_refill(con, tsc_now);
    ret = conn_tx(c, con);
    
    if (ret == 0)
    {
      conn_rearm(c, con);
    }
    else if (ret < 0)
    {
      /* Add to tcp wait */
      ret = tcp_wait_mod(c->wait, con->fd, 
          TCP_WAIT_OUT | TCP_WAIT_IN, (__u64) con);
      if (ret < 0)
      {
        fprintf(stderr, "failed to modify conn event to trigger TCP_WAIT_OUT");
        abort();
      }
    }
  }
}

static inline void conn_rx(struct core *c, struct conn *con)
{
  int nrx, pos;
  __u64 rtt_us;
  struct payload_hdr *ph;
  
  while (1)
  {
    assert(con->rx_remain != 0);
    pos = msize - con->rx_remain;
    nrx = tcp_recvfrom(c->tcp_ctx, con->fd, con->rxbuf + pos, 
        con->rx_remain, NULL, 0);
    assert(nrx <= 0 || (nrx <= msize && nrx <= con->rx_remain));
    
    if (nrx <= 0)
      break;
    
    con->rx_remain -= nrx;
    __sync_fetch_and_add(&c->txb_tp_interval, nrx);
    if (con->rx_remain == 0)
    {
      ph = (struct payload_hdr *) con->rxbuf;
      rtt_us = us_since_tsc(ph->tsc);
      c->total_rx_pkts++;
      con->rx_remain = msize;
      __sync_fetch_and_add(&c->txp_tp_interval, 1);
      hist_add(&rtt_hist, rtt_us);
    }
  }
}

static inline void core_rx(struct core *c)
{
  int i, ret;
  __u32 events;
  struct tcp_wait_event *ev;
  struct conn *con;

  tcp_poll_fast(c->tcp_ctx);
  tcp_poll_slow(c->tcp_ctx);
  ret = tcp_wait(c->wait, c->rx_evs, c->nrx_evs, TCP_WAIT_NONBLOCK);
  tcp_poll_fast(c->tcp_ctx);
  tcp_poll_slow(c->tcp_ctx);
  if (ret < 0)
  {
    fprintf(stderr, "tcp_wait failed\n");
    abort();
  }
  
  for (i = 0; i < ret; i++)
  {
    ev = &c->rx_evs[i];
    events = ev->events;
    con = (struct conn *) ev->data;

    /* Add connection to ready queue */
    if (events & TCP_WAIT_OUT)
    {
      conn_rearm(c, con);
      
      /* Disable TCP_WAIT_OUT events */
      ret = tcp_wait_mod(c->wait, con->fd, TCP_WAIT_IN, (__u64) con);
      if (ret < 0)
      {
        perror("failed to disable TCP_WAIT_OUT");
        abort();
      }
    }
     
    /* Process received bytes */
    if (events & TCP_WAIT_IN)
      conn_rx(c, con);
  }
}

static void open_conn(struct core *c, struct conn *con)
{
  int ret;
  __u32 events;
  
  ret = tcp_connect(c->tcp_ctx, con->fd, (struct sockaddr *) &c->dst_addr, 
      c->dst_len, SOCK_NONBLOCK);
  if (ret == 0)
  {
    /* Connected succesfully */
    con->connected = 1;
    c->nconnected++;
    events = TCP_WAIT_IN | TCP_WAIT_OUT;
    ret = tcp_wait_add(c->wait, con->fd, events, (__u64) con);
    if (ret < 0)
    {
      fprintf(stderr, "failed to wait for connection event");
      abort();
    }
  }
  else if (ret == -1 && errno != EINPROGRESS)
  {
    /* Failed to connect */
    fprintf(stderr, "failed to open connection core=%d conn=%d\n", 
        c->id, con->id);
    perror("");
    abort();
  }
  else if (ret == -1 && errno == EINPROGRESS)
  {
    /* Connection is still pending */
    c->npending++;
    events = TCP_WAIT_CONNECTED;
    ret = tcp_wait_add(c->wait, con->fd, events, (__u64) con);
    if (ret < 0)
    {
      fprintf(stderr, "failed to wait for connection event");
      abort();
    }
  }
}

static void open_conns(struct core *c)
{
  int ret, i;
  struct conn *con;
  struct tcp_wait_event evs[mpending];
  
  while (c->nconnected != nconns)
  {
    /* Open up to max pending conns */
    while (c->npending < mpending && 
        (c->npending + c->nconnected) < nconns)
    {
      /* Open more connections */
      con = &c->conns[c->nconnected + c->npending];
      open_conn(c, con);
      
      /* Opened all connections */
      if (c->nconnected == nconns)
        return;
    }
    
    /* Wait for new connections to be opened*/
    tcp_poll_slow(c->tcp_ctx);
    ret = tcp_wait(c->wait, evs, mpending, 0);
    if (ret < 0)
    {
      fprintf(stderr, "tcp_wait failed\n");
      abort();
    }
   
    /* Process connection events */
    for (i = 0; i < ret; i++)
    {
      con = (struct conn *) evs[i].data;
      if (evs[i].events & TCP_WAIT_CONNECTED)
      {
        assert(!con->connected);
        con->connected = 1;
        c->nconnected++;
        c->npending--;
        
        /* Add connection to tcp_wait for tx and rx */
        ret = tcp_wait_mod(c->wait, con->fd, 
            TCP_WAIT_IN | TCP_WAIT_OUT, (__u64) con);
        if (ret < 0)
        {
          fprintf(stderr, "tcp_wait_mod failed\n");
          abort();
        }
      }
      
    }
  }
}

static inline void * core_thread(void *arg)
{
  struct core *c;
  __u64 start_tsc, elapsed;

  c = (struct core *) arg;
  core_init(c);
  
  /* Open all connections */
  open_conns(c);
  fprintf(stderr, "Opened all conns core=%d\n", c->id);

  /* Synchronize start with all other cores and main */
  pthread_barrier_wait(&start_barrier);
  start_tsc = util_rdtsc();
  if (burst)
  {
    for (int i = 0; i < nconns; i++)
    {
      c->conns[i].tokens = 0.0;
      c->conns[i].last_tsc = start_tsc;
      burst_start(&c->conns[i].burst, start_tsc, burst_dur_mean,
          burst_dur_std);
    }
  }
  
  while(1)
  {
    elapsed = us_since_tsc(start_tsc);
    if (duration > 0 && elapsed >= (duration * 1000000))
      break;

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
    
    memset(&cores[i].dst_addr, 0, sizeof(cores[i].dst_addr));
    cores[i].dst_addr.sin_family = AF_INET;
    cores[i].dst_addr.sin_port   = htons((__u16) rem_port);
    if (inet_pton(AF_INET, rem_ip, &cores[i].dst_addr.sin_addr) != 1)
    {
      fprintf(stderr, "Invalid rem_ip\n");
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
  
  /* Initialise src address */
  memset(&src_addr, 0, sizeof(src_addr));
  src_addr.sin_family = AF_INET;
  src_addr.sin_port   = htons((__u16) src_port);
  if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
  {
    fprintf(stderr, "Invalid src_ip\n");
    abort();
  }
  src_len = sizeof(src_addr);
  
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
  
  if (tcp_connect_slow() != 0)
  {
    fprintf(stderr, "failed to connect to Chamelio\n");
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

  /* Cleanup */
  free(cores);
  free(threads);
  hist_cleanup(&rtt_hist);
  pthread_barrier_destroy(&start_barrier);

  return 0;
}
