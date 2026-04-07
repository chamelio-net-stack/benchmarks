/* Reports per-second throughput (Mbits/s and packets/s) and cumulative
 * latency percentiles (p50, p99, p99.9) in microseconds.
 *
 * Usage:
 *   ./tcp_cham_client <src_ip> <src_port> <rem_ip> <rem_port>
 *     [--msize N] [--duration S] [--rate R] [--ncores N]
 *     [--nconns N] [--mpending N] [--cwait S] [--cores LIST]
 *
 *   --msize Total TCP payload bytes.
 *   --duration Duration in seconds to run benchmark
 *   --rate Per-connection send rate in megabits per second
 *   --ncores Number of cores to use
 *   --nconns Number of TCP connections per core
 *   --mpending Max number of pending connects per core
 *   --cwait Time in seconds to wait before starting traffic
 *   --cores Comma-separated core list (main, thread0, thread1, ...)
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
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include "../utils/clock.h"
#include "../utils/hist.h"
#include "../utils/core.h"

struct payload_hdr {
  __u64 tsc;
}__attribute__((packed));

struct conn {
  int fd;
  bool active;
  bool connected;
  __u8 *txbuf;
  __u8 *rxbuf;
  size_t tx_off;
  size_t rx_off;
  double tokens;
  __u64 total_tx_pkts;
  __u64 total_rx_pkts;
};

/* Per-core context */
struct core {
  int id;
  struct tcp_context_lib *tcp_ctx;
  struct tcp_wait *wait;
  struct tcp_wait_event *events;
  struct conn *conns;
  struct sockaddr_in dst_addr;
  socklen_t dst_len;
  __u64 txb_load_interval;
  __u64 txp_load_interval;
  __u64 txb_tp_interval;
  __u64 txp_tp_interval;
  int established_conns;
  int pending_conns;
  __u64 last_tsc;
};

/* Default arg values */
size_t msize = 64;
int duration = 30;
double rate = 10.0;
int ncores = 1;
int nconns = 1;
int mpending = 0;
double cwait = 0.0;
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

static inline int src_ip_is_auto(void)
{
  return strcmp(src_ip, "-1") == 0;
}

static inline int should_bind_source(void)
{
  return nconns == 1 && src_port > 0 && !src_ip_is_auto();
}

static inline int core_src_port(int core_id)
{
  return src_port + core_id;
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
  if (argc < 5)
  {
    fprintf(stderr,
        "Usage: %s <src_ip> <src_port> <rem_ip> <rem_port> [--msize N]"
        " [--duration S] [--rate R] [--ncores N] [--nconns N]"
        " [--mpending N] [--cwait S] [--cores LIST]\n",
        argv[0]);
    return -1;
  }

  src_ip = argv[1];
  src_port = atoi(argv[2]);
  if (src_port < -1 || src_port > 65535)
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
      msize = (size_t) strtoul(argv[++i], NULL, 10);
    }
    else if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc)
    {
      duration = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--rate") == 0 && i + 1 < argc)
    {
      rate = strtod(argv[++i], NULL);
    }
    else if (strcmp(argv[i], "--ncores") == 0 && i + 1 < argc)
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
      cwait = strtod(argv[++i], NULL);
    }
    else if (strcmp(argv[i], "--cores") == 0 && i + 1 < argc)
    {
      cores_arg = argv[++i];
    }
    else
    {
      fprintf(stderr, "Unknown arg: %s\n", argv[i]);
      return -1;
    }
  }

  if (duration <= 0)
  {
    fprintf(stderr, "duration must be > 0\n");
    return -1;
  }

  if (rate <= 0.0)
  {
    fprintf(stderr, "rate must be > 0\n");
    return -1;
  }

  if (cwait < 0.0)
  {
    fprintf(stderr, "cwait must be >= 0\n");
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
  if (nconns <= 0)
    nconns = 1;

  if (mpending == 0)
    mpending = nconns;
  if (mpending <= 0 || mpending > nconns)
  {
    fprintf(stderr, "mpending must be in [1, nconns]\n");
    return -1;
  }

  if (should_bind_source() &&
      (src_port + ncores - 1 > 65535))
  {
    fprintf(stderr, "src_port + ncores - 1 exceeds 65535\n");
    return -1;
  }

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
  if (interval_s <= 0.0)
    interval_s = 1.0;

  for (int i = 0; i < ncores; i++)
  {
    bytes_load += __sync_fetch_and_add(&cores[i].txb_load_interval, 0);
    pkts_load += __sync_fetch_and_add(&cores[i].txp_load_interval, 0);
    bytes_tp += __sync_fetch_and_add(&cores[i].txb_tp_interval, 0);
    pkts_tp += __sync_fetch_and_add(&cores[i].txp_tp_interval, 0);

    __sync_fetch_and_and(&cores[i].txb_load_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_load_interval, 0);
    __sync_fetch_and_and(&cores[i].txb_tp_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_tp_interval, 0);
  }

  txb_load_interval_total = bytes_load;
  txp_load_interval_total = pkts_load;
  txb_tp_interval_total = bytes_tp;
  txp_tp_interval_total = pkts_tp;

  rps_load = (double) txp_load_interval_total / interval_s;
  mbps_load = ((double) txb_load_interval_total * 8.0 / 1e6) / interval_s;
  rps_tp = (double) txp_tp_interval_total / interval_s;
  mbps_tp = ((double) txb_tp_interval_total * 8.0 / 1e6) / interval_s;

  hist_percentiles(&rtt_hist, &p50, &p99, &p999);

  printf("[ %3lus ] load=%lld rps | load=%.2f Mb/s | tp=%lld rps | tp=%.2f Mb/s | ",
      (unsigned long) ((t_next_report_ns - t_start_ns) / 1000000000ULL),
      (unsigned long long) rps_load, mbps_load,
      (unsigned long long) rps_tp, mbps_tp);
  printf("p50=%llu us  p99=%llu us  p99.9=%llu us\n",
      (unsigned long long) p50,
      (unsigned long long) p99,
      (unsigned long long) p999);
  fflush(stdout);

  t_last_report_ns = t_next_report_ns;
  t_next_report_ns += 1000000000ULL;

  if (now > t_next_report_ns + 5000000000ULL)
  {
    t_next_report_ns = now + 1000000000ULL;
    t_last_report_ns = now;
  }
}

static inline void conn_reset(struct conn *cn)
{
  cn->fd = -1;
  cn->active = false;
  cn->connected = false;
  cn->tx_off = 0;
  cn->rx_off = 0;
  cn->tokens = 0.0;
}

static inline __u32 conn_wait_events(struct conn *cn)
{
  __u32 events = TCP_WAIT_CLOSED;

  if (!cn->active)
    return 0;

  if (!cn->connected)
    return events | TCP_WAIT_CONNECTED | TCP_WAIT_OP;

  events |= TCP_WAIT_IN;
  if (cn->tx_off > 0 || cn->tokens >= (double) msize)
    events |= TCP_WAIT_OUT;

  return events;
}

static inline void conn_wait_add(struct core *c, struct conn *cn)
{
  if (tcp_wait_add(c->wait, cn->fd, conn_wait_events(cn),
          (__u64) (uintptr_t) cn) != 0)
  {
    fprintf(stderr, "core %d fd=%d: wait_add failed: %s\n",
        c->id, cn->fd, strerror(errno));
    abort();
  }
}

static inline void conn_wait_update(struct core *c, struct conn *cn)
{
  if (!cn->active)
    return;

  if (tcp_wait_mod(c->wait, cn->fd, conn_wait_events(cn),
          (__u64) (uintptr_t) cn) != 0)
  {
    fprintf(stderr, "core %d fd=%d: wait_mod failed: %s\n",
        c->id, cn->fd, strerror(errno));
    abort();
  }
}

static inline void core_drop_conn(struct core *c, struct conn *cn,
    const char *op, int err)
{
  if (cn->fd >= 0)
  {
    tcp_wait_del(c->wait, cn->fd);
    tcp_close(c->tcp_ctx, cn->fd);
  }

  if (cn->active)
  {
    if (cn->connected)
    {
      if (c->established_conns > 0)
        c->established_conns--;
    }
    else if (c->pending_conns > 0)
    {
      c->pending_conns--;
    }
  }

  fprintf(stderr, "core %d fd=%d: %s: %s\n",
      c->id, cn->fd, op,
      err == 0 ? "connection closed" : strerror(err));
  fflush(stderr);

  conn_reset(cn);
}

static inline int core_start_conn(struct core *c, struct conn *cn)
{
  int fd, ret;

  fd = tcp_socket(c->tcp_ctx);
  if (fd < 0)
    return -1;

  if (should_bind_source())
  {
    struct sockaddr_in bind_addr = src_addr;

    bind_addr.sin_port = htons((__u16) core_src_port(c->id));
    ret = tcp_bind(c->tcp_ctx, fd,
        (struct sockaddr *) &bind_addr, sizeof(bind_addr));
    if (ret < 0)
    {
      tcp_close(c->tcp_ctx, fd);
      return -1;
    }
  }

  cn->fd = fd;
  cn->active = true;
  cn->connected = false;
  cn->tx_off = 0;
  cn->rx_off = 0;
  cn->tokens = 0.0;

  ret = tcp_connect(c->tcp_ctx, fd,
      (struct sockaddr *) &c->dst_addr, c->dst_len, SOCK_NONBLOCK);
  if (ret == 0)
  {
    cn->connected = true;
    c->established_conns++;
    conn_wait_add(c, cn);
    return 0;
  }

  if (errno == EINPROGRESS)
  {
    c->pending_conns++;
    conn_wait_add(c, cn);
    return 0;
  }

  tcp_close(c->tcp_ctx, fd);
  conn_reset(cn);
  return -1;
}

static inline int core_open_pending(struct core *c)
{
  int started = 0;

  while ((c->established_conns + c->pending_conns) < nconns &&
      c->pending_conns < mpending)
  {
    bool found = false;

    for (int i = 0; i < nconns; i++)
    {
      if (c->conns[i].active)
        continue;

      found = true;
      if (core_start_conn(c, &c->conns[i]) != 0)
      {
        fprintf(stderr, "core %d: connect setup failed: %s\n",
            c->id, strerror(errno));
        fflush(stderr);
      }
      else
      {
        started++;
      }
      break;
    }

    if (!found)
      break;
  }

  return started;
}

static inline int core_finish_pending(struct core *c, struct conn *cn)
{
  int ret;

  if (!cn->active || cn->connected)
    return 0;

  ret = tcp_connect(c->tcp_ctx, cn->fd,
      (struct sockaddr *) &c->dst_addr, c->dst_len, SOCK_NONBLOCK);
  if (ret == 0)
  {
    cn->connected = true;
    if (c->pending_conns > 0)
      c->pending_conns--;
    c->established_conns++;
    conn_wait_update(c, cn);
    return 1;
  }

  if (errno == EALREADY || errno == EINPROGRESS)
    return 0;

  core_drop_conn(c, cn, "connect failed", errno);
  return 1;
}

static inline void core_connect_all(struct core *c)
{
  while (c->established_conns < nconns)
  {
    int opened = core_open_pending(c);
    int nevents, progress = 0;

    nevents = tcp_wait(c->wait, c->events, nconns, TCP_WAIT_NONBLOCK);
    if (nevents < 0)
    {
      fprintf(stderr, "core %d: tcp_wait failed: %s\n", c->id, strerror(errno));
      pthread_exit((void *) (intptr_t) -1);
    }

    for (int i = 0; i < nevents; i++)
    {
      struct conn *cn = (struct conn *) (uintptr_t) c->events[i].data;

      if (cn == NULL || !cn->active || cn->connected || cn->fd != c->events[i].sockfd)
        continue;

      if ((c->events[i].events &
              (TCP_WAIT_CONNECTED | TCP_WAIT_OP | TCP_WAIT_CLOSED)) == 0)
      {
        continue;
      }

      progress |= core_finish_pending(c, cn);
    }

    if (opened == 0 && progress == 0)
      sched_yield();
  }
}

static inline void core_init(struct core *c)
{
  c->tcp_ctx = tcp_ctx_new();
  if (c->tcp_ctx == NULL)
  {
    fprintf(stderr, "core %d: failed to create TCP context\n", c->id);
    pthread_exit((void *) (intptr_t) -1);
  }

  c->wait = tcp_wait_new(c->tcp_ctx);
  if (c->wait == NULL)
  {
    fprintf(stderr, "core %d: failed to create TCP wait handle\n", c->id);
    pthread_exit((void *) (intptr_t) -1);
  }

  c->conns = (struct conn *) calloc((size_t) nconns, sizeof(struct conn));
  c->events = calloc((size_t) nconns, sizeof(*c->events));
  if (c->conns == NULL || c->events == NULL)
  {
    perror("calloc conn state");
    pthread_exit((void *) (intptr_t) -1);
  }

  for (int i = 0; i < nconns; i++)
  {
    conn_reset(&c->conns[i]);
    c->conns[i].txbuf = (__u8 *) malloc(msize);
    c->conns[i].rxbuf = (__u8 *) malloc(msize);
    if (c->conns[i].txbuf == NULL || c->conns[i].rxbuf == NULL)
    {
      perror("malloc buffers");
      pthread_exit((void *) (intptr_t) -1);
    }
    memset(c->conns[i].txbuf, 0, msize);
    memset(c->conns[i].rxbuf, 0, msize);
  }

  c->txb_load_interval = 0;
  c->txp_load_interval = 0;
  c->txb_tp_interval = 0;
  c->txp_tp_interval = 0;
  c->established_conns = 0;
  c->pending_conns = 0;
  c->last_tsc = 0;
}

static inline void core_tx_conn(struct core *c, struct conn *cn)
{
  while (cn->tokens >= (double) msize)
  {
    int ntx;
    struct payload_hdr *ph;

    if (!cn->active || !cn->connected)
      break;

    if (cn->tx_off == 0)
    {
      ph = (struct payload_hdr *) cn->txbuf;
      ph->tsc = util_rdtsc();
    }

    ntx = tcp_sendto(c->tcp_ctx, cn->fd, cn->txbuf + cn->tx_off,
        msize - cn->tx_off, NULL, 0);
    if (ntx < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;

      core_drop_conn(c, cn, "send failed", errno);
      break;
    }

    if (ntx == 0)
    {
      core_drop_conn(c, cn, "send failed", 0);
      break;
    }

    cn->tx_off += (size_t) ntx;
    if (cn->tx_off < msize)
      break;

    cn->tx_off = 0;
    cn->tokens -= (double) msize;
    cn->total_tx_pkts++;
    __sync_fetch_and_add(&c->txb_load_interval, msize);
    __sync_fetch_and_add(&c->txp_load_interval, 1);
  }

  if (cn->active)
    conn_wait_update(c, cn);
}

static inline void core_rx_conn(struct core *c, struct conn *cn)
{
  while (1)
  {
    int nrx;
    struct payload_hdr *ph;
    __u64 rtt_us;

    nrx = tcp_recvfrom(c->tcp_ctx, cn->fd, cn->rxbuf + cn->rx_off,
        msize - cn->rx_off, NULL, 0);
    if (nrx < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;

      core_drop_conn(c, cn, "recv failed", errno);
      break;
    }

    if (nrx == 0)
    {
      core_drop_conn(c, cn, "recv failed", 0);
      break;
    }

    cn->rx_off += (size_t) nrx;
    if (cn->rx_off < msize)
      continue;

    ph = (struct payload_hdr *) cn->rxbuf;
    rtt_us = us_since_tsc(ph->tsc);
    cn->rx_off = 0;
    cn->total_rx_pkts++;
    __sync_fetch_and_add(&c->txb_tp_interval, msize);
    __sync_fetch_and_add(&c->txp_tp_interval, 1);
    hist_add(&rtt_hist, rtt_us);
  }

  if (cn->active)
    conn_wait_update(c, cn);
}

static inline void wait_for_start(void)
{
  while (now_ns() < t_start_ns)
  {
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
    nanosleep(&ts, NULL);
  }
}

static inline void * core_thread(void *arg)
{
  struct core *c;
  __u64 start_tsc, elapsed, tsc_now;
  double bytes_per_us, delta_cycles, delta_us, max_tokens;

  c = (struct core *) arg;
  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[c->id + 1]) != 0)
      pthread_exit((void *) (intptr_t) -1);
  }

  core_init(c);
  core_connect_all(c);

  pthread_barrier_wait(&start_barrier);
  wait_for_start();

  c->last_tsc = util_rdtsc();
  start_tsc = c->last_tsc;
  bytes_per_us = rate / 8.0;
  max_tokens = bytes_per_us * 1000.0;

  while (1)
  {
    int active = 0;
    int nevents;

    elapsed = us_since_tsc(start_tsc);
    if (elapsed >= (__u64) duration * 1000000ULL)
      break;

    tsc_now = util_rdtsc();
    delta_cycles = (double) (tsc_now - c->last_tsc);
    delta_us = delta_cycles / clock_tsc_per_us();
    assert(delta_us >= 0.0);
    c->last_tsc = tsc_now;

    for (int i = 0; i < nconns; i++)
    {
      struct conn *cn = &c->conns[i];

      if (!cn->active || !cn->connected)
        continue;

      active++;
      cn->tokens += delta_us * bytes_per_us;
      if (cn->tokens > max_tokens)
        cn->tokens = max_tokens;
      conn_wait_update(c, cn);
    }

    nevents = tcp_wait_fast(c->wait, c->events, nconns, TCP_WAIT_NONBLOCK);
    if (nevents < 0)
    {
      fprintf(stderr, "core %d: tcp_wait_fast failed: %s\n",
          c->id, strerror(errno));
      pthread_exit((void *) (intptr_t) -1);
    }

    for (int i = 0; i < nevents; i++)
    {
      struct tcp_wait_event *ev = &c->events[i];
      struct conn *cn = (struct conn *) (uintptr_t) ev->data;

      if (cn == NULL || !cn->active || !cn->connected || cn->fd != ev->sockfd)
        continue;

      if ((ev->events & TCP_WAIT_OUT) != 0)
        core_tx_conn(c, cn);
      if ((ev->events & (TCP_WAIT_IN | TCP_WAIT_CLOSED)) != 0)
        core_rx_conn(c, cn);
    }

    if (active == 0)
    {
      struct timespec ts = { .tv_sec = 0, .tv_nsec = 2000000 };
      nanosleep(&ts, NULL);
    }
  }

  pthread_exit((void *) (intptr_t) 0);
}

static inline void prepare_cores(void)
{
  for (int i = 0; i < ncores; i++)
  {
    memset(&cores[i], 0, sizeof(struct core));
    cores[i].id = i;
    memset(&cores[i].dst_addr, 0, sizeof(cores[i].dst_addr));
    cores[i].dst_addr.sin_family = AF_INET;
    cores[i].dst_addr.sin_port = htons((__u16) rem_port);
    if (inet_pton(AF_INET, rem_ip, &cores[i].dst_addr.sin_addr) != 1)
    {
      fprintf(stderr, "Invalid rem_ip\n");
      abort();
    }
    cores[i].dst_len = sizeof(cores[i].dst_addr);
  }
}

static inline void start_cores(void)
{
  for (int i = 0; i < ncores; i++)
  {
    int rc = pthread_create(&threads[i], NULL, core_thread, &cores[i]);
    if (rc != 0)
    {
      errno = rc;
      perror("pthread_create");
      exit(EXIT_FAILURE);
    }
  }
}

static inline void init_client(void)
{
  memset(&src_addr, 0, sizeof(src_addr));
  src_addr.sin_family = AF_INET;
  src_addr.sin_port = htons((__u16) src_port);
  if (should_bind_source())
  {
    if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
    {
      fprintf(stderr, "invalid source bind_ip\n");
      abort();
    }
  }

  calibrate_tsc();

  cores = (struct core *) calloc((size_t) ncores, sizeof(struct core));
  threads = (pthread_t *) calloc((size_t) ncores, sizeof(pthread_t));
  if (!cores || !threads)
  {
    perror("calloc cores/threads");
    exit(EXIT_FAILURE);
  }

  if (pthread_barrier_init(&start_barrier, NULL,
      (unsigned) ncores + 1) != 0)
  {
    perror("pthread_barrier_init");
    exit(EXIT_FAILURE);
  }
}

int main(int argc, char **argv)
{
  __u64 now;
  __u64 p50 = 0, p99 = 0, p999 = 0;
  __u64 totalp_tx = 0, totalp_rx = 0;

  if (parse_args(argc, argv) != 0)
  {
    fprintf(stderr, "failed to parse args\n");
    return EXIT_FAILURE;
  }

  if (init_hist(&rtt_hist, MAX_RTT_US) != 0)
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

  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[0]) != 0)
      return EXIT_FAILURE;
  }

  start_cores();

  printf("Initialized %d core(s), %d connection(s)/core. Waiting for barrier\n",
      ncores, nconns);
  fflush(stdout);

  t_start_ns = now_ns() + (__u64) (cwait * 1000000000.0);
  t_last_report_ns = t_start_ns;
  t_next_report_ns = t_start_ns + 1000000000ULL;
  t_end_ns = t_start_ns + (__u64) duration * 1000000000ULL;

  pthread_barrier_wait(&start_barrier);
  wait_for_start();

  while (1)
  {
    now = now_ns();
    if (now >= t_end_ns)
      break;

    if (now >= t_next_report_ns)
      print_stats(now);

    {
      struct timespec ts = { .tv_sec = 0, .tv_nsec = 2000000 };
      nanosleep(&ts, NULL);
    }
  }

  for (int i = 0; i < ncores; i++)
    pthread_join(threads[i], NULL);

  print_stats(now_ns());
  hist_percentiles(&rtt_hist, &p50, &p99, &p999);

  for (int i = 0; i < ncores; i++)
  {
    for (int j = 0; j < nconns; j++)
    {
      totalp_tx += cores[i].conns[j].total_tx_pkts;
      totalp_rx += cores[i].conns[j].total_rx_pkts;
    }
  }

  printf("Cores               : %d\n", ncores);
  printf("Connections/core    : %d\n", nconns);
  printf("Pending/core        : %d\n", mpending);
  printf("Wait before start   : %.3f s\n", cwait);
  printf("TX packets          : %llu\n", (unsigned long long) totalp_tx);
  printf("RX packets          : %llu\n", (unsigned long long) totalp_rx);
  printf("Avg Load            : %lld rps %.2f Mb/s\n",
      (unsigned long long) (totalp_tx / (__u64) duration),
      ((double) totalp_tx * msize * 8.0 / 1000000.0) / duration);
  printf("Avg Throughput      : %lld rps %.2f Mb/s\n",
      (unsigned long long) (totalp_rx / (__u64) duration),
      ((double) totalp_rx * msize * 8.0 / 1000000.0) / duration);
  printf("RTT percentiles (us): p50=%llu  p99=%llu  p99.9=%llu\n",
      (unsigned long long) p50,
      (unsigned long long) p99,
      (unsigned long long) p999);

  for (int i = 0; i < ncores; i++)
  {
    if (cores[i].conns != NULL)
    {
      for (int j = 0; j < nconns; j++)
      {
        if (cores[i].conns[j].fd >= 0)
          tcp_close(cores[i].tcp_ctx, cores[i].conns[j].fd);
        free(cores[i].conns[j].txbuf);
        free(cores[i].conns[j].rxbuf);
      }
    }
    free(cores[i].conns);
  }

  free(core_list);
  free(cores);
  free(threads);
  hist_cleanup(&rtt_hist);
  return 0;
}
