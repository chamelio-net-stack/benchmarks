/* Reports per-second throughput (Mbits/s and packets/s) and cumulative
 * latency percentiles (p50, p99, p99.9) in microseconds.
 *
 * Usage:
 *   ./tcp_linux_client <src_ip> <src_port> <rem_ip> <rem_port>
 *     [--msize N] [--duration S] [--rate R] [--ncores N]
 *     [--nconns N] [--mpending N] [--cwait S]
 *
 *   --msize Total TCP payload bytes.
 *   --duration Duration in seconds to run benchmark
 *   --rate Per-connection send rate in megabits per second
 *   --ncores Number of cores to use
 *   --nconns Number of TCP connections per core
 *   --mpending Max number of pending connects per core
 *   --cwait Time in seconds to wait before starting traffic
 */

#define _GNU_SOURCE
#include <errno.h>
#include <stdlib.h>
#include <assert.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <stdio.h>
#include <time.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <poll.h>
#include <stdarg.h>
#include <linux/types.h>
#include "../utils/clock.h"
#include "../utils/hist.h"

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
  int logged_connect;
};

/* Per-core context */
struct core {
  int id;
  int epoll_fd;
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

static inline int parse_args(int argc, char **argv)
{
  if (argc < 5)
  {
    fprintf(stderr,
        "Usage: %s <src_ip> <src_port> <rem_ip> <rem_port> [--msize N]"
        " [--duration S] [--rate R] [--ncores N] [--nconns N]"
        " [--mpending N] [--cwait S]\n",
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

static void set_nonblock(int fd)
{
  int fl = fcntl(fd, F_GETFL, 0);
  if (fl < 0)
  {
    perror("fcntl(F_GETFL)");
    exit(EXIT_FAILURE);
  }

  if (fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0)
  {
    perror("fcntl(F_SETFL O_NONBLOCK)");
    exit(EXIT_FAILURE);
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
  cn->logged_connect = 0;
}

static void client_log(struct core *c, struct conn *cn, const char *tag,
    const char *fmt, ...)
{
  va_list ap;

  fprintf(stderr, "[%s] core=%d", tag, c->id);
  if (cn != NULL)
    fprintf(stderr, " fd=%d", cn->fd);

  fputs(": ", stderr);
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  fflush(stderr);
}

static const char *epoll_flags_str(uint32_t events, char *buf, size_t len)
{
  int off = 0;

  if (len == 0)
    return "";

  if (events & EPOLLIN)
    off += snprintf(buf + off, len - (size_t) off, "%sIN", off == 0 ? "" : "|");
  if (events & EPOLLOUT)
    off += snprintf(buf + off, len - (size_t) off, "%sOUT", off == 0 ? "" : "|");
  if (events & EPOLLERR)
    off += snprintf(buf + off, len - (size_t) off, "%sERR", off == 0 ? "" : "|");
  if (events & EPOLLHUP)
    off += snprintf(buf + off, len - (size_t) off, "%sHUP", off == 0 ? "" : "|");
  if (events & EPOLLRDHUP)
    off += snprintf(buf + off, len - (size_t) off, "%sRDHUP", off == 0 ? "" : "|");
  if (events & EPOLLET)
    off += snprintf(buf + off, len - (size_t) off, "%sET", off == 0 ? "" : "|");
  if (off == 0)
    snprintf(buf, len, "0");

  return buf;
}

static inline void core_drop_conn(struct core *c, struct conn *cn,
    const char *op, int err)
{
  if (cn->fd >= 0)
  {
    epoll_ctl(c->epoll_fd, EPOLL_CTL_DEL, cn->fd, NULL);
    close(cn->fd);
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

  client_log(c, cn, "CLOSE",
      "%s: %s (connected=%d tx=%llu rx=%llu tx_off=%zu rx_off=%zu)",
      op, err == 0 ? "connection closed" : strerror(err),
      cn->connected ? 1 : 0,
      (unsigned long long) cn->total_tx_pkts,
      (unsigned long long) cn->total_rx_pkts,
      cn->tx_off, cn->rx_off);

  conn_reset(cn);
}

static inline int core_start_conn(struct core *c, struct conn *cn)
{
  int fd, ret, err;

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
  {
    perror("socket");
    return -1;
  }
  set_nonblock(fd);

  if (should_bind_source())
  {
    struct sockaddr_in bind_addr = src_addr;

    bind_addr.sin_port = htons((__u16) core_src_port(c->id));
    ret = bind(fd, (struct sockaddr *) &bind_addr, sizeof(bind_addr));
    if (ret < 0)
    {
      err = errno;
      close(fd);
      errno = err;
      return -1;
    }
  }

  cn->fd = fd;
  cn->active = true;
  cn->connected = false;
  cn->tx_off = 0;
  cn->rx_off = 0;
  cn->tokens = 0.0;
  cn->logged_connect = 0;

  ret = connect(fd, (struct sockaddr *) &c->dst_addr, c->dst_len);
  if (ret == 0)
  {
    cn->connected = true;
    c->established_conns++;
    client_log(c, cn, "CONNECT", "connected immediately");
    /* Add to epoll for I/O */
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLET;
    ev.data.fd = fd;
    if (epoll_ctl(c->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0)
    {
      err = errno;
      close(fd);
      conn_reset(cn);
      errno = err;
      return -1;
    }
    return 0;
  }

  if (errno == EINPROGRESS)
  {
    c->pending_conns++;
    client_log(c, cn, "CONNECT", "connect in progress (pending=%d)",
        c->pending_conns);
    /* Add to epoll for connection completion */
    struct epoll_event ev;
    ev.events = EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLET;
    ev.data.fd = fd;
    if (epoll_ctl(c->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0)
    {
      err = errno;
      close(fd);
      conn_reset(cn);
      errno = err;
      return -1;
    }
    return 0;
  }

  err = errno;
  close(fd);
  conn_reset(cn);
  errno = err;
  return -1;
}



static inline void core_connect_all(struct core *c)
{
  struct epoll_event events[nconns];
  int nev;

  client_log(c, NULL, "CONNECT", "starting connection phase target=%d pending_limit=%d",
      nconns, mpending);

  /* Try to initiate all connects */
  for (int i = 0; i < nconns; i++)
  {
    if (core_start_conn(c, &c->conns[i]) != 0)
    {
      fprintf(stderr, "core %d: connect setup failed: %s\n",
          c->id, strerror(errno));
    }
  }

  /* Wait for all connections to complete (using epoll) */
  while (c->established_conns < nconns)
  {
    nev = epoll_wait(c->epoll_fd, events, nconns, 1);
    if (nev < 0)
    {
      if (errno == EINTR)
        continue;
      perror("epoll_wait");
      pthread_exit((void *) (intptr_t) -1);
    }

    /* Process connection completion events */
    for (int i = 0; i < nev; i++)
    {
      int fd = events[i].data.fd;
      uint32_t ev = events[i].events;
      char evbuf[64];

      struct conn *cn = NULL;
      for (int j = 0; j < nconns; j++)
      {
        if (c->conns[j].active && c->conns[j].fd == fd)
        {
          cn = &c->conns[j];
          break;
        }
      }

      if (cn == NULL)
        continue;

      client_log(c, cn, "EPOLL-CONNECT", "events=%s connected=%d pending=%d established=%d",
          epoll_flags_str(ev, evbuf, sizeof(evbuf)),
          cn->connected ? 1 : 0, c->pending_conns, c->established_conns);

      /* Check for errors */
      if (ev & (EPOLLERR | EPOLLHUP))
      {
        int err = 0;
        socklen_t len = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        core_drop_conn(c, cn, "connect failed", err);
        continue;
      }

      /* Connection successful */
      if (ev & EPOLLOUT)
      {
        int err = 0;
        socklen_t len = sizeof(err);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0)
          err = errno;

        if (err == 0 && !cn->connected)
        {
          cn->connected = true;
          if (c->pending_conns > 0)
            c->pending_conns--;
          c->established_conns++;
          client_log(c, cn, "CONNECT", "connection established (established=%d pending=%d)",
              c->established_conns, c->pending_conns);

          /* Modify epoll mask for established connection (add EPOLLIN) */
          struct epoll_event ev_mod;
          ev_mod.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLET;
          ev_mod.data.fd = fd;
          epoll_ctl(c->epoll_fd, EPOLL_CTL_MOD, fd, &ev_mod);
        }
        else if (err != 0)
        {
          core_drop_conn(c, cn, "connect failed", err);
        }
      }
    }

    /* Initiate more connects if we have room */
    for (int i = 0; i < nconns; i++)
    {
      if (!c->conns[i].active && c->pending_conns < mpending)
      {
        if (core_start_conn(c, &c->conns[i]) != 0)
        {
          fprintf(stderr, "core %d: connect setup failed: %s\n",
              c->id, strerror(errno));
        }
      }
    }
  }
}

static inline void core_init(struct core *c)
{
  c->conns = (struct conn *) calloc((size_t) nconns, sizeof(struct conn));
  if (c->conns == NULL)
  {
    perror("calloc core state");
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

  c->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (c->epoll_fd < 0)
  {
    perror("epoll_create1");
    pthread_exit((void *) (intptr_t) -1);
  }

  c->txb_load_interval = 0;
  c->txp_load_interval = 0;
  c->txb_tp_interval = 0;
  c->txp_tp_interval = 0;
  c->established_conns = 0;
  c->pending_conns = 0;
  c->last_tsc = 0;
}

static inline void core_tx(struct core *c)
{
  int ntx;

  for (int i = 0; i < nconns; i++)
  {
    struct conn *cn = &c->conns[i];

    if (!cn->active || !cn->connected)
      continue;

    while (cn->tokens >= (double) msize)
    {
      struct payload_hdr *ph;

      if (!cn->active || !cn->connected)
        break;

      if (cn->tx_off == 0)
      {
        ph = (struct payload_hdr *) cn->txbuf;
        ph->tsc = util_rdtsc();
      }

      ntx = send(cn->fd, cn->txbuf + cn->tx_off, msize - cn->tx_off, 0);
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
  }
}

static inline void core_rx(struct core *c)
{
  int nrx;

  for (int i = 0; i < nconns; i++)
  {
    struct conn *cn = &c->conns[i];

    if (!cn->active || !cn->connected)
      continue;

    while (1)
    {
      struct payload_hdr *ph;
      __u64 rtt_us;

      nrx = recv(cn->fd, cn->rxbuf + cn->rx_off, msize - cn->rx_off, 0);
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
  }
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
  struct epoll_event events[nconns];
  int nev;

  c = (struct core *) arg;
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

    elapsed = us_since_tsc(start_tsc);
    if (elapsed >= (__u64) duration * 1000000ULL)
      break;

    tsc_now = util_rdtsc();
    delta_cycles = (double) (tsc_now - c->last_tsc);
    delta_us = delta_cycles / clock_tsc_per_us();
    assert(delta_us >= 0.0);
    c->last_tsc = tsc_now;

    /* Update tokens for all active connections */
    for (int i = 0; i < nconns; i++)
    {
      struct conn *cn = &c->conns[i];

      if (!cn->active || !cn->connected)
        continue;

      active++;
      cn->tokens += delta_us * bytes_per_us;
      if (cn->tokens > max_tokens)
        cn->tokens = max_tokens;
    }

    /* Non-blocking epoll wait (0ms timeout) */
    nev = epoll_wait(c->epoll_fd, events, nconns, 0);
    if (nev < 0)
    {
      if (errno != EINTR)
        perror("epoll_wait");
      continue;
    }

    /* Process epoll events for specific connections */
    for (int i = 0; i < nev; i++)
    {
      int fd = events[i].data.fd;
      uint32_t ev = events[i].events;
      char evbuf[64];

      /* Find connection by fd */
      struct conn *cn = NULL;
      for (int j = 0; j < nconns; j++)
      {
        if (c->conns[j].active && c->conns[j].fd == fd)
        {
          cn = &c->conns[j];
          break;
        }
      }

      if (cn == NULL)
        continue;

      /* Handle errors first */
      if (ev & (EPOLLERR | EPOLLHUP))
      {
        int err = 0;
        socklen_t len = sizeof(err);

        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0)
          err = errno;
        client_log(c, cn, "EPOLL", "events=%s so_error=%s",
            epoll_flags_str(ev, evbuf, sizeof(evbuf)),
            err == 0 ? "connection closed" : strerror(err));
        core_drop_conn(c, cn, "epoll error", err);
        continue;
      }

      if ((ev & (EPOLLIN | EPOLLOUT)) && !cn->logged_connect)
      {
        cn->logged_connect = 1;
        client_log(c, cn, "EPOLL", "first data-path event=%s tokens=%.0f",
            epoll_flags_str(ev, evbuf, sizeof(evbuf)), cn->tokens);
      }
    }

    /* Try TX on all connections with sufficient tokens (outside event loop) */
    if (active > 0)
    {
      core_rx(c);
      core_tx(c);
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

  init_client();
  prepare_cores();
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
          close(cores[i].conns[j].fd);
        free(cores[i].conns[j].txbuf);
        free(cores[i].conns[j].rxbuf);
      }
    }
    if (cores[i].epoll_fd >= 0)
      close(cores[i].epoll_fd);
    free(cores[i].conns);
  }

  free(cores);
  free(threads);
  hist_cleanup(&rtt_hist);
  return 0;
}
