/* Usage:
 *   ./tcp_linux_server <bind_ip> <port>
 *     [--msize N] [--ncores N] [--mconns N] [--buf-size N]
 *
 * Example:
 *   ./tcp_linux_server 0.0.0.0 9000 --msize 64 --ncores 2 --mconns 32 --buf-size 4096
 *
 *   --msize Size of expected message
 *   --ncores Number of cores to use
 *   --mconns Max number of accepted connections per core
 *   --buf-size Per-connection buffer size
 */

#define _GNU_SOURCE
#include <errno.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdarg.h>
#include <linux/types.h>
#include "../utils/clock.h"

enum epoll_item_type {
  EPOLL_ITEM_LISTEN,
  EPOLL_ITEM_CONN,
};

struct epoll_item {
  enum epoll_item_type type;
  void *ptr;
};

struct conn {
  int fd;
  int active;
  size_t nrx;
  size_t ntx;
  __u8 *buf;
  int next_free;
  struct epoll_item ev_item;
};

struct core {
  int id;
  int listen_fd;
  int epoll_fd;
  int nconns;
  int free_head;
  struct conn *conns;
  __u64 rxb_interval;
  __u64 rxp_interval;
  __u64 txb_interval;
  __u64 txp_interval;
  struct epoll_item listen_item;
};

/* Default arg values */
size_t msize = 64;
size_t bsize = 4096;
int ncores = 1;
int mconns = 1;

__u64 rxb_interval_total, rxp_interval_total;
__u64 txb_interval_total, txp_interval_total;
__u64 t_start_ns;
__u64 t_last_report_ns, t_next_report_ns;

int port;
const char *server_ip;
static pthread_barrier_t start_barrier;
static pthread_t *threads = NULL;
static struct core *cores = NULL;
struct sockaddr_in src_addr;
static int debug_enabled = 0;

static inline void debug_log_conn(struct core *c, struct conn *cn,
    const char *fmt, ...)
{
  va_list ap;

  if (!debug_enabled)
    return;

  fprintf(stderr, "[tcp_linux_server] core=%d fd=%d conn=%td t=%llu ",
      c != NULL ? c->id : -1,
      cn != NULL ? cn->fd : -1,
      (c != NULL && cn != NULL && c->conns != NULL) ? (ptrdiff_t) (cn - c->conns) : -1,
      (unsigned long long) now_ns());
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  fflush(stderr);
}

static inline void debug_log_core(struct core *c, const char *fmt, ...)
{
  va_list ap;

  if (!debug_enabled)
    return;

  fprintf(stderr, "[tcp_linux_server] core=%d t=%llu ",
      c != NULL ? c->id : -1,
      (unsigned long long) now_ns());
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  fflush(stderr);
}

static inline void debug_init(void)
{
  const char *env = getenv("TCP_BENCH_DEBUG");

  if (env != NULL && strcmp(env, "0") != 0)
    debug_enabled = 1;
}

static inline void core_rx_conn(struct core *c, struct conn *cn);
static inline void core_tx_conn(struct core *c, struct conn *cn);
static inline void core_update_events(struct core *c, struct conn *cn);
static inline void core_push_free_conn(struct core *c, struct conn *cn);
static inline struct conn *core_pop_free_conn(struct core *c);

static inline int parse_args(int argc, char **argv)
{
  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <bind_ip> <port>"
        " [--msize N] [--ncores N] [--mconns N] [--buf-size N]\n", argv[0]);
    return -1;
  }

  server_ip = argv[1];
  port = atoi(argv[2]);
  if (port <= 0 || port > 65535)
  {
    fprintf(stderr, "Invalid port\n");
    return -1;
  }

  for (int i = 3; i < argc; i++)
  {
    if (strcmp(argv[i], "--msize") == 0 && i + 1 < argc)
    {
      msize = (size_t) strtoul(argv[++i], NULL, 10);
    }
    else if (strcmp(argv[i], "--ncores") == 0 && i + 1 < argc)
    {
      ncores = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--mconns") == 0 && i + 1 < argc)
    {
      mconns = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--buf-size") == 0 && i + 1 < argc)
    {
      bsize = (size_t) strtoul(argv[++i], NULL, 10);
    }
    else
    {
      fprintf(stderr, "Unknown arg: %s\n", argv[i]);
      return -1;
    }
  }

  if (ncores <= 0)
    ncores = 1;
  if (mconns <= 0)
    mconns = 1;
  if (bsize < msize)
  {
    fprintf(stderr, "buf-size must be >= msize\n");
    return -1;
  }

  return 0;
}

static inline void print_stats(__u64 now)
{
  double interval_s, rps_rx, mbps_rx, rps_tx, mbps_tx;
  __u64 bytes_rx = 0, pkts_rx = 0, bytes_tx = 0, pkts_tx = 0;

  interval_s = (double) (t_next_report_ns - t_last_report_ns) / 1e9;
  if (interval_s <= 0.0)
    interval_s = 1.0;

  for (int i = 0; i < ncores; i++)
  {
    bytes_rx += __sync_fetch_and_add(&cores[i].rxb_interval, 0);
    pkts_rx += __sync_fetch_and_add(&cores[i].rxp_interval, 0);
    bytes_tx += __sync_fetch_and_add(&cores[i].txb_interval, 0);
    pkts_tx += __sync_fetch_and_add(&cores[i].txp_interval, 0);
    __sync_fetch_and_and(&cores[i].rxb_interval, 0);
    __sync_fetch_and_and(&cores[i].rxp_interval, 0);
    __sync_fetch_and_and(&cores[i].txb_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_interval, 0);
  }

  rxb_interval_total = bytes_rx;
  rxp_interval_total = pkts_rx;
  txb_interval_total = bytes_tx;
  txp_interval_total = pkts_tx;

  rps_rx = (double) rxp_interval_total / interval_s;
  mbps_rx = ((double) rxb_interval_total * 8.0 / 1e6) / interval_s;
  rps_tx = (double) txp_interval_total / interval_s;
  mbps_tx = ((double) txb_interval_total * 8.0 / 1e6) / interval_s;

  printf("[ %3lus ] rx=%lld rps | rx=%.2f Mb/s | tx=%lld rps | tx=%.2f Mb/s \n",
      (unsigned long) ((t_next_report_ns - t_start_ns) / 1000000000ULL),
      (unsigned long long) rps_rx, mbps_rx,
      (unsigned long long) rps_tx, mbps_tx);
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
  cn->active = 0;
  cn->nrx = 0;
  cn->ntx = 0;
  cn->next_free = -1;
}

static inline void core_push_free_conn(struct core *c, struct conn *cn)
{
  cn->next_free = c->free_head;
  c->free_head = (int) (cn - c->conns);
  debug_log_conn(c, cn, "free push free_head=%d", c->free_head);
}

static inline struct conn *core_pop_free_conn(struct core *c)
{
  int idx;
  struct conn *cn;

  if (c->free_head < 0)
    return NULL;

  idx = c->free_head;
  cn = &c->conns[idx];
  c->free_head = cn->next_free;
  cn->next_free = -1;
  debug_log_conn(c, cn, "free pop new_free_head=%d", c->free_head);
  return cn;
}

static inline void core_close_conn(struct core *c, struct conn *cn,
    const char *op, int err)
{
  fprintf(stderr, "core %d fd=%d: %s: %s\n",
      c->id, cn->fd, op,
      err == 0 ? "connection closed" : strerror(err));
  fflush(stderr);

  if (cn->fd >= 0)
  {
    epoll_ctl(c->epoll_fd, EPOLL_CTL_DEL, cn->fd, NULL);
    close(cn->fd);
  }
  if (c->nconns > 0)
    c->nconns--;
  debug_log_conn(c, cn, "close active=%d nrx=%zu ntx=%zu remaining=%d",
      cn->active, cn->nrx, cn->ntx, c->nconns);
  conn_reset(cn);
  core_push_free_conn(c, cn);
}

static inline int core_accept_conn(struct core *c)
{
  int fd;
  int opt = 1;
  struct conn *slot;

  if (c->nconns >= mconns)
    return 0;

  while (c->nconns < mconns)
  {
    slot = core_pop_free_conn(c);
    if (slot == NULL)
      return 0;

    fd = accept4(c->listen_fd, NULL, NULL, SOCK_NONBLOCK);
    if (fd < 0)
    {
      core_push_free_conn(c, slot);
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return 0;

      fprintf(stderr, "core %d: accept failed: %s\n",
          c->id, strerror(errno));
      fflush(stderr);
      return -1;
    }

    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
    slot->fd = fd;
    slot->active = 1;
    slot->nrx = 0;
    slot->ntx = 0;
    c->nconns++;
    debug_log_conn(c, slot, "accepted nconns=%d", c->nconns);

    /* Start with read interest; writable interest is armed only while echo data is pending. */
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    ev.data.ptr = &slot->ev_item;
    if (epoll_ctl(c->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0)
    {
      fprintf(stderr, "core %d: epoll_ctl add failed: %s\n",
          c->id, strerror(errno));
      fflush(stderr);
      close(fd);
      if (c->nconns > 0)
        c->nconns--;
      conn_reset(slot);
      return -1;
    }

    /*
     * In ET mode the peer may have already queued data before this accepted
     * socket was added to epoll, so try to drain it immediately once.
     */
    core_rx_conn(c, slot);
    if (slot->active)
      core_tx_conn(c, slot);
  }

  return 0;
}

static inline void core_rx_conn(struct core *c, struct conn *cn)
{
  while (cn->nrx < bsize)
  {
    int nrx;
    size_t before_msgs, after_msgs, delta_msgs;

    nrx = recv(cn->fd, cn->buf + cn->nrx, bsize - cn->nrx, 0);
    if (nrx < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
        debug_log_conn(c, cn, "recv would block nrx=%zu ntx=%zu",
            cn->nrx, cn->ntx);
        return;
      }

      core_close_conn(c, cn, "recv failed", errno);
      return;
    }

    if (nrx == 0)
    {
      core_close_conn(c, cn, "recv failed", 0);
      return;
    }

    before_msgs = cn->nrx / msize;
    cn->nrx += (size_t) nrx;
    after_msgs = cn->nrx / msize;
    delta_msgs = after_msgs - before_msgs;
    if (delta_msgs > 0)
    {
      __sync_fetch_and_add(&c->rxp_interval, delta_msgs);
      __sync_fetch_and_add(&c->rxb_interval, delta_msgs * msize);
    }
    debug_log_conn(c, cn, "recv progress nrx=%d total_buf=%zu delta_msgs=%zu",
        nrx, cn->nrx, delta_msgs);
  }
}

static inline void core_tx_conn(struct core *c, struct conn *cn)
{
  size_t ready = cn->nrx - (cn->nrx % msize);

  while (cn->ntx < ready)
  {
    int ntx;
    size_t before_msgs, after_msgs, delta_msgs;

    ntx = send(cn->fd, cn->buf + cn->ntx, ready - cn->ntx, 0);
    if (ntx < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
        debug_log_conn(c, cn, "send would block ready=%zu ntx=%zu",
            ready, cn->ntx);
        break;
      }

      core_close_conn(c, cn, "send failed", errno);
      return;
    }

    if (ntx == 0)
    {
      core_close_conn(c, cn, "send failed", 0);
      return;
    }

    before_msgs = cn->ntx / msize;
    cn->ntx += (size_t) ntx;
    after_msgs = cn->ntx / msize;
    delta_msgs = after_msgs - before_msgs;
    if (delta_msgs > 0)
    {
      __sync_fetch_and_add(&c->txp_interval, delta_msgs);
      __sync_fetch_and_add(&c->txb_interval, delta_msgs * msize);
    }
    debug_log_conn(c, cn, "send progress ntx=%d sent_off=%zu delta_msgs=%zu",
        ntx, cn->ntx, delta_msgs);
  }

  if (cn->ntx == ready && ready > 0)
  {
    size_t left = cn->nrx - ready;

    if (left > 0)
      memmove(cn->buf, cn->buf + ready, left);

    cn->nrx = left;
    cn->ntx = 0;
  }
}

static inline void core_update_events(struct core *c, struct conn *cn)
{
  struct epoll_event ev;
  size_t ready;
  uint32_t events;

  if (!cn->active || cn->fd < 0)
    return;

  ready = cn->nrx - (cn->nrx % msize);
  events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
  if (ready > cn->ntx)
    events |= EPOLLOUT;

  ev.events = events;
  ev.data.ptr = &cn->ev_item;
  debug_log_conn(c, cn, "epoll mod events=0x%x ready=%zu ntx=%zu",
      events, ready, cn->ntx);
  if (epoll_ctl(c->epoll_fd, EPOLL_CTL_MOD, cn->fd, &ev) < 0)
    core_close_conn(c, cn, "epoll_ctl mod failed", errno);
}

static inline void core_init(struct core *c)
{
  int ret, opt, fd;

  c->conns = (struct conn *) calloc((size_t) mconns, sizeof(struct conn));
  if (c->conns == NULL)
  {
    fprintf(stderr, "failed to calloc conn array\n");
    abort();
  }

  for (int i = 0; i < mconns; i++)
  {
    conn_reset(&c->conns[i]);
    c->conns[i].buf = (__u8 *) malloc(bsize);
    c->conns[i].ev_item.type = EPOLL_ITEM_CONN;
    c->conns[i].ev_item.ptr = &c->conns[i];
    if (c->conns[i].buf == NULL)
    {
      fprintf(stderr, "failed to malloc conn buf\n");
      abort();
    }
    memset(c->conns[i].buf, 0, bsize);
  }
  c->free_head = -1;
  for (int i = mconns - 1; i >= 0; i--)
    core_push_free_conn(c, &c->conns[i]);

  if (c->id != 0)
    pthread_barrier_wait(&start_barrier);

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
  {
    perror("socket");
    abort();
  }
  set_nonblock(fd);

  opt = 1;
  ret = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
  if (ret < 0)
  {
    perror("setsockopt SO_REUSEPORT");
    abort();
  }

  ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  if (ret < 0)
  {
    perror("setsockopt SO_REUSEADDR");
    abort();
  }

  if (bind(fd, (struct sockaddr *) &src_addr, sizeof(src_addr)) < 0)
  {
    perror("bind");
    abort();
  }

  if (listen(fd, mconns) < 0)
  {
    perror("listen");
    abort();
  }

  c->listen_fd = fd;

  /* Create epoll and add listen socket */
  c->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (c->epoll_fd < 0)
  {
    perror("epoll_create1");
    abort();
  }

  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
  c->listen_item.type = EPOLL_ITEM_LISTEN;
  c->listen_item.ptr = c;
  ev.data.ptr = &c->listen_item;
  if (epoll_ctl(c->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0)
  {
    perror("epoll_ctl add listen");
    abort();
  }

  c->nconns = 0;
  c->rxb_interval = 0;
  c->rxp_interval = 0;
  c->txb_interval = 0;
  c->txp_interval = 0;
  debug_log_core(c, "core init listen_fd=%d epoll_fd=%d", c->listen_fd,
      c->epoll_fd);
}

static inline void * core_thread(void *arg)
{
  struct core *c = (struct core *) arg;
  struct epoll_event events[mconns + 1];
  int nev;

  core_init(c);

  if (c->id == 0)
    pthread_barrier_wait(&start_barrier);

  while (1)
  {
    /* Non-blocking epoll wait (0ms timeout) */
    nev = epoll_wait(c->epoll_fd, events, mconns + 1, 0);
    debug_log_core(c, "epoll_wait nev=%d nconns=%d", nev, c->nconns);

    if (nev < 0)
    {
      if (errno != EINTR)
      {
        perror("epoll_wait");
        pthread_exit((void *) (intptr_t) -1);
      }
      continue;
    }

    /* Process all events returned by epoll */
    for (int i = 0; i < nev; i++)
    {
      uint32_t ev = events[i].events;
      struct epoll_item *item = (struct epoll_item *) events[i].data.ptr;

      if (item == NULL)
        continue;

      /* Accept on listen socket */
      if (item->type == EPOLL_ITEM_LISTEN)
      {
        debug_log_core(c, "listen event events=0x%x", ev);
        if (ev & EPOLLIN)
        {
          while (1)
          {
            if (core_accept_conn(c) != 0)
              break;
          }
        }
        if (ev & (EPOLLERR | EPOLLHUP))
        {
          fprintf(stderr, "core %d: listen socket error\n", c->id);
          fflush(stderr);
        }
        continue;
      }

      struct conn *cn = (struct conn *) item->ptr;
      if (cn == NULL || !cn->active)
        continue;

      debug_log_conn(c, cn, "conn event events=0x%x nrx=%zu ntx=%zu",
          ev, cn->nrx, cn->ntx);

      /* Handle errors first */
      if (ev & (EPOLLERR | EPOLLHUP))
      {
        core_close_conn(c, cn, "epoll error", 0);
        continue;
      }

      if (ev & EPOLLIN)
        core_rx_conn(c, cn);

      if (cn->active)
        core_tx_conn(c, cn);

      if (cn->active)
        core_update_events(c, cn);
    }

    /* Try to accept if we have room and got no events */
    if (nev == 0)
    {
      core_accept_conn(c);
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
    cores[i].listen_fd = -1;
    cores[i].epoll_fd = -1;

    if (inet_pton(AF_INET, server_ip, &src_addr.sin_addr) != 1)
    {
      fprintf(stderr, "Invalid server_ip\n");
      abort();
    }
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
      abort();
    }
  }
}

static inline void init_server(void)
{
  memset(&src_addr, 0, sizeof(src_addr));
  src_addr.sin_family = AF_INET;
  src_addr.sin_port = htons((__u16) port);
  if (inet_pton(AF_INET, server_ip, &src_addr.sin_addr) != 1)
  {
    fprintf(stderr, "Invalid bind_ip\n");
    abort();
  }

  cores = (struct core *) calloc((size_t) ncores, sizeof(struct core));
  threads = (pthread_t *) calloc((size_t) ncores, sizeof(pthread_t));
  debug_init();
  if (!cores || !threads)
  {
    perror("calloc cores/threads");
    abort();
  }

  if (pthread_barrier_init(&start_barrier, NULL,
      (unsigned) ncores + 1) != 0)
  {
    perror("pthread_barrier_init");
    abort();
  }

  rxb_interval_total = 0;
  rxp_interval_total = 0;
  txb_interval_total = 0;
  txp_interval_total = 0;
}

int main(int argc, char **argv)
{
  __u64 now;

  if (parse_args(argc, argv) != 0)
  {
    fprintf(stderr, "failed to parse args\n");
    abort();
  }

  init_server();
  if (debug_enabled)
  {
    fprintf(stderr,
        "[tcp_linux_server] debug enabled bind=%s:%d msize=%zu bsize=%zu ncores=%d mconns=%d\n",
        server_ip, port, msize, bsize, ncores, mconns);
    fflush(stderr);
  }
  prepare_cores();
  start_cores();

  printf("Initialized %d core(s). Waiting for barrier\n", ncores);
  fflush(stdout);
  pthread_barrier_wait(&start_barrier);

  printf("TCP echo server listening on %s:%d (msize=%zu) (bsize=%zu) (ncores=%d) (mconns=%d)\n",
      server_ip, port, msize, bsize, ncores, mconns);
  fflush(stdout);

  t_start_ns = now_ns();
  t_last_report_ns = t_start_ns;
  t_next_report_ns = t_start_ns + 1000000000ULL;

  while (1)
  {
    now = now_ns();
    if (now >= t_next_report_ns)
      print_stats(now);

    {
      struct timespec ts = { .tv_sec = 0, .tv_nsec = 2000000 };
      nanosleep(&ts, NULL);
    }
  }

  return 0;
}
