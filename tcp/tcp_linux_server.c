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
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <fcntl.h>
#include <linux/types.h>
#include "../utils/clock.h"

struct conn {
  int fd;
  int active;
  size_t nrx;
  size_t ntx;
  __u8 *buf;
};

struct core {
  int id;
  int listen_fd;
  int nconns;
  struct conn *conns;
  __u64 rxb_interval;
  __u64 rxp_interval;
  __u64 txb_interval;
  __u64 txp_interval;
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
}

static inline void core_close_conn(struct core *c, struct conn *cn,
    const char *op, int err)
{
  fprintf(stderr, "core %d fd=%d: %s: %s\n",
      c->id, cn->fd, op,
      err == 0 ? "connection closed" : strerror(err));
  fflush(stderr);

  if (cn->fd >= 0)
    close(cn->fd);
  if (c->nconns > 0)
    c->nconns--;
  conn_reset(cn);
}

static inline int core_accept_conn(struct core *c)
{
  int fd;
  int opt = 1;

  if (c->nconns >= mconns)
    return 0;

  while (c->nconns < mconns)
  {
    struct conn *slot = NULL;

    for (int i = 0; i < mconns; i++)
    {
      if (!c->conns[i].active)
      {
        slot = &c->conns[i];
        break;
      }
    }

    if (slot == NULL)
      return 0;

    fd = accept4(c->listen_fd, NULL, NULL, SOCK_NONBLOCK);
    if (fd < 0)
    {
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
        return;

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
        break;

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
    if (c->conns[i].buf == NULL)
    {
      fprintf(stderr, "failed to malloc conn buf\n");
      abort();
    }
    memset(c->conns[i].buf, 0, bsize);
  }

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
  c->nconns = 0;
  c->rxb_interval = 0;
  c->rxp_interval = 0;
  c->txb_interval = 0;
  c->txp_interval = 0;
}

static inline void * core_thread(void *arg)
{
  struct core *c = (struct core *) arg;

  core_init(c);

  if (c->id == 0)
    pthread_barrier_wait(&start_barrier);

  while (1)
  {
    core_accept_conn(c);

    for (int i = 0; i < mconns; i++)
    {
      if (!c->conns[i].active)
        continue;
      core_rx_conn(c, &c->conns[i]);
      if (!c->conns[i].active)
        continue;
      core_tx_conn(c, &c->conns[i]);
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
