/* Usage:
 *   ./tcp_linux_server <bind_ip> <port> [--msize N] [--ncores N] [--buf-size N]
 *
 * Example:
 *   ./tcp_linux_server 0.0.0.0 9000 --msize 64 --ncores 2 --buf-size 4096
 * 
 *   --msize Size of expected message
 *   --ncores Number of cores to use
 *   --buf-size Size of transmit buffer
 */

#define _GNU_SOURCE
#include <errno.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <fcntl.h>
#include <linux/types.h>
#include "../utils/clock.h"
#ifdef ETRAN
#include "etran_compat.h"
#endif

struct conn {
  int id;
  int fd;
  int tx_remain;
  int rx_remain;
  int nrx;
  int ntx;
  __u8 *buf;
};

struct core {
  int id;
  int listen_fd;
  int ep_fd;
  struct epoll_event *evs;
  int nconns;
  struct conn *conns;
  __u64 rxb_interval;
  __u64 rxp_interval;
  __u64 txb_interval;
  __u64 txp_interval;
  struct sockaddr_in dst_addr;
  socklen_t dst_len;
};

/* Constants */
__u32 listen_backlog = 512;
__u32 max_events = 128;

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
  int i;

  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <bind_ip> <port>" 
        " [--msize N] [--ncores N] [--buf-size]\n", argv[0]);
    return -1;
  }

  server_ip = argv[1];
  port = atoi(argv[2]);
  if (port <= 0 || port > 65535)
  {
    fprintf(stderr, "Invalid port\n");
    return -1;
  }

  for (i = 3; i < argc; i++)
  {
    if (strcmp(argv[i], "--msize") == 0 && i + 1 < argc)
    {
      msize = (size_t)strtoul(argv[++i], NULL, 10);
    }
    else if (strcmp(argv[i], "--ncores") == 0 && i + 1 < argc)
    {
      ncores = (size_t)strtoul(argv[++i], NULL, 10);
    }
    else if (strcmp(argv[i], "--mconns") == 0 && i + 1 < argc)
    {
      mconns = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--buf-size") == 0 && i + 1 < argc)
    {
      bsize = (size_t)strtoul(argv[++i], NULL, 10);
    }
    else
    {
      fprintf(stderr, "Unknown arg: %s\n", argv[i]);
      return -1;
    }
  }

  return 0;
}

static inline void print_stats(__u64 now)
{
  double interval_s, rps_rx, mbps_rx, rps_tx, mbps_tx;
  __u64 bytes_rx = 0, pkts_rx = 0, bytes_tx = 0, pkts_tx = 0;

  interval_s = (double) (t_next_report_ns - t_last_report_ns) / 1e9;
  if (interval_s <= 0.0) interval_s = 1.0;

  /* Aggregate per-core and reset per-core intervals */
  for (int i = 0; i < ncores; i++)
  {
    bytes_rx += __sync_fetch_and_add(&cores[i].rxb_interval, 0);
    pkts_rx  += __sync_fetch_and_add(&cores[i].rxp_interval, 0);
    bytes_tx += __sync_fetch_and_add(&cores[i].txb_interval, 0);
    pkts_tx  += __sync_fetch_and_add(&cores[i].txp_interval, 0);
    fprintf(stderr, "core=%d bytes_rx=%.2f Mb/s bytes_tx=%.2f Mb/s\n", i, (double) cores[i].rxb_interval * 8.0 / 1e6, (double) cores[i].txb_interval * 8.0 / 1e6);
    __sync_fetch_and_and(&cores[i].rxb_interval, 0);
    __sync_fetch_and_and(&cores[i].rxp_interval, 0);
    __sync_fetch_and_and(&cores[i].txb_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_interval, 0);
  }
  
  rxb_interval_total = bytes_rx;
  rxp_interval_total  = pkts_rx;
  txb_interval_total = bytes_tx;
  txp_interval_total  = pkts_tx;

  rps_rx  = (double) rxp_interval_total / interval_s;
  mbps_rx = ((double) rxb_interval_total * 8.0 / 1e6) / interval_s;
  rps_tx  = (double) txp_interval_total / interval_s;
  mbps_tx = ((double) txb_interval_total * 8.0 / 1e6) / interval_s;

  printf("[ %3lus ] rx=%lld rps | rx=%.2f Mb/s | tx=%lld rps | tx=%.2f Mb/s \n",
          (unsigned long)((t_next_report_ns - t_start_ns) / 1000000000ULL),
          (__u64) rps_rx, mbps_rx, (__u64) rps_tx, mbps_tx);
  fflush(stdout);

  t_last_report_ns  = t_next_report_ns;
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

#ifndef ETRAN
/* eTRAN sends each write directly and does not implement TCP_NODELAY. */
static void set_nonagle(int fd)
{
  int ret;
  int flag = 1;
  ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
  if (ret < 0)
  {
    perror("failed to set no nagle\n");
    abort();
  }
  
}
#endif

static inline void core_init(struct core *c)
{
  __u8 *buf;
  int ret, i, opt = 1;
  struct conn *con;
  struct epoll_event ev;
#ifdef ETRAN
  struct sockaddr_in listen_addr = src_addr;
  listen_addr.sin_port = htons((__u16) (port + c->id));
#endif
  
  /* Cores that are not core 0 wait here so that ARP can complete */
  if (c->id != 0)
    pthread_barrier_wait(&start_barrier);

  
  /* Open listening socket */
  c->listen_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (c->listen_fd < 0)
  {
    perror("failed to create listening socket\n");
    abort();
  }
  
  ret = setsockopt(c->listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
  if (ret < 0) 
  {
    perror("setsockopt SO_REUSEADDR");
    abort();
  }
  
  ret = setsockopt(c->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  if (ret < 0) 
  {
    perror("setsockopt SO_REUSEADDR");
    abort();
  }
  
  set_nonblock(c->listen_fd);
#ifdef ETRAN
  ret = bind(c->listen_fd, (struct sockaddr *)&listen_addr, sizeof(listen_addr));
#else
  ret = bind(c->listen_fd, (struct sockaddr *)&src_addr, sizeof(src_addr));
#endif
  if (ret < 0)
  {
    perror("bind");
    abort();
  }

  ret = listen(c->listen_fd, listen_backlog);
  if (ret < 0)
  {
    perror("listen failed\n");
    abort();
  }
  
  /* Prepare epoll */
  c->ep_fd = epoll_create(mconns);
  if (c->ep_fd < 0)
  {
    perror("epoll create failed\n");
    abort();
  }
  
  /* Add listening socket to epoll */
  ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
  ev.data.ptr = NULL;
  ret = epoll_ctl(c->ep_fd, EPOLL_CTL_ADD, c->listen_fd, &ev);
  if (ret < 0)
  {
    perror("epoll_ctl failed to add listen fd\n");
    abort();
  }
  
    c->conns = calloc(mconns, sizeof(struct conn));
  if (c->conns == NULL)
  {
    fprintf(stderr, "failed to allocate conns\n");
    abort();
  }
  
  /* Prepare connections */
  for (i = 0; i < mconns; i++)
  {
    con = &c->conns[i];
    
    buf = (__u8 *) malloc(bsize);
    if (buf == NULL)
    {
      fprintf(stderr, "failed to malloc tx buf");
      abort();
    }
    con->buf = buf;
    memset(con->buf, 0, bsize);
    
    con->fd = -1;
    con->id = i;
    con->nrx = 0;
    con->ntx = 0;
    con->tx_remain = msize;
    con->rx_remain = msize;
  }

  c->dst_len = sizeof(c->dst_addr);
  c->rxb_interval = 0;
  c->rxp_interval = 0;
  c->txb_interval = 0;
  c->txp_interval = 0;
}

static inline void conn_rx(struct core *c, struct conn *con)
{
  int nrx;
  
  while (con->nrx < bsize)
  {
    assert(con->rx_remain != 0);
    nrx = recvfrom(con->fd, con->buf + con->nrx, con->rx_remain, 
        0, (struct sockaddr *) &c->dst_addr, &c->dst_len);
    assert(nrx <= 0 || (nrx <= msize && nrx <= con->rx_remain));

    if (nrx <= 0)
      break;
    
    con->nrx += nrx;
    con->rx_remain -= nrx;
    __sync_fetch_and_add(&c->rxb_interval, nrx);
    if (con->rx_remain == 0)
    {
      con->rx_remain = msize;
      __sync_fetch_and_add(&c->rxp_interval, 1);
    }
  }
}

static inline void conn_tx(struct core *c, 
    struct conn *con, struct epoll_event *ev)
{
  int ntx, ret, tx_len;

  while (con->ntx < con->nrx)
  {
    assert(con->tx_remain != 0);
    tx_len = con->nrx - con->ntx;
    if (tx_len > con->tx_remain)
      tx_len = con->tx_remain;

    ntx = sendto(con->fd, con->buf + con->ntx, tx_len, 0,
        (struct sockaddr *) &c->dst_addr, c->dst_len);
    assert(ntx <= 0 || (ntx <= msize && ntx <= tx_len));
          
    if (ntx <= 0)
    {
      /* Trigger event if we didn't echo everything */
      ev->data.ptr = con;
      ev->events = EPOLLOUT | EPOLLIN | EPOLLERR | EPOLLHUP;
      ret = epoll_ctl(c->ep_fd, EPOLL_CTL_MOD, con->fd, ev);
      if (ret < 0)
      {
        perror("failed to modify conn event to trigger EPOLLOUT");
        abort();
      }
      return;
    }
      
    con->ntx += ntx;
    con->tx_remain -= ntx;
    __sync_fetch_and_add(&c->txb_interval, ntx);
    if (con->tx_remain == 0)
    {
      con->tx_remain = msize;
      __sync_fetch_and_add(&c->txp_interval, 1);
    }
  }
  
  /* We sent all the data so do not trigger EPOLLOUT anymore */
  ev->events = EPOLLIN | EPOLLERR | EPOLLHUP;
  ret = epoll_ctl(c->ep_fd, EPOLL_CTL_MOD, con->fd, ev);
  if (ret < 0)
  {
    perror("failed to modify conn event to stop triggering EPOLLOUT");
    abort();
  }
  
  con->ntx = 0;
  con->nrx = 0;
}

static inline void conn_accept(struct core *c, struct epoll_event *ev)
{
  int ret;
  struct conn *con;
  
  /* There was some error on the listening socket so abort */
  if ((ev->events != EPOLLIN)) 
  {
      fprintf(stderr, "error on listening socket\n");
      abort();
  }
  
  if (c->nconns == mconns)
  {
    fprintf(stderr, "already accepted max connections\n");
    abort();
  }
  
  con = &c->conns[c->nconns];
  con->fd = accept(c->listen_fd, NULL, NULL);
  if (con->fd < 0)
  {
    fprintf(stderr, "accept failed\n");
    abort();
  }
  
  set_nonblock(con->fd);
#ifndef ETRAN
  set_nonagle(con->fd);
#endif
  
  /* Add conn to epoll */
  ev->data.ptr = con;
  ev->events = EPOLLIN | EPOLLERR | EPOLLHUP;
  ret = epoll_ctl(c->ep_fd, EPOLL_CTL_ADD, con->fd, ev);
  if (ret < 0)
  {
    perror("epoll ctl failed to add conn\n");
    abort();
  }
  
  con->id = c->nconns;
  con->nrx = 0;
  con->ntx = 0;  
  c->nconns++;
}

static inline void * core_thread(void *arg)
{
  int ret, i;
  struct core *c;
  struct conn *con;
  struct epoll_event *evs;
  
  c = (struct core *) arg;
  core_init(c);
  
  /* Allocate event structure */
  evs = calloc(max_events, sizeof(struct epoll_event));
  if (evs == NULL)
  {
    fprintf(stderr, "failed to allocate epoll_event struct\n");
    abort();
  }
  c->evs = evs;

  /* Core 0 waits here while others wait in init */
  if (c->id == 0)
    pthread_barrier_wait(&start_barrier);
  
  fprintf(stderr, "Initialized core=%d\n", c->id);

  while (1)
  {
    ret = epoll_wait(c->ep_fd, c->evs, max_events, -1);
    if (ret < 0)
    {
      perror("epoll_wait failed\n");
      abort();
    }
    
    for (i = 0; i < ret; i++)
    {
      con = c->evs[i].data.ptr;
      if (con == NULL)
      {
        conn_accept(c, &c->evs[i]);
      }
      else if (c->evs[i].events & ~(EPOLLIN | EPOLLOUT))
      {
        fprintf(stderr, "error on connection\n");
        abort();
      }
      else 
      {        
        /* Drain send before receiving so we have more buffer space */
        if (c->evs[i].events & EPOLLOUT)
          conn_tx(c, con, &c->evs[i]);
          
        /* Receive data and echo back */
        if (c->evs[i].events & EPOLLIN)
        {
          conn_rx(c, con);
          conn_tx(c, con, &c->evs[i]);
        }
      }
    }
  }

  pthread_exit((void *)(intptr_t)0);
}

static inline void prepare_cores()
{
  int i;

  for (i = 0; i < ncores; i++)
  {
    memset(&cores[i], 0, sizeof(struct core));
    cores[i].id = i;
    memset(&cores[i].dst_addr, 0, sizeof(cores[i].dst_addr));

    if (inet_pton(AF_INET, server_ip, &src_addr.sin_addr) != 1)
    {
      fprintf(stderr, "Invalid server_ip\n");
      abort();
    }
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
      abort();
    }
  }
}

static inline void init_server()
{
  memset(&src_addr, 0, sizeof(src_addr));
  src_addr.sin_family = AF_INET;
  src_addr.sin_port   = htons((__u16)port);
  if (inet_pton(AF_INET, server_ip, &src_addr.sin_addr) != 1)
  {
    fprintf(stderr, "Invalid bind_ip\n");
    abort();
  }

  /* Allocate cores and thread handles */
  cores = (struct core *) calloc((size_t) ncores, 
      sizeof(struct core));
  threads = (pthread_t *) calloc((size_t) ncores, sizeof(pthread_t));
  if (!cores || !threads)
  {
    perror("calloc cores/threads");
    abort();
  }

  /* Barrier for all cores + main (to synchronize start of sending) */
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
  int ret, i;
  __u64 now;

  ret = parse_args(argc, argv);
  if (ret != 0)
  {
    fprintf(stderr, "failed to parse args\n");
    abort();
  }
    
  init_server();
  prepare_cores();
  start_cores();

  /* Wait for work threads brefore continuing */
  printf("Initialized %d core(s). Waiting for barrier\n", ncores);
  fflush(stdout);
  pthread_barrier_wait(&start_barrier);

  printf("TCP echo server listening on %s:%d (msize=%zu) (bsize=%zu) (ncores=%d)\n", 
      server_ip, port, msize, bsize, ncores);
  fflush(stdout);
  
  /* Establish timebase and reporting windows */
  t_start_ns = now_ns();
  t_last_report_ns = t_start_ns;
  t_next_report_ns = t_start_ns + 1000000000ULL;
  
  /* Per-second reporting loop in main thread */
  while (1)
  {
    now = now_ns();
    if (now >= t_next_report_ns)
      print_stats(now);

    /* Sleep a bit to avoid busy wait */
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 2000000 };
    nanosleep(&ts, NULL);
  }
  
  /* Join all cores */
  for (i = 0; i < ncores; i++)
    pthread_join(threads[i], NULL);
    
  return 0;
}
