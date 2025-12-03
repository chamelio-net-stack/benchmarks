/* Usage:
 *   ./udp_demi_server <src_bind_ip> <src_port> <rem_bind_ip> <rem_port> [--msize N] [--ncores N] [--buf-size N]
 *
 * Example:
 *   ./udp_linux_server 0.0.0.0 9000 0.0.0.1 9001 --msize 64 --ncores 2 --buf-size 4096
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
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <fcntl.h>
#include <linux/types.h>
#include <demi/libos.h>
#include <demi/sga.h>
#include <demi/wait.h>

struct core {
  int id;
  int fd;
  int npop;
  int npush;
  demi_qresult_t res;
  demi_sgarray_t *sgas;
  __u64 rxb_interval;
  __u64 rxp_interval;
  __u64 txb_interval;
  __u64 txp_interval;
  struct sockaddr_in dst_addr;
  socklen_t dst_len;
};

/* Default arg values */
size_t msize = 64;
size_t bsize = 4096;
size_t nsgas = 0;
int ncores = 1;

__u64 rxb_interval_total, rxp_interval_total;
__u64 txb_interval_total, txp_interval_total;
__u64 t_start_ns;
__u64 t_last_report_ns, t_next_report_ns;

int src_port;
const char *src_server_ip;
int rem_port;
const char *rem_server_ip;
static pthread_barrier_t start_barrier;
static pthread_t *threads = NULL;
static struct core *cores = NULL;
struct sockaddr_in src_addr;

static inline __u64 now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return (__u64) ts.tv_sec * 1000000000ULL + (__u64) ts.tv_nsec;
}

static inline int parse_args(int argc, char **argv)
{
  int i;

  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <src_bind_ip> <src_port> <rem_bind_ip> <rem_port>" 
        " [--msize N] [--ncores N] [--buf-size]\n", argv[0]);
    return -1;
  }

  src_server_ip = argv[1];
  src_port = atoi(argv[2]);
  if (src_port <= 0 || src_port > 65535)
  {
    fprintf(stderr, "Invalid src port\n");
    return -1;
  }

  rem_server_ip = argv[3];
  rem_port = atoi(argv[4]);
  if (rem_port <= 0 || rem_port > 65535)
  {
    fprintf(stderr, "Invalid port\n");
    return -1;
  }


  for (i = 5; i < argc; i++)
  {
    if (strcmp(argv[i], "--msize") == 0 && i + 1 < argc)
    {
      msize = (size_t)strtoul(argv[++i], NULL, 10);
    }
    else if (strcmp(argv[i], "--ncores") == 0 && i + 1 < argc)
    {
      ncores = (size_t)strtoul(argv[++i], NULL, 10);
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
    fprintf(stderr, "core=%d bytes_rx=%.2f Mb/s bytes_tx=%.2f Mb/s\n", i,
      (double) cores[i].rxb_interval * 8.0 / 1e6, 
      (double) cores[i].txb_interval * 8.0 / 1e6);
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

static inline void core_init(struct core *c)
{
  int i, ret, fd;
  demi_sgarray_t *sgas;

  ret = demi_socket(&fd, AF_INET, SOCK_DGRAM, 0);
  if (ret < 0) 
  {
    perror("socket");
    abort();
  }

  ret = demi_bind(fd, (struct sockaddr *)&src_addr, sizeof(src_addr));
  if (ret < 0)
  {
    perror("bind");
    abort();
  }

  nsgas = bsize / msize;
  sgas = calloc(nsgas, sizeof(demi_sgarray_t));
  if (sgas == NULL)
  {
    fprintf(stderr, "failed to allocate array of sgas for core\n");
    abort();
  }
  c->sgas = sgas;

  for (i = 0; i < nsgas; i++)
  {
    c->sgas[i] = demi_sgaalloc(msize);
    if (c->sgas[i].sga_numsegs == 0)
    {
      fprintf(stderr, "failed to allocate sga");
      abort();
    }
  }

  c->dst_addr.sin_family = AF_INET;
  c->dst_addr.sin_port = htons((__u16) rem_port);
  memset(&c->dst_addr, 0, sizeof(c->dst_addr));

  c->fd = fd;
  c->npop = 0;
  c->npush = 0;
  c->dst_len = sizeof(c->dst_addr);
  c->rxb_interval = 0;
  c->rxp_interval = 0;
  c->txb_interval = 0;
  c->txp_interval = 0;
}

static inline void core_rx(struct core *c)
{
  int ret;
  demi_qtoken_t qt = -1;

  while (c->npop < nsgas)
  {
    assert(demi_pop(&qt, c->fd) == 0);
    ret = demi_wait(&c->res, qt, 0) ;    
    assert(ret == 0 || ret == ETIMEDOUT);
   
    if (ret != 0)
      break;
    
    assert(c->res.qr_opcode == DEMI_OPC_POP);
    assert(c->res.qr_value.sga.segments[0].data_len_bytes == msize);
    memcpy(&c->sgas[c->npop], &c->res.qr_value.sga, sizeof(demi_sgarray_t));
    __sync_fetch_and_add(&c->rxp_interval, 1);
    __sync_fetch_and_add(&c->rxb_interval, 
        c->res.qr_value.sga.segments[0].data_len_bytes);
    c->npop++;
  }
}

static inline void core_tx(struct core *c)
{
  int ret;
  demi_qtoken_t qt = -1;
  
  while (c->npush < c->npop)
  {
    assert(demi_pushto(&qt, c->fd, &c->sgas[c->npush], 
        (const struct sockaddr *) &c->dst_addr, c->dst_len) == 0);
    ret = demi_wait(&c->res, qt, 0);
    assert(ret == 0 || ret == ETIMEDOUT);
          
    if (ret != 0)
      break;
    
    assert(c->res.qr_opcode == DEMI_OPC_PUSH);
    __sync_fetch_and_add(&c->txp_interval, 1);
    __sync_fetch_and_add(&c->txb_interval, msize);
    c->npush++;
  }
  
  c->npop = 0;
  c->npush = 0;
}

static inline void * core_thread(void *arg)
{
  int ret;
  struct core *c;

  c = (struct core *) arg;
  core_init(c);

  /* Core 0 waits here while others wait in init */
  if (c->id == 0)
    pthread_barrier_wait(&start_barrier);

  const struct demi_args args = {
    .argc = 0,
    .argv = NULL,
    .callback = NULL,
  };

  ret = demi_init(&args);
  if (ret != 0)
  {
    fprintf(stderr, "Failed to initialize the demikernel");
    abort();
  }

  while (1)
  {
    core_rx(c);
    core_tx(c);
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

    if (inet_pton(AF_INET, src_server_ip, &src_addr.sin_addr) != 1)
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
  src_addr.sin_family = AF_INET;
  src_addr.sin_port = htons((__u16) src_port);
  memset(&src_addr, 0, sizeof(src_addr));
  if (inet_pton(AF_INET, src_server_ip, &src_addr.sin_addr) != 1)
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

  printf("UDP echo server listening on %s:%d (msize=%zu) (bsize=%zu) (ncores=%d)\n", 
      src_server_ip, src_port, msize, bsize, ncores);
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
