/* Usage:
 *   ./udp_cham_server <bind_ip> <port> [--msize N] [--ncores N] [--buf-size N] [--cores LIST]
 *
 * Example:
 *   ./udp_cham_server 0.0.0.0 9000 --msize 64 --ncores 2 --buf-size 4096
 * 
 *   --msize Size of expected message
 *   --ncores Number of cores to use
 *   --buf-size Size of transmit buffer
 *   --cores Comma-separated core list (main, thread0, thread1, ...)
 */

#define _GNU_SOURCE
#include <chamelio/udp_lib.h>
#include <errno.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <sched.h>
#include "../utils/clock.h"
#include "../utils/core.h"

struct core {
  int id;
  int fd;
  int nrx;
  int ntx;
  __u8 *buf;
  __u64 rxb_interval;
  __u64 rxp_interval;
  __u64 txb_interval;
  __u64 txp_interval;
  struct sockaddr_in dst_addr;
};

/* Default arg values */
size_t msize = 64;
size_t bsize = 4096;
int ncores = 1;
static char *cores_arg = NULL;
static int *core_list = NULL;
static int core_list_len = 0;

__u64 rxb_interval_total, rxp_interval_total;
__u64 txb_interval_total, txp_interval_total;
__u64 t_start_ns;
__u64 t_last_report_ns, t_next_report_ns;

int src_port;
const char *src_ip;
static pthread_barrier_t start_barrier;
static pthread_t *threads = NULL;
static struct core *cores = NULL;
struct sockaddr_in src_addr;

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
  int i;

  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <bind_ip> <port>" 
        " [--msize N] [--ncores N] [--buf-size N] [--cores LIST]\n", argv[0]);
    return -1;
  }

  src_ip = argv[1];
  src_port = atoi(argv[2]);
  if (src_port <= 0 || src_port > 65535)
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
    else if (strcmp(argv[i], "--buf-size") == 0 && i + 1 < argc)
    {
      bsize = (size_t)strtoul(argv[++i], NULL, 10);
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

  if (cores_arg != NULL)
  {
    if (parse_core_list(cores_arg, ncores + 1) != 0)
      return -1;
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

static inline struct udp_context_lib * core_init(struct core *c)
{
  __u8 *buf;
  int ret, fd, n;
  struct udp_context_lib *udp_ctx;

  buf = (__u8 *) malloc(bsize);
  if (buf == NULL)
  {
    fprintf(stderr, "failed to malloc buf");
    abort();
  }
  c->buf = buf;
  memset(c->buf, 0, bsize);

  /* Cores that are not core 0 wait here so that ARP can complete */
  if (c->id != 0)
    pthread_barrier_wait(&start_barrier);
  
  udp_ctx = udp_ctx_new();
  if (udp_ctx == NULL)
    abort();

  fd = udp_socket(udp_ctx);
  if (fd < 0)
    abort();
    
  ret = udp_setsockopt(udp_ctx, fd, SO_REUSEPORT);
  if (ret < 0)
  {
    fprintf(stderr, "failed to set sockopt\n");
    abort();
  }

  ret = udp_bind(udp_ctx, fd, 
      (struct sockaddr *) &src_addr, sizeof(src_addr));
  if (ret < 0)
  {
    fprintf(stderr, "failed to bind ip and port");
    abort();
  }

  /* Core 0 resolves ARP */
  if (c->id == 0)
  {
    while(udp_poll_fast(udp_ctx) == 0){}
    n = udp_recvfrom(udp_ctx, fd, c->buf, msize, 
        (struct sockaddr *) &c->dst_addr, sizeof(c->dst_addr));
    udp_sendto(udp_ctx, fd, c->buf, (size_t) n, 
      (struct sockaddr *) &c->dst_addr, sizeof(c->dst_addr));
    printf("ARP initialized\n");
  }

  c->fd = fd;
  c->nrx = 0;
  c->ntx = 0;
  c->rxb_interval = 0;
  c->rxp_interval = 0;
  c->txb_interval = 0;
  c->txp_interval = 0;
  return udp_ctx;
}

static inline void core_rx(struct udp_context_lib *udp_ctx, struct core *c)
{
  int nrx;
  
  while (c->nrx < bsize)
  {
    assert(msize <= bsize - c->nrx);
    nrx = udp_recvfrom(udp_ctx, c->fd, c->buf + c->nrx, msize, 
        (struct sockaddr *) &c->dst_addr, sizeof(c->dst_addr));
    assert(nrx == msize || nrx < 0);
    
    if (nrx <= 0)
      break;
    
    c->nrx += nrx;
    __sync_fetch_and_add(&c->rxp_interval, 1);
    __sync_fetch_and_add(&c->rxb_interval, msize);
  }
}

static inline void core_tx(struct udp_context_lib *udp_ctx, struct core *c)
{
  int ntx;
  
  while (c->ntx < c->nrx)
  {
    assert(msize <= c->nrx - c->ntx);
    ntx = udp_sendto(udp_ctx, c->fd, c->buf + c->ntx, msize, 
        (struct sockaddr *) &c->dst_addr, sizeof(c->dst_addr));
    assert(ntx == msize || ntx < 0);
          
    if (ntx <= 0)
      break;
      
    c->ntx += ntx;
    __sync_fetch_and_add(&c->txp_interval, 1);
    __sync_fetch_and_add(&c->txb_interval, msize);
  }
  
  c->ntx = 0;
  c->nrx = 0;
}

static inline void * core_thread(void *arg)
{
  struct core *c;
  struct udp_context_lib *udp_ctx;
  
  c = (struct core *) arg;
  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[c->id + 1]) != 0)
      pthread_exit((void *) (intptr_t) -1);
  }
  udp_ctx = core_init(c);

  /* Core 0 waits here while others wait in init */
  if (c->id == 0)
    pthread_barrier_wait(&start_barrier);

  while (1)
  {
    udp_poll_fast(udp_ctx);
    core_rx(udp_ctx, c);
    udp_poll_fast(udp_ctx);
    core_tx(udp_ctx, c);
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

    if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
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
  src_addr.sin_port   = htons((__u16) src_port);
  if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
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

  ret = udp_connect_slow();
  if (ret != 0)
    abort();
    
  init_server();
  prepare_cores();

  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[0]) != 0)
      return EXIT_FAILURE;
  }

  start_cores();

  printf("UDP echo server listening on %s:%d (msize=%zu) (bsize=%zu) (ncores=%d)\n", 
      src_ip, src_port, msize, bsize, ncores);
  fflush(stdout);
  
  /* Wait for work threads brefore continuing */
  printf("Initialized %d core(s). Waiting for barrier\n", ncores);
  fflush(stdout);
  pthread_barrier_wait(&start_barrier);
  
  printf("Running\n");
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
  
  free(core_list);
  return 0;
}
