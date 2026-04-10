/* Usage:
 *   ./rpc_cham_server <bind_ip> <port> [--nworkers N][--service id1,id2,...]
 *  [--ncores N] [--buf-size] [--msize N] [--cores LIST]
 * 
 * Example:
 *   ./rpc_cham_server 0.0.0.0 9000 --nworkers 2 --service 1,2,3,4
 *   --ncores 2 --buf-size 4096 --msize 64 
 *                
 *   --nworkers Number of worker threads to create (default: 1)
 *   --service Comma-separated list of service IDs to register (default: none)  
 *   --msize Size of expected message
 *   --ncores Number of cores to use
 *   --buf-size Size of transmit buffer
 *   --cores Comma-separated core list (main, thread0, thread1, ...)
 */

#define _GNU_SOURCE
#include <chamelio/rpc_lib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>
#include <sched.h>
#include "../utils/clock.h"
#include "../utils/core.h"

#define MAX_WORKERS 16

struct core
{
  int id;
  struct rpc_server_lib *rpc_server;
  // struct rpc_worker_lib *worker;
  int nrx;
  int ntx;

  __u8 *buf;
  __u64 rxb_interval;
  __u64 rxp_interval;
  __u64 txb_interval;
  __u64 txp_interval;
  // struct sockaddr_in dst_addr;
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
static int nworkers = 1;
static int nservices;
static __u8 *service_id;

static pthread_barrier_t start_barrier;
static pthread_t *threads = NULL;
static struct core *cores = NULL;
struct sockaddr_in src_addr;

struct rpc_context_lib *rpc_ctx;
static struct rpc_server_lib *rpc_server;

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
  char *service_str, *tmp, *token, *saveptr;

  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <bind_ip> <port> [--nworkers N]"
                    " [--service id1,id2,...] [--ncores N] [--buf-size]"
                    " [--msize N] [--cores LIST]\n",argv[0]);
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
    if (strcmp(argv[i], "--nworkers") == 0 && i + 1 < argc)
    {
      nworkers = atoi(argv[++i]);
      if (nworkers <= 0)
        nworkers = 1;
      if (nworkers >= MAX_WORKERS)
      {
        fprintf(stderr, "nworkers should not be more than %d\n", MAX_WORKERS);
        return -1;
      }
    }
    else if (strcmp(argv[i], "--service") == 0 && i + 1 < argc)
    {
      service_str = argv[++i];
      // token = strtok(service_str, ",");

      nservices = 0;
      service_id = NULL;
      saveptr = NULL;

      tmp = strdup(service_str);
      if (!tmp)
      {
        perror("strdup service_str");
        return -1;
      }
      token = strtok_r(tmp, ",", &saveptr);

      while (token != NULL)
      {
        nservices++;
        service_id = (__u8 *)realloc(service_id, nservices * sizeof(__u8));
        if (!service_id)
        {
          perror("realloc service_id");
          return -1;
        }
        service_id[nservices - 1] = (__u8)strtoul(token, NULL, 10);
        if (service_id[nservices - 1] > 255)
        {
          fprintf(stderr, "service id should be between 0 and 255\n");
          return -1;
        }
        token = strtok_r(NULL, ",", &saveptr);
      }
      free(tmp);
    }
    else if (strcmp(argv[i], "--ncores") == 0 && i + 1 < argc)
    {
      ncores = (size_t)strtoul(argv[++i], NULL, 10);
      if (ncores <= 0)
        ncores = 1;
    }
    else if (strcmp(argv[i], "--buf-size") == 0 && i + 1 < argc)
    {
      bsize = (size_t)strtoul(argv[++i], NULL, 10);
      if (bsize <= 0)
        bsize = 1;
    }
    else if (strcmp(argv[i], "--msize") == 0 && i + 1 < argc)
    {
      msize = (size_t)strtoul(argv[++i], NULL, 10);
      if (msize <= 0)
        msize = 1;
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
    if (parse_core_list(cores_arg, ncores + 1) != 0) return -1;
  }

  return 0;
}

static inline void print_stats(__u64 now)
{
  double interval_s, rps_rx, mbps_rx, rps_tx, mbps_tx;
  __u64 bytes_rx = 0, pkts_rx = 0, bytes_tx = 0, pkts_tx = 0;

  interval_s = (double)(t_next_report_ns - t_last_report_ns) / 1e9;
  if (interval_s <= 0.0)
    interval_s = 1.0;

  /* Aggregate per-core and reset per-core intervals */
  for (int i = 0; i < ncores; i++)
  {
    bytes_rx += __sync_fetch_and_add(&cores[i].rxb_interval, 0);
    pkts_rx += __sync_fetch_and_add(&cores[i].rxp_interval, 0);
    bytes_tx += __sync_fetch_and_add(&cores[i].txb_interval, 0);
    pkts_tx += __sync_fetch_and_add(&cores[i].txp_interval, 0);
    fprintf(stderr, "core=%d bytes_rx=%.2f Mb/s bytes_tx=%.2f Mb/s\n", i, (double)cores[i].rxb_interval * 8.0 / 1e6, (double)cores[i].txb_interval * 8.0 / 1e6);
    __sync_fetch_and_and(&cores[i].rxb_interval, 0);
    __sync_fetch_and_and(&cores[i].rxp_interval, 0);
    __sync_fetch_and_and(&cores[i].txb_interval, 0);
    __sync_fetch_and_and(&cores[i].txp_interval, 0);
  }

  rxb_interval_total = bytes_rx;
  rxp_interval_total = pkts_rx;
  txb_interval_total = bytes_tx;
  txp_interval_total = pkts_tx;

  rps_rx = (double)rxp_interval_total / interval_s;
  mbps_rx = ((double)rxb_interval_total * 8.0 / 1e6) / interval_s;
  rps_tx = (double)txp_interval_total / interval_s;
  mbps_tx = ((double)txb_interval_total * 8.0 / 1e6) / interval_s;

  printf("[ %3lus ] rx=%lld rps | rx=%.2f Mb/s | tx=%lld rps | tx=%.2f Mb/s \n",
         (unsigned long)((t_next_report_ns - t_start_ns) / 1000000000ULL),
         (__u64)rps_rx, mbps_rx, (__u64)rps_tx, mbps_tx);
  fflush(stdout);

  t_last_report_ns = t_next_report_ns;
  t_next_report_ns += 1000000000ULL;

  if (now > t_next_report_ns + 5000000000ULL)
  {
    t_next_report_ns = now + 1000000000ULL;
    t_last_report_ns = now;
  }
}

// TODO: do this with rpc_return when we have implemented it
static inline void core_tx(struct rpc_context_lib *rpc_ctx, struct core *c)
{
  int ntx;
  struct rpc_worker_lib *worker;

  worker = &c->rpc_server->workers[0];

  while (c->ntx < c->nrx)
  {
    assert(msize <= c->nrx - c->ntx);
    ntx = rpc_return(c->rpc_server, worker->rx_ip, worker->rx_port,
                     0, c->buf + c->ntx, msize);

    if (ntx <= 0)
      break;

    c->ntx += ntx;
    __sync_fetch_and_add(&c->txp_interval, 1);
    __sync_fetch_and_add(&c->txb_interval, msize);
  }
  c->ntx = 0;
  c->nrx = 0;
}

static inline void core_rx(struct rpc_context_lib *rpc_ctx, struct core *c)
{
  int nrx;
  struct rpc_worker_lib *worker;

  worker = &c->rpc_server->workers[0];

  while (c->nrx < bsize)
  {
    assert(msize <= bsize - c->nrx);
    nrx = rpc_handle_call(worker, c->buf + c->nrx, msize);
    if (nrx < 0)
      break;

    c->nrx += nrx;
    __sync_fetch_and_add(&c->rxp_interval, 1);
    __sync_fetch_and_add(&c->rxb_interval, nrx);
  }
}

static inline struct rpc_context_lib *core_init(struct core *c)
{
  __u8 *buf;
  int ret, i;
  struct rpc_worker_lib *rpc_workers;

  buf = (__u8 *)malloc(bsize);
  if (buf == NULL)
  {
    perror("malloc core buf");
    abort();
  }

  c->buf = buf;
  memset(c->buf, 0, bsize);

  if (c->id != 0)
    pthread_barrier_wait(&start_barrier);

  // create server on core 0 (slow path)
  if (c->id == 0)
  {
    rpc_ctx = rpc_ctx_new();
    if (rpc_ctx == NULL)
    {
      fprintf(stderr, "Failed to create RPC context\n");
      abort();
    }

    fprintf(stderr, "thread %d creating server %s:%d\n", c->id, src_ip, src_port);
    rpc_server = rpc_new_server(rpc_ctx, ntohl(inet_addr(src_ip)), (__u16)src_port); // sets global
    fflush(stderr);

    if (!rpc_server)
    {
      fprintf(stderr, "Failed to create RPC server\n");
      abort();
    }

    if (nservices == 0)
    {
      nservices = 1;
      service_id = (__u8 *)malloc(sizeof(*service_id));
      if (!service_id)
      {
        perror("malloc service_id");
        abort();
      }
      service_id[0] = 1;
    }

    for (i = 0; i < nservices; i++)
    {
      ret = rpc_register(rpc_server, service_id[i]);
      if (ret != 0)
      {
        fprintf(stderr, "Failed to register service %u\n", service_id[0]);
        abort();
      }
    }
    fprintf(stdout, "RPC server created & service %u registered.\n", service_id[0]);

    for (i = 0; i < nworkers; i++)
    {
      fprintf(stderr, "  Creating RPC worker %d...\n", i);
      rpc_workers = rpc_new_worker(rpc_server);
      if (!rpc_workers)
      {
        fprintf(stderr, "Failed to create RPC worker\n");
        abort();
      }
    }
  }

  // create workers on each core hosted on the created server on core 0
  // TODO: change this when we have multiple workers
  pthread_barrier_wait(&start_barrier);
  c->rpc_server = rpc_server;

  c->nrx = 0;
  c->ntx = 0;
  c->rxb_interval = 0;
  c->rxp_interval = 0;
  c->txb_interval = 0;
  c->txp_interval = 0;
  return rpc_ctx;
}

static inline void *core_thread(void *arg)
{
  struct core *c;
  struct rpc_context_lib *rpc_ctx;

  c = (struct core *)arg;
  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[c->id + 1]) != 0)
      pthread_exit((void *) (intptr_t) -1);
  }
  rpc_ctx = core_init(c);

  while (1)
  {
    rpc_poll_calls(rpc_ctx);
    core_rx(rpc_ctx, c);
    rpc_poll_calls(rpc_ctx);
    core_tx(rpc_ctx, c);
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
    // memset(&cores[i].dst_addr, 0, sizeof(cores[i].dst_addr));
  }
  if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
  {
    fprintf(stderr, "Invalid server_ip\n");
    abort();
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
  src_addr.sin_port = htons((__u16)src_port);
  if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1)
  {
    fprintf(stderr, "Invalid bind_ip\n");
    abort();
  }
  /* Allocate cores and thread handles */
  cores = (struct core *)calloc((size_t)ncores, sizeof(struct core));
  threads = (pthread_t *)calloc((size_t)ncores, sizeof(pthread_t));
  if (!cores || !threads)
  {
    perror("calloc cores/threads");
    abort();
  }

  /* Barrier for all cores + main (to synchronize start of sending) */
  if (pthread_barrier_init(&start_barrier, NULL,
                           (unsigned)ncores + 1) != 0)
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
  // struct rpc_context_lib *rpc_ctx;
  // struct rpc_server_lib *rpc_server;
  // struct rpc_worker_lib *rpc_worker[MAX_WORKERS];

  ret = parse_args(argc, argv);
  if (ret != 0)
  {
    fprintf(stderr, "Failed to parse args\n");
    abort();
  }

  ret = rpc_connect_slow();
  if (ret != 0)
    abort();

  // memset(&src_addr, 0, sizeof(src_addr));
  // src_addr.sin_family = AF_INET;
  // src_addr.sin_port   = htons((__u16) src_port);

  // if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) != 1) {
  //     fprintf(stderr, "Invalid bind_ip\n");
  //     abort();
  // }

  // rpc_ctx = rpc_ctx_new();
  // if (rpc_ctx == NULL) {
  //     fprintf(stderr, "Failed to create RPC context\n");
  //     abort();
  // }

  // rpc_server = rpc_new_server(rpc_ctx, ntohl(src_addr.sin_addr.s_addr), (__u16) src_port);
  // if (rpc_server == NULL) {
  //     fprintf(stderr, "Failed to create RPC server\n");
  //     abort();
  // }
  // printf("Server created successfully.\n");
  // printf("  core        : %u\n", rpc_server->core);
  // printf("  nservices   : %u\n", rpc_server->nservices);
  // printf("  nworkers    : %u\n", rpc_server->nworkers);
  // printf("  bind_success: %d\n", rpc_server->bind_success);

  // printf("Creating %d RPC workers...\n", nworkers);

  // for (int i = 0; i < nworkers; i++) {
  //     printf("  Creating RPC worker %d...\n", i);
  //     rpc_worker[i] = rpc_new_worker(rpc_server);
  //     printf("  RPC worker %d created on core %u.\n", i, rpc_server->core);
  //     if (rpc_worker[i] == NULL) {
  //         fprintf(stderr, "Failed to create RPC worker %d\n", i);
  //         abort();
  //     }
  // }
  // printf("RPC workers created successfully.\n");

  // ret = rpc_register(rpc_server, service_id);
  // if (ret != 0) {
  //     fprintf(stderr, "Failed to register service %d\n", service_id);
  //     abort();
  // }
  // for (int i = 0; i < ncores; i++) {
  //   int rc = pthread_create(&threads[i], NULL, core_thread, &cores[i]);
  //   if (rc != 0) {
  //     errno = rc;
  //     perror("pthread_create");
  //     return -1;
  //   }
  // }
  // printf("RPC server listening on %s:%d (msize=%zu) (bsize=%zu) (ncores=%d) (nworkers=%d) (service=%u)\n",
  //        src_ip, src_port, msize, bsize, ncores, nworkers, service_id);
  // fflush(stdout);

  // pthread_barrier_wait(&start_barrier);

  // t_start_ns = now_ns();
  // t_last_report_ns = t_start_ns;
  // t_next_report_ns = t_start_ns + 1000000000ULL;

  // while (1) {
  //   __u64 now = now_ns();
  //   if (now >= t_next_report_ns) print_stats(now);

  //   struct timespec ts = {.tv_sec = 0, .tv_nsec = 2000000};
  //   nanosleep(&ts, NULL);
  // }

  init_server();
  prepare_cores();
  if (core_list_len > 0)
  {
    if (pin_thread_to_core(core_list[0]) != 0)
      return EXIT_FAILURE;
  }

  start_cores();

  printf("RPC server listening on %s:%d (msize=%zu) (bsize=%zu) (ncores=%d) \
    (nworkers=%d) (service=%u)\n",
         src_ip, src_port, msize, bsize, ncores,
         nworkers, service_id[0]);

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
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 2000000};
    nanosleep(&ts, NULL);
  }

  /* Join all cores */
  for (i = 0; i < ncores; i++)
    pthread_join(threads[i], NULL);

  free(core_list);
  return 0;
}
