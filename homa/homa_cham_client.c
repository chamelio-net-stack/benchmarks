#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "homa_cham.h"
#include "homa_workers.h"
#include "../utils/clock.h"
#include "../utils/hist.h"

#define NS_PER_SECOND 1000000000ULL
#define ECHO_TIMEOUT_NS (5 * NS_PER_SECOND)
#define MAX_RTT_US 1000000
#define TOKEN_BURST_US 1000

struct pending_rpc
{
  __u64 id;
  __u64 tsc;
  __u64 sequence;
};

struct client
{
  struct homa_cham_config config;
  struct homa_context_lib *ctx;
  struct pending_rpc pending[HOMA_CHAM_MAX_PENDING];
  unsigned char *payload;
  int fd;
  unsigned int active;
  __u64 sequence;
  __u64 sent;
  __u64 received;
  __u64 errors;
  __u64 window_received;
  __u64 interval_sent;
  __u64 interval_received;
  __u64 start_ns;
  __u64 end_ns;
  __u64 last_report_ns;
  __u64 next_report_ns;
  __u64 last_update_ns;
  double tokens;
};

/* Main owns reporting; workers share only the histogram and startup gate. */
static struct hist_state rtt_hist;
static pthread_mutex_t state_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t state_changed = PTHREAD_COND_INITIALIZER;
static unsigned int ready_workers, done_workers;
static int released, abort_start;
static __u64 measurement_start_ns;

static int run_client(struct client *client);
static void start_measurement(struct client *client, __u64 now);
static void update_tokens(struct client *client, __u64 now);
static int is_send_allowed(struct client *client, __u64 now);
static int send_requests(struct client *client);
static int finish_request(struct client *client, struct homa_event *event);
static int is_reply_valid(struct client *client, struct homa_event *event);
static void print_interval(struct client *client, __u64 now);
static void print_summary(struct client *client);
static void print_latency(struct hist_state *hist, const char *unit);

static int client_worker(void *arg)
{
  struct client *client = arg;
  int ret;

  client->payload = calloc(client->config.msize ? client->config.msize : 1, 1);
  if (client->payload == NULL)
  {
    perror("Homa client payload");
    return 1;
  }

  client->fd = homa_cham_init(&client->config, &client->ctx);
  if (client->fd < 0)
  {
    perror("Homa client initialization");
    free(client->payload);
    return 1;
  }

  ret = run_client(client);

  if (homa_lib_close(client->ctx, client->fd) != 0)
  {
    perror("homa_lib_close");
    ret = -1;
  }

  homa_lib_ctx_free(client->ctx);
  free(client->payload);
  return ret == 0 ? 0 : 1;
}

static void *client_thread(void *arg)
{
  int ret = client_worker(arg);

  pthread_mutex_lock(&state_lock);
  done_workers++;
  pthread_cond_broadcast(&state_changed);
  pthread_mutex_unlock(&state_lock);
  return (void *) (intptr_t) ret;
}

static int run_client(struct client *client)
{
  struct homa_event events[HOMA_CHAM_MAX_PENDING];
  __u64 now, deadline = now_ns() + ECHO_TIMEOUT_NS;
  int i, ret;

  while (1)
  {
    now = now_ns();
    if (client->start_ns == 0 && client->received == 1)
    {
      pthread_mutex_lock(&state_lock);
      ready_workers++;
      pthread_cond_broadcast(&state_changed);
      while (!released)
        pthread_cond_wait(&state_changed, &state_lock);
      pthread_mutex_unlock(&state_lock);
      if (abort_start)
        return -1;
      start_measurement(client, measurement_start_ns);
      now = now_ns();
    }

    if (client->start_ns != 0 && now >= client->end_ns)
    {
      if (client->active == 0)
        break;

      if (now >= client->end_ns + ECHO_TIMEOUT_NS)
      {
        fprintf(stderr, "Drain timed out with %u outstanding RPCs\n",
            client->active);
        return -1;
      }
    }
    else if (client->start_ns == 0)
    {
      if (now >= deadline)
      {
        fprintf(stderr, "Timed out: %llu/%llu replies\n",
            (unsigned long long) client->received,
            (unsigned long long) client->sent);
        return -1;
      }
    }

    if (client->start_ns != 0)
      update_tokens(client, now);

    if (send_requests(client) != 0)
      return -1;

    ret = homa_lib_poll(client->ctx, events, client->config.mpending);
    if (ret < 0)
    {
      perror("homa_lib_poll");
      return -1;
    }

    for (i = 0; i < ret; i++)
    {
      if (finish_request(client, &events[i]) != 0)
        return -1;
    }
  }

  return client->errors == 0 ? 0 : -1;
}

static void start_measurement(struct client *client, __u64 now)
{
  client->sent = 0;
  client->received = 0;
  client->start_ns = now;
  client->end_ns = now + (__u64) client->config.duration * NS_PER_SECOND;
  client->last_report_ns = now;
  client->next_report_ns = now + NS_PER_SECOND;
  client->last_update_ns = now;
  client->tokens = 0.0;
}

static void update_tokens(struct client *client, __u64 now)
{
  double bytes_per_us = client->config.rate / 8.0;
  double delta_us = (double) (now - client->last_update_ns) / 1000.0;
  double max_tokens = bytes_per_us * TOKEN_BURST_US;

  if (max_tokens < client->config.msize)
    max_tokens = client->config.msize;
  client->tokens += delta_us * bytes_per_us;
  if (client->tokens > max_tokens)
    client->tokens = max_tokens;
  client->last_update_ns = now;
}

static int is_send_allowed(struct client *client, __u64 now)
{
  if (client->start_ns == 0)
    return client->sent == 0;

  return now < client->end_ns && client->active < client->config.mpending &&
      client->tokens >= client->config.msize;
}

static int send_requests(struct client *client)
{
  struct homa_startup startup = { htonl(HOMA_STARTUP_MAGIC),
      htonl(client->config.ncores) };
  struct pending_rpc *rpc;
  unsigned int i, j;

  for (i = 0; i < client->config.mpending; i++)
  {
    if (!is_send_allowed(client, now_ns()))
      break;

    rpc = &client->pending[i];
    if (rpc->id != 0)
      continue;

    if (client->config.validate)
    {
      for (j = 0; j < client->config.msize; j++)
        client->payload[j] = (unsigned char) (client->sequence + j);
    }

    rpc->tsc = util_rdtsc();
    if (homa_lib_request(client->ctx, client->fd,
        client->start_ns == 0 ? (void *) &startup : client->payload,
        client->start_ns == 0 ? sizeof(startup) : client->config.msize,
        (const struct sockaddr *) &client->config.remote,
        sizeof(client->config.remote), &rpc->id) != 0)
    {
      if (errno == EAGAIN || errno == ENOBUFS)
        break;

      perror("homa_lib_request");
      return -1;
    }

    rpc->sequence = client->sequence++;
    client->active++;
    client->sent++;
    if (client->start_ns != 0)
    {
      client->tokens -= client->config.msize;
      __atomic_fetch_add(&client->interval_sent, 1, __ATOMIC_RELAXED);
    }
  }

  return 0;
}

static int finish_request(struct client *client, struct homa_event *event)
{
  struct pending_rpc *rpc = NULL;
  __u64 now, rtt_us;
  unsigned int i, j;
  int ret;

  for (i = 0; i < client->config.mpending; i++)
  {
    if (client->pending[i].id == event->rpc_id)
    {
      rpc = &client->pending[i];
      break;
    }
  }

  if (rpc == NULL)
  {
    fprintf(stderr, "Unexpected Homa RPC handle\n");
    return -1;
  }

  if (event->type == HOMA_EVENT_ERROR)
  {
    fprintf(stderr, "Homa RPC failed: %s\n", strerror(event->status));
    client->errors++;
    client->active--;
    rpc->id = 0;
    return client->start_ns == 0 ? -1 : 0;
  }

  if (!is_reply_valid(client, event))
  {
    fprintf(stderr, "Unexpected Homa reply\n");
    return -1;
  }

  now = now_ns();
  rtt_us = client->start_ns != 0 ? us_since_tsc(rpc->tsc) : 0;
  if (client->config.validate && client->start_ns != 0)
  {
    for (j = 0; j < client->config.msize; j++)
    {
      unsigned char expected = rpc->sequence + j;

      if (((const unsigned char *) event->data)[j] != expected)
      {
        fprintf(stderr, "Echo mismatch in request %llu, byte %u\n",
            (unsigned long long) rpc->sequence, j);
        return -1;
      }
    }
  }

  do
  {
    ret = homa_lib_release(client->ctx, client->fd, event->rpc_id);
  } while (ret != 0 && errno == EAGAIN);

  if (ret != 0)
  {
    perror("homa_lib_release");
    return -1;
  }

  rpc->id = 0;
  client->active--;
  client->received++;
  if (client->start_ns != 0)
  {
    hist_add(&rtt_hist, rtt_us);
    if (now < client->end_ns)
    {
      client->window_received++;
      __atomic_fetch_add(&client->interval_received, 1, __ATOMIC_RELAXED);
    }
  }

  return 0;
}

static int is_reply_valid(struct client *client, struct homa_event *event)
{
  int response = event->type == HOMA_EVENT_RESPONSE;
  int same_socket = event->sockfd == client->fd;
  int same_address =
      event->peer.sin_addr.s_addr == client->config.remote.sin_addr.s_addr;
  int same_port = event->peer.sin_port == client->config.remote.sin_port;
  int length_valid = event->len == client->config.msize;
  int success = event->status == 0;

  if (client->start_ns == 0)
  {
    struct homa_startup startup;

    if (event->len != sizeof(startup))
      return 0;
    memcpy(&startup, event->data, sizeof(startup));
    length_valid = ntohl(startup.magic) == HOMA_STARTUP_MAGIC &&
        ntohl(startup.ncores) == client->config.ncores;
    if (!length_valid)
      fprintf(stderr, "Client/server worker counts must match\n");
  }

  return response && same_socket && same_address && same_port &&
      length_valid && success;
}

static void print_interval(struct client *client, __u64 now)
{
  double seconds = (double) (now - client->last_report_ns) / NS_PER_SECOND;
  double load = client->interval_sent / seconds;
  double throughput = client->interval_received / seconds;
  double payload_mbits = client->config.msize * 8.0 / 1000000.0;

  printf("[ %3llus ] load=%llu rps | load=%.2f Mb/s | "
      "tp=%llu rps | tp=%.2f Mb/s | ",
      (unsigned long long) ((now - client->start_ns) / NS_PER_SECOND),
      (unsigned long long) load, load * payload_mbits,
      (unsigned long long) throughput, throughput * payload_mbits);
  print_latency(&rtt_hist, " us");
  fflush(stdout);
  client->interval_sent = 0;
  client->interval_received = 0;
  client->last_report_ns = now;
  client->next_report_ns = now + NS_PER_SECOND;
}

static void print_summary(struct client *client)
{
  __u64 stop_ns = now_ns();
  double seconds;
  double load, throughput, payload_mbits;

  if (stop_ns > client->end_ns)
    stop_ns = client->end_ns;
  seconds = (double) (stop_ns - client->start_ns) / NS_PER_SECOND;
  load = client->sent / seconds;
  throughput = client->window_received / seconds;
  payload_mbits = client->config.msize * 8.0 / 1000000.0;

  printf("Cores               : %u\n", client->config.ncores);
  printf("TX requests         : %llu\n", (unsigned long long) client->sent);
  printf("RX replies          : %llu\n", (unsigned long long) client->received);
  printf("RPC errors          : %llu\n", (unsigned long long) client->errors);
  printf("Unfinished RPCs     : %u\n", client->active);
  printf("Avg Load            : %llu rps %.2f Mb/s\n",
      (unsigned long long) load, load * payload_mbits);
  printf("Avg Throughput      : %llu rps %.2f Mb/s\n",
      (unsigned long long) throughput, throughput * payload_mbits);
  printf("RTT percentiles (us): ");
  print_latency(&rtt_hist, "");
  fflush(stdout);
}

static void print_latency(struct hist_state *hist, const char *unit)
{
  __u64 p50, p99, p999;

  if (!hist_percentiles(hist, &p50, &p99, &p999))
  {
    printf("p50=-%s  p99=-%s  p99.9=-%s\n", unit, unit, unit);
    return;
  }

  printf("p50=%llu%s  p99=%llu%s  p99.9=%llu%s\n",
      (unsigned long long) p50, unit, (unsigned long long) p99, unit,
      (unsigned long long) p999, unit);
}

static void collect_interval(struct client *total, struct client *clients)
{
  unsigned int i;

  for (i = 0; i < total->config.ncores; i++)
  {
    total->interval_sent += __atomic_exchange_n(&clients[i].interval_sent,
        0, __ATOMIC_RELAXED);
    total->interval_received += __atomic_exchange_n(&clients[i].interval_received,
        0, __ATOMIC_RELAXED);
  }
}

int main(int argc, char **argv)
{
  struct homa_cham_config config;
  struct client clients[HOMA_CHAM_MAX_CORES] = { 0 }, total = { 0 };
  pthread_t threads[HOMA_CHAM_MAX_CORES];
  struct timespec pause = { .tv_sec = 0, .tv_nsec = 2000000 };
  unsigned int i, started = 0, done;
  int ret = 0, error;
  void *result;
  __u64 now;

  if (homa_cham_parse(&config, argc, argv, 1) != 0)
    return 1;
  if (init_hist(&rtt_hist, MAX_RTT_US) != 0)
    return 1;
  calibrate_tsc();
  total.config = config;

  printf("Homa benchmark: ncores=%u, msize=%u, rate=%.2f Mb/s per endpoint, "
      "mpending=%u\n", config.ncores, config.msize, config.rate, config.mpending);
  printf("One startup echo per endpoint is excluded from measurements.\n");
  printf("Rates count request/reply payload bytes in one direction.\n");
  printf("RTTs include drained replies; throughput uses the load interval.\n");
  printf("RTT histogram cap: %u us\n", MAX_RTT_US);
  printf("Payload validation: %s\n", config.validate ? "enabled" : "disabled");
  fflush(stdout);

  for (i = 0; i < config.ncores; i++)
  {
    clients[i].config = config;
    clients[i].config.core = config.cores[i];
    if (config.local.sin_port != 0)
      clients[i].config.local.sin_port = htons(ntohs(config.local.sin_port) + i);
    clients[i].config.remote.sin_port = htons(ntohs(config.remote.sin_port) + i);
    error = pthread_create(&threads[i], NULL, client_thread, &clients[i]);
    if (error != 0)
    {
      fprintf(stderr, "Cannot start Homa worker: %s\n", strerror(error));
      ret = 1;
      break;
    }
    started++;
  }

  /* Release all endpoints on one timebase, or wake ready workers on failure. */
  pthread_mutex_lock(&state_lock);
  while (started == config.ncores && ready_workers < started && done_workers == 0)
    pthread_cond_wait(&state_changed, &state_lock);
  abort_start = started != config.ncores || done_workers != 0;
  if (!abort_start)
  {
    measurement_start_ns = now_ns();
    start_measurement(&total, measurement_start_ns);
  }
  released = 1;
  pthread_cond_broadcast(&state_changed);
  pthread_mutex_unlock(&state_lock);

  do
  {
    pthread_mutex_lock(&state_lock);
    done = done_workers;
    pthread_mutex_unlock(&state_lock);
    now = now_ns();
    if (!abort_start && now >= total.next_report_ns && now < total.end_ns)
    {
      collect_interval(&total, clients);
      print_interval(&total, now);
    }
    if (done < started)
      nanosleep(&pause, NULL);
  } while (done < started);

  for (i = 0; i < started; i++)
  {
    pthread_join(threads[i], &result);
    if ((intptr_t) result != 0)
      ret = 1;
    if (clients[i].start_ns == 0)
      continue;
    total.sent += clients[i].sent;
    total.received += clients[i].received;
    total.errors += clients[i].errors;
    total.window_received += clients[i].window_received;
    total.active += clients[i].active;
  }
  if (!abort_start)
  {
    collect_interval(&total, clients);
    now = now_ns();
    if (now > total.end_ns)
      now = total.end_ns;
    if (now > total.last_report_ns)
      print_interval(&total, now);
    print_summary(&total);
  }
  hist_cleanup(&rtt_hist);
  return ret;
}
