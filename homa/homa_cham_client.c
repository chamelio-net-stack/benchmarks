#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "homa_cham.h"
#include "../utils/clock.h"
#include "../utils/hist.h"

#define NS_PER_SECOND 1000000000ULL
#define ECHO_TIMEOUT_NS (5 * NS_PER_SECOND)
#define MAX_RTT_US 1000000
#define TOKEN_BURST_US 1000

struct pending_rpc {
  __u64 id;
  __u64 tsc;
  __u64 sequence;
};

struct client {
  struct homa_cham_config config;
  struct homa_context_lib *ctx;
  struct pending_rpc pending[HOMA_CHAM_MAX_PENDING];
  struct hist_state hist;
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

static int run_client(struct client *client);
static void start_measurement(struct client *client, __u64 now);
static void update_tokens(struct client *client, __u64 now);
static int is_send_allowed(struct client *client, __u64 now);
static int send_requests(struct client *client);
static int finish_request(struct client *client, struct homa_event *event);
static int is_reply_valid(struct client *client, struct homa_event *event);
static void print_interval(struct client *client, __u64 now);
static void print_summary(struct client *client);
static void print_latency(struct hist_state *hist);

int main(int argc, char **argv)
{
  struct client client = { 0 };
  int ret;

  if (homa_cham_parse(&client.config, argc, argv, 1) != 0)
    return 1;

  client.payload = calloc(client.config.msize ? client.config.msize : 1, 1);
  if (client.payload == NULL)
  {
    perror("Homa client payload");
    return 1;
  }

  client.fd = homa_cham_init(&client.config, &client.ctx);
  if (client.fd < 0)
  {
    perror("Homa client initialization");
    free(client.payload);
    return 1;
  }

  if (init_hist(&client.hist, MAX_RTT_US) != 0)
  {
    homa_lib_ctx_free(client.ctx);
    free(client.payload);
    return 1;
  }

  calibrate_tsc();
  printf("Homa benchmark: msize=%u, rate=%.2f Mb/s, mpending=%u\n",
      client.config.msize, client.config.rate, client.config.mpending);
  printf("One startup echo is excluded from measurements.\n");
  printf("Rates count request/reply payload bytes in one direction.\n");
  printf("RTTs include drained replies; throughput uses the load interval.\n");
  printf("RTT histogram cap: %u us\n", MAX_RTT_US);
  printf("Payload validation: %s\n",
      client.config.validate ? "enabled" : "disabled");
  fflush(stdout);

  ret = run_client(&client);
  if (client.start_ns != 0)
    print_summary(&client);

  if (homa_lib_close(client.ctx, client.fd) != 0)
  {
    perror("homa_lib_close");
    ret = -1;
  }

  homa_lib_ctx_free(client.ctx);
  hist_cleanup(&client.hist);
  free(client.payload);
  return ret == 0 ? 0 : 1;
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
      start_measurement(client, now);

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
    {
      update_tokens(client, now);
      if (now >= client->next_report_ns && now < client->end_ns)
        print_interval(client, now);
    }

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
    if (homa_lib_request(client->ctx, client->fd, client->payload,
        client->config.msize, (const struct sockaddr *) &client->config.remote,
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
      client->interval_sent++;
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
  if (client->config.validate)
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
    hist_add(&client->hist, rtt_us);
    if (now < client->end_ns)
    {
      client->window_received++;
      client->interval_received++;
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
      "tp=%llu rps | tp=%.2f Mb/s | RTT (us): ",
      (unsigned long long) ((now - client->start_ns) / NS_PER_SECOND),
      (unsigned long long) load, load * payload_mbits,
      (unsigned long long) throughput, throughput * payload_mbits);
  print_latency(&client->hist);
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

  printf("Cores               : 1\n");
  printf("Load interval (s)   : %.3f\n", seconds);
  printf("TX requests         : %llu\n", (unsigned long long) client->sent);
  printf("RX replies          : %llu\n", (unsigned long long) client->received);
  printf("Measured replies    : %llu\n",
      (unsigned long long) client->window_received);
  printf("RPC errors          : %llu\n", (unsigned long long) client->errors);
  printf("Unfinished RPCs     : %u\n", client->active);
  printf("Avg Load            : %llu rps %.2f Mb/s\n",
      (unsigned long long) load, load * payload_mbits);
  printf("Avg Throughput      : %llu rps %.2f Mb/s\n",
      (unsigned long long) throughput, throughput * payload_mbits);
  printf("RTT percentiles (us): ");
  print_latency(&client->hist);
  fflush(stdout);
}

static void print_latency(struct hist_state *hist)
{
  __u64 p50, p99, p999;

  if (!hist_percentiles(hist, &p50, &p99, &p999))
  {
    printf("p50=-  p99=-  p99.9=-\n");
    return;
  }

  printf("p50=%llu  p99=%llu  p99.9=%llu\n",
      (unsigned long long) p50, (unsigned long long) p99,
      (unsigned long long) p999);
}
