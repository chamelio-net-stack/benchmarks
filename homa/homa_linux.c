#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <sched.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <homa.h>
#include "homa_linux.h"
#include "../utils/core.h"

#define RECEIVE_POOL_BYTES (128 * HOMA_BPAGE_SIZE)

struct homa_linux
{
  int fd;
  int client;
  void *pool;
  unsigned char *message;
  struct sockaddr_in peer;
  struct homa_recvmsg_args recv;
};

static int send_message(struct homa_linux *ctx, int fd,
    const void *data, size_t len, const struct sockaddr *peer,
    socklen_t peerlen, __u64 *rpc_id);

static int parse_cores(struct homa_linux_config *config, const char *list);
static int parse_number(const char *text, unsigned int *value);
static int parse_rate(const char *text, double *value);
static int parse_address(struct sockaddr_in *addr, char *ip, char *port);

int homa_linux_parse(struct homa_linux_config *config, int argc, char **argv,
    int client)
{
  unsigned int value;
  int first = client ? 5 : 3;
  int i;
  const char *cores = NULL;

  memset(config, 0, sizeof(*config));
  config->msize = 64;
  config->duration = 30;
  config->mpending = HOMA_LINUX_DEFAULT_PENDING;
  config->rate = 10.0;
  config->core = -1;
  config->ncores = 1;

  if (argc < first ||
      parse_address(&config->local, argv[1], argv[2]) != 0)
    goto failed;

  if (client && parse_address(&config->remote, argv[3], argv[4]) != 0)
    goto failed;

  for (i = first; i < argc; i++)
  {
    if (client && strcmp(argv[i], "--validate") == 0)
    {
      config->validate = 1;
      continue;
    }

    if (i + 1 == argc)
      goto failed;

    if (strcmp(argv[i], "--cores") == 0)
    {
      cores = argv[++i];
      continue;
    }

    if (client && strcmp(argv[i], "--rate") == 0)
    {
      if (parse_rate(argv[++i], &config->rate) != 0)
        goto failed;
      continue;
    }

    if (parse_number(argv[i + 1], &value) != 0)
      goto failed;

    if (strcmp(argv[i], "--msize") == 0 && value <= HOMA_LINUX_MAX_MESSAGE)
    {
      config->msize = value;
    }
    else if (strcmp(argv[i], "--ncores") == 0)
    {
      if (value == 0 || value > HOMA_LINUX_MAX_CORES)
        goto failed;
      config->ncores = value;
    }
    else if (client && strcmp(argv[i], "--duration") == 0 && value > 0)
    {
      config->duration = value;
    }
    else if (client && strcmp(argv[i], "--mpending") == 0 &&
        value > 0 && value <= HOMA_LINUX_MAX_PENDING)
    {
      config->mpending = value;
    }
    else
    {
      goto failed;
    }

    i++;
  }

  if (client && config->msize == 0)
    goto failed;

  unsigned int local_port = ntohs(config->local.sin_port);
  unsigned int remote_port = ntohs(config->remote.sin_port);

  int local_valid = local_port == 0 ||
      local_port + config->ncores - 1 <= UINT16_MAX;

  int remote_valid = !client || (remote_port != 0 &&
      remote_port + config->ncores - 1 <= UINT16_MAX);

  int server_valid = client || config->ncores == 1 || local_port != 0;

  if (!local_valid || !remote_valid || !server_valid)
    goto failed;

  if (parse_cores(config, cores) != 0)
    return -1;

  return 0;

failed:
  fprintf(stderr, "Usage: %s <bind_ip> <port>%s"
      " [--msize 0..1000000] [--ncores N] [--cores CPU,...]%s\n", argv[0],
      client ? " <rem_ip> <rem_port>" : "",
      client ? " [--duration S] [--rate Mb/s]"
      " [--mpending 1..128] [--validate]" : "");
  return -1;
}

int homa_linux_init(const struct homa_linux_config *config,
    struct homa_linux **ctx)
{
  struct homa_set_buf_args buffer;
  struct homa_linux *transport;

  if (config->core >= 0 && pin_thread_to_core(config->core) != 0)
    return -1;
  transport = calloc(1, sizeof(*transport));
  if (transport == NULL)
    return -1;

  transport->fd = -1;
  transport->pool = MAP_FAILED;
  transport->client = config->remote.sin_family == AF_INET;
  transport->message = malloc(HOMA_LINUX_MAX_MESSAGE);

  if (transport->message == NULL)
    goto failed;

  transport->pool = mmap(NULL, RECEIVE_POOL_BYTES, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (transport->pool == MAP_FAILED)
    goto failed;

  transport->fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, IPPROTO_HOMA);
  if (transport->fd < 0)
    goto failed;

  buffer.start = transport->pool;
  buffer.length = RECEIVE_POOL_BYTES;
  if (setsockopt(transport->fd, IPPROTO_HOMA, SO_HOMA_SET_BUF,
      &buffer, sizeof(buffer)) != 0)
    goto failed;

  if (bind(transport->fd, (const struct sockaddr *) &config->local,
      sizeof(config->local)) != 0)
    goto failed;

  *ctx = transport;
  return transport->fd;

failed:
  {
    int saved_errno = errno;

    homa_linux_free(transport);
    errno = saved_errno;
    return -1;
  }
}

int homa_linux_poll(struct homa_linux *ctx,
    struct homa_linux_event *events, int maxevents)
{
  struct msghdr msg = { 0 };
  size_t copied = 0, length;
  ssize_t received;
  unsigned int i;

  if (maxevents <= 0)
    return 0;
  ctx->recv.id = 0;
  ctx->recv.flags = HOMA_RECVMSG_NONBLOCKING |
      (ctx->client ? HOMA_RECVMSG_RESPONSE : HOMA_RECVMSG_REQUEST);

  msg.msg_name = &ctx->peer;
  msg.msg_namelen = sizeof(ctx->peer);
  msg.msg_control = &ctx->recv;
  msg.msg_controllen = sizeof(ctx->recv);

  received = recvmsg(ctx->fd, &msg, MSG_DONTWAIT);
  if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK ||
      errno == EINTR))
    return 0;

  memset(events, 0, sizeof(*events));
  events->sockfd = ctx->fd;
  events->rpc_id = ctx->recv.id;
  events->peer = ctx->peer;

  if (received < 0)
  {
    if (ctx->recv.id == 0)
      return -1;
    events->type = HOMA_LINUX_ERROR;
    events->status = errno;
    return 1;
  }

  if (received > HOMA_LINUX_MAX_MESSAGE ||
      ctx->recv.num_bpages > HOMA_MAX_BPAGES)
  {
    errno = EMSGSIZE;
    return -1;
  }

  for (i = 0; copied < (size_t) received && i < ctx->recv.num_bpages; i++)
  {
    length = received - copied;
    if (length > HOMA_BPAGE_SIZE)
      length = HOMA_BPAGE_SIZE;
   
    if (ctx->recv.bpage_offsets[i] > RECEIVE_POOL_BYTES - length)
    {
      errno = EPROTO;
      return -1;
    }

    memcpy(ctx->message + copied,
        (char *) ctx->pool + ctx->recv.bpage_offsets[i], length);
    copied += length;
  }

  if (copied != (size_t) received)
  {
    errno = EPROTO;
    return -1;
  }

  events->type = ctx->client ? HOMA_LINUX_RESPONSE : HOMA_LINUX_REQUEST;
  events->data = ctx->message;
  events->len = received;

  return 1;
}

int homa_linux_request(struct homa_linux *ctx, int fd,
    const void *data, size_t len, const struct sockaddr *peer,
    socklen_t peerlen, __u64 *rpc_id)
{
  *rpc_id = 0;
  return send_message(ctx, fd, data, len, peer, peerlen, rpc_id);
}

int homa_linux_reply(struct homa_linux *ctx, int fd, __u64 rpc_id,
    const void *data, size_t len)
{
  return send_message(ctx, fd, data, len,
      (const struct sockaddr *) &ctx->peer, sizeof(ctx->peer), &rpc_id);
}

int homa_linux_release(struct homa_linux *ctx, int fd, __u64 rpc_id)
{
  /* recvmsg returns the previous receive pages on the next poll. */
  if (fd != ctx->fd || rpc_id != ctx->recv.id)
  {
    errno = EINVAL;
    return -1;
  }

  return 0;
}

int homa_linux_close(struct homa_linux *ctx, int fd)
{
  int ret = close(fd);

  if (fd == ctx->fd)
    ctx->fd = -1;

  return ret;
}

void homa_linux_free(struct homa_linux *ctx)
{
  if (ctx->fd >= 0)
    close(ctx->fd);

  if (ctx->pool != MAP_FAILED)
    munmap(ctx->pool, RECEIVE_POOL_BYTES);
  
  free(ctx->message);
  free(ctx);
}

static int parse_number(const char *text, unsigned int *value)
{
  unsigned long parsed;
  char *end;

  errno = 0;
  parsed = strtoul(text, &end, 10);
  if (errno != 0 || *text == '\0' || *text == '-' || *end != '\0' ||
      parsed > UINT_MAX)
    return -1;

  *value = parsed;
  return 0;
}

static int parse_address(struct sockaddr_in *addr, char *ip, char *port)
{
  unsigned int value;

  if (parse_number(port, &value) != 0 || value > 65535)
    return -1;

  addr->sin_family = AF_INET;
  addr->sin_port = htons(value);
  return inet_pton(AF_INET, ip, &addr->sin_addr) == 1 ? 0 : -1;
}

static int parse_rate(const char *text, double *value)
{
  char *end;
  double parsed;

  errno = 0;
  parsed = strtod(text, &end);
  if (errno != 0 || *text == '\0' || *end != '\0')
    return -1;

  if (!isfinite(parsed) || parsed <= 0.0)
    return -1;

  *value = parsed;
  return 0;
}

static int send_message(struct homa_linux *ctx, int fd,
    const void *data, size_t len, const struct sockaddr *peer,
    socklen_t peerlen, __u64 *rpc_id)
{
  struct homa_sendmsg_args args = { 0 };
  struct iovec iov = { .iov_base = (void *) data, .iov_len = len };
  struct msghdr msg = { 0 };

  if (fd != ctx->fd)
  {
    errno = EBADF;
    return -1;
  }

  args.id = *rpc_id;
  msg.msg_name = (void *) peer;
  msg.msg_namelen = peerlen;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = &args;

  if (sendmsg(fd, &msg, MSG_DONTWAIT) < 0)
    return -1;
  
  *rpc_id = args.id;
  return 0;
}

static int parse_cores(struct homa_linux_config *config, const char *list)
{
  unsigned int count = 0, cpu, i;
  char *copy, *next, *token;

  for (i = 0; i < config->ncores; i++)
    config->cores[i] = -1;

  if (list == NULL)
    return 0;

  copy = strdup(list);
  if (copy == NULL)
    return -1;

  next = copy;
  while ((token = strsep(&next, ",")) != NULL)
  {
    if (count == config->ncores || parse_number(token, &cpu) != 0 ||
        cpu >= CPU_SETSIZE)
      break;

    config->cores[count++] = cpu;
  }
  free(copy);

  if (token != NULL || count != config->ncores)
  {
    fprintf(stderr, "--cores requires one CPU per worker\n");
    return -1;
  }

  return 0;
}
