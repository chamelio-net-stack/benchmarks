#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "homa_cham.h"
#include "../utils/clock.h"

#define HOMA_ECHO_WINDOW 16

static int submit_request(struct homa_context_lib *ctx, int fd,
    const struct homa_cham_config *config, unsigned int index, __u64 *id);

int main(int argc, char **argv)
{
  struct homa_cham_config config;
  struct homa_context_lib *ctx;
  struct homa_event event;
  __u64 ids[HOMA_ECHO_WINDOW] = { 0 }, deadline;
  unsigned int indices[HOMA_ECHO_WINDOW];
  unsigned int i, j, limit, submitted = 0, active = 0, completed = 0;
  int fd, ret;

  if (homa_cham_parse(&config, argc, argv, 1) != 0)
    return 1;

  fd = homa_cham_init(&config, &ctx);
  if (fd < 0)
  {
    perror("Homa client initialization");
    return 1;
  }

  deadline = now_ns() + 5000000000ULL;
  while (completed < config.requests)
  {
    if (now_ns() >= deadline)
    {
      fprintf(stderr, "Timed out: %u/%u replies\n", completed,
          config.requests);
      goto failed;
    }

    /* Resolve ARP with the first RPC before allowing a burst. */
    limit = completed == 0 ? 1 : HOMA_ECHO_WINDOW;
    for (i = 0; i < HOMA_ECHO_WINDOW && submitted < config.requests &&
        active < limit; i++)
    {
      if (ids[i] != 0)
        continue;

      if (submit_request(ctx, fd, &config, submitted, &ids[i]) != 0)
      {
        if (errno == EAGAIN || errno == ENOBUFS)
          break;

        perror("homa_lib_request");
        goto failed;
      }

      indices[i] = submitted++;
      active++;
    }

    ret = homa_lib_poll(ctx, &event, 1);
    if (ret < 0)
    {
      perror("homa_lib_poll");
      goto failed;
    }

    if (ret == 0)
      continue;

    for (i = 0; i < HOMA_ECHO_WINDOW; i++)
    {
      if (ids[i] == event.rpc_id)
        break;
    }

    if (i == HOMA_ECHO_WINDOW ||
        event.type != HOMA_EVENT_RESPONSE || event.sockfd != fd ||
        event.status != 0 || event.len != config.msize ||
        event.peer.sin_addr.s_addr != config.remote.sin_addr.s_addr ||
        event.peer.sin_port != config.remote.sin_port)
    {
      fprintf(stderr, "Unexpected Homa reply\n");
      goto failed;
    }

    for (j = 0; j < config.msize; j++)
    {
      unsigned char expected = indices[i] + j;

      if (((const unsigned char *) event.data)[j] != expected)
      {
        fprintf(stderr, "Echo payload mismatch in request %u, byte %u\n",
            indices[i], j);
        goto failed;
      }
    }

    do
    {
      ret = homa_lib_release(ctx, fd, event.rpc_id);
    } while (ret != 0 && errno == EAGAIN);

    if (ret != 0)
    {
      perror("homa_lib_release");
      goto failed;
    }

    ids[i] = 0;
    active--;
    completed++;
  }

  printf("Verified %u Homa replies (%u bytes each)\n", completed, config.msize);
  if (homa_lib_close(ctx, fd) != 0)
  {
    perror("homa_lib_close");
    goto failed;
  }

  homa_lib_ctx_free(ctx);
  return 0;

failed:
  homa_lib_ctx_free(ctx);
  return 1;
}

static int submit_request(struct homa_context_lib *ctx, int fd,
    const struct homa_cham_config *config, unsigned int index, __u64 *id)
{
  unsigned char payload[1000000];
  unsigned int j;

  for (j = 0; j < config->msize; j++)
    payload[j] = (unsigned char) (index + j);

  if (homa_lib_request(ctx, fd, payload, config->msize,
      (const struct sockaddr *) &config->remote, sizeof(config->remote),
      id) != 0)
  {
    return -1;
  }

  return 0;
}
