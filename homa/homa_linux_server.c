#include <errno.h>
#include <stdio.h>

#include "homa_linux.h"

int main(int argc, char **argv)
{
  struct homa_linux_config config;
  struct homa_linux *ctx;
  struct homa_linux_event event;
  int fd, ret;

  if (homa_linux_parse(&config, argc, argv, 0) != 0)
    return 1;

  fd = homa_linux_init(&config, &ctx);
  if (fd < 0)
  {
    perror("Homa server initialization");
    return 1;
  }

  printf("Homa echo server ready (msize=%u, ncores=1)\n",
      config.msize);
  fflush(stdout);

  while (1)
  {
    ret = homa_linux_poll(ctx, &event, 1);
    if (ret < 0)
    {
      perror("homa_linux_poll");
      break;
    }

    if (ret == 0)
      continue;

    if (event.type != HOMA_LINUX_REQUEST || event.sockfd != fd ||
        event.status != 0 || event.len != config.msize)
    {
      fprintf(stderr, "Unexpected Homa request\n");
      break;
    }

    do
    {
      ret = homa_linux_reply(ctx, fd, event.rpc_id, event.data, event.len);
    } while (ret != 0 && errno == EAGAIN);

    if (ret != 0)
    {
      perror("Homa echo reply");
      break;
    }

    do
    {
      ret = homa_linux_release(ctx, fd, event.rpc_id);
    } while (ret != 0 && errno == EAGAIN);

    if (ret != 0)
    {
      perror("homa_linux_release");
      break;
    }
  }

  homa_linux_free(ctx);
  return 1;
}
