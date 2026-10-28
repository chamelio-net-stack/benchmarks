#include <errno.h>
#include <stdio.h>

#include "homa_linux.h"
#include "homa_workers.h"

static int server_worker(void *arg)
{
  struct homa_linux_config config = *(struct homa_linux_config *) arg;
  struct homa_linux *ctx;
  struct homa_linux_event event;
  int fd, ret;

  fd = homa_linux_init(&config, &ctx);
  if (fd < 0)
  {
    perror("Homa server initialization");
    return 1;
  }

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

    struct homa_startup startup;
    int control = 0;

    if (event.type == HOMA_LINUX_REQUEST &&
        event.status == 0 && event.len == sizeof(startup))
    {
      memcpy(&startup, event.data, sizeof(startup));
      control = ntohl(startup.magic) == HOMA_STARTUP_MAGIC;
    }

    if (event.type != HOMA_LINUX_REQUEST || event.sockfd != fd ||
        event.status != 0 || (!control && event.len != config.msize))
    {
      fprintf(stderr, "Unexpected Homa request\n");
      break;
    }

    if (control)
      startup.ncores = htonl(config.ncores);

    do
    {
      ret = homa_linux_reply(ctx, fd, event.rpc_id,
          control ? (void *) &startup : event.data, event.len);
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

int main(int argc, char **argv)
{
  struct homa_linux_config config, configs[HOMA_LINUX_MAX_CORES];
  struct homa_worker workers[HOMA_LINUX_MAX_CORES];
  unsigned int i;

  if (homa_linux_parse(&config, argc, argv, 0) != 0)
    return 1;

  for (i = 0; i < config.ncores; i++)
  {
    configs[i] = config;
    configs[i].core = config.cores[i];

    if (config.local.sin_port != 0)
      configs[i].local.sin_port = htons(ntohs(config.local.sin_port) + i);
    
    workers[i].config = &configs[i];
    workers[i].run = server_worker;
    workers[i].client = 0;
  }
  printf("Homa echo server: msize=%u, ncores=%u\n",
      config.msize, config.ncores);
  fflush(stdout);
  return homa_run_workers(workers, config.ncores);
}
