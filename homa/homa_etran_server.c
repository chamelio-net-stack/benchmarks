#include <errno.h>
#include <stdio.h>

#include "homa_etran.h"
#include "homa_workers.h"

static int server_worker(void *arg)
{
  struct homa_etran_config config = *(struct homa_etran_config *) arg;
  struct homa_etran *ctx;
  struct homa_etran_event event;
  int fd, ret;

  fd = homa_etran_init(&config, &ctx);
  if (fd < 0)
  {
    perror("Homa server initialization");
    return 1;
  }

  while (1)
  {
    ret = homa_etran_poll(ctx, &event, 1);
    if (ret < 0)
    {
      perror("homa_etran_poll");
      break;
    }

    if (ret == 0)
      continue;

    struct homa_startup startup;
    int control = 0;

    if (event.type == HOMA_ETRAN_REQUEST &&
        event.status == 0 && event.len == sizeof(startup))
    {
      memcpy(&startup, event.data, sizeof(startup));
      control = ntohl(startup.magic) == HOMA_STARTUP_MAGIC;
    }

    if (event.type != HOMA_ETRAN_REQUEST || event.sockfd != fd ||
        event.status != 0 || (!control && event.len != config.msize))
    {
      fprintf(stderr, "Unexpected Homa request\n");
      break;
    }

    if (control)
      startup.ncores = htonl(config.ncores);

    do
    {
      ret = homa_etran_reply(ctx, fd, event.rpc_id,
          control ? (void *) &startup : event.data, event.len);
    } while (ret != 0 && errno == EAGAIN);

    if (ret != 0)
    {
      perror("Homa echo reply");
      break;
    }

    do
    {
      ret = homa_etran_release(ctx, fd, event.rpc_id);
    } while (ret != 0 && errno == EAGAIN);

    if (ret != 0)
    {
      perror("homa_etran_release");
      break;
    }
  }

  homa_etran_free(ctx);
  return 1;
}

int main(int argc, char **argv)
{
  struct homa_etran_config config, configs[HOMA_ETRAN_MAX_CORES];
  struct homa_worker workers[HOMA_ETRAN_MAX_CORES];
  unsigned int i;

  if (homa_etran_parse(&config, argc, argv, 0) != 0)
    return 1;
  if (homa_etran_prepare(&config) != 0)
  {
    perror("eTran initialization");
    /* Upstream eTran_exit assumes initialization completed. Avoid its
     * shared-library destructor accessing partially initialized state. */
    _Exit(EXIT_FAILURE);
  }

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
