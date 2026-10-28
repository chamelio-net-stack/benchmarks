#ifndef HOMA_CHAM_H
#define HOMA_CHAM_H

#include <chamelio/homa_lib.h>

/* Maximum outstanding client RPCs supported by the single context. */
#define HOMA_CHAM_MAX_PENDING 128
/* Maximum benchmark workers. */
#define HOMA_CHAM_MAX_CORES 8
/* Default outstanding RPC limit. */
#define HOMA_CHAM_DEFAULT_PENDING 16
/* Maximum message payload size. */
#define HOMA_CHAM_MAX_MESSAGE 1000000

/* Arguments shared by the echo client and server. */
struct homa_cham_config
{
  /* Local endpoint. */
  struct sockaddr_in local;
  /* Remote endpoint, used by the client. */
  struct sockaddr_in remote;
  /* Message payload size, up to 1,000,000 bytes. */
  unsigned int msize;
  /* Timed load interval in seconds. */
  unsigned int duration;
  /* Maximum outstanding client RPCs. */
  unsigned int mpending;
  /* Target request payload rate per endpoint in megabits per second. */
  double rate;
  /* Check all echoed payload bytes. */
  int validate;
  /* Number of endpoints and application workers. */
  unsigned int ncores;
  /* Optional worker CPUs; -1 inherits process affinity. */
  int cores[HOMA_CHAM_MAX_CORES];
  /* Optional CPU for this worker; -1 leaves affinity unchanged. */
  int core;
};

/* Parse endpoints and measurement options. */
int homa_cham_parse(struct homa_cham_config *config, int argc, char **argv,
    int client);
/* Attach to Homa, create a context, and bind an endpoint. */
int homa_cham_init(const struct homa_cham_config *config,
    struct homa_context_lib **ctx);

#endif
