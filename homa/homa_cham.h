#ifndef HOMA_CHAM_H
#define HOMA_CHAM_H

#include <chamelio/homa_lib.h>

/* Maximum outstanding client RPCs supported by the single context. */
#define HOMA_CHAM_MAX_PENDING 128
/* Default outstanding RPC limit. */
#define HOMA_CHAM_DEFAULT_PENDING 16
/* Maximum message payload size. */
#define HOMA_CHAM_MAX_MESSAGE 1000000

/* Arguments shared by the single-threaded echo client and server. */
struct homa_cham_config {
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
  /* Target request payload rate in megabits per second. */
  double rate;
  /* Check all echoed payload bytes. */
  int validate;
  /* Optional CPU for the application, or -1. */
  int core;
};

/* Parse endpoints and measurement options. */
int homa_cham_parse(struct homa_cham_config *config, int argc, char **argv,
    int client);
/* Attach to Homa, create a context, and bind an endpoint. */
int homa_cham_init(const struct homa_cham_config *config,
    struct homa_context_lib **ctx);

#endif
