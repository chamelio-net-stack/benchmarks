#ifndef HOMA_CHAM_H
#define HOMA_CHAM_H

#include <chamelio/homa_lib.h>

/* Arguments shared by the single-threaded echo client and server. */
struct homa_cham_config {
  /* Local endpoint. */
  struct sockaddr_in local;
  /* Remote endpoint, used by the client. */
  struct sockaddr_in remote;
  /* Message payload size, up to 1,000,000 bytes. */
  unsigned int msize;
  /* Number of requests in the client's bounded batch. */
  unsigned int requests;
  /* Optional CPU for the application, or -1. */
  int core;
};

/* Parse endpoint arguments and bounded echo options. */
int homa_cham_parse(struct homa_cham_config *config, int argc, char **argv,
    int client);
/* Attach to Homa, create a context, and bind an endpoint. */
int homa_cham_init(const struct homa_cham_config *config,
    struct homa_context_lib **ctx);

#endif
