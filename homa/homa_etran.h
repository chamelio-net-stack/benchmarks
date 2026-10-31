#ifndef HOMA_ETRAN_H
#define HOMA_ETRAN_H

#include <stddef.h>
#include <linux/types.h>
#include <netinet/in.h>

/* Maximum outstanding client RPCs supported by the single context. */
#define HOMA_ETRAN_MAX_PENDING 128
#define HOMA_ETRAN_MAX_CORES 8
/* Default outstanding RPC limit. */
#define HOMA_ETRAN_DEFAULT_PENDING 16
/* Maximum message payload size. */
#define HOMA_ETRAN_MAX_MESSAGE 1000000

/* Arguments shared by the echo client and server. */
struct homa_etran_config
{
  /* Local endpoint. */
  struct sockaddr_in local;
  /* Remote endpoint, used by the client. */
  struct sockaddr_in remote;
  /* Message payload size, 1..1,000,000 bytes. */
  unsigned int msize;
  /* Timed load interval in seconds. */
  unsigned int duration;
  /* Maximum outstanding client RPCs. */
  unsigned int mpending;
  /* Target request payload rate per endpoint in megabits per second. */
  double rate;
  /* Check all echoed payload bytes. */
  int validate;
  /* Number of workers */
  unsigned int ncores;
  /* Optional worker CPUs; -1 inherits process affinity. */
  int cores[HOMA_ETRAN_MAX_CORES];
  /* Optional CPU for this worker; -1 leaves affinity unchanged. */
  int core;
};

#ifdef __cplusplus
extern "C"
{
#endif

/* Register workers and NIC queues once, before starting worker threads. */
int homa_etran_prepare(const struct homa_etran_config *config);

/* Parse endpoints and measurement options. */
int homa_etran_parse(struct homa_etran_config *config, int argc, char **argv,
    int client);
/* eTran Homa endpoint and receive storage. */
struct homa_etran;

/* eTran Homa completion kinds. */
enum homa_etran_type
{
  HOMA_ETRAN_REQUEST,
  HOMA_ETRAN_RESPONSE,
  HOMA_ETRAN_ERROR,
};

/* One received message or failed RPC, valid until release. */
struct homa_etran_event
{
  /* Request, response, or error. */
  enum homa_etran_type type;
  /* Worker-local endpoint token. */
  int sockfd;
  /* Benchmark RPC identifier. */
  __u64 rpc_id;
  /* Remote endpoint. */
  struct sockaddr_in peer;
  /* Received message bytes. */
  const void *data;
  /* Received message length. */
  size_t len;
  /* Positive errno for an error event. */
  int status;
};

/* Open and bind one endpoint, returning a worker-local token. */
int homa_etran_init(const struct homa_etran_config *config,
    struct homa_etran **ctx);
/* Poll without blocking; return a count or -1 with errno. */
int homa_etran_poll(struct homa_etran *ctx,
    struct homa_etran_event *events, int maxevents);
/* Submit a request and return its RPC identifier. */
int homa_etran_request(struct homa_etran *ctx, int fd,
    const void *data, size_t len, const struct sockaddr *peer,
    socklen_t peerlen, __u64 *rpc_id);
/* Reply to a received request. */
int homa_etran_reply(struct homa_etran *ctx, int fd, __u64 rpc_id,
    const void *data, size_t len);
/* Release a received message. */
int homa_etran_release(struct homa_etran *ctx, int fd, __u64 rpc_id);
/* Close the endpoint. */
int homa_etran_close(struct homa_etran *ctx, int fd);
/* Free the endpoint and transport state. */
void homa_etran_free(struct homa_etran *ctx);

#ifdef __cplusplus
}
#endif

#endif
