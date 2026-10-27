#ifndef HOMA_LINUX_H
#define HOMA_LINUX_H

#include <stddef.h>
#include <linux/types.h>
#include <netinet/in.h>

/* Maximum outstanding client RPCs supported by the single context. */
#define HOMA_LINUX_MAX_PENDING 128
/* Default outstanding RPC limit. */
#define HOMA_LINUX_DEFAULT_PENDING 16
/* Maximum message payload size. */
#define HOMA_LINUX_MAX_MESSAGE 1000000

/* Arguments shared by the single-threaded echo client and server. */
struct homa_linux_config
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
  /* Target request payload rate in megabits per second. */
  double rate;
  /* Check all echoed payload bytes. */
  int validate;
  /* Optional CPU for the application, or -1. */
  int core;
};

/* Parse endpoints and measurement options. */
int homa_linux_parse(struct homa_linux_config *config, int argc, char **argv,
    int client);
/* Linux Homa endpoint and receive storage. */
struct homa_linux;

/* Linux Homa completion kinds. */
enum homa_linux_type
{
  HOMA_LINUX_REQUEST,
  HOMA_LINUX_RESPONSE,
  HOMA_LINUX_ERROR,
};

/* One received message or failed RPC, valid until release. */
struct homa_linux_event
{
  /* Request, response, or error. */
  enum homa_linux_type type;
  /* Endpoint descriptor. */
  int sockfd;
  /* Kernel RPC identifier. */
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

/* Open and bind one endpoint, returning its descriptor. */
int homa_linux_init(const struct homa_linux_config *config,
    struct homa_linux **ctx);
/* Poll without blocking; return a count or -1 with errno. */
int homa_linux_poll(struct homa_linux *ctx,
    struct homa_linux_event *events, int maxevents);
/* Submit a request and return its RPC identifier. */
int homa_linux_request(struct homa_linux *ctx, int fd,
    const void *data, size_t len, const struct sockaddr *peer,
    socklen_t peerlen, __u64 *rpc_id);
/* Reply to a received request. */
int homa_linux_reply(struct homa_linux *ctx, int fd, __u64 rpc_id,
    const void *data, size_t len);
/* Release a received message. */
int homa_linux_release(struct homa_linux *ctx, int fd, __u64 rpc_id);
/* Close the endpoint. */
int homa_linux_close(struct homa_linux *ctx, int fd);
/* Free the endpoint and transport state. */
void homa_linux_free(struct homa_linux *ctx);

#endif
