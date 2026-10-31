#include <eTran_rpc.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <vector>

#include "homa_etran.h"
extern "C"
{
#include "../utils/core.h"
}

/* Callbacks only lend their receive buffers. Preserve bytes and reply metadata
 * until the C benchmark releases the event. Wire RPC IDs may start at zero;
 * benchmark handles start at one and are independent of wire IDs. */
struct received_message
{
  homa_etran_event event = {};
  std::vector<unsigned char> data;
  std::unique_ptr<ReqHandle> request;
};

struct homa_etran
{
  std::unique_ptr<RpcSocket> socket;
  std::deque<received_message> received;
  __u64 next_id = 1;
};

static int transport_error(const std::exception &error)
{
  fprintf(stderr, "eTran Homa: %s\n", error.what());
  errno = EIO;
  return -1;
}

static void receive_message(struct homa_etran *ctx, const Buffer &buffer,
    const sockaddr_in &peer, __u64 id, ReqHandle *request)
{
  ctx->received.emplace_back();
  received_message &message = ctx->received.back();
  message.data.assign(buffer._buf, buffer._buf + buffer.actual_size);
  message.event.type = request ? HOMA_ETRAN_REQUEST : HOMA_ETRAN_RESPONSE;
  message.event.rpc_id = id;
  message.event.peer = peer;
  message.event.len = buffer.actual_size;
  if (request != nullptr)
    message.request.reset(new ReqHandle(*request));
}

int homa_etran_prepare(const struct homa_etran_config *config)
{
  struct eTran_cfg cfg = {};
  cfg.proto = IPPROTO_HOMA;
  cfg.nr_app_threads = config->ncores;
  cfg.nr_nic_queues = config->ncores;
  try
  {
    int ret = eTran_init(&cfg);
    if (ret != 0)
    {
      errno = ret < 0 ? -ret : EIO;
      return -1;
    }
    return 0;
  }
  catch (const std::exception &error)
  {
    return transport_error(error);
  }
}

int homa_etran_init(const struct homa_etran_config *config,
    struct homa_etran **ctx)
{
  char ip[INET_ADDRSTRLEN];

  if (config->core >= 0 && pin_thread_to_core(config->core) != 0)
    return -1;
  if (inet_ntop(AF_INET, &config->local.sin_addr, ip, sizeof(ip)) == nullptr)
    return -1;

  try
  {
    std::unique_ptr<homa_etran> transport(new homa_etran);
    transport->socket.reset(new RpcSocket(transport.get(), ip,
        ntohs(config->local.sin_port)));
    transport->socket->set_req_handler([](ReqHandle *request, void *arg)
    {
      auto *ctx = static_cast<homa_etran *>(arg);
      receive_message(ctx, request->buffer, request->dest_addr,
          ctx->next_id++, request);
    });
    *ctx = transport.release();
    /* Each worker owns exactly one RpcSocket; the C API uses a local token. */
    return 0;
  }
  catch (const std::exception &error)
  {
    return transport_error(error);
  }
}

int homa_etran_poll(struct homa_etran *ctx,
    struct homa_etran_event *events, int maxevents)
{
  if (maxevents <= 0)
    return 0;
  try
  {
    /* Deliver one event at a time, as the Linux adapter does. */
    if (ctx->received.empty())
      ctx->socket->run_event_loop();
    if (ctx->received.empty())
      return 0;
    received_message &message = ctx->received.front();
    *events = message.event;
    events->data = message.data.data();
    return 1;
  }
  catch (const std::exception &error)
  {
    return transport_error(error);
  }
}

int homa_etran_request(struct homa_etran *ctx, int fd,
    const void *data, size_t len, const struct sockaddr *peer,
    socklen_t peerlen, __u64 *rpc_id)
{
  (void) fd;
  if (peerlen != sizeof(sockaddr_in) || len == 0 ||
      len > HOMA_ETRAN_MAX_MESSAGE)
  {
    errno = EINVAL;
    return -1;
  }
  try
  {
    Buffer buffer = ctx->socket->alloc_buffer(len);
    if (buffer._buf == nullptr)
    {
      errno = ENOBUFS;
      return -1;
    }
    memcpy(buffer._buf, data, len);
    sockaddr_in destination = *reinterpret_cast<const sockaddr_in *>(peer);
    __u64 id = ctx->next_id++;
    ctx->socket->enqueue_request(buffer, &destination,
        [ctx, id](ContHandle *response, void *)
        {
          receive_message(ctx, response->buffer, response->dest_addr, id, nullptr);
        });
    /* enqueue_request owns the buffer, including when transmission is queued. */
    *rpc_id = id;
    return 0;
  }
  catch (const std::exception &error)
  {
    return transport_error(error);
  }
}

int homa_etran_reply(struct homa_etran *ctx, int fd, __u64 rpc_id,
    const void *data, size_t len)
{
  (void) fd;
  if (ctx->received.empty() || !ctx->received.front().request ||
      ctx->received.front().event.rpc_id != rpc_id || len == 0 ||
      len > HOMA_ETRAN_MAX_MESSAGE)
  {
    errno = EINVAL;
    return -1;
  }
  try
  {
    Buffer buffer = ctx->socket->alloc_buffer(len);
    if (buffer._buf == nullptr)
    {
      errno = ENOBUFS;
      return -1;
    }
    memcpy(buffer._buf, data, len);
    ctx->socket->enqueue_response(ctx->received.front().request.get(), buffer);
    return 0;
  }
  catch (const std::exception &error)
  {
    return transport_error(error);
  }
}

int homa_etran_release(struct homa_etran *ctx, int fd, __u64 rpc_id)
{
  (void) fd;
  if (ctx->received.empty() || ctx->received.front().event.rpc_id != rpc_id)
  {
    errno = EINVAL;
    return -1;
  }
  ctx->received.pop_front();
  return 0;
}

int homa_etran_close(struct homa_etran *ctx, int fd)
{
  (void) fd;
  ctx->socket.reset();
  return 0;
}

void homa_etran_free(struct homa_etran *ctx)
{
  delete ctx;
}
