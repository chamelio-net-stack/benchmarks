#ifndef RPC_SERVICES_H_
#define RPC_SERVICES_H_

#include <linux/types.h>

/* Service IDs registered with the RPC server */
#define SERVICE_ECHO    1
#define SERVICE_COMPUTE 2

/* Arithmetic operations for SERVICE_COMPUTE */
#define COMPUTE_OP_ADD 0
#define COMPUTE_OP_MUL 1
#define COMPUTE_OP_MAX 2

/*
 * SERVICE_COMPUTE (2)
 * Client sends two u64 operands and an op code; server returns the result.
 *
 * Both req and resp share tsc at offset 0 so the client's RTT measurement
 * works without knowing which struct it holds.
 *
 * service_id is embedded in the payload at offset 8 because rpc_handle_call()
 * strips the RPC wire header before handing the payload to the application.
 */
struct compute_req
{
  __u64 tsc;        /* RTT timestamp */
  __u8  service_id; /* SERVICE_COMPUTE */
  __u8  op;         /* COMPUTE_OP_ADD / MUL / MAX */
  __u8  _pad[6];    //padding to align operands to 16 bytes for better performance
  __u64 a;
  __u64 b;
} __attribute__((packed));

struct compute_resp
{
  __u64 tsc;        /* echoed from request for RTT measurement */
  __u8  service_id; /* SERVICE_COMPUTE */
  __u8  status;     /* 0 = ok, 1 = unknown op */
  __u8  _pad[6];    //padding to align result to 16 bytes for better performance
  __u64 result;
} __attribute__((packed));

#define COMPUTE_REQ_MIN_MSIZE ((size_t)sizeof(struct compute_req))

#endif /* RPC_SERVICES_H_ */
