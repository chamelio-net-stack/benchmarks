#ifndef KVS_TRACE_H_
#define KVS_TRACE_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define KVS_TRACE_MAGIC 0x4b565354U
#define KVS_TRACE_VERSION 1
#define KVS_TRACE_MAX_EVENTS 22

enum kvs_trace_event_type {
    KVS_TRACE_CLIENT_REQ_GENERATED = 1,
    KVS_TRACE_CLIENT_BEFORE_SEND,
    KVS_TRACE_CLIENT_AFTER_HANDOFF,
    KVS_TRACE_CLIENT_RECVFROM,
    KVS_TRACE_CLIENT_RESPONSE_ACCOUNTED,
    KVS_TRACE_SERVER_REQUEST_PARSED,
    KVS_TRACE_SERVER_HASH_LOOKUP_START,
    KVS_TRACE_SERVER_HASH_LOOKUP_END,
    KVS_TRACE_SERVER_SET_ALLOC_START,
    KVS_TRACE_SERVER_SET_ALLOC_END,
    KVS_TRACE_SERVER_CLEANUP_START,
    KVS_TRACE_SERVER_CLEANUP_END,
    KVS_TRACE_SERVER_RESPONSE_GENERATED,
    KVS_TRACE_SERVER_BEFORE_SEND,
    KVS_TRACE_SERVER_AFTER_HANDOFF,
    KVS_TRACE_TCP_APP_TX_ACCEPTED,
    KVS_TRACE_TCP_RX_DELIVERED,
    KVS_TRACE_TCP_TX_SCHED_SENT,
    KVS_TRACE_TCP_ACK_PROCESSED,
    KVS_TRACE_TCP_INVALID_ACK,
    KVS_TRACE_TCP_DUPACK_THRESHOLD,
    KVS_TRACE_TCP_OUT_OF_ORDER,
    KVS_TRACE_TCP_DUP_PAYLOAD,
    KVS_TRACE_TCP_FLOW_BLOCKED,
    KVS_TRACE_TCP_QUEUE_FULL,
};

enum kvs_trace_where {
    KVS_TRACE_WHERE_CLIENT_APP = 1,
    KVS_TRACE_WHERE_CLIENT_TCP,
    KVS_TRACE_WHERE_SERVER_APP,
    KVS_TRACE_WHERE_SERVER_TCP,
    KVS_TRACE_WHERE_CHAMELIO_TCP,
};

struct kvs_trace_event {
    uint64_t tsc;
    uint8_t type;
    uint8_t where;
    int16_t sock;
    uint16_t arg0;
    uint16_t arg1;
} __attribute__((packed));

struct kvs_trace {
    uint32_t magic;
    uint32_t trace_id;
    uint16_t orig_bodylen;
    uint8_t count;
    uint8_t capacity;
    uint8_t version;
    uint8_t flags;
    uint16_t reserved;
    struct kvs_trace_event events[KVS_TRACE_MAX_EVENTS];
} __attribute__((packed));

static inline uint64_t kvs_trace_rdtsc(void)
{
    uint32_t eax;
    uint32_t edx;

    __asm__ __volatile__("rdtsc" : "=a" (eax), "=d" (edx));
    return ((uint64_t) edx << 32) | eax;
}

static inline uint32_t kvs_trace_bodylen(const uint8_t *msg)
{
    return ((uint32_t) msg[8] << 24) |
        ((uint32_t) msg[9] << 16) |
        ((uint32_t) msg[10] << 8) |
        (uint32_t) msg[11];
}

static inline void kvs_trace_set_bodylen(uint8_t *msg, uint32_t bodylen)
{
    msg[8] = (uint8_t) (bodylen >> 24);
    msg[9] = (uint8_t) (bodylen >> 16);
    msg[10] = (uint8_t) (bodylen >> 8);
    msg[11] = (uint8_t) bodylen;
}

static inline struct kvs_trace *kvs_trace_find_msg(const void *buf, size_t len)
{
    const uint8_t *msg = (const uint8_t *) buf;
    uint32_t bodylen;
    size_t msg_len;
    struct kvs_trace *tr;

    if (len < 24 + sizeof(*tr)) {
        return NULL;
    }
    if (msg[0] != 0x80 && msg[0] != 0x81) {
        return NULL;
    }

    bodylen = kvs_trace_bodylen(msg);
    msg_len = 24 + (size_t) bodylen;
    if (msg_len > len || bodylen < sizeof(*tr)) {
        return NULL;
    }

    tr = (struct kvs_trace *) (msg + msg_len - sizeof(*tr));
    if (tr->magic != KVS_TRACE_MAGIC ||
            tr->version != KVS_TRACE_VERSION ||
            tr->capacity != KVS_TRACE_MAX_EVENTS)
    {
        return NULL;
    }

    return tr;
}

static inline struct kvs_trace *kvs_trace_find_trailer(const void *buf,
    size_t len)
{
    struct kvs_trace *tr;

    if (len < sizeof(*tr)) {
        return NULL;
    }

    tr = (struct kvs_trace *) ((uint8_t *) buf + len - sizeof(*tr));
    if (tr->magic != KVS_TRACE_MAGIC ||
            tr->version != KVS_TRACE_VERSION ||
            tr->capacity != KVS_TRACE_MAX_EVENTS)
    {
        return NULL;
    }

    return tr;
}

static inline bool kvs_trace_add(struct kvs_trace *tr, uint8_t type,
    uint8_t where, int sock, uint16_t arg0, uint16_t arg1)
{
    struct kvs_trace_event *ev;

    if (tr == NULL || tr->magic != KVS_TRACE_MAGIC ||
            tr->count >= tr->capacity || tr->count >= KVS_TRACE_MAX_EVENTS)
    {
        return false;
    }

    ev = &tr->events[tr->count++];
    ev->tsc = kvs_trace_rdtsc();
    ev->type = type;
    ev->where = where;
    ev->sock = (sock < INT16_MIN || sock > INT16_MAX) ? -1 : (int16_t) sock;
    ev->arg0 = arg0;
    ev->arg1 = arg1;
    return true;
}

static inline void kvs_trace_init(struct kvs_trace *tr, uint32_t trace_id,
    uint16_t orig_bodylen)
{
    tr->magic = KVS_TRACE_MAGIC;
    tr->trace_id = trace_id;
    tr->orig_bodylen = orig_bodylen;
    tr->count = 0;
    tr->capacity = KVS_TRACE_MAX_EVENTS;
    tr->version = KVS_TRACE_VERSION;
    tr->flags = 0;
    tr->reserved = 0;
}

#endif
