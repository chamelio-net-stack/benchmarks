#define _GNU_SOURCE
#include <chamelio/udp_lib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <sched.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include <protocol_binary.h>

#include "iokvs.h"
#include "sock.h"

#define CHAM_DUMMY_BATCH_SIZE 32

struct core {
    int fd;
    uint16_t id;
    uint64_t rx_malformed;
    uint8_t *rx_buf;
    uint8_t *tx_buf;
    struct sockaddr_in peer_addr;
    socklen_t peer_addr_len;
    struct item_allocator ia;
    pthread_t pthread;
    struct udp_context_lib *ctx;
};

static struct core *cores;
static struct item_allocator **iallocs;
static volatile size_t n_ready = 0;

static int core_socket(struct core *c)
{
    uint16_t id;
    int fd, ret;
    struct sockaddr_in si;

    id = c->id;
    if ((fd = udp_socket(c->ctx)) < 0) {
        fprintf(stderr, "[%u] socket failed\n", id);
        return -1;
    }

    ret = udp_setsockopt(c->ctx, fd, SO_REUSEPORT);
    if (ret < 0)
    {
        fprintf(stderr, "failed to set sockopt\n");
        abort();
    }

    memset(&si, 0, sizeof(si));
    si.sin_family = AF_INET;
    si.sin_addr.s_addr = settings.bindip;
    si.sin_port = htons(settings.udpport);

    if (udp_bind(c->ctx, fd, (struct sockaddr *) &si, sizeof(si)) != 0) {
        perror("bind failed");
        fprintf(stderr, "[%u] bind failed\n", id);
        return -1;
    }

    /* Core 0 resolves ARP */
    int arp_size = 8;
    if (c->id == 0)
    {
        ssize_t n;
        while (udp_poll_fast(c->ctx) == 0) {}
        n = udp_recvfrom(c->ctx, fd, c->rx_buf, arp_size,
            (struct sockaddr *) &c->peer_addr, sizeof(c->peer_addr));
        udp_sendto(c->ctx, fd, c->rx_buf, (size_t) n,
            (struct sockaddr *) &c->peer_addr, sizeof(c->peer_addr));
        printf("ARP initialized\n");
    }

    return fd;
}

static int core_rx(struct core *c, ssize_t *rx_len, bool do_poll)
{
    ssize_t ret;

    c->peer_addr_len = sizeof(c->peer_addr);

    if (do_poll) {
        udp_poll_fast(c->ctx);
    }
    ret = udp_recvfrom(c->ctx, c->fd, c->rx_buf, settings.max_msg_size,
        (struct sockaddr *) &c->peer_addr, c->peer_addr_len);

    if (ret == 0)
        return 0;

    if (ret < 0) {
        fprintf(stderr, "errror when receiving\n");
        return -1;
    }
    
    *rx_len = ret;
    return 1;
}

static int core_tx(struct core *c, size_t tx_len)
{
    ssize_t ret;

    while (1) {
        ret = udp_sendto(c->ctx, c->fd, c->tx_buf, tx_len,
            (const struct sockaddr *) &c->peer_addr, c->peer_addr_len);
        if (ret == (ssize_t) tx_len) {
            return 0;
        }

        if (ret == 0 ||
            (ret < 0 &&
             (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)))
        {
            udp_poll_fast(c->ctx);
            continue;
        }

        if (ret > 0) {
            fprintf(stderr, "[%u] short udp_sendto: %zd of %zu\n",
                c->id, ret, tx_len);
        } else {
            fprintf(stderr, "[%u] udp_sendto failed errno=%d\n", c->id, errno);
        }
        return -1;
    }
}

enum request_parse_result {
    REQ_PARSE_ERR = -1,
    REQ_PARSE_NEED_MORE = 0,
    REQ_PARSE_OK = 1,
};

static int parse_one_request(const uint8_t *buf, size_t len, size_t *reqlen)
{
    const protocol_binary_request_header *req;

    if (len < sizeof(*req)) {
        return REQ_PARSE_NEED_MORE;
    }

    req = (const protocol_binary_request_header *) buf;
    if (req->request.magic != PROTOCOL_BINARY_REQ) {
        return REQ_PARSE_ERR;
    }

    *reqlen = sizeof(*req) + ntohl(req->request.bodylen);
    if (*reqlen > settings.max_msg_size) {
        return REQ_PARSE_ERR;
    }

    if (len < *reqlen) {
        return REQ_PARSE_NEED_MORE;
    }

    return REQ_PARSE_OK;
}

static size_t process_request(struct core *c, const uint8_t *req_buf,
    size_t rx_len)
{
    protocol_binary_request_header *req;
    protocol_binary_response_header *res;
    protocol_binary_response_get *gres;
    uint16_t kl;
    uint8_t extlen;
    uint8_t op;
    uint32_t bl;
    uint32_t vl;
    uint32_t h;
    size_t total_len;
    size_t tx_len;
    void *k;
    void *v;
    struct item *it;

    req = (protocol_binary_request_header *) req_buf;
    bl = ntohl(req->request.bodylen);
    total_len = sizeof(*req) + bl;
    if (total_len != rx_len) {
        return 0;
    }

    if (req->request.magic != PROTOCOL_BINARY_REQ) {
        return 0;
    }

    kl = ntohs(req->request.keylen);
    extlen = req->request.extlen;
    if ((uint32_t) extlen + kl > bl) {
        return 0;
    }

    op = req->request.opcode;
    res = (protocol_binary_response_header *) c->tx_buf;
    gres = (protocol_binary_response_get *) c->tx_buf;

    if (op == PROTOCOL_BINARY_CMD_GET) {
        k = (uint8_t *) (req + 1) + extlen;
        h = jenkins_hash(k, kl);
        it = hasht_get(k, kl, h);

        tx_len = sizeof(*gres);
        if (it != NULL) {
            tx_len += it->vallen;
        }

        if (tx_len > settings.max_msg_size) {
            if (it != NULL) {
                item_unref(it);
            }
            return 0;
        }

        res->response.magic = PROTOCOL_BINARY_RES;
        res->response.opcode = PROTOCOL_BINARY_CMD_GET;
        res->response.keylen = htons(0);
        res->response.extlen = 4;
        res->response.datatype = 0;
        if (it != NULL) {
            res->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
            res->response.bodylen = htonl(4 + it->vallen);
        } else {
            res->response.status = htons(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
            res->response.bodylen = htonl(4);
        }
        res->response.opaque = req->request.opaque;
        gres->message.body.flags = htonl(0);

        if (it != NULL) {
            memcpy(gres + 1, item_value(it), it->vallen);
            item_unref(it);
        }

        return tx_len;
    }

    if (op == PROTOCOL_BINARY_CMD_SET) {
        k = (uint8_t *) (req + 1) + extlen;
        v = (uint8_t *) k + kl;
        vl = bl - kl - extlen;

        it = ialloc_alloc(&c->ia, sizeof(struct item) + kl + vl, false);
        if (it != NULL) {
            h = jenkins_hash(k, kl);

            it->hv = h;
            it->vallen = vl;
            it->keylen = kl;
            memcpy(item_key(it), k, kl);
            memcpy(item_value(it), v, vl);

            hasht_put(it, NULL);
            item_unref(it);
        }

        tx_len = sizeof(protocol_binary_response_set);
        if (tx_len > settings.max_msg_size) {
            return 0;
        }

        res->response.magic = PROTOCOL_BINARY_RES;
        res->response.opcode = PROTOCOL_BINARY_CMD_SET;
        res->response.keylen = htons(0);
        res->response.extlen = 0;
        res->response.datatype = 0;
        res->response.status = htons(it != NULL ?
            PROTOCOL_BINARY_RESPONSE_SUCCESS : PROTOCOL_BINARY_RESPONSE_ENOMEM);
        res->response.bodylen = htonl(0);
        res->response.opaque = req->request.opaque;
        return tx_len;
    }

    return 0;
}

static size_t clean_log(struct item_allocator *ia, bool idle)
{
    struct item *it;
    struct item *nit;
    size_t n;

    if (!idle) {
        ialloc_cleanup_nextrequest(ia);
    }

    n = 0;
    while ((it = ialloc_cleanup_item(ia, idle)) != NULL) {
        n++;
        if (it->refcount != 1) {
            nit = ialloc_alloc(ia, sizeof(*nit) + it->keylen + it->vallen, true);
            if (nit == NULL) {
                fprintf(stderr, "Warning: ialloc_alloc failed during cleanup\n");
                abort();
            }

            nit->hv = it->hv;
            nit->vallen = it->vallen;
            nit->keylen = it->keylen;
            memcpy(item_key(nit), item_key(it), it->keylen + it->vallen);
            hasht_put(nit, it);
            item_unref(nit);
        }
        item_unref(it);
    }

    return n;
}

static void *processing_thread(void *data)
{
    struct udp_context_lib *udp_ctx;
    struct core *c = (struct core *) data;

    /* Each core has its own UDP context and socket */
    udp_ctx = udp_ctx_new();
    if (udp_ctx == NULL)
    {
        fprintf(stderr, "core %d: failed to create UDP context\n", c->id);
        pthread_exit((void *) (intptr_t) -1);
    }
    c->ctx = udp_ctx;

    c->rx_buf = malloc((size_t) settings.max_msg_size * 2);
    if (c->rx_buf == NULL) {
        fprintf(stderr, "[%u] rx/tx buffer allocation failed\n", c->id);
        abort();
    }
    c->tx_buf = c->rx_buf + settings.max_msg_size;

    ialloc_init_allocator(&c->ia);

    if ((c->fd = core_socket(c)) < 0) {
        abort();
    }

    iallocs[c->id] = &c->ia;
    __sync_fetch_and_add(&n_ready, 1);

    while (1) {
        size_t i;
        int rx_ret;

        for (i = 0; i < CHAM_DUMMY_BATCH_SIZE; i++) {
            ssize_t rx_len = 0;
            size_t parsed = 0;

            rx_ret = core_rx(c, &rx_len, i == 0);
            if (rx_ret < 0) {
                abort();
            }
            if (rx_ret == 0) {
                break;
            }

            while (parsed < (size_t) rx_len) {
                size_t reqlen;
                size_t tx_len;
                int parse_ret;

                parse_ret = parse_one_request(c->rx_buf + parsed,
                    (size_t) rx_len - parsed, &reqlen);
                if (parse_ret == REQ_PARSE_NEED_MORE) {
                    break;
                }
                if (parse_ret == REQ_PARSE_ERR) {
                    c->rx_malformed++;
                    if (c->rx_malformed <= 3) {
                        fprintf(stderr,
                            "[%u] malformed request byte (rx_len=%zd parsed=%zu byte=0x%02x)\n",
                            c->id, rx_len, parsed, c->rx_buf[parsed]);
                    }
                    parsed++;
                    continue;
                }

                tx_len = process_request(c, c->rx_buf + parsed, reqlen);
                if (tx_len != 0) {
                    if (core_tx(c, tx_len) != 0) {
                        abort();
                    }
                    clean_log(&c->ia, false);
                }
                parsed += reqlen;
            }
        }

        if (i == 0) {
            clean_log(&c->ia, true);
        }
    }

    return NULL;
}

static void maintenance(void)
{
    size_t i;

    while (1) {
        for (i = 0; i < settings.numcores; i++) {
            ialloc_maintenance(iallocs[i]);
        }
        usleep(10);
    }
}

int main(int argc, char *argv[])
{
    int ret;
    unsigned i;
    char name[32];

    if (settings_init(argc, argv) != 0) {
        return EXIT_FAILURE;
    }

    hasht_init();
    ialloc_init();

    cores = calloc(settings.numcores, sizeof(*cores));
    iallocs = calloc(settings.numcores, sizeof(*iallocs));
    if (cores == NULL || iallocs == NULL) {
        fprintf(stderr, "allocation failed\n");
        return EXIT_FAILURE;
    }

    ret = udp_connect_slow();
    if (ret != 0)
    {
        fprintf(stderr, "failed to connect to Chamelio");
        exit(EXIT_FAILURE);
    }

    for (i = 0; i < settings.numcores; i++) {
        cores[i].id = (uint16_t) i;
        if (pthread_create(&cores[i].pthread, NULL, processing_thread, &cores[i]) != 0) {
            fprintf(stderr, "pthread_create failed\n");
            return EXIT_FAILURE;
        }

        snprintf(name, sizeof(name), "flexkvs-w%u", i);
        pthread_setname_np(cores[i].pthread, name);
    }

    while (n_ready < settings.numcores) {
        sched_yield();
    }

    printf("Starting maintenance\n");
    fflush(stdout);
    maintenance();
    return 0;
}
