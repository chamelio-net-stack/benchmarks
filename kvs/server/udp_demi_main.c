#define _GNU_SOURCE
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

#include <demi/libos.h>
#include <demi/sga.h>
#include <demi/wait.h>

#include <protocol_binary.h>

#include "iokvs.h"
#include "sock.h"

struct core {
    int fd;
    demi_qtoken_t rx_qt;
    bool rx_pending;
    uint16_t id;
    uint8_t *rx_buf;
    uint8_t *tx_buf;
    struct sockaddr_in peer_addr;
    socklen_t peer_addr_len;
    struct item_allocator ia;
    pthread_t pthread;
};

static struct core *cores;
static struct item_allocator **iallocs;
static volatile size_t n_ready = 0;
static const struct timespec zero_timeout = { .tv_sec = 0, .tv_nsec = 0 };

static int core_socket(uint16_t id)
{
    int fd;
    struct sockaddr_in si;
    int ret;

    ret = demi_socket(&fd, AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (ret != 0) {
        errno = ret;
        perror("demi_socket failed");
        fprintf(stderr, "[%u] socket failed\n", id);
        return -1;
    }

    memset(&si, 0, sizeof(si));
    si.sin_family = AF_INET;
    si.sin_addr.s_addr = settings.bindip;
    si.sin_port = htons(settings.port);

    ret = demi_bind(fd, (const struct sockaddr *) &si, sizeof(si));
    if (ret != 0) {
        errno = ret;
        perror("demi_bind failed");
        fprintf(stderr, "[%u] bind failed\n", id);
        return -1;
    }

    return fd;
}

static int core_rx(struct core *c, ssize_t *rx_len)
{
    demi_qresult_t qr;
    demi_sgarray_t *sga;
    int ret;
    ssize_t ret_len;

    if (!c->rx_pending) {
        ret = demi_pop(&c->rx_qt, c->fd);
        if (ret != 0) {
            errno = ret;
            perror("demi_pop failed");
            return -1;
        }
        c->rx_pending = true;
    }

    ret = demi_wait(&qr, c->rx_qt, &zero_timeout);
    if (ret == ETIMEDOUT) {
        return 0;
    }
    if (ret != 0) {
        errno = ret;
        perror("demi_wait pop failed");
        c->rx_pending = false;
        return -1;
    }
    c->rx_pending = false;

    if (qr.qr_opcode != DEMI_OPC_POP) {
        fprintf(stderr, "[%u] unexpected opcode on pop: %d\n",
            c->id, (int) qr.qr_opcode);
        return -1;
    }

    sga = &qr.qr_value.sga;
    if (sga->sga_numsegs != 1) {
        fprintf(stderr, "[%u] invalid pop segment count: %u\n",
            c->id, sga->sga_numsegs);
        demi_sgafree(sga);
        return -1;
    }

    ret_len = (ssize_t) sga->segments[0].data_len_bytes;
    if (ret_len < 0 || ret_len > (ssize_t) settings.max_msg_size) {
        fprintf(stderr, "[%u] invalid pop length: %zd\n", c->id, ret_len);
        demi_sgafree(sga);
        return -1;
    }

    c->peer_addr = sga->sga_addr;
    c->peer_addr_len = sizeof(c->peer_addr);
    memcpy(c->rx_buf, sga->segments[0].data_buf_ptr, (size_t) ret_len);

    ret = demi_sgafree(sga);
    if (ret != 0) {
        errno = ret;
        perror("demi_sgafree failed");
        return -1;
    }

    *rx_len = ret_len;
    return 1;
}

static int core_tx(struct core *c, size_t tx_len)
{
    demi_qtoken_t tx_qt;
    demi_qresult_t qr;
    demi_sgarray_t sga;
    int ret;

    if (tx_len > settings.max_msg_size) {
        fprintf(stderr, "[%u] tx_len too large: %zu\n", c->id, tx_len);
        return -1;
    }

    sga = demi_sgaalloc(tx_len);
    if (sga.sga_numsegs != 1 || sga.segments[0].data_len_bytes < tx_len) {
        fprintf(stderr, "[%u] demi_sgaalloc failed for tx_len=%zu\n",
            c->id, tx_len);
        return -1;
    }
    sga.sga_numsegs = 1;
    sga.segments[0].data_len_bytes = (uint32_t) tx_len;
    memcpy(sga.segments[0].data_buf_ptr, c->tx_buf, tx_len);

    ret = demi_pushto(&tx_qt, c->fd, &sga,
        (const struct sockaddr *) &c->peer_addr, c->peer_addr_len);
    if (ret != 0) {
        errno = ret;
        perror("demi_pushto failed");
        (void) demi_sgafree(&sga);
        return -1;
    }

    while (1) {
        ret = demi_wait(&qr, tx_qt, &zero_timeout);
        if (ret == ETIMEDOUT) {
            sched_yield();
            continue;
        }
        if (ret != 0) {
            errno = ret;
            perror("demi_wait push failed");
            (void) demi_sgafree(&sga);
            return -1;
        }
        if (qr.qr_opcode != DEMI_OPC_PUSH) {
            fprintf(stderr, "[%u] unexpected opcode on push: %d\n",
                c->id, (int) qr.qr_opcode);
            (void) demi_sgafree(&sga);
            return -1;
        }
        ret = demi_sgafree(&sga);
        if (ret != 0) {
            errno = ret;
            perror("demi_sgafree failed");
            return -1;
        }
        return 0;
    }
}

static size_t process_request(struct core *c, ssize_t rx_len)
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

    if (rx_len < (ssize_t) sizeof(*req)) {
        return 0;
    }

    req = (protocol_binary_request_header *) c->rx_buf;
    bl = ntohl(req->request.bodylen);
    total_len = sizeof(*req) + bl;
    if (total_len != (size_t) rx_len) {
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
    int ret;
    struct core *c = (struct core *) data;
    const struct demi_args args = {
        .argc = 0,
        .argv = NULL,
        .callback = NULL,
    };

    ret = demi_init(&args);
    if (ret != 0) {
        errno = ret;
        perror("demi_init failed");
        abort();
    }

    if ((c->fd = core_socket(c->id)) < 0) {
        abort();
    }

    c->rx_buf = malloc((size_t) settings.max_msg_size * 2);
    if (c->rx_buf == NULL) {
        fprintf(stderr, "[%u] rx/tx buffer allocation failed\n", c->id);
        abort();
    }
    c->tx_buf = c->rx_buf + settings.max_msg_size;
    c->rx_pending = false;
    c->rx_qt = 0;

    ialloc_init_allocator(&c->ia);

    iallocs[c->id] = &c->ia;
    __sync_fetch_and_add(&n_ready, 1);

    while (1) {
        ssize_t rx_len = 0;
        size_t tx_len;
        int rx_ret;

        rx_ret = core_rx(c, &rx_len);
        if (rx_ret < 0) {
            abort();
        }

        if (rx_ret == 0) {
            clean_log(&c->ia, true);
            continue;
        }

        tx_len = process_request(c, rx_len);
        if (tx_len != 0 && core_tx(c, tx_len) != 0) {
            abort();
        }

        clean_log(&c->ia, false);
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
    unsigned i;
    char name[32];

    if (settings_init(argc, argv) != 0) {
        return EXIT_FAILURE;
    }
    if (settings.numcores != 1) {
        fprintf(stderr,
            "Demikernel server is single-threaded, forcing THREADS=1\n");
        settings.numcores = 1;
    }

    hasht_init();
    ialloc_init();

    cores = calloc(settings.numcores, sizeof(*cores));
    iallocs = calloc(settings.numcores, sizeof(*iallocs));
    if (cores == NULL || iallocs == NULL) {
        fprintf(stderr, "allocation failed\n");
        return EXIT_FAILURE;
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
