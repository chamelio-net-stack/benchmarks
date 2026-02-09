/*
 * Copyright 2019 University of Washington, Max Planck Institute for
 * Software Systems, and The University of Texas at Austin
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <protocol_binary.h>

#include "iokvs.h"

#define MAX_MSGSIZE 2048
#define EPOLL_EVENTS 16

static inline int set_nonblock(int fd)
{
    int flag;

    if ((flag = fcntl(fd, F_GETFL, 0)) == -1) {
        return -1;
    }
    flag |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flag);
}

static inline int set_reuseport(int fd)
{
    int flag = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &flag, sizeof(flag));
}

static int open_listening(int cn)
{
    int fd, ret;
    struct sockaddr_in si;

    if ((fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
        fprintf(stderr, "[%d] socket failed\n", cn);
        abort();
    }

    if (set_reuseport(fd) != 0) {
        fprintf(stderr, "[%d] set reuseport failed\n", cn);
        abort();
    }

    if ((ret = set_nonblock(fd)) < 0) {
        fprintf(stderr, "[%d] setsock_nonblock failed: %d\n", cn, ret);
        abort();
    }

    memset(&si, 0, sizeof(si));
    si.sin_family = AF_INET;
    si.sin_addr.s_addr = htonl(settings.srcip);
    si.sin_port = htons(settings.srcport);

    if ((ret = bind(fd, (struct sockaddr *) &si, sizeof(si))) < 0) {
        fprintf(stderr, "[%d] bind failed: %d\n", cn, ret);
        abort();
    }

    return fd;
}

static void network_init(int *p_epfd, int *p_lfd, int cn)
{
    int lfd, epfd;
    struct epoll_event ev;

    lfd = open_listening(cn);

    if ((epfd = epoll_create(4)) < 0) {
        fprintf(stderr, "[%d] epoll_create failed\n", cn);
        abort();
    }

    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    ev.data.ptr = NULL;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev) < 0) {
        fprintf(stderr, "[%d] epoll_ctl failed on listener\n", cn);
        abort();
    }

    *p_epfd = epfd;
    *p_lfd = lfd;
}

static int process_request(struct item_allocator *ia, const uint8_t *req_buf,
        size_t req_len, uint8_t *tx_buf, size_t *tx_len)
{
    const protocol_binary_request_header *req;
    protocol_binary_response_header *res;
    protocol_binary_response_get *gres;
    struct item *it;
    const uint8_t *k, *v;
    uint8_t op;
    uint16_t kl;
    uint32_t bl, vl, h;
    size_t body_off;

    if (req_len < sizeof(protocol_binary_request_header)) {
        return -1;
    }

    req = (const protocol_binary_request_header *) req_buf;
    res = (protocol_binary_response_header *) tx_buf;
    gres = (protocol_binary_response_get *) tx_buf;

    if (req->request.magic != PROTOCOL_BINARY_REQ) {
        return -1;
    }

    op = req->request.opcode;
    kl = ntohs(req->request.keylen);
    bl = ntohl(req->request.bodylen);
    body_off = sizeof(*req) + req->request.extlen;

    if (req_len != sizeof(*req) + bl || body_off > req_len ||
            body_off + kl > req_len)
    {
        return -1;
    }

    k = req_buf + body_off;

    if (op == PROTOCOL_BINARY_CMD_GET) {
        h = jenkins_hash(k, kl);
        it = hasht_get(k, kl, h);

        res->response.magic = PROTOCOL_BINARY_RES;
        res->response.opcode = PROTOCOL_BINARY_CMD_GET;
        res->response.keylen = htons(0);
        res->response.extlen = 4;
        res->response.datatype = 0;
        res->response.opaque = req->request.opaque;
        gres->message.body.flags = htonl(0);

        *tx_len = sizeof(*gres);
        if (it != NULL) {
            if (*tx_len + it->vallen > MAX_MSGSIZE) {
                res->response.status = htons(PROTOCOL_BINARY_RESPONSE_E2BIG);
                res->response.bodylen = htonl(4);
                item_unref(it);
                return 0;
            }

            res->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
            res->response.bodylen = htonl(4 + it->vallen);
            memcpy(gres + 1, item_value(it), it->vallen);
            *tx_len += it->vallen;
            item_unref(it);
        } else {
            res->response.status = htons(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
            res->response.bodylen = htonl(4);
        }

        return 0;
    } else if (op == PROTOCOL_BINARY_CMD_SET) {
        if (bl < req->request.extlen + kl) {
            return -1;
        }

        vl = bl - req->request.extlen - kl;
        v = k + kl;
        if ((size_t) (v - req_buf) + vl > req_len) {
            return -1;
        }

        it = ialloc_alloc(ia, sizeof(struct item) + kl + vl, false);
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

        res->response.magic = PROTOCOL_BINARY_RES;
        res->response.opcode = PROTOCOL_BINARY_CMD_SET;
        res->response.keylen = htons(0);
        res->response.extlen = 0;
        res->response.datatype = 0;
        res->response.status = htons(it == NULL ? PROTOCOL_BINARY_RESPONSE_ENOMEM :
                PROTOCOL_BINARY_RESPONSE_SUCCESS);
        res->response.bodylen = htonl(0);
        res->response.opaque = req->request.opaque;

        *tx_len = sizeof(protocol_binary_response_set);
        return 0;
    }

    return -1;
}

static size_t process_datagrams(struct item_allocator *ia, int lfd)
{
    uint8_t rx_buf[MAX_MSGSIZE];
    uint8_t tx_buf[MAX_MSGSIZE];
    size_t tx_len, handled;
    struct sockaddr_storage src_addr;
    socklen_t src_len;
    ssize_t ret;

    handled = 0;
    while (1) {
        src_len = sizeof(src_addr);
        ret = recvfrom(lfd, rx_buf, sizeof(rx_buf), 0,
                (struct sockaddr *) &src_addr, &src_len);
        if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else if (ret < 0) {
            perror("recvfrom failed");
            abort();
        }

        handled++;
        if (process_request(ia, rx_buf, ret, tx_buf, &tx_len) != 0) {
            continue;
        }

        ret = sendto(lfd, tx_buf, tx_len, 0, (struct sockaddr *) &src_addr,
                src_len);
        if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            continue;
        } else if (ret < 0) {
            perror("sendto failed");
            abort();
        }
    }

    return handled;
}

static size_t clean_log(struct item_allocator *ia, bool idle)
{
    struct item *it, *nit;
    size_t n;

    if (!idle) {
        /* We're starting processing for a new request */
        ialloc_cleanup_nextrequest(ia);
    }

    n = 0;
    while ((it = ialloc_cleanup_item(ia, idle)) != NULL) {
        n++;
        if (it->refcount != 1) {
            if ((nit = ialloc_alloc(ia, sizeof(*nit) + it->keylen + it->vallen,
                    true)) == NULL)
            {
                fprintf(stderr, "Warning: ialloc_alloc failed during cleanup :-/\n");
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

static struct item_allocator **iallocs;
static volatile size_t n_ready = 0;

static void *processing_thread(void *data)
{
    int i, epfd, lfd, n;
    struct item_allocator ia;
    size_t total_reqs = 0, total_clean = 0;
    static uint16_t qcounter;
    uint16_t q;
    struct epoll_event *evs;

    ialloc_init_allocator(&ia);
    q = __sync_fetch_and_add(&qcounter, 1);

    network_init(&epfd, &lfd, q);

    evs = calloc(EPOLL_EVENTS, sizeof(*evs));
    if (evs == NULL) {
        fprintf(stderr, "Allocating event buffer failed\n");
        abort();
    }

    iallocs[q] = &ia;
    __sync_fetch_and_add(&n_ready, 1);

    printf("Worker starting\n");

    while (1) {
        if ((n = epoll_wait(epfd, evs, EPOLL_EVENTS, 1)) < 0) {
            perror("epoll_wait failed");
            abort();
        }

        for (i = 0; i < n; i++) {
            if ((evs[i].events & (EPOLLERR | EPOLLHUP)) != 0) {
                fprintf(stderr, "[%u] epoll error on listener\n", q);
                abort();
            }
            if ((evs[i].events & EPOLLIN) == EPOLLIN) {
                total_reqs += process_datagrams(&ia, lfd);
            }
        }

        total_clean += clean_log(&ia, n == 0);
        (void) total_clean;
    }

    return NULL;
}

static void maintenance(void)
{
    size_t i, n;

    n = settings.numcores;
    while (1) {
        for (i = 0; i < n; i++) {
            ialloc_maintenance(iallocs[i]);
        }
        usleep(10);
    }
}

int main(int argc, char *argv[])
{
    char name[32];
    unsigned num_threads, i;
    pthread_t *pts;
    int ret;

    ret = settings_init(argc, argv);
    if (ret == SETTINGS_INIT_HELP) {
        return EXIT_SUCCESS;
    }
    if (ret != SETTINGS_INIT_OK) {
        return EXIT_FAILURE;
    }
    num_threads = settings.numcores;

    hasht_init();
    ialloc_init();

    iallocs = calloc(num_threads, sizeof(*iallocs));
    pts = calloc(num_threads, sizeof(*pts));
    for (i = 0; i < num_threads; i++) {
        if (pthread_create(pts + i, NULL, processing_thread, NULL)) {
            fprintf(stderr, "pthread_create failed\n");
            return EXIT_FAILURE;
        }

        snprintf(name, sizeof(name), "flexkvs-w%u", i);
        pthread_setname_np(pts[i], name);
    }

    while (n_ready < num_threads);
    printf("Starting maintenance\n");
    fflush(stdout);
    while (1) {
        maintenance();
    }
    return 0;
}
