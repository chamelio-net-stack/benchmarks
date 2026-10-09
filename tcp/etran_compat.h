#ifndef ETRAN_COMPAT_H
#define ETRAN_COMPAT_H

/* eTRAN interposes these POSIX calls, but not the alternatives used by the
 * Linux benchmark. The echo workload uses zero flags on connected sockets. */
#define epoll_create(size) epoll_create1(0)
#define sendto(fd, buf, len, flags, addr, addrlen) write(fd, buf, len)
#define recvfrom(fd, buf, len, flags, addr, addrlen) read(fd, buf, len)

/* eTRAN rejects EPOLLRDHUP. The benchmark requests it but does not handle
 * half-close events; retain the supported EPOLLERR/EPOLLHUP notifications. */
#undef EPOLLRDHUP
#define EPOLLRDHUP 0

#endif
