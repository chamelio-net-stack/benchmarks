#include <fcntl.h>
#include <sys/socket.h>

int set_reuseport(int fd)
{
    int flag;
    flag = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &flag, sizeof(flag));
}

int set_nonblock(int fd)
{
    int flag;

    if ((flag = fcntl(fd, F_GETFL, 0)) == -1) {
        return -1;
    }
    flag |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flag);
}