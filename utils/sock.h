#ifndef UTILS_SOCK_H_
#define UTILS_SOCK_H_

int set_reuseport(int fd);
int set_nonblock(int fd);

#endif