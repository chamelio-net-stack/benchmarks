#ifndef UTILS_PORTS_H_
#define UTILS_PORTS_H_

#include <stddef.h>
#include <stdint.h>

/* Caller owns *ports on success. Returns -1 for an invalid list. */
int parse_ports(const char *text, uint16_t **ports, size_t *count);

#endif
