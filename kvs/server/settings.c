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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>

#include "iokvs.h"

struct settings settings;

static void settings_print_usage(void)
{
    fprintf(stderr,
            "Usage: flexkvs THREADS [PORT] [--ip IP --port PORT]\n"
            "Options:\n"
            "  -h, --help        Print this help message and exit.\n"
            "  THREADS           Number of worker threads (1..255).\n"
            "  PORT              UDP listen port (1..65535).\n"
            "  --ip IP           IPv4 address to bind (default 0.0.0.0).\n"
            "  --port PORT       UDP listen port (overrides PORT).\n");
}

static int parse_ipv4_addr(const char *addr_s, uint32_t *addr_out)
{
    struct in_addr addr;

    if (inet_pton(AF_INET, addr_s, &addr) != 1) {
        return -1;
    }

    *addr_out = ntohl(addr.s_addr);
    return 0;
}

int settings_init(int argc, char *argv[])
{
    char *end;
    unsigned long n, p;
    int i;
    int have_src_port = 0;

    settings.udpport = 11211;
    settings.srcip = 0;
    settings.srcport = 11211;
    settings.verbose = 1;
    settings.segsize = 256 * 1024;
    settings.segmaxnum = 4096;
    settings.segcqsize = 32 * 1024;
    settings.clean_ratio = 0.8;

    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) {
            settings_print_usage();
            return SETTINGS_INIT_HELP;
        }
    }

    if (argc < 2) {
        settings_print_usage();
        return SETTINGS_INIT_ERR;
    }

    errno = 0;
    n = strtoul(argv[1], &end, 10);
    if (errno != 0 || argv[1][0] == '\0' || *end != '\0' || n == 0 || n > 255) {
        fprintf(stderr, "Invalid thread count: %s\n", argv[1]);
        return SETTINGS_INIT_ERR;
    }
    settings.numcores = (uint8_t) n;

    for (i = 2; i < argc; i++) {
        if (!strcmp(argv[i], "--ip")) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--ip requires an argument\n");
                return SETTINGS_INIT_ERR;
            }
            if (parse_ipv4_addr(argv[++i], &settings.srcip) != 0) {
                fprintf(stderr, "Invalid server IPv4 address: %s\n", argv[i]);
                return SETTINGS_INIT_ERR;
            }
        } else if (!strcmp(argv[i], "--port")) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--port requires an argument\n");
                return SETTINGS_INIT_ERR;
            }
            errno = 0;
            p = strtoul(argv[++i], &end, 10);
            if (errno != 0 || argv[i][0] == '\0' || *end != '\0' ||
                    p == 0 || p > 65535)
            {
                fprintf(stderr, "Invalid server UDP port: %s\n", argv[i]);
                return SETTINGS_INIT_ERR;
            }
            settings.srcport = (uint16_t) p;
            settings.udpport = settings.srcport;
            have_src_port = 1;
        } else if (i == 2 && argv[i][0] != '-') {
            errno = 0;
            n = strtoul(argv[i], &end, 10);
            if (errno != 0 || argv[i][0] == '\0' || *end != '\0' ||
                    n == 0 || n > 65535)
            {
                fprintf(stderr, "Invalid UDP port: %s\n", argv[i]);
                return SETTINGS_INIT_ERR;
            }
            settings.udpport = (uint16_t) n;
            settings.srcport = settings.udpport;
            have_src_port = 1;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            settings_print_usage();
            return SETTINGS_INIT_ERR;
        }
    }

    if (!have_src_port) {
        settings.srcport = settings.udpport;
    }

    return SETTINGS_INIT_OK;
}
