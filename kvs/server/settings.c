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

#include "iokvs.h"

struct settings settings;

int settings_init(int argc, char *argv[])
{
    char *end;
    unsigned long n;

    settings.udpport = 11211;
    settings.verbose = 1;
    settings.segsize = 256 * 1024;
    settings.segmaxnum = 4096;
    settings.segcqsize = 32 * 1024;
    settings.clean_ratio = 0.8;

    if (argc != 2) {
        fprintf(stderr,
            "Usage: flexkvs THREADS [--ip IPv4] [--port PORT]\n"
            "  THREADS: number of worker threads (1..255)\n"
            "  --ip: bind IPv4 address (default 0.0.0.0)\n"
            "  --port: bind TCP port (default 11211)\n");
        return -1;
    }

    errno = 0;
    n = strtoul(argv[1], &end, 10);
    if (errno != 0 || argv[1][0] == '\0' || *end != '\0' || n == 0 || n > 255) {
        fprintf(stderr, "Invalid THREADS value: %s\n", argv[1]);
        return -1;
    }

    settings.numcores = (uint8_t) n;
    return 0;
}
