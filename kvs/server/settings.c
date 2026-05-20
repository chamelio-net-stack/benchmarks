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
#include <limits.h>
#include <arpa/inet.h>

#include "iokvs.h"

struct settings settings;

static void settings_print_usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s THREADS [options]\n"
        "  THREADS: worker threads (1..255)\n"
        "  --ip IPv4 | --ip=IPv4\n"
        "  --port PORT | --port=PORT\n"
        "  --msize BYTES | --msize=BYTES\n"
        "  --max-conns COUNT | --max-conns=COUNT\n"
        "  --segsize BYTES | --segsize=BYTES\n"
        "  --segmaxnum COUNT | --segmaxnum=COUNT\n"
        "  --segcqsize COUNT | --segcqsize=COUNT\n"
        "  --clean-ratio VALUE | --clean-ratio=VALUE\n",
        prog);
}

static int parse_u32_arg(const char *name, const char *val, uint32_t min,
        uint32_t max, uint32_t *out)
{
    char *end;
    unsigned long n;

    errno = 0;
    n = strtoul(val, &end, 10);
    if (errno != 0 || *val == '\0' || *end != '\0' || n < min || n > max) {
        fprintf(stderr, "Invalid %s value: %s\n", name, val);
        return -1;
    }

    *out = (uint32_t) n;
    return 0;
}

static int parse_size_arg(const char *name, const char *val, size_t *out)
{
    char *end;
    unsigned long long n;

    errno = 0;
    n = strtoull(val, &end, 10);
    if (errno != 0 || *val == '\0' || *end != '\0' ||
            n > (unsigned long long) SIZE_MAX)
    {
        fprintf(stderr, "Invalid %s value: %s\n", name, val);
        return -1;
    }

    *out = (size_t) n;
    return 0;
}

static int parse_double_arg(const char *name, const char *val, double min,
        double max, double *out)
{
    char *end;
    double n;

    errno = 0;
    n = strtod(val, &end);
    if (errno != 0 || *val == '\0' || *end != '\0' || n < min || n > max) {
        fprintf(stderr, "Invalid %s value: %s\n", name, val);
        return -1;
    }

    *out = n;
    return 0;
}

int settings_init(int argc, char *argv[])
{
    int i;
    bool have_threads = false;

    settings.port = 11211;
    settings.bindip = htonl(INADDR_ANY);
    settings.max_msg_size = 2048;
    settings.max_conns = 128 * 1024;
    settings.verbose = 1;
    settings.segsize = 256 * 1024;
    settings.segmaxnum = 4096;
    settings.segcqsize = 32 * 1024;
    settings.clean_ratio = 0.8;

    if (argc < 2) {
        settings_print_usage(argv[0]);
        return -1;
    }

    for (i = 1; i < argc; i++) {
        const char *arg = argv[i];
        const char *val = NULL;
        uint32_t tmp_u32;
        struct in_addr in_addr;

        if (arg[0] != '-') {
            if (have_threads) {
                fprintf(stderr, "THREADS provided multiple times\n");
                return -1;
            }

            if (parse_u32_arg("THREADS", arg, 1, 255, &tmp_u32) != 0) {
                return -1;
            }
            settings.numcores = (uint8_t) tmp_u32;
            have_threads = true;
            continue;
        }

        if (strcmp(arg, "--ip") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--ip requires a value\n");
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--ip=", 5) == 0) {
            val = arg + 5;
        }
        if (val != NULL) {
            if (inet_pton(AF_INET, val, &in_addr) != 1) {
                fprintf(stderr, "Invalid IPv4 value: %s\n", val);
                return -1;
            }
            settings.bindip = in_addr.s_addr;
            continue;
        }

        val = NULL;
        if (strcmp(arg, "--port") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--port requires a value\n");
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--port=", 7) == 0) {
            val = arg + 7;
        }
        if (val != NULL) {
            if (parse_u32_arg("port", val, 1, 65535, &tmp_u32) != 0) {
                return -1;
            }
            settings.port = (uint16_t) tmp_u32;
            continue;
        }

        val = NULL;
        if (strcmp(arg, "--msize") == 0 || strcmp(arg, "--max-msize") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "%s requires a value\n", arg);
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--msize=", 8) == 0) {
            val = arg + 8;
        } else if (strncmp(arg, "--max-msize=", 12) == 0) {
            val = arg + 12;
        }
        if (val != NULL) {
            if (parse_u32_arg("msize", val, 64, UINT32_MAX, &tmp_u32) != 0) {
                return -1;
            }
            settings.max_msg_size = tmp_u32;
            continue;
        }

        val = NULL;
        if (strcmp(arg, "--max-conns") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--max-conns requires a value\n");
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--max-conns=", 12) == 0) {
            val = arg + 12;
        }
        if (val != NULL) {
            if (parse_u32_arg("max-conns", val, 1, INT_MAX - 1, &tmp_u32) != 0) {
                return -1;
            }
            settings.max_conns = (int) tmp_u32;
            continue;
        }

        val = NULL;
        if (strcmp(arg, "--segsize") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--segsize requires a value\n");
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--segsize=", 10) == 0) {
            val = arg + 10;
        }
        if (val != NULL) {
            if (parse_size_arg("segsize", val, &settings.segsize) != 0 ||
                    settings.segsize == 0)
            {
                return -1;
            }
            continue;
        }

        val = NULL;
        if (strcmp(arg, "--segmaxnum") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--segmaxnum requires a value\n");
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--segmaxnum=", 12) == 0) {
            val = arg + 12;
        }
        if (val != NULL) {
            if (parse_size_arg("segmaxnum", val, &settings.segmaxnum) != 0 ||
                    settings.segmaxnum == 0)
            {
                return -1;
            }
            continue;
        }

        val = NULL;
        if (strcmp(arg, "--segcqsize") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--segcqsize requires a value\n");
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--segcqsize=", 11) == 0) {
            val = arg + 11;
        }
        if (val != NULL) {
            if (parse_size_arg("segcqsize", val, &settings.segcqsize) != 0 ||
                    settings.segcqsize == 0)
            {
                return -1;
            }
            continue;
        }

        val = NULL;
        if (strcmp(arg, "--clean-ratio") == 0 || strcmp(arg, "--clean-ration") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "%s requires a value\n", arg);
                return -1;
            }
            val = argv[++i];
        } else if (strncmp(arg, "--clean-ratio=", 14) == 0) {
            val = arg + 14;
        } else if (strncmp(arg, "--clean-ration=", 15) == 0) {
            val = arg + 15;
        }
        if (val != NULL) {
            if (parse_double_arg("clean-ratio", val, 0.0, 1.0, &settings.clean_ratio) != 0 ||
                    settings.clean_ratio <= 0.0)
            {
                return -1;
            }
            continue;
        }

        if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
            settings_print_usage(argv[0]);
            return -1;
        }

        fprintf(stderr, "Unknown argument: %s\n", arg);
        settings_print_usage(argv[0]);
        return -1;
    }

    if (!have_threads) {
        fprintf(stderr, "Missing THREADS argument\n");
        settings_print_usage(argv[0]);
        return -1;
    }

    return 0;
}
