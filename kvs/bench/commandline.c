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
#include <string.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <stdint.h>

#include "benchmark.h"

static int parse_ipv4(const char *s, uint32_t *ip);

void print_usage(void)
{
    fprintf(stderr,
        "./benchmark [options] dst-ip:dst-port\n"
        "Options:\n"
        "  -t, --threads=COUNT   Number of sending threads [default 1].\n"
        "  -R, --rate=KREQPS     UDP per-thread/TCP per-connection rate [default 1000].\n"
        "      --nconns=COUNT    TCP connections per thread [default 1].\n"
        "      --mpending=COUNT  TCP pending connects per thread [default 1].\n"
        "  -k, --key-size=BYTES  Key size in bytes         [default 32].\n"
        "  -n, --key-num=COUNT   Number of keys            [default 1000].\n"
        "  -u, --key-uniform     Uniform key distribution  [default]\n"
        "  -z, --key-zipf=S      Zipf key distribution;\n"
        "                        S is the zipf parameter.\n"
        "  -v, --val-size=BYTES  Value size in bytes       [default 1024].\n"
        "  -g, --get-prob=PROB   Probability of GET Reqs.  [default .9].\n"
        "  -T, --time=SECS       Measurement time in [s].  [default 10].\n"
        "  -D, --duration=SECS   Benchmark duration in [s] [default 10].\n"
        "  -w, --warmup=SECS     Warmup time [s].          [default 5].\n"
        "  -c, --cooldown=SECS   Cooldown time [s].        [default 5].\n"
        "  -s, --key-seed=SEED   Seed for key PRG.\n"
        "  -o, --op-seed=SEED    Seed for operation PRG.\n"
        "  -S, --src-ip=IP       Source IPv4 address to bind [default 0.0.0.0].\n"
        "  -P, --src-port=PORT   Source port to bind         [default 0, -1 disables bind].\n"
        "  -r, --trace=FILE      Write operation trace to file.\n"
        "      --trace-slow-us=US Print request-carried trace for slow responses.\n"
        "      --trace-sample=N   Trace every Nth request when slow tracing is on [default 1].\n"
        "  -K, --keysteer        Key-based steering.\n"
        "  -B  --bypass-arp      Bypass ARP wait");
}

void init_settings(struct settings *s)
{
    s->threads = 1;
    s->rate = 1000;
    s->nconns = 1;
    s->mpending = 1;
    s->keysize = 32;
    s->keynum = 1000;
    s->keydist = DIST_UNIFORM;
    s->valuesize = 1024;
    s->get_prob = 0.9;
    s->warmup_time = 5;
    s->cooldown_time = 5;
    s->run_time = 10;
    s->key_seed = 0x123457890123ULL;
    s->op_seed =  0x987654321098ULL;
    s->srcip = INADDR_ANY;
    s->srcport = 0;
    s->keybased = false;
    s->bypass_arp = false;
    s->trace_slow_us = 0;
    s->trace_sample = 1;
}

int parse_settings(int argc, char *argv[], struct settings *s)
{
    static struct option long_opts[] = {
            {"threads",     required_argument, NULL, 't'},
            {"rate",        required_argument, NULL, 'R'},
            {"nconns",      required_argument, NULL, 1000},
            {"mpending",    required_argument, NULL, 1001},
            {"key-size",    required_argument, NULL, 'k'},
            {"key-num",     required_argument, NULL, 'n'},
            {"key-uniform", no_argument,       NULL, 'u'},
            {"key-zipf",    required_argument, NULL, 'z'},
            {"val-size",    required_argument, NULL, 'v'},
            {"get-prob",    required_argument, NULL, 'g'},
            {"time",        required_argument, NULL, 'T'},
            {"duration",    required_argument, NULL, 'D'},
            {"warmup",      required_argument, NULL, 'w'},
            {"cooldown",    required_argument, NULL, 'c'},
            {"key-seed",    required_argument, NULL, 's'},
            {"op-seed",     required_argument, NULL, 'o'},
            {"src-ip",      required_argument, NULL, 'S'},
            {"src-port",    required_argument, NULL, 'P'},
            {"trace-slow-us", required_argument, NULL, 1002},
            {"trace-sample", required_argument, NULL, 1003},
            {"keysteer",    no_argument,       NULL, 'K'},
            {"bypass-arp",  no_argument,       NULL, 'B'},
        };
    static const char *short_opts = "t:R:k:n:uz:v:g:T:D:w:c:s:o:S:P:r:K:B";
    int c, opt_idx, done = 0;
    char *end;

    while (!done) {
        c = getopt_long(argc, argv, short_opts, long_opts, &opt_idx);
        switch (c) {
            case 't':
                s->threads = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->threads < 1) {
                    fprintf(stderr, "threads needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case 'R':
                s->rate = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->rate < 1) {
                    fprintf(stderr, "rate needs to be a positive integer "
                            "(kreq/s)\n");
                    return -1;
                }
                break;
            case 1000:
                s->nconns = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->nconns < 1) {
                    fprintf(stderr, "nconns needs to be a positive integer\n");
                    return -1;
                }
                break;
            case 1001:
                s->mpending = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->mpending < 1) {
                    fprintf(stderr, "mpending needs to be a positive integer\n");
                    return -1;
                }
                break;
            case 1002:
                s->trace_slow_us = strtoul(optarg, &end, 10);
                if (!*optarg || *end) {
                    fprintf(stderr, "trace-slow-us needs to be an integer\n");
                    return -1;
                }
                break;
            case 1003:
                s->trace_sample = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->trace_sample < 1) {
                    fprintf(stderr, "trace-sample needs to be a positive integer\n");
                    return -1;
                }
                break;
            case 'k':
                s->keysize = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->keysize < 1) {
                    fprintf(stderr, "Key size needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case 'n':
                s->keynum = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->keynum < 1) {
                    fprintf(stderr, "Key count needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case 'v':
                s->valuesize = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->valuesize < 1) {
                    fprintf(stderr, "Value size needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;

            case 'u':
                s->keydist = DIST_UNIFORM;
                break;
            case 'z':
                s->keydist = DIST_ZIPF;
                s->keydistparams.zipf.s = strtod(optarg, &end);
                if (!*optarg || *end) {
                    fprintf(stderr, "Zipf parameter needs to be a floating "
                            "point number.\n");
                    return -1;
                }
                break;
            case'g':
                s->get_prob = strtod(optarg, &end);
                if (!*optarg || *end || s->get_prob < 0 || s->get_prob > 1) {
                    fprintf(stderr, "GET probability needs to be a floating "
                            "point number between 0 and 1.\n");
                    return -1;
                }
                break;
            case 'T':
                s->run_time = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->run_time < 1) {
                    fprintf(stderr, "Run time needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case 'D':
                s->run_time = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->run_time < 1) {
                    fprintf(stderr, "Duration needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case 'w':
                s->warmup_time = strtoul(optarg, &end, 10);
                if (!*optarg || *end) {
                    fprintf(stderr, "Warmup time needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case 'c':
                s->cooldown_time = strtoul(optarg, &end, 10);
                if (!*optarg || *end) {
                    fprintf(stderr, "Cool down time needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case 's':
                s->key_seed = strtoull(optarg, &end, 0);
                if (!*optarg || *end) {
                    fprintf(stderr, "Key seed needs to be an integer.\n");
                    return -1;
                }
                break;
            case 'o':
                s->op_seed = strtoull(optarg, &end, 0);
                if (!*optarg || *end) {
                    fprintf(stderr, "Op seed needs to be an integer.\n");
                    return -1;
                }
                break;
            case 'S':
                if (parse_ipv4(optarg, &s->srcip) != 0) {
                    fprintf(stderr, "Parsing source ip address failed\n");
                    return -1;
                }
                break;
            case 'P': {
                long port = strtol(optarg, &end, 10);
                if (!*optarg || *end || port < -1 || port > UINT16_MAX) {
                    fprintf(stderr, "Source port must be in range [-1, 65535].\n");
                    return -1;
                }
                s->srcport = (int) port;
                break;
            }
            case 'K':
                settings.keybased = true;
                break;
            case 'B':
                settings.bypass_arp = true;
                break;
            case -1:
                done = 1;
                break;
            case '?':
                return -1;
            default:
                abort();
        }
    }

    if (optind + 1 != argc) {
        return -1;
    }

    /* separate ip and port at colon */
    if ((end = strchr(argv[optind], ':')) == NULL) {
        fprintf(stderr, "Colon separating IP and port not found\n");
        return -1;
    }
    *end = '\0';
    end++;

    /* parse ip */
    if (parse_ipv4(argv[optind], &s->dstip) != 0) {
        fprintf(stderr, "Parsing ip address failed\n");
        return -1;
    }

    /* parse port */
    s->dstport = strtoul(end, NULL, 10);

    // TODO: ensure key size / key num combination is valid

    return 0;
}

static int parse_ipv4(const char *s, uint32_t *ip)
{
  if (inet_pton(AF_INET, s, ip) != 1) 
  {
    return -1;
  }
  
  *ip = htonl(*ip);
  return 0;
}
