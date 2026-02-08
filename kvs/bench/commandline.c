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

#include "benchmark.h"

static int parse_ipv4_addr(const char *addr_s, uint32_t *addr_out)
{
    struct in_addr addr;

    if (inet_pton(AF_INET, addr_s, &addr) != 1) {
        return -1;
    }

    *addr_out = ntohl(addr.s_addr);
    return 0;
}

void print_usage(void)
{
    fprintf(stderr,
        "./benchmark [options] dst-ip:dst-port\n"
        "Options:\n"
        "  -t, --threads=COUNT   Number of sending threads [default 1].\n"
        "  -C, --connns=COUNT    # connections / thread    [default 1].\n"
        "      --rate=RPS        Request rate / thread      [default 0=max].\n"
        "  -k, --key-size=BYTES  Key size in bytes         [default 32].\n"
        "  -n, --key-num=COUNT   Number of keys            [default 1000].\n"
        "  -u, --key-uniform     Uniform key distribution  [default]\n"
        "  -z, --key-zipf=S      Zipf key distribution;\n"
        "                        S is the zipf parameter.\n"
        "  -v, --val-size=BYTES  Value size in bytes       [default 1024].\n"
        "  -g, --get-prob=PROB   Probability of GET Reqs.  [default .9].\n"
        "  -d, --duration=SECS   Benchmark duration in [s] [default 0].\n"
        "      --src-ip=IP       Source/bind IPv4 address  [default 0.0.0.0].\n"
        "      --src-port=PORT   Source/bind UDP port      [default 0].\n"
        "      --rem-ip=IP       Remote IPv4 address (alt to dst-ip:dst-port).\n"
        "      --rem-port=P      Remote UDP port (alt to dst-ip:dst-port).\n"
        "  -w, --warmup=SECS     Warmup time [s].          [default 5].\n"
        "  -c, --cooldown=SECS   Cooldown time [s].        [default 5].\n"
        "  -s, --key-seed=SEED   Seed for key PRG.\n"
        "  -o, --op-seed=SEED    Seed for operation PRG.\n"
        "  -r, --trace=FILE      Write operation trace to file.\n"
        "  -K, --keysteer        Key-based steering.\n");
}

void init_settings(struct settings *s)
{
    s->srcip = 0;
    s->srcport = 0;
    s->threads = 1;
    s->conns = 1;
    s->rate = 0.0;
    s->keysize = 32;
    s->keynum = 1000;
    s->keydist = DIST_UNIFORM;
    s->valuesize = 1024;
    s->get_prob = 0.9;
    s->warmup_time = 5;
    s->cooldown_time = 5;
    s->run_time = 0;
    s->request_gap = 100 * 1000;
    s->key_seed = 0x123457890123ULL;
    s->op_seed =  0x987654321098ULL;
    s->keybased = false;
    s->batchsize = 32;
}

int parse_settings(int argc, char *argv[], struct settings *s)
{
    enum {
        OPT_SRC_IP = 1000,
        OPT_SRC_PORT,
        OPT_REMOTE_IP,
        OPT_REMOTE_PORT,
        OPT_RATE,
    };
    static struct option long_opts[] = {
            {"threads",     required_argument, NULL, 't'},
            {"conns",       required_argument, NULL, 'C'},
            {"rate",        required_argument, NULL, OPT_RATE},
            {"key-size",    required_argument, NULL, 'k'},
            {"key-num",     required_argument, NULL, 'n'},
            {"key-uniform", no_argument,       NULL, 'u'},
            {"key-zipf",    required_argument, NULL, 'z'},
            {"val-size",    required_argument, NULL, 'v'},
            {"get-prob",    required_argument, NULL, 'g'},
            {"duration",    required_argument, NULL, 'd'},
            {"src-ip",      required_argument, NULL, OPT_SRC_IP},
            {"src-port",    required_argument, NULL, OPT_SRC_PORT},
            {"rem-ip",      required_argument, NULL, OPT_REMOTE_IP},
            {"rem-port",    required_argument, NULL, OPT_REMOTE_PORT},
            {"remote-ip",   required_argument, NULL, OPT_REMOTE_IP},
            {"remote-port", required_argument, NULL, OPT_REMOTE_PORT},
            {"warmup",      required_argument, NULL, 'w'},
            {"cooldown",    required_argument, NULL, 'c'},
            {"key-seed",    required_argument, NULL, 's'},
            {"op-seed",     required_argument, NULL, 'o'},
            {"keysteer",    no_argument,       NULL, 'K'},
        };
    static const char *short_opts = "t:C:k:n:uz:v:g:d:w:c:s:o:r:K";
    int c, opt_idx, done = 0;
    char *end;
    unsigned long port_ul;
    uint32_t srcip = 0, dstip = 0;
    uint16_t srcport = 0, dstport = 0;
    int have_src_ip = 0, have_src_port = 0;
    int have_dst_ip = 0, have_dst_port = 0;
    int opt_dst_ip = 0, opt_dst_port = 0;

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
            case 'C':
                s->conns = strtoul(optarg, &end, 10);
                if (!*optarg || *end || s->conns < 1) {
                    fprintf(stderr, "conns needs to be a positive "
                            "integer\n");
                    return -1;
                }
                break;
            case OPT_RATE:
                s->rate = strtod(optarg, &end);
                if (!*optarg || *end || s->rate < 0.0) {
                    fprintf(stderr, "rate needs to be a non-negative "
                            "number (requests/s)\n");
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
            case 'd':
                s->run_time = strtoul(optarg, &end, 10);
                if (!*optarg || *end) {
                    fprintf(stderr, "Run time needs to be a non-negative "
                            "integer\n");
                    return -1;
                }
                break;
            case OPT_SRC_IP:
                if (parse_ipv4_addr(optarg, &srcip) != 0) {
                    fprintf(stderr, "Parsing source ip address failed\n");
                    return -1;
                }
                have_src_ip = 1;
                break;
            case OPT_SRC_PORT:
                port_ul = strtoul(optarg, &end, 10);
                if (!*optarg || *end || port_ul == 0 || port_ul > 65535) {
                    fprintf(stderr, "Source port needs to be an integer.\n");
                    return -1;
                }
                srcport = (uint16_t) port_ul;
                have_src_port = 1;
                break;
            case OPT_REMOTE_IP:
                if (parse_ipv4_addr(optarg, &dstip) != 0) {
                    fprintf(stderr, "Parsing remote ip address failed\n");
                    return -1;
                }
                have_dst_ip = 1;
                opt_dst_ip = 1;
                break;
            case OPT_REMOTE_PORT:
                port_ul = strtoul(optarg, &end, 10);
                if (!*optarg || *end || port_ul == 0 || port_ul > 65535) {
                    fprintf(stderr, "Remote port needs to be an integer.\n");
                    return -1;
                }
                dstport = (uint16_t) port_ul;
                have_dst_port = 1;
                opt_dst_port = 1;
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
            case 'K':
                settings.keybased = true;
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

    if (optind + 1 == argc) {
        if (opt_dst_ip || opt_dst_port) {
            fprintf(stderr, "cannot mix positional dst-ip:dst-port with "
                    "--remote-ip/--remote-port\n");
            return -1;
        }
        /* separate ip and port at colon */
        if ((end = strchr(argv[optind], ':')) == NULL) {
            fprintf(stderr, "Colon separating IP and port not found\n");
            return -1;
        }
        *end = '\0';
        end++;

        if (parse_ipv4_addr(argv[optind], &dstip) != 0) {
            fprintf(stderr, "Parsing ip address failed\n");
            return -1;
        }
        port_ul = strtoul(end, &end, 10);
        if (*end != '\0' || port_ul == 0 || port_ul > 65535) {
            fprintf(stderr, "Parsing remote port failed\n");
            return -1;
        }
        dstport = (uint16_t) port_ul;
        have_dst_ip = have_dst_port = 1;
    } else if (optind != argc) {
        return -1;
    }

    if (!have_dst_ip || !have_dst_port) {
        fprintf(stderr, "remote endpoint required (dst-ip:dst-port or "
                "--rem-ip/--rem-port)\n");
        return -1;
    }
    if (!have_src_ip) {
        srcip = 0;
    }
    if (!have_src_port) {
        srcport = 0;
    }
    s->srcip = srcip;
    s->srcport = srcport;
    s->dstip = dstip;
    s->dstport = dstport;

    // TODO: ensure key size / key num combination is valid

    return 0;
}
