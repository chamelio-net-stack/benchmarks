#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "homa_cham.h"
#include "../utils/core.h"

static int parse_number(const char *text, unsigned int *value);
static int parse_address(struct sockaddr_in *addr, char *ip, char *port);

int homa_cham_parse(struct homa_cham_config *config, int argc, char **argv,
    int client)
{
  unsigned int value;
  int first = client ? 5 : 3;
  int i;

  memset(config, 0, sizeof(*config));
  config->msize = 64;
  config->requests = 1;
  config->core = -1;

  if (argc < first ||
      parse_address(&config->local, argv[1], argv[2]) != 0)
    goto failed;

  if (client && parse_address(&config->remote, argv[3], argv[4]) != 0)
    goto failed;

  for (i = first; i < argc; i += 2)
  {
    if (i + 1 == argc || parse_number(argv[i + 1], &value) != 0)
      goto failed;

    if (strcmp(argv[i], "--msize") == 0 && value <= 1000000)
      config->msize = value;
    else if (strcmp(argv[i], "--ncores") == 0 && value == 1)
      continue;
    else if (strcmp(argv[i], "--cores") == 0 && value <= INT_MAX)
      config->core = value;
    else if (client && strcmp(argv[i], "--requests") == 0 && value > 0)
      config->requests = value;
    else
      goto failed;
  }

  return 0;

failed:
  fprintf(stderr, "Usage: %s <bind_ip> <port>%s"
      " [--msize 0..1000000] [--ncores 1] [--cores CPU]%s\n", argv[0],
      client ? " <rem_ip> <rem_port>" : "",
      client ? " [--requests N]" : "");
  return -1;
}

int homa_cham_init(const struct homa_cham_config *config,
    struct homa_context_lib **ctx)
{
  int fd;

  if (config->core >= 0 && pin_thread_to_core(config->core) != 0)
    return -1;

  if (homa_lib_connect_slow() != 0)
    return -1;

  *ctx = homa_lib_ctx_new();
  if (*ctx == NULL)
    return -1;

  fd = homa_lib_socket(*ctx);
  if (fd < 0 || homa_lib_bind(*ctx, fd,
      (const struct sockaddr *) &config->local, sizeof(config->local)) != 0)
  {
    homa_lib_ctx_free(*ctx);
    return -1;
  }

  return fd;
}

static int parse_number(const char *text, unsigned int *value)
{
  unsigned long parsed;
  char *end;

  errno = 0;
  parsed = strtoul(text, &end, 10);
  if (errno != 0 || *text == '\0' || *text == '-' || *end != '\0' ||
      parsed > UINT_MAX)
    return -1;

  *value = parsed;
  return 0;
}

static int parse_address(struct sockaddr_in *addr, char *ip, char *port)
{
  unsigned int value;

  if (parse_number(port, &value) != 0 || value > 65535)
    return -1;

  addr->sin_family = AF_INET;
  addr->sin_port = htons(value);
  return inet_pton(AF_INET, ip, &addr->sin_addr) == 1 ? 0 : -1;
}
