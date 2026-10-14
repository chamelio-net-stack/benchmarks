#include "ports.h"
#include <stdlib.h>

int parse_ports(const char *text, uint16_t **ports, size_t *count)
{
  size_t capacity = 1, n = 0;
  const char *p;
  uint16_t *values;

  for (p = text; *p; p++)
    if (*p == ',') capacity++;
  values = malloc(capacity * sizeof(*values));
  if (!values) return -1;

  p = text;
  for (;;) {
    char *end;
    unsigned long value;
    if (*p < '0' || *p > '9') goto invalid;
    value = strtoul(p, &end, 10);
    if (value == 0 || value > 65535 || (*end && *end != ','))
      goto invalid;
    for (size_t i = 0; i < n; i++)
      if (values[i] == value) goto invalid;
    values[n++] = (uint16_t)value;
    if (!*end) break;
    p = end + 1;
  }
  *ports = values;
  *count = n;
  return 0;

invalid:
  free(values);
  return -1;
}
