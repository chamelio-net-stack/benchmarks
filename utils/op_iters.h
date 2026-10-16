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

#ifndef UTILS_OP_ITERS_H_
#define UTILS_OP_ITERS_H_

#include <immintrin.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

struct op_iters_config
{
  uint32_t iterations;
  unsigned int avx_width;
};

/* Plain arrays avoid imposing vector alignment on calloc'ed core structs. */
struct op_iters_state
{
  uint32_t lanes[4][16];
};

static inline void op_iters_scalar(uint32_t iterations, struct op_iters_state *state)
{
  uint32_t a = state->lanes[0][0];
  uint32_t b = state->lanes[1][0];
  uint32_t c = state->lanes[2][0];
  uint32_t d = state->lanes[3][0];

  for (uint32_t i = 0; i < iterations; i++)
  {
    a = a * 1664525u + 1013904223u;
    b = b * 1664525u + 1013904223u;
    c = c * 1664525u + 1013904223u;
    d = d * 1664525u + 1013904223u;
    /* Keep all four chains live across every iteration without memory traffic. */
    asm volatile ("" : "+r" (a), "+r" (b), "+r" (c), "+r" (d));
  }
  state->lanes[0][0] = a;
  state->lanes[1][0] = b;
  state->lanes[2][0] = c;
  state->lanes[3][0] = d;
}

static __attribute__((target("avx2"), noinline))
void op_iters_avx256(uint32_t iterations, struct op_iters_state *state)
{
  __m256i a = _mm256_loadu_si256((const __m256i_u *) state->lanes[0]);
  __m256i b = _mm256_loadu_si256((const __m256i_u *) state->lanes[1]);
  __m256i c = _mm256_loadu_si256((const __m256i_u *) state->lanes[2]);
  __m256i d = _mm256_loadu_si256((const __m256i_u *) state->lanes[3]);
  const __m256i mul = _mm256_set1_epi32(1664525);
  const __m256i add = _mm256_set1_epi32(1013904223);

  for (uint32_t i = 0; i < iterations; i++)
  {
    a = _mm256_add_epi32(_mm256_mullo_epi32(a, mul), add);
    b = _mm256_add_epi32(_mm256_mullo_epi32(b, mul), add);
    c = _mm256_add_epi32(_mm256_mullo_epi32(c, mul), add);
    d = _mm256_add_epi32(_mm256_mullo_epi32(d, mul), add);
    asm volatile ("" : "+x" (a), "+x" (b), "+x" (c), "+x" (d));
  }
  _mm256_storeu_si256((__m256i_u *) state->lanes[0], a);
  _mm256_storeu_si256((__m256i_u *) state->lanes[1], b);
  _mm256_storeu_si256((__m256i_u *) state->lanes[2], c);
  _mm256_storeu_si256((__m256i_u *) state->lanes[3], d);
}

static __attribute__((target("avx512f"), noinline))
void op_iters_avx512(uint32_t iterations, struct op_iters_state *state)
{
  __m512i a = _mm512_loadu_si512((const void *) state->lanes[0]);
  __m512i b = _mm512_loadu_si512((const void *) state->lanes[1]);
  __m512i c = _mm512_loadu_si512((const void *) state->lanes[2]);
  __m512i d = _mm512_loadu_si512((const void *) state->lanes[3]);
  const __m512i mul = _mm512_set1_epi32(1664525);
  const __m512i add = _mm512_set1_epi32(1013904223);

  for (uint32_t i = 0; i < iterations; i++)
  {
    a = _mm512_add_epi32(_mm512_mullo_epi32(a, mul), add);
    b = _mm512_add_epi32(_mm512_mullo_epi32(b, mul), add);
    c = _mm512_add_epi32(_mm512_mullo_epi32(c, mul), add);
    d = _mm512_add_epi32(_mm512_mullo_epi32(d, mul), add);
    asm volatile ("" : "+v" (a), "+v" (b), "+v" (c), "+v" (d));
  }
  _mm512_storeu_si512((void *) state->lanes[0], a);
  _mm512_storeu_si512((void *) state->lanes[1], b);
  _mm512_storeu_si512((void *) state->lanes[2], c);
  _mm512_storeu_si512((void *) state->lanes[3], d);
}

static inline void op_iters_init(struct op_iters_state *state)
{
  for (unsigned int chain = 0; chain < 4; chain++)
  {
    for (unsigned int lane = 0; lane < 16; lane++)
    {
      state->lanes[chain][lane] = chain + lane + 1;
    }
  }
}

static inline void op_iters_run(const struct op_iters_config *config,
    struct op_iters_state *state)
{
  if (config->iterations == 0)
  {
    return;
  }

  if (config->avx_width == 256)
  {
    op_iters_avx256(config->iterations, state);
  }
  else if (config->avx_width == 512)
  {
    op_iters_avx512(config->iterations, state);
  }
  else
  {
    op_iters_scalar(config->iterations, state);
  }

  /* Make the persistent result observable without touching echo payloads. */
  asm volatile ("" : : "m" (*state) : "memory");
}

/* Return 1 for a consumed option, 0 for another option, -1 on invalid input. */
static inline int op_iters_parse_arg(struct op_iters_config *config,
    int argc, char **argv, int *index)
{
  const char *option = argv[*index];
  const char *value;
  uint32_t iterations = 0;

  if (strcmp(option, "--op-iters") != 0 &&
      strcmp(option, "--op-iters-avx") != 0)
  {
    return 0;
  }

  if (*index + 1 >= argc)
  {
    fprintf(stderr, "%s requires an argument\n", option);
    return -1;
  }
  value = argv[++*index];
  if (strcmp(option, "--op-iters-avx") == 0)
  {
    if (strcmp(value, "256") == 0)
    {
      config->avx_width = 256;
    }
    else if (strcmp(value, "512") == 0)
    {
      config->avx_width = 512;
    }
    else
    {
      fprintf(stderr, "--op-iters-avx must be 256 or 512\n");
      return -1;
    }
    return 1;
  }

  if (*value == '\0')
  {
    goto invalid_iterations;
  }
  for (const char *p = value; *p != '\0'; p++)
  {
    if (*p < '0' || *p > '9' ||
        iterations > (UINT32_MAX - (uint32_t) (*p - '0')) / 10)
    {
      goto invalid_iterations;
    }
    iterations = iterations * 10 + (uint32_t) (*p - '0');
  }
  config->iterations = iterations;
  return 1;

invalid_iterations:
  fprintf(stderr,
      "--op-iters must be a decimal integer from 0 to 4294967295\n");
  return -1;
}

static inline int op_iters_validate(const struct op_iters_config *config)
{
  __builtin_cpu_init();
  /* These checks include OS support for the required extended registers. */
  if ((config->avx_width == 256 && !__builtin_cpu_supports("avx2")) ||
      (config->avx_width == 512 && !__builtin_cpu_supports("avx512f")))
  {
    fprintf(stderr, "--op-iters-avx %u is unsupported by this CPU/OS\n",
        config->avx_width);
    return -1;
  }
  return 0;
}

#endif /* UTILS_OP_ITERS_H_ */
