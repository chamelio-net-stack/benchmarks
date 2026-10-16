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
  uint32_t opaque;
  uint32_t lanes[16];
};

static inline uint32_t op_iters_scalar(uint32_t iterations, uint32_t opaque)
{
  for (uint32_t i = 0; i < iterations; i++)
  {
    opaque = opaque * 42 + 37;
    opaque ^= 0x12345678;
    opaque = opaque * 42 + 37;
    opaque ^= 0x87654321;
    asm volatile ("" : "+r" (opaque));
  }
  return opaque;
}

static __attribute__((target("avx2"), noinline))
void op_iters_avx256(uint32_t iterations, struct op_iters_state *state)
{
  __m256i opaque = _mm256_loadu_si256((const __m256i_u *) state->lanes);
  const __m256i mul = _mm256_set1_epi32(42);
  const __m256i add = _mm256_set1_epi32(37);
  const __m256i xor1 = _mm256_set1_epi32(0x12345678);
  const __m256i xor2 = _mm256_set1_epi32((int32_t) UINT32_C(0x87654321));

  for (uint32_t i = 0; i < iterations; i++)
  {
    opaque = _mm256_add_epi32(_mm256_mullo_epi32(opaque, mul), add);
    opaque = _mm256_xor_si256(opaque, xor1);
    opaque = _mm256_add_epi32(_mm256_mullo_epi32(opaque, mul), add);
    opaque = _mm256_xor_si256(opaque, xor2);
    /* Keep every lane live and require a vector result on every iteration. */
    asm volatile ("" : "+x" (opaque));
  }
  _mm256_storeu_si256((__m256i_u *) state->lanes, opaque);
}

static __attribute__((target("avx512f"), noinline))
void op_iters_avx512(uint32_t iterations, struct op_iters_state *state)
{
  __m512i opaque = _mm512_loadu_si512((const void *) state->lanes);
  const __m512i mul = _mm512_set1_epi32(42);
  const __m512i add = _mm512_set1_epi32(37);
  const __m512i xor1 = _mm512_set1_epi32(0x12345678);
  const __m512i xor2 = _mm512_set1_epi32((int32_t) UINT32_C(0x87654321));

  for (uint32_t i = 0; i < iterations; i++)
  {
    opaque = _mm512_add_epi32(_mm512_mullo_epi32(opaque, mul), add);
    opaque = _mm512_xor_si512(opaque, xor1);
    opaque = _mm512_add_epi32(_mm512_mullo_epi32(opaque, mul), add);
    opaque = _mm512_xor_si512(opaque, xor2);
    asm volatile ("" : "+v" (opaque));
  }
  _mm512_storeu_si512((void *) state->lanes, opaque);
}

static inline void op_iters_init(struct op_iters_state *state)
{
  state->opaque = 0;
  for (unsigned int i = 0; i < 16; i++)
  {
    state->lanes[i] = i;
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
    state->opaque = op_iters_scalar(config->iterations, state->opaque);
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
