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

#ifndef UTILS_OP_DELAY_H_
#define UTILS_OP_DELAY_H_

#include <immintrin.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

struct op_delay_config
{
  uint32_t cycles;
  unsigned int avx_width;
};

/* Plain arrays avoid imposing vector alignment on calloc'ed core structs. */
struct op_delay_state
{
  uint32_t opaque;
  uint32_t lanes[16];
};

static inline uint64_t op_delay_rdtsc(void)
{
  uint32_t eax, edx;
  asm volatile ("rdtsc" : "=a" (eax), "=d" (edx));
  return ((uint64_t) edx << 32) | eax;
}

/* From benchmarks/common/microbench.h: same arithmetic and TSC deadline. */
static inline uint32_t kill_cycles(uint32_t cyc, uint32_t opaque)
{
  uint64_t start = op_delay_rdtsc();
  uint64_t end = start + cyc;

  if (end >= start)
  {
    while (op_delay_rdtsc() < end)
    {
      opaque = opaque * 42 + 37;
      opaque ^= 0x12345678;
      opaque = opaque * 42 + 37;
      opaque ^= 0x87654321;
    }
  }
  else
  {
    while (op_delay_rdtsc() >= start || op_delay_rdtsc() < end)
    {
      opaque = opaque * 42 + 37;
      opaque ^= 0x12345678;
      opaque = opaque * 42 + 37;
      opaque ^= 0x87654321;
    }
  }
  return opaque;
}

static __attribute__((target("avx2"), noinline))
void kill_cycles_avx256(uint32_t cyc, struct op_delay_state *state)
{
  __m256i opaque = _mm256_loadu_si256((const __m256i_u *) state->lanes);
  const __m256i mul = _mm256_set1_epi32(42);
  const __m256i add = _mm256_set1_epi32(37);
  const __m256i xor1 = _mm256_set1_epi32(0x12345678);
  const __m256i xor2 = _mm256_set1_epi32((int32_t) UINT32_C(0x87654321));
  uint64_t start = op_delay_rdtsc();
  uint64_t end = start + cyc;

  while (end >= start ? op_delay_rdtsc() < end :
      (op_delay_rdtsc() >= start || op_delay_rdtsc() < end))
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
void kill_cycles_avx512(uint32_t cyc, struct op_delay_state *state)
{
  __m512i opaque = _mm512_loadu_si512((const void *) state->lanes);
  const __m512i mul = _mm512_set1_epi32(42);
  const __m512i add = _mm512_set1_epi32(37);
  const __m512i xor1 = _mm512_set1_epi32(0x12345678);
  const __m512i xor2 = _mm512_set1_epi32((int32_t) UINT32_C(0x87654321));
  uint64_t start = op_delay_rdtsc();
  uint64_t end = start + cyc;

  while (end >= start ? op_delay_rdtsc() < end :
      (op_delay_rdtsc() >= start || op_delay_rdtsc() < end))
  {
    opaque = _mm512_add_epi32(_mm512_mullo_epi32(opaque, mul), add);
    opaque = _mm512_xor_si512(opaque, xor1);
    opaque = _mm512_add_epi32(_mm512_mullo_epi32(opaque, mul), add);
    opaque = _mm512_xor_si512(opaque, xor2);
    asm volatile ("" : "+v" (opaque));
  }
  _mm512_storeu_si512((void *) state->lanes, opaque);
}

static inline void op_delay_init(struct op_delay_state *state)
{
  state->opaque = 0;
  for (unsigned int i = 0; i < 16; i++)
    state->lanes[i] = i;
}

static inline void op_delay_run(const struct op_delay_config *config,
    struct op_delay_state *state)
{
  if (config->cycles == 0)
    return;

  if (config->avx_width == 256)
    kill_cycles_avx256(config->cycles, state);
  else if (config->avx_width == 512)
    kill_cycles_avx512(config->cycles, state);
  else
    state->opaque = kill_cycles(config->cycles, state->opaque);

  /* Make the persistent result observable without touching echo payloads. */
  asm volatile ("" : : "m" (*state) : "memory");
}

/* Return 1 for a consumed option, 0 for another option, -1 on invalid input. */
static inline int op_delay_parse_arg(struct op_delay_config *config,
    int argc, char **argv, int *index)
{
  const char *option = argv[*index];
  const char *value;
  uint32_t cycles = 0;

  if (strcmp(option, "--op-delay") != 0 &&
      strcmp(option, "--op-delay-avx") != 0)
    return 0;

  if (*index + 1 >= argc)
  {
    fprintf(stderr, "%s requires an argument\n", option);
    return -1;
  }
  value = argv[++*index];
  if (strcmp(option, "--op-delay-avx") == 0)
  {
    if (strcmp(value, "256") == 0)
      config->avx_width = 256;
    else if (strcmp(value, "512") == 0)
      config->avx_width = 512;
    else
    {
      fprintf(stderr, "--op-delay-avx must be 256 or 512\n");
      return -1;
    }
    return 1;
  }

  if (*value == '\0')
    goto invalid_cycles;
  for (const char *p = value; *p != '\0'; p++)
  {
    if (*p < '0' || *p > '9' ||
        cycles > (UINT32_MAX - (uint32_t) (*p - '0')) / 10)
      goto invalid_cycles;
    cycles = cycles * 10 + (uint32_t) (*p - '0');
  }
  config->cycles = cycles;
  return 1;

invalid_cycles:
  fprintf(stderr, "--op-delay must be a decimal integer from 0 to 4294967295\n");
  return -1;
}

static inline int op_delay_validate(const struct op_delay_config *config)
{
  __builtin_cpu_init();
  /* These checks include OS support for the required extended registers. */
  if ((config->avx_width == 256 && !__builtin_cpu_supports("avx2")) ||
      (config->avx_width == 512 && !__builtin_cpu_supports("avx512f")))
  {
    fprintf(stderr, "--op-delay-avx %u is unsupported by this CPU/OS\n",
        config->avx_width);
    return -1;
  }
  return 0;
}

#endif /* UTILS_OP_DELAY_H_ */
