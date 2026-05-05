#ifndef BURST_H_
#define BURST_H_

#include <linux/types.h>
#include <math.h>
#include <stdlib.h>
#include "clock.h"

struct burst_state {
  int on;
  __u64 next_tsc;
  unsigned int seed;
};

static inline double burst_normal(unsigned int *seed, double mean, double std)
{
  double u1, u2, z;

  if (std == 0.0)
    return mean;

  u1 = ((double) rand_r(seed) + 1.0) / ((double) RAND_MAX + 2.0);
  u2 = ((double) rand_r(seed) + 1.0) / ((double) RAND_MAX + 2.0);
  z = sqrt(-2.0 * log(u1)) * cos(6.28318530717958647692 * u2);

  return mean + std * z;
}

static inline __u64 burst_sample_cycles(unsigned int *seed, 
    double mean, double std)
{
  double us = burst_normal(seed, mean, std);
  __u64 cycles;

  if (us < 1.0)
    us = 1.0;

  cycles = (__u64) (us * clock_tsc_per_us());
  return cycles == 0 ? 1 : cycles;
}

static inline void burst_start(struct burst_state *b, __u64 now,
    double dur_mean, double dur_std)
{
  b->on = 1;
  b->next_tsc = now + burst_sample_cycles(&b->seed, dur_mean, dur_std);
}

static inline void burst_stop(struct burst_state *b, __u64 now,
    double per_mean, double per_std)
{
  b->on = 0;
  b->next_tsc = now + burst_sample_cycles(&b->seed, per_mean, per_std);
}

static inline int burst_expired(struct burst_state *b, __u64 now)
{
  return now >= b->next_tsc;
}

static inline void burst_toggle(struct burst_state *b, __u64 now,
    double dur_mean, double dur_std, double per_mean, double per_std)
{
  if (b->on)
    burst_stop(b, now, per_mean, per_std);
  else
    burst_start(b, now, dur_mean, dur_std);
}

#endif /* BURST_H_ */
