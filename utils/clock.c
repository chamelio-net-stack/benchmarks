#include "clock.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

static double tsc_per_us = 0.0;

__u64 util_rdtsc(void)
{
  __u32 eax;
  __u32 edx;

  asm volatile ("rdtsc" : "=a" (eax), "=d" (edx));
  return ((__u64) edx << 32) | eax;
}

void calibrate_tsc(void)
{
  struct timespec ts_before;
  struct timespec ts_after;
  __u64 t0;
  __u64 t1;
  __u64 us_before;
  __u64 us_after;
  __u64 dt_cycles;
  __u64 dt_us;

  if (tsc_per_us != 0.0)
  {
    return;
  }

  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts_before) != 0)
  {
    perror("calibrate_tsc: clock_gettime before");
    exit(EXIT_FAILURE);
  }

  t0 = util_rdtsc();
  usleep(10000);
  t1 = util_rdtsc();

  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts_after) != 0)
  {
    perror("calibrate_tsc: clock_gettime after");
    exit(EXIT_FAILURE);
  }

  dt_cycles = t1 - t0;
  us_before = (__u64) ts_before.tv_sec * 1000000ULL
      + (__u64) (ts_before.tv_nsec / 1000ULL);
  us_after  = (__u64) ts_after.tv_sec  * 1000000ULL
      + (__u64) (ts_after.tv_nsec  / 1000ULL);
  dt_us = us_after - us_before;

  if (dt_us == 0)
  {
    dt_us = 1;
  }

  tsc_per_us = (double) dt_cycles / (double) dt_us;
  if (tsc_per_us <= 0.0)
  {
    fprintf(stderr, "calibrate_tsc: invalid tsc_per_us: %f\n", tsc_per_us);
    exit(EXIT_FAILURE);
  }
}

__u64 us_since_tsc(__u64 tsc_then)
{
  __u64 tsc_now;
  double delta;
  double us;

  calibrate_tsc();
  tsc_now = util_rdtsc();
  delta = (double) (tsc_now - tsc_then);
  assert(delta >= 0);
  us = delta / tsc_per_us;
  assert(us >= 0);

  return (__u64) us;
}

__u64 now_ns(void)
{
  struct timespec ts;

  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return (__u64) ts.tv_sec * 1000000000ULL + (__u64) ts.tv_nsec;
}

double clock_tsc_per_us(void)
{
  calibrate_tsc();
  return tsc_per_us;
}
