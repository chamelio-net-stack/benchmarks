#include "hist.h"

#include <stdio.h>
#include <stdlib.h>

int init_hist(struct hist_state *hist, __u64 max_value_us)
{
  if (hist == NULL)
  {
    return -1;
  }

  hist->buckets = (__u64 *) calloc((size_t) max_value_us + 1, sizeof(__u64));
  if (hist->buckets == NULL)
  {
    perror("calloc rtt_hist");
    hist->max_value_us = 0;
    hist->total_samples = 0;
    return -1;
  }

  hist->max_value_us = max_value_us;
  hist->total_samples = 0;
  return 0;
}

void hist_cleanup(struct hist_state *hist)
{
  if (hist == NULL)
  {
    return;
  }

  free(hist->buckets);
  hist->buckets = NULL;
  hist->max_value_us = 0;
  hist->total_samples = 0;
}

void hist_add(struct hist_state *hist, __u64 value_us)
{
  if (hist == NULL || hist->buckets == NULL)
  {
    return;
  }

  if (value_us > hist->max_value_us)
  {
    value_us = hist->max_value_us;
  }

  __sync_fetch_and_add(&hist->buckets[value_us], 1);
  __sync_fetch_and_add(&hist->total_samples, 1);
}

bool hist_percentiles(struct hist_state *hist, __u64 *p50, __u64 *p99, __u64 *p999)
{
  __u64 i;
  __u64 samples;
  __u64 c;
  __u64 t50;
  __u64 t99;
  __u64 t999;
  __u64 acc = 0;
  __u64 v50 = 0;
  __u64 v99 = 0;
  __u64 v999 = 0;
  __u8 got50 = 0;
  __u8 got99 = 0;
  __u8 got999 = 0;

  if (hist == NULL || hist->buckets == NULL)
  {
    return false;
  }

  samples = __sync_fetch_and_add(&hist->total_samples, 0);
  if (samples == 0)
  {
    return false;
  }

  t50  = (samples * 50  + 100 - 1) / 100;
  t99  = (samples * 99  + 100 - 1) / 100;
  t999 = (samples * 999 + 1000 - 1) / 1000;

  for (i = 0; i <= hist->max_value_us; i++)
  {
    c = __sync_fetch_and_add(&hist->buckets[i], 0);
    if (c == 0)
    {
      continue;
    }

    acc += c;

    if (!got50 && acc >= t50)
    {
      v50 = i;
      got50 = 1;
    }

    if (!got99 && acc >= t99)
    {
      v99 = i;
      got99 = 1;
    }

    if (!got999 && acc >= t999)
    {
      v999 = i;
      got999 = 1;
    }

    if (got50 && got99 && got999)
    {
      break;
    }
  }

  *p50 = v50;
  *p99 = v99;
  *p999 = v999;
  return true;
}
