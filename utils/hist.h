#ifndef UTILS_HIST_H_
#define UTILS_HIST_H_

#include <stdbool.h>
#include <linux/types.h>

struct hist_state {
  __u64 max_value_us;
  __u64 total_samples;
  __u64 *buckets;
};

int init_hist(struct hist_state *hist, __u64 max_value_us);
void hist_cleanup(struct hist_state *hist);
void hist_add(struct hist_state *hist, __u64 value_us);
bool hist_percentiles(struct hist_state *hist, __u64 *p50, __u64 *p99, __u64 *p999);

#endif
