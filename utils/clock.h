#ifndef UTILS_CLOCK_H_
#define UTILS_CLOCK_H_

#include <linux/types.h>

__u64 util_rdtsc(void);
void calibrate_tsc(void);
__u64 us_since_tsc(__u64 tsc_then);
__u64 now_ns(void);
double clock_tsc_per_us(void);

#endif
