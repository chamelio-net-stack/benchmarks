#define _GNU_SOURCE
#include "core.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>

int pin_thread_to_core(int core_id)
{
  cpu_set_t cpuset;
  int ret;

  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  ret = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
  if (ret != 0)
  {
    errno = ret;
    perror("pthread_setaffinity_np");
    return -1;
  }

  return 0;
}
