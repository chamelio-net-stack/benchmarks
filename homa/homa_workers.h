#ifndef HOMA_WORKERS_H
#define HOMA_WORKERS_H

#include <arpa/inet.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Startup RPCs negotiate the worker count before timed payload traffic. */
#define HOMA_STARTUP_MAGIC 0x484f4d41U
struct homa_startup
{
  uint32_t magic;
  uint32_t ncores;
};

struct homa_worker
{
  void *config;
  int (*run)(void *config);
  int client;
};

static inline void *homa_worker_main(void *arg)
{
  struct homa_worker *worker = arg;
  int ret = worker->run(worker->config);

  if (!worker->client && ret != 0)
    exit(EXIT_FAILURE);
  
  return (void *) (intptr_t) ret;
}

static inline int homa_run_workers(struct homa_worker *workers, unsigned int count)
{
  pthread_t *threads = calloc(count, sizeof(*threads));
  unsigned int i, started = 0;
  int ret = 0, error;
  void *result;

  if (threads == NULL)
    return 1;

  for (i = 0; i < count; i++)
  {
    error = pthread_create(&threads[i], NULL, homa_worker_main, &workers[i]);
    if (error != 0)
    {
      fprintf(stderr, "Cannot start Homa worker: %s\n", strerror(error));
      if (!workers[i].client)
        exit(EXIT_FAILURE);
      ret = 1;
      break;
    }
    started++;
  }

  for (i = 0; i < started; i++)
  {
    pthread_join(threads[i], &result);
    if ((intptr_t) result != 0)
      ret = 1;
  }

  free(threads);
  return ret;
}

#endif
