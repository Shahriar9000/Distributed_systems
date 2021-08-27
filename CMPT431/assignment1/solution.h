#include "core/circular_queue.h"
#include "core/utils.h"
#include <pthread.h>
#include <stdlib.h>
#include <iostream>
#include <cassert>

using namespace std;

struct thread_stats{
  int thread_id; 
  long items_produced; 
  long value_produced;
  long items_consumed; 
  long value_consumed;  
  double time_taken;
};


struct thread_data_{

  long item_nums;
  int thread_id;
  int n_producers;
  CircularQueue* production_buffer;
  int *active_producer_count;
  int *active_consumer_count;
  pthread_cond_t* full ;
  pthread_cond_t* empty;
  pthread_mutex_t* mutex;
};

class ProducerConsumerProblem {
  long n_items;
  int n_producers;
  int n_consumers;
  CircularQueue production_buffer;


  pthread_t *producer_threads;
  pthread_t *consumer_threads;
  thread_data_ *producer_thread_data;
  thread_data_ *consumer_thread_data;

  pthread_cond_t full;
  pthread_cond_t empty;
  pthread_mutex_t mutex;

  int active_producer_count;
  int active_consumer_count;

public:
  // The following 6 methods should be defined in the implementation file (solution.cpp)
  ProducerConsumerProblem(long _n_items, int _n_producers, int _n_consumers,
                          long _queue_size);
  ~ProducerConsumerProblem();
  void startProducers();
  void startConsumers();
  void joinProducers();
  void joinConsumers();
  void printStats();
  // void *makeItems(void* thread_data);
};