#include "solution.h"
#include "math.h"


struct thread_stats* producer_thread_stats;
struct thread_stats* consumer_thread_stats;
long** place_holder_array_for_stats;

void *producerFunction(void* _arg){

  timer t1;
  t1.start();
  struct thread_data_* data;
  data = ( struct thread_data_* ) _arg;

  int t_id = data->thread_id;
  long np = data->item_nums;
  long items_produced = 0;
  long value_produced = t_id;
  long sum = 0;

  //lock mutex before doing work
  pthread_mutex_lock(data->mutex);
  while (items_produced < np)
  { 

    bool ret = data->production_buffer->enqueue( value_produced, t_id);
   
    if (ret == true)
    {
      //signal all consumer threads if buffer not empty

      if (!data->production_buffer->isEmpty())
      {
        pthread_cond_broadcast(data->empty);
      }
      sum = sum + value_produced;
      value_produced= value_produced+data->n_producers;
      items_produced++;
    }
    else{

        pthread_cond_wait(data->full, data->mutex);
    }
    
  }
  pthread_mutex_unlock(data->mutex);

  pthread_mutex_lock(data->mutex);
  *data->active_producer_count = *data->active_producer_count - 1 ;

  if (*data->active_producer_count == 0)
  { 
    while(*data->active_consumer_count > 0)
    {
      //signal all consumer threads and wait
      pthread_cond_broadcast(data->empty);
      pthread_cond_wait(data->full, data->mutex);
    }
  }

  pthread_mutex_unlock(data->mutex);

  //getting producer stats
  producer_thread_stats[t_id].thread_id = t_id;
  producer_thread_stats[t_id].items_produced = items_produced;
  producer_thread_stats[t_id].value_produced = sum;
  producer_thread_stats[t_id].items_consumed = 0;
  producer_thread_stats[t_id].value_consumed = 0;
  producer_thread_stats[t_id].time_taken = t1.stop();

  pthread_exit(NULL);

}

void *consumerFunction(void* _arg){
  
  //start timer
  timer t1;
  t1.start();

  struct thread_data_* data;
  data = ( struct thread_data_* ) _arg;
  
  int t_id = data->thread_id;
  long item;
  long items_consumed = 0;
  long value_consumed = 0;
  long value;
  int source;


  //lock mutex before doing work
  pthread_mutex_lock(data->mutex);
  while (true)
  { 

    bool ret= data->production_buffer->dequeue( &value, &source );
    if (ret == true)
    {
      //check if buffer is not full then signal producer threads
      if (!data->production_buffer->isFull())
      {

        pthread_cond_broadcast(data->full);

      }

      value_consumed += value;
      items_consumed++;
      place_holder_array_for_stats[t_id][source]=value + place_holder_array_for_stats[t_id][source];
      
    }
    else
    {
        if (*data->active_producer_count == 0)
        {   
            *data->active_consumer_count = *data->active_consumer_count -1;
            pthread_cond_broadcast(data->full);
            break;
        }
        else
        {
          pthread_cond_wait(data->empty, data->mutex);

        }
    }

  }

  pthread_mutex_unlock(data->mutex);
  consumer_thread_stats[t_id].thread_id = t_id;
  consumer_thread_stats[t_id].items_consumed = items_consumed;
  consumer_thread_stats[t_id].value_consumed = value_consumed;
  consumer_thread_stats[t_id].value_produced = 0;
  consumer_thread_stats[t_id].items_produced = 0;
  consumer_thread_stats[t_id].time_taken = t1.stop();
  pthread_exit(NULL);

}

ProducerConsumerProblem::ProducerConsumerProblem(long _n_items,
                                                 int _n_producers,
                                                 int _n_consumers,
                                                 long _queue_size)
    : n_items(_n_items), n_producers(_n_producers), n_consumers(_n_consumers),
      production_buffer(_queue_size) {
  std::cout << "Constructor\n";
  // std::cout << "Number of items: " << n_items << "\n";

  assert(n_items>0);
  assert(n_producers>0);
  assert(n_consumers>0);
  assert(_queue_size>1);
  std::cout << "Number of producers: " << n_producers << "\n";
  std::cout << "Number of consumers: " << n_consumers << "\n";
  // std::cout << "Queue size: " << _queue_size << "\n";

  producer_threads = new pthread_t[n_producers];
  consumer_threads = new pthread_t[n_consumers];


  // Initialize all mutex and conditional variables here.
  producer_thread_data = new thread_data_[n_producers];
  consumer_thread_data = new thread_data_[n_consumers];
  producer_thread_stats = new thread_stats[n_producers];
  consumer_thread_stats = new thread_stats[n_consumers];

  place_holder_array_for_stats = new long *[n_consumers];

  //initialize value to zero
  for (int i =0; i < n_consumers; i++){
        place_holder_array_for_stats[i] = new long[n_producers];
  }

  for (size_t i = 0; i < n_consumers; i++)
  {
     for (int j =0; j < n_producers; j++){
        place_holder_array_for_stats[i][j] = 0;
    }
  }
  
  pthread_cond_init(&full, NULL);
  pthread_cond_init(&empty, NULL);
  pthread_mutex_init(&mutex, NULL);
  
  
}

ProducerConsumerProblem::~ProducerConsumerProblem() {
  std::cout << "Destructor\n";

  // Destroy all mutex and conditional variables here.
  delete[] producer_threads;
  delete[] consumer_threads;
  delete[] producer_thread_data;
  delete[] consumer_thread_data;
  
  for (int i =0; i < n_consumers; i++){
        delete [] place_holder_array_for_stats[i];
  }
  delete[] place_holder_array_for_stats;


  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&full);
  pthread_cond_destroy(&empty);

  
}



void ProducerConsumerProblem::startProducers() {
  std::cout << "Starting Producers\n";
   // Create producer threads P1, P2, P3,.. using pthread_create.
  active_producer_count = n_producers;
  int rc;
  int items_produced_by_thread0 = (n_items/n_producers)+n_items% n_producers;
  int items_produced_by_rest_threads = (n_items/n_producers);
  
  for (int i=0; i<n_producers; i++)
  {
    producer_thread_data[i].active_consumer_count= &active_consumer_count;
    producer_thread_data[i].active_producer_count = &active_producer_count;
    producer_thread_data[i].production_buffer = &production_buffer;
    producer_thread_data[i].full = &full;
    producer_thread_data[i].empty = &empty;
    producer_thread_data[i].n_producers = n_producers;
    producer_thread_data[i].mutex = &mutex;

    if (i == 0)
    {
     producer_thread_data[i].item_nums = items_produced_by_thread0;
     producer_thread_data[i].thread_id = i;
     rc = pthread_create(&producer_threads[i], NULL, producerFunction, (void*) &producer_thread_data[i]);

    }
    else
    {
      producer_thread_data[i].item_nums = items_produced_by_rest_threads;
      producer_thread_data[i].thread_id = i;
      rc = pthread_create(&producer_threads[i], NULL, producerFunction, (void*) &producer_thread_data[i]);
    }
    if(rc)
      {
          cout << "Error creating thread" << i << "error:" << rc << endl;
      }
  }
 
}





void ProducerConsumerProblem::startConsumers() {
  std::cout << "Starting Consumers\n";

   // Create consumer threads C1, C2, C3,.. using pthread_create.

  active_consumer_count = n_consumers;
  int rc;

  for (int i=0; i<n_consumers; i++)
  {
    
    consumer_thread_data[i].active_consumer_count= &active_consumer_count;
    consumer_thread_data[i].active_producer_count= &active_producer_count;
    consumer_thread_data[i].production_buffer = &production_buffer;
    consumer_thread_data[i].full = &full;
    consumer_thread_data[i].empty = &empty;
    consumer_thread_data[i].n_producers = n_producers;
    consumer_thread_data[i].mutex = &mutex;
    consumer_thread_data[i].thread_id = i;

    rc = pthread_create(&consumer_threads[i], NULL, consumerFunction, (void*) &consumer_thread_data[i]);
    if (rc)
    {
        cout << "Error creating thread" << i << "error:" << rc << endl;
    }
  }
}

void ProducerConsumerProblem::joinProducers() {
  std::cout << "Joining Producers\n";
  // Join the producer threads with the main thread using pthread_join
  int rc;
  for (int i=0; i< n_producers; i++)
  {
    rc = pthread_join(producer_threads[i], NULL);
    if (rc)
      {
          cout << "Error joining thread" << i << "error:" << rc << endl;
      }
  }

}

void ProducerConsumerProblem::joinConsumers() {
  std::cout << "Joining Consumers\n";
  // Join the consumer threads with the main thread using pthread_join
  int rc;
  for (int i=0; i<n_consumers; i++)
  {
    rc = pthread_join(consumer_threads[i], NULL);
    if (rc)
    {
      cout << "Error joining thread" << i << "error:" << rc << endl;
    }
  }
  
  
}

void ProducerConsumerProblem::printStats() {
  std::cout << "Producer stats\n";
  std::cout << "producer_id, items_produced, value_produced, time_taken \n";
  // Make sure you print the producer stats in the following manner
  // 0, 1000, 499500, 0.00123596
  // 1, 1000, 499500, 0.00154686
  // 2, 1000, 499500, 0.00122881 
  long total_produced = 0;
  long total_value_produced = 0;
  for (int i = 0; i < n_producers; i++) 
  {
    cout << producer_thread_stats[i].thread_id << ", " <<producer_thread_stats[i].items_produced << ", " <<producer_thread_stats[i].value_produced << ", " <<producer_thread_stats[i].time_taken << "\n";
    total_produced += producer_thread_stats[i].items_produced; 
    total_value_produced += producer_thread_stats[i].value_produced; 
  }
  std::cout << "Total produced = " << total_produced << "\n";
  std::cout << "Total value produced = " << total_value_produced << "\n";
  std::cout << "Consumer stats\n";
  std::cout << "consumer_id, items_consumed, value_consumed, time_taken, ";
  for (int i = 0; i<n_producers-1; i++)
  {
    cout<<"value_from_producer_"<< i << ", ";
  }
  cout<<"value_from_producer_"<< n_producers-1 <<endl;
  // Make sure you print the consumer stats in the following manner
  // 0, 677, 302674, 0.00147414, 120000, 130000, 140000
  // 1, 648, 323301, 0.00142694, 130000, 140000, 120000
  // 2, 866, 493382, 0.00139689, 140000, 120000, 130000
  // 3, 809, 379143, 0.00134516, 109500, 109500, 109500
  long total_consumed = 0;
  long total_value_consumed = 0;
  for (int i = 0; i < n_consumers; i++) {
    cout<<consumer_thread_stats[i].thread_id << ", " <<consumer_thread_stats[i].items_consumed << ", " <<consumer_thread_stats[i].value_consumed << ", " <<consumer_thread_stats[i].time_taken << ", ";
    for (int j=0; j<n_producers-1; j++){
      cout<<place_holder_array_for_stats[i][j]<< ", ";
    }
    cout<<place_holder_array_for_stats[i][n_producers-1]<< endl;
    total_consumed +=consumer_thread_stats[i].items_consumed;
    total_value_consumed += consumer_thread_stats[i].value_consumed;
  }
  std::cout << "Total consumed = " << total_consumed << "\n";
  std::cout << "Total value consumed = " << total_value_consumed << "\n";
}