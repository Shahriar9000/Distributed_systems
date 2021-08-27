#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>
#include <pthread.h>
#include <cassert>
#include <cstring>

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
typedef double PageRankType;
#endif

using namespace std;
//thread shared
struct thread_data{
    int t_id;
    Graph *g;
    uint iterStart;
    uint iterEnd;
    uint tid;
    uintV n;
    double thread_time;
    PageRankType *pr_next;
    PageRankType *pr_curr;
    pthread_mutex_t* mutex;
    CustomBarrier* my_barrier;
};

struct thread_data* thread_args;
pthread_t* thread_list;

void page_rank_push_parallel(Graph *g, uint iterStart, uint iterEnd, uint thread_id, uint n, double *thread_time, std::atomic<PageRankType>*pr_next, PageRankType *pr_curr, CustomBarrier* barrier_){

  timer thread_timer;
  thread_timer.start();

  PageRankType* temporary_placeholder = new PageRankType [n];

  for (uintV u = iterStart; u < iterEnd; u++)
  {
      uintE out_degree = g->vertices_[u].getOutDegree();
      for (uintE i = 0; i < out_degree; i++)
      {
        uintV v = g->vertices_[u].getOutNeighbor(i);
        temporary_placeholder[v] = pr_next[v];
        while (!pr_next[v].compare_exchange_weak(temporary_placeholder[v], pr_next[v] + (pr_curr[u] / out_degree)));
      }
  }

  barrier_->wait();

  for (uintV v = iterStart; v < iterEnd; v++)
  {
    temporary_placeholder[v] = pr_next[v];
    while (!pr_next[v].compare_exchange_weak(temporary_placeholder[v], PAGE_RANK(pr_next[v])));

    // reset pr_curr for the next iteration
    pr_curr[v] = pr_next[v];
    pr_next[v] = 0.0;
  }
  *thread_time += thread_timer.stop();
  delete[] temporary_placeholder;

}

void Create_Threads(Graph &g, int max_iters, uint n_threads, CustomBarrier &barrier_) {
  uintV n = g.n_;
  PageRankType *pr_curr = new PageRankType[n];
  std::atomic<PageRankType>* pr_next = new std::atomic <PageRankType>[n];

  double thread_time[n_threads];

  for(int i =0; i< n_threads; i++)
  {
   thread_time[i] = 0.0;
  }
  uint start;
  uint end;
  for (uintV i = 0; i < n; i++)
  {
     pr_curr[i] = INIT_PAGE_RANK;
     pr_next[i] = 0.0;
  }
  // Push based pagerank
  timer t1;
  double total_time_taken = 0.0;
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  std::thread thread_array[n_threads];
  int thread_id;
  int last_thread = n_threads -1;

  t1.start();
  for (int iter = 0; iter < max_iters; iter++)
  {
   for(uint i = 0; i < n_threads - 1; i++){
     thread_id = i;
     start = (n/n_threads)*i;
     end = ((i+1)*(n/n_threads));

     thread_array[i] = std::thread(page_rank_push_parallel, &g, start, end, thread_id, n, &thread_time[thread_id], std::ref(pr_next), std::ref(pr_curr), &barrier_);
   }

   start = n/n_threads*(last_thread);
   end = n;
   thread_array[last_thread] = std::thread(page_rank_push_parallel, &g, start, end, last_thread, n, &thread_time[last_thread], std::ref(pr_next), std::ref(pr_curr), &barrier_);
    // for each vertex 'u', process all its outNeighbors 'v'
   for(uint i = 0; i < n_threads; i++)
   {
     thread_array[i].join();
   }
  }
  total_time_taken = t1.stop();
  // -------------------------------------------------------------------
  std::cout << "thread_id, time_taken" << std::endl;
  // Print the above statistics for each thread
  // Example output for 2 threads:
  // thread_id, time_taken
  // 0, 0.12
  // 1, 0.12
  for(uint i = 0; i < n_threads; i++)
  {
   std::cout <<i<< ", " << std::setprecision(TIME_PRECISION) << thread_time[i]<< std::endl;
  }
  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < n; u++){
     sum_of_page_ranks += pr_curr[u];
  }

  std::cout << "Sum of page ranks : " << sum_of_page_ranks << std::endl;
  std::cout << "Time taken (in seconds) : " << total_time_taken << std::endl;
  delete[] pr_curr;
  delete[] pr_next;
}

int main(int argc, char *argv[]) {
  cxxopts::Options options(
      "page_rank_push",
      "Calculate page_rank using serial and parallel execution");
  options.add_options(
      "",
      {
          {"nThreads", "Number of Threads",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_THREADS)},
          {"nIterations", "Maximum number of iterations",
           cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_threads = cl_options["nThreads"].as<uint>();
  assert(n_threads > 0);
  uint max_iterations = cl_options["nIterations"].as<uint>();
  assert(max_iterations > 0);
  std::string input_file_path = cl_options["inputFile"].as<std::string>();
  assert(!input_file_path.empty());

#ifdef USE_INT
  std::cout << "Using INT" << std::endl;
#else
  std::cout << "Using DOUBLE" << std::endl;
#endif
  std::cout << std::fixed;
  std::cout << "Number of Threads : " << n_threads << std::endl;
  std::cout << "Number of Iterations: " << max_iterations << std::endl;

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";

  thread_args = new thread_data[n_threads];
  thread_list = new pthread_t[n_threads];
  CustomBarrier barrier_(n_threads);
  Create_Threads(g, max_iterations, n_threads, barrier_);


  delete[] thread_args;
  delete[] thread_list;

  return 0;
}
