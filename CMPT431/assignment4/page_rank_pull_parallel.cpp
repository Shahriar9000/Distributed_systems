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
    uintV vertices_;
    uintE edges_;
    uint iterStart;
    uint iterEnd;
    double thread_time;
    PageRankType *pr_next;
    PageRankType *pr_curr;
    pthread_mutex_t* mutex;
    CustomBarrier* barrier_1;
    CustomBarrier* barrier_2;

};

struct thread_data* thread_args;
pthread_t* thread_list;

void page_rank_strategy1OR2(Graph *g, uint iterStart, uint iterEnd, uint thread_id, uint n, double *thread_time, std::atomic<PageRankType>*pr_next, PageRankType *pr_curr, CustomBarrier* barrier_,
  double *barrier1_time, double *barrier2_time, long* edgeCountThread,
     long* vertexCountThread, int max_iters){

  timer thread_timer, b1_time, b2_time;
  long edgeCount = 0;
  long vertexCount = 0;
  PageRankType* temporary_placeholder = new PageRankType [n];
  thread_timer.start();
  for(int i = 0; i < max_iters; i++)
  {
    for (uintV u = iterStart; u < iterEnd; u++)
    {
        uintE in_degree = g->vertices_[u].getInDegree();
        for (uintE i = 0; i < in_degree; i++)
        {
          edgeCount++;
          uintV v = g->vertices_[u].getInNeighbor(i);
          uintE u_out_degree = g->vertices_[v].getOutDegree();
          if (u_out_degree > 0)
          {
            pr_next[u] = pr_next[u] + (pr_curr[v] / u_out_degree);
          }
        }
    }
    *edgeCountThread = edgeCount;
    b1_time.start();
    barrier_->wait();
    *barrier1_time += b1_time.stop();

    for (uintV v = iterStart; v < iterEnd; v++)
    {
      vertexCount++;
      pr_next[v] = PAGE_RANK(pr_next[v]);
      // reset pr_curr for the next iteration
      pr_curr[v] = pr_next[v];
      pr_next[v] = 0.0;
    }
    *vertexCountThread = vertexCount;
    *thread_time += thread_timer.stop();

    b2_time.start();
    barrier_->wait();
    *barrier2_time += b2_time.stop();
  }

}

void Edge_based_decomposition(Graph *g, uintV *start, uintV *end ,uintV vertices_, uintE edges_, uint n_threads){
  int thread_id = 0;
  uintE limit = 0;
  uintE out_degree;
  start[thread_id] = 0;
  for(int v = 0; v < vertices_; v++){
    out_degree = g->vertices_[v].getOutDegree();
    limit = limit + out_degree;
    if(limit >= ((thread_id+1) * edges_/n_threads))
    {
      end[thread_id]=v;
      thread_id++;
      start[thread_id]=v;
    }
    if(thread_id == n_threads-1)
    {
      break;
    }
  }
  end[thread_id]=vertices_;
}

uintV getNextVertexToBeProcessed(std::atomic <long> &v, uintV n, uint granularity){

  if(v >= n){
    return -1;
  }
  return v.fetch_add(granularity);
}

void page_rank_strategy3OR4(Graph *g, uint thread_id, uintV vertices_, double *thread_time, std::atomic<PageRankType>* pr_next,
  PageRankType *pr_curr, CustomBarrier* barrier_, double *barrier1_time,
  double *barrier2_time, long* edgeCountThread, long* vertexCountThread, double* getvertexTime,
  std::atomic <long>* getVertexShared1,std::atomic <long>* getVertexShared2, uint granularity){

    timer thread_timer, b1_time, b2_time, gvTimer;
    PageRankType* temporary_placeholder = new PageRankType [vertices_];
     long edgeCount = 0;
     long vertexCount = 0;
     double getVertecTimer = 0.0;
     thread_timer.start();

     while(true)
     {
       gvTimer.start();
       uintV vertex_to_process = getNextVertexToBeProcessed(*getVertexShared1, vertices_, granularity);
       *getvertexTime += gvTimer.stop();
       if(vertex_to_process == -1){break;}

       for(int u = vertex_to_process; u  < vertex_to_process+granularity && u < vertices_; u++)
       {
         uintE in_degree = g->vertices_[u].getInDegree();

         for (uintE i = 0; i < in_degree; i++)
         {
           edgeCount ++;
           uintV v = g->vertices_[u].getInNeighbor(i);
           uintE u_out_degree = g->vertices_[v].getOutDegree();
           if (u_out_degree > 0)
           {
             pr_next[u] = pr_next[u]  + (pr_curr[v] / u_out_degree);
           }
         }
       }
     }
     *edgeCountThread += edgeCount;

     b1_time.start();
     barrier_->wait();
     *barrier1_time += b1_time.stop();

     while(true)
     {
       gvTimer.start();
       uintV vertex_to_process = getNextVertexToBeProcessed(*getVertexShared2, vertices_, granularity);
       *getvertexTime += gvTimer.stop();
       if(vertex_to_process == -1){ break;}

      for(int v = vertex_to_process; v < vertex_to_process+granularity && v < vertices_; v++){
        vertexCount++;
        pr_next[v] = PAGE_RANK(pr_next[v]);

        // reset pr_curr for the next iteration
        pr_curr[v] = pr_next[v];
        pr_next[v] = 0.0;
      }
     }
     *vertexCountThread += vertexCount;
     *thread_time += thread_timer.stop();

     b2_time.start();
     barrier_->wait();
     *barrier2_time += b2_time.stop();

}

void Create_Threads(Graph &g, int max_iters, uint n_threads, CustomBarrier &barrier_, uint strategy, uint granularity) {
  uintV vertices_ = g.n_;
  uintE edges_= g.m_;

  PageRankType *pr_curr = new PageRankType[vertices_];
  std::atomic<PageRankType>* pr_next = new std::atomic <PageRankType>[vertices_];

  long vertexCount[n_threads];
  long edgeCount[n_threads];
  double barrier1_time[n_threads];
  double barrier2_time[n_threads];
  double getVertex_time[n_threads];
  double thread_time[n_threads];
  uintV start[n_threads];
  uintV end[n_threads];

  for(int i =0; i< n_threads; i++)
  {
   thread_time[i] = 0.0;
   vertexCount[i] = 0;
   edgeCount[i] = 0;
   barrier1_time[i] = 0.0;
   barrier2_time[i] = 0.0;
   getVertex_time[i] = 0.0;
  }
  for (uintV i = 0; i < vertices_; i++)
  {
     pr_curr[i] = INIT_PAGE_RANK;
     pr_next[i] = 0.0;
  }
  // Push based pagerank
  timer t1;
  double total_time_taken = 0.0;
  double partitioning_time = 0.0;
  atomic <long> getVertexShared1{0};
  atomic <long> getVertexShared2{0};
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  std::thread thread_array[n_threads];
  int thread_id;
  int last_thread = n_threads -1;

  switch(strategy)
  {
     case 1:
       for (int i = 0; i < n_threads-1; i++) {
         start[i]= (vertices_/n_threads)*i;
         end[i]  = ((i+1)*(vertices_/n_threads));
       }
       start[last_thread]= vertices_/n_threads*(last_thread);
       end[last_thread] = vertices_;
       break;
     case 2:
       //EDGE based Decomposition
       Edge_based_decomposition(&g, start, end,  vertices_, edges_, n_threads);
       break;
     default:
        break;
  }
  t1.start();
  if (strategy == 3 || strategy == 4)
  {
    if (strategy == 3) {granularity = 1;}

    // std::cout << "Granularity=" << granularity << '\n';
    for(int i = 0; i < max_iters; i++)
    {
      getVertexShared1 = 0;
      getVertexShared2 = 0;
      for(uint i = 0; i < n_threads; i++)
      {
        thread_id = i;
        thread_array[i] = std::thread(page_rank_strategy3OR4, &g, thread_id, vertices_, &thread_time[i], std::ref(pr_next), std::ref(pr_curr), &barrier_,
                                        &barrier1_time[i],  &barrier2_time[i], &edgeCount[i], &vertexCount[i], &getVertex_time[i], &getVertexShared1, &getVertexShared2, granularity);
      }

      for(uint i = 0; i < n_threads; i++)
      {
       thread_array[i].join();
      }
    }
  }
  else
  {
    for(uint i = 0; i < n_threads; i++)
    {
       thread_id = i;
       thread_array[i] = std::thread(page_rank_strategy1OR2, &g, start[i], end[i], thread_id, vertices_, &thread_time[i], std::ref(pr_next), std::ref(pr_curr), &barrier_,
                                        &barrier1_time[i],  &barrier2_time[i], &edgeCount[i], &vertexCount[i], max_iters);
    }
    for(uint i = 0; i < n_threads; i++)
    {
     thread_array[i].join();
    }
  }

  total_time_taken = t1.stop();
  // -------------------------------------------------------------------
  std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, total_time" << endl;
  for(uint i = 0; i < n_threads; i++)
  {
      std::cout <<i<< ", "<<vertexCount[i]<< ", "<<edgeCount[i]<< ", "<<barrier1_time[i]<< ", "<<barrier2_time[i]<< ", "<<getVertex_time[i]<< ", "
      << std::setprecision(TIME_PRECISION) << thread_time[i]<< endl;
  }
  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < vertices_; u++){
     sum_of_page_ranks += pr_curr[u];
  }

  std::cout << "Sum of page ranks : " << sum_of_page_ranks << std::endl;
  std::cout << "Time taken (in seconds) : " << total_time_taken << std::endl;
  delete[] pr_curr;
  delete[] pr_next;
  // delete[] thread_args;
  // delete[] thread_list;
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
          {"strategy",  "Task Decomposition Strategy",
        cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
        {"granularity",  "granularity for Strategy 4",
      cxxopts::value<uint>()->default_value(DEFAULT_GRANULARITY)},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_threads = cl_options["nThreads"].as<uint>();
  assert(n_threads > 0);
  uint max_iterations = cl_options["nIterations"].as<uint>();
  assert(max_iterations > 0);
  std::string input_file_path = cl_options["inputFile"].as<std::string>();
  assert(!input_file_path.empty());
  uint strategy = cl_options["strategy"].as<uint>();
  assert(strategy > 0 && strategy <=4);
  uint granularity = cl_options["granularity"].as<uint>();
  assert(granularity > 0);




#ifdef USE_INT
  std::cout << "Using INT" << std::endl;
#else
  std::cout << "Using DOUBLE" << std::endl;
#endif
  std::cout << std::fixed;
  std::cout << "Number of Threads : " << n_threads << std::endl;
  std::cout << "Strategy: " << strategy << std::endl;
  std::cout << "Granularity: " << granularity << std::endl;
  std::cout << "Iterations: " << max_iterations << std::endl;

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";


  CustomBarrier barrier_(n_threads);
  Create_Threads(g, max_iterations, n_threads, barrier_, strategy, granularity);

  return 0;
}
