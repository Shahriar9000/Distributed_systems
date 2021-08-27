#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <mpi.h>
using namespace std;

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
#define MPI_PAGERANK_TYPE MPI_LONG
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
#define MPI_PAGERANK_TYPE MPI_DOUBLE
typedef double PageRankType;
#endif


void pageRankParallel_1(Graph &g, int max_iters) {
  uintV n = g.n_;
  uintE m = g.m_;

  int world_size;
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  PageRankType *pr_curr = new PageRankType[n];
  PageRankType *pr_next = new PageRankType[n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0;
  }
  timer communication_timer;
  timer total_timer;
	total_timer.start();
  int edges_processed = 0;
  double total_time = 0;
  double communication_time = 0;

  uintE total_assigned_edges = 0;
  uintV last_assigned_vertex = 0;

  int start_arr[world_size];
  int end_arr[world_size];

  int counter = 0;

  for (uintV v = 0; v < n; v++){
    uintE out_degree = g.vertices_[v].getOutDegree();
    total_assigned_edges = total_assigned_edges + out_degree;

    if(total_assigned_edges >= (counter+1)*(m/world_size)){

      if(counter==0){
        start_arr[counter] = 0;
        end_arr[counter] = v+1;
      }

      else if(counter == world_size-1){
        start_arr[counter] = end_arr[counter-1];
        end_arr[counter] = n;
      }

      else{
        start_arr[counter] = end_arr[counter-1];
        end_arr[counter] = v+1;
      }
      counter++;
    }
  }


	for (int iter = 0; iter < max_iters; iter++) {
		// for each vertex 'u', process all its outNeighbors 'v'

    for (uintV u = start_arr[world_rank]; u < end_arr[world_rank]; u++) {
				uintE out_degree = g.vertices_[u].getOutDegree();
				edges_processed = edges_processed + out_degree;

        for (uintE i = 0; i < out_degree; i++) {
						uintV v = g.vertices_[u].getOutNeighbor(i);
						pr_next[v] += (pr_curr[u] / (PageRankType) out_degree);
				}
		}
		communication_timer.start();

		if(world_rank!=0){
        MPI_Send(pr_next, n, MPI_PAGERANK_TYPE, 0, 0, MPI_COMM_WORLD);
				int idx_start = start_arr[world_rank];
        int size = end_arr[world_rank] - start_arr[world_rank];
        MPI_Recv(&pr_next[idx_start], size, MPI_PAGERANK_TYPE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

		if(world_rank==0){
          PageRankType *pr_recv = new PageRankType[n];

          for (int i=1; i<world_size; i++){
            MPI_Recv(pr_recv, n, MPI_PAGERANK_TYPE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (uintV v = 0; v < n; v++){
              pr_next[v] += pr_recv[v];
            }
          }
          delete[] pr_recv;

          for (int i=1; i<world_size; i++){
            int size = end_arr[i] - start_arr[i];
            int start = start_arr[i];
            MPI_Send(&pr_next[start], size, MPI_PAGERANK_TYPE, i, 0, MPI_COMM_WORLD);
          }
    }
		communication_time += communication_timer.stop();

		for (uintV v = start_arr[world_rank]; v < end_arr[world_rank]; v++) {
				pr_next[v] = PAGE_RANK(pr_next[v]);
				pr_curr[v] = pr_next[v];
		}

		for (uintV v = 0; v < n; v++){
			pr_next[v] = 0;
		}
	}
	PageRankType global_sum;
	PageRankType local_sum = 0;

  for (uintV v = start_arr[world_rank]; v < end_arr[world_rank]; v++) {
		local_sum += pr_curr[v];
	}

  global_sum = local_sum;

  if(world_rank != 0){
		MPI_Send(&local_sum, 1, MPI_PAGERANK_TYPE, 0, 0, MPI_COMM_WORLD);
		std::cout << world_rank <<", "<< edges_processed <<", "<< communication_time <<"\n";
	}


  if(world_rank == 0){
    std::cout << world_rank <<", "<< edges_processed <<", "<< communication_time <<"\n";
    PageRankType sum_recieved;

    for (int i=1; i<world_size; i++){
      MPI_Recv(&sum_recieved, 1, MPI_PAGERANK_TYPE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      global_sum += sum_recieved;
    }
  }


  MPI_Barrier(MPI_COMM_WORLD);
  if(world_rank == 0){
		std::cout<< "Sum of page rank : " << global_sum << "\n";
    total_time = total_timer.stop();
    std::cout<< "Time taken (in seconds) : " << total_time << "\n";
  }
	delete[] pr_curr;
  delete[] pr_next;

}


void pageRankParallel_2(Graph &g, int max_iters) {
  uintV n = g.n_;
  uintE m = g.m_;

  int world_size;
  int world_rank;

  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  PageRankType *pr_curr = new PageRankType[n];
  PageRankType *pr_next = new PageRankType[n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0;
  }

  timer communication_timer;
  timer total_timer;

  total_timer.start();

  int edges_processed = 0;
  double total_time = 0;
  double communication_time = 0;

  uintE total_assigned_edges = 0;
  uintV last_assigned_vertex = 0;

  int start_arr[world_size];
  int end_arr[world_size];

	int cnts_arr[world_size];
  int disp_arr[world_size];

  int counter = 0;
  for (uintV v = 0; v < n; v++){
    uintE out_degree = g.vertices_[v].getOutDegree();
    total_assigned_edges = total_assigned_edges + out_degree;
    if(total_assigned_edges >= (counter+1)*(m/world_size)){

      if(counter==0){
        start_arr[counter] = 0;
        end_arr[counter] = v+1;
      }

      else if(counter == world_size-1){
        start_arr[counter] = end_arr[counter-1];
        end_arr[counter] = n;
      }

      else{
        start_arr[counter] = end_arr[counter-1];
        end_arr[counter] = v+1;
      }
      cnts_arr[counter] = end_arr[counter] - start_arr[counter];
      disp_arr[counter] = start_arr[counter];
      counter++;
    }
  }

	for (int iter = 0; iter < max_iters; iter++) {
      // for each vertex 'u', process all its outNeighbors 'v'

      for (uintV u = start_arr[world_rank]; u < end_arr[world_rank]; u++) {
          uintE out_degree = g.vertices_[u].getOutDegree();
          edges_processed = edges_processed + out_degree;

          for (uintE i = 0; i < out_degree; i++) {
              uintV v = g.vertices_[u].getOutNeighbor(i);
              pr_next[v] += (pr_curr[u] / (PageRankType) out_degree);
          }
      }
			communication_timer.start();

      if(world_rank!=0){
        MPI_Reduce(pr_next, pr_next, n, MPI_PAGERANK_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);
        int rec_count = end_arr[world_rank]-start_arr[world_rank];
        MPI_Scatterv(pr_next, cnts_arr, disp_arr, MPI_PAGERANK_TYPE, &pr_next[start_arr[world_rank]], rec_count, MPI_PAGERANK_TYPE, 0, MPI_COMM_WORLD);
      }

      if(world_rank==0){
        MPI_Reduce(MPI_IN_PLACE, pr_next, n, MPI_PAGERANK_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);
        int rec_count = end_arr[world_rank]-start_arr[world_rank];
        MPI_Scatterv(pr_next, cnts_arr, disp_arr, MPI_PAGERANK_TYPE, MPI_IN_PLACE, rec_count, MPI_PAGERANK_TYPE, 0, MPI_COMM_WORLD);
      }
      communication_time += communication_timer.stop();

      for (uintV v = start_arr[world_rank]; v < end_arr[world_rank]; v++) {
				pr_next[v] = PAGE_RANK(pr_next[v]);
				pr_curr[v] = pr_next[v];
			}

      for (uintV v = 0; v < n; v++){
				pr_next[v] = 0;
			}
	}
	PageRankType global_sum;
	PageRankType local_sum = 0.0;

  for (uintV v = start_arr[world_rank]; v < end_arr[world_rank]; v++) {
		local_sum += pr_curr[v];
	}
  MPI_Reduce(&local_sum, &global_sum, 1, MPI_PAGERANK_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);


	std::cout << world_rank << ", " << edges_processed << ", " << communication_time <<"\n";

  MPI_Barrier(MPI_COMM_WORLD);

  if(world_rank == 0){
		std::cout<< "Sum of page rank : " << global_sum << "\n";
    total_time = total_timer.stop();
    std::cout<< "Time taken (in seconds) : " << total_time << "\n";
  }

  delete[] pr_curr;
  delete[] pr_next;
}


void pageRankParallel_3(Graph &g, int max_iters) {
  uintV n = g.n_;
  uintE m = g.m_;

  int world_size;
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  PageRankType *pr_curr = new PageRankType[n];
  PageRankType *pr_next = new PageRankType[n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0;
  }

  timer communication_timer;
  timer total_timer;
	total_timer.start();
	int edges_processed = 0;
  double total_time = 0;
  double communication_time = 0;

  uintE total_assigned_edges = 0;
  uintV last_assigned_vertex = 0;

  int start_arr[world_size];
  int end_arr[world_size];

  int counter = 0;

  for (uintV v = 0; v < n; v++){
    uintE out_degree = g.vertices_[v].getOutDegree();
    total_assigned_edges = total_assigned_edges + out_degree;

    if(total_assigned_edges >= (counter+1)*(m/world_size)){

      if(counter==0){
        start_arr[counter] = 0;
        end_arr[counter] = v+1;
      }

      else if(counter == world_size-1){
        start_arr[counter] = end_arr[counter-1];
        end_arr[counter] = n;
      }

      else{
        start_arr[counter] = end_arr[counter-1];
        end_arr[counter] = v+1;
      }
      counter++;
    }
  }

	for (int iter = 0; iter < max_iters; iter++) {
      // for each vertex 'u', process all its outNeighbors 'v'

      for (uintV u = start_arr[world_rank]; u < end_arr[world_rank]; u++) {
          uintE out_degree = g.vertices_[u].getOutDegree();
          edges_processed = edges_processed + out_degree;

          for (uintE i = 0; i < out_degree; i++) {
              uintV v = g.vertices_[u].getOutNeighbor(i);
              pr_next[v] += (pr_curr[u] / (PageRankType) out_degree);
          }
      }
      communication_timer.start();

      for(int i = 0; i < world_size; i++) {
          int count = end_arr[i] - start_arr[i];

          if (i == world_rank) {
						MPI_Reduce(MPI_IN_PLACE, &pr_next[start_arr[i]], count, MPI_PAGERANK_TYPE, MPI_SUM, i, MPI_COMM_WORLD);
					}

          else {
						MPI_Reduce(&pr_next[start_arr[i]], &pr_next[start_arr[i]], count, MPI_PAGERANK_TYPE, MPI_SUM, i, MPI_COMM_WORLD);
					}
      }
      communication_time += communication_timer.stop();

      for (uintV v = start_arr[world_rank]; v < end_arr[world_rank]; v++) {
				pr_next[v] = PAGE_RANK(pr_next[v]);
				pr_curr[v] = pr_next[v];
			}

      for (uintV v = 0; v < n; v++){
				pr_next[v] = 0;
			}
	}
	PageRankType local_sum = 0.0;

  for (uintV v = start_arr[world_rank]; v < end_arr[world_rank]; v++) {
		local_sum += pr_curr[v];
	}
  PageRankType global_sum;
  MPI_Reduce(&local_sum, &global_sum, 1, MPI_PAGERANK_TYPE, MPI_SUM,0, MPI_COMM_WORLD);

	std::cout << world_rank << ", " << edges_processed << ", " << communication_time <<"\n";

  MPI_Barrier(MPI_COMM_WORLD);

  if(world_rank == 0){
		std::cout<< "Sum of page rank : " << global_sum << "\n";
    total_time = total_timer.stop();
    std::cout<< "Time taken (in seconds) : " << total_time << "\n";
  }
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
          {"nIterations", "Maximum number of iterations",
           cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
          {"strategy", "Mpi Strategy number",
           cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
      });

  auto cl_options = options.parse(argc, argv);
  uint max_iterations = cl_options["nIterations"].as<uint>();
  uint strategy = cl_options["strategy"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();

  if (max_iterations < 1) {
    std::cout << "Must have at least 1 iteration Terminating..." << std::endl;
    return 1;
  }
  if (strategy < 1 || strategy > 3) {
    std::cout << "Strategy must be an integer between 1 and 4 (inclusive) Terminating..." << std::endl;
    return 1;
  }

  MPI_Init(NULL, NULL);
  int world_size;
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  if (world_rank == 0){
      #ifdef USE_INT
        std::cout << "Using INT" << std::endl;
      #else
        std::cout << "Using DOUBLE" << std::endl;
      #endif
        std::cout << std::fixed;
        std::cout << "World size : " << world_size << std::endl;
        std::cout << "Communication strategy : " << strategy << std::endl;
        std::cout << "Iterations : " << max_iterations << std::endl;
        std::cout << "rank, num_edges, communication_time"<<std::endl;
  }


  Graph g;
  g.readGraphFromBinary<int>(input_file_path);

  if (g.n_ < 1) {
    std::cout << "Graph must have at least 1 Vertex..." << std::endl;
    return 1;
  }
  if (g.m_ < 1) {
    std::cout << "Graph must have at least 1 Edge..." << std::endl;
    return 1;
  }

	if (strategy ==1){
		pageRankParallel_1(g, max_iterations);
	}
	else if (strategy == 2){
		pageRankParallel_2(g, max_iterations);
	}
	else if (strategy == 3){
		pageRankParallel_3(g, max_iterations);
	}

  MPI_Finalize();
  return 0;

}
