//Shahriar Arefin
#include "core/graph.h"
#include "core/utils.h"
#include "core/cxxopts.h"
#include "core/get_time.h"
#include <mpi.h>
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <cstdio>
#include <cassert>


#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
#define PAGERANK_MPI_TYPE MPI_LONG
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
#define PAGERANK_MPI_TYPE MPI_DOUBLE
typedef double PageRankType;
#endif

void Direct_Reduce  (Graph &g, int max_iters,int world_size, int world_rank){
    uintV n = g.n_;
    uintV m = g.m_;
    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];
    uintE edges_processed = 0;
    double communication_time = 0.0;
    timer communication_timer;
    timer global_timer;
    global_timer.start();
    communication_timer.start();
    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    int global_rank = world_rank;
    int P = world_size;
    uintV process_start = 0;
    uintV process_end = 0;
    uintV start=0;
    uintV end_ = 0;
    uintV work_distribution[world_size];
    for (int i = 0; i<P; i++) {
            start = end_;
            long count = 0;
            // add vertices until we reach m/world_size edges.
             while(end_ < n)
               {
                   count += g.vertices_[end_].getOutDegree();
                   end_ +=1;
                   if(count >= m/P)
                       break;

               }
        if(i == global_rank){
            process_start = start;
            process_end = end_;
        }
        work_distribution[i] = end_ - start;
    }
    for (int iter = 0; iter < max_iters; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = process_start; u < process_end; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            edges_processed += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }
        // --- synchronization phase 1 start ---
        communication_timer.start();
        uintV index = 0;
        for(int i = 0; i < world_size; i++) {
            if (i == world_rank) {
                MPI_Reduce(MPI_IN_PLACE, &pr_next[index], work_distribution[i], PAGERANK_MPI_TYPE, MPI_SUM, i, MPI_COMM_WORLD);
            }
            else{
                MPI_Reduce(&pr_next[index], &pr_next[index], work_distribution[i], PAGERANK_MPI_TYPE, MPI_SUM, i, MPI_COMM_WORLD);
            }
            index += work_distribution[i];
        }
        communication_time += communication_timer.stop();
        // --- synchronization phase 1 end ---

        for (uintV v = process_start; v < process_end; v++)
        {
            pr_next[v] = PAGE_RANK(pr_next[v]);
            // reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
        }
        for(uintV v = 0; v < n; v++) {
            pr_next[v] = 0.0;
        }

    }
    PageRankType local_sum = 0;
    for (uintV v = process_start; v < process_end; v++) {  // Loop 3
        local_sum += pr_curr[v];
    }
    // --- synchronization phase 2 start ---
    PageRankType global_sum;
    MPI_Reduce(&local_sum, &global_sum, 1, PAGERANK_MPI_TYPE, MPI_SUM,0, MPI_COMM_WORLD);
    // print process statistics and other results
    MPI_Barrier(MPI_COMM_WORLD);
    if(world_rank == 0){
        printf("%d, %d, %lf\n", world_rank, edges_processed, communication_time);
    }
    else{
        // print process statistics.
        printf("%d, %d, %lf\n", world_rank, edges_processed, communication_time);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(world_rank == 0){
      #ifdef USE_INT
          printf("Sum of page rank : %lu\n", global_sum);
      #else
          printf("Sum of page rank : %f\n", global_sum);
      #endif
      printf("Time taken (in seconds) : %lf\n", global_timer.stop());
    }

    // --- synchronization phase 2 end ---
    delete[] pr_curr;
    delete[] pr_next;

}
void Reduce_and_Scatter (Graph &g, int max_iters,int world_size, int world_rank){
    uintV n = g.n_;
    uintV m = g.m_;
    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];

    uintE edges_processed = 0;
    double communication_time = 0.0;
    timer communication_timer;
    timer global_timer;
    global_timer.start();
    communication_timer.start();
    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    int global_rank = world_rank;
    int P = world_size;
    uintV process_start = 0;
    uintV process_end = 0;
    uintV start=0;
    uintV end_ = 0;
    uintV work_distribution[world_size];
    uintV displs[world_size];
    int counter = 0;
    for (int i = 0; i<P; i++) {
            start = end_;
            long count = 0;
             while(end_ < n)
               {
                   count += g.vertices_[end_].getOutDegree();
                   end_ +=1;
                   if(count >= m/P)
                       break;

               }
        if(i == global_rank){
            process_start = start;
            process_end = end_;
        }
        work_distribution[i] = end_ - start;
        displs[i] = counter;
        counter += work_distribution[i];
    }
    for (int iter = 0; iter < max_iters; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = process_start; u < process_end; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            edges_processed += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }
        // --- synchronization phase 1 start ---
        communication_timer.start();
        if(world_rank == 0){
            MPI_Reduce(MPI_IN_PLACE, pr_next, n, PAGERANK_MPI_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);
            MPI_Scatterv(pr_next, work_distribution, displs, PAGERANK_MPI_TYPE, MPI_IN_PLACE, process_end-process_start, PAGERANK_MPI_TYPE, 0, MPI_COMM_WORLD);

        }
        else{
            MPI_Reduce(pr_next, pr_next, n, PAGERANK_MPI_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);
            MPI_Scatterv(pr_next, work_distribution, displs, PAGERANK_MPI_TYPE, &pr_next[process_start], process_end-process_start, PAGERANK_MPI_TYPE, 0, MPI_COMM_WORLD);
        }
        communication_time += communication_timer.stop();
        // --- synchronization phase 1 end ---

        //Loop 2
        for (uintV v = process_start; v < process_end; v++)
        {
            pr_next[v] = PAGE_RANK(pr_next[v]);
            // reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
        }
        for(uintV v = 0; v < n; v++) {
            pr_next[v] = 0.0;
        }

    }
    PageRankType local_sum = 0;
    for (uintV v = process_start; v < process_end; v++) {  // Loop 3
        local_sum += pr_curr[v];
    }
    // --- synchronization phase 2 start ---
    PageRankType global_sum;
    MPI_Reduce(&local_sum, &global_sum, 1, PAGERANK_MPI_TYPE, MPI_SUM,0, MPI_COMM_WORLD);
    // print process statistics and other results
    if(world_rank == 0){
        printf("%d, %d, %lf\n", world_rank, edges_processed, communication_time);
    }
    else{
        // print process statistics.
        printf("%d, %d, %lf\n", world_rank, edges_processed, communication_time);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(world_rank == 0){
      #ifdef USE_INT
          printf("Sum of page rank : %lu\n", global_sum);
      #else
          printf("Sum of page rank : %f\n", global_sum);
      #endif
      printf("Time taken (in seconds) : %lf\n", global_timer.stop());
    }

    // --- synchronization phase 2 end ---
    delete[] pr_curr;
    delete[] pr_next;

}

void Point_to_Point(Graph &g, int max_iters,int world_size, int world_rank){
    uintV n = g.n_;
    uintV m = g.m_;
    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];
    uintE edges_processed = 0;
    double communication_time = 0.0;
    timer communication_timer;
    timer global_timer;
    global_timer.start();
    communication_timer.start();
    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    int global_rank = world_rank;
    uintV process_start = 0;
    uintV process_end = 0;
    uintV start=0;
    uintV end_ = 0;
    uintV work_distribution[world_size];

    for (int i = 0; i<world_size; i++) {
            start = end_;
            long count = 0;
             while(end_ < n)
               {
                   count += g.vertices_[end_].getOutDegree();
                   end_ +=1;
                   if(count >= m/world_size)
                       break;
               }

        if(i == global_rank){
            process_start = start;
            process_end = end_;
        }
        work_distribution[i] = end_ - start;
    }
    for (int iter = 0; iter < max_iters; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = process_start; u < process_end; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            edges_processed += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }
        // --- synchronization phase 1 start ---
        communication_timer.start();
        if(world_rank == 0){
            PageRankType *temp_next = new PageRankType[n];
            for(int i =1;i < world_size; i++){
              //receive next page value from processes
                MPI_Recv(temp_next, n, PAGERANK_MPI_TYPE, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                for (uintV v = 0; v < n; v++) {
                    pr_next[v] += temp_next[v];
                }
            }
            delete[] temp_next;
            uintV index = work_distribution[0];
            for(int i = 1; i < world_size; i++){
                MPI_Send(&pr_next[index], work_distribution[i], PAGERANK_MPI_TYPE, i, i, MPI_COMM_WORLD);
                index+=work_distribution[i];
            }

        }
        else{
            MPI_Send(pr_next, n, PAGERANK_MPI_TYPE, 0, world_rank, MPI_COMM_WORLD);
            MPI_Recv(&pr_next[process_start],process_end-process_start, PAGERANK_MPI_TYPE, 0, world_rank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        communication_time += communication_timer.stop();
        // --- synchronization phase 1 end ---

        //Loop 2
        for (uintV v = process_start; v < process_end; v++)
        {
            pr_next[v] = PAGE_RANK(pr_next[v]);
            // reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
        }
        for(uintV v = 0; v < n; v++) {
            pr_next[v] = 0.0;
        }

    }
    PageRankType local_sum = 0;
    for (uintV v = process_start; v < process_end; v++) {  // Loop 3
        local_sum += pr_curr[v];
    }
    // --- synchronization phase 2 start ---
    PageRankType global_sum;
    MPI_Reduce(&local_sum, &global_sum, 1, PAGERANK_MPI_TYPE, MPI_SUM,0, MPI_COMM_WORLD);
    // print process statistics and other results
    if(world_rank == 0){
        printf("%d, %d, %lf\n", world_rank, edges_processed, communication_time);
    }
    else{
        // print process statistics.
        printf("%d, %d, %lf\n", world_rank, edges_processed, communication_time);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(world_rank == 0){
      #ifdef USE_INT
          printf("Sum of page rank : %lu\n", global_sum);
      #else
          printf("Sum of page rank : %f\n", global_sum);
      #endif
      printf("Time taken (in seconds) : %lf\n", global_timer.stop());
    }

    // --- synchronization phase 2 end ---
    delete[] pr_curr;
    delete[] pr_next;
    // delete[] temp_next;

}

void print_statistics(){}

int main(int argc, char *argv[])
{
    cxxopts::Options options("page_rank_push", "Calculate page_rank using serial and parallel execution");
    options.add_options("", {
                                {"nIterations", "Maximum number of iterations", cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
                                {"strategy", "Strategy to be used", cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
                                {"inputFile", "Input graph file path", cxxopts::value<std::string>()->default_value("/scratch/assignment1/input_graphs/roadNet-CA")},
                            });

    auto cl_options = options.parse(argc, argv);
    uint strategy = cl_options["strategy"].as<uint>();
    assert(strategy > 0 && strategy <=3);
    uint max_iterations = cl_options["nIterations"].as<uint>();
    assert(max_iterations > 0);
    std::string input_file_path = cl_options["inputFile"].as<std::string>();
    assert(!input_file_path.empty());

    // Initialize the MPI environment
    MPI_Init(NULL, NULL);

    // Get the number of processes
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Get the rank of the process
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    Graph g;
    g.readGraphFromBinary<int>(input_file_path);

    if(world_rank == 0){
        #ifdef USE_INT
            std::cout << "Using INT\n";
        #else
            std::cout << "Using DOUBLE\n";
        #endif
        std::cout << "World Size : " << world_size << "\n";
        std::cout << "Communication strategy : " << strategy << "\n";
        std::cout << "Iterations: " << max_iterations << "\n";
        std::cout <<"rank, num_edges, communication_time\n";
        // printf("World size : %d\n", world_size);
        // printf("Communication strategy : %u\n", strategy);
        // printf("Iterations : %u\n", max_iterations);
        // printf("rank, num_edges, communication_time\n");
    }
    switch (strategy)
    {
    case 1:
        Point_to_Point(g,max_iterations,world_size,world_rank);
        break;
    case 2:
        Reduce_and_Scatter (g,max_iterations,world_size,world_rank);
        break;
    case 3:
        Direct_Reduce  (g,max_iterations,world_size,world_rank);
        break;
    default:
        break;
    }
    MPI_Finalize();

    return 0;
}
