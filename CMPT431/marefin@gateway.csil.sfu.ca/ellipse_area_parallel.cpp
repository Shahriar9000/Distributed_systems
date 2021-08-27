#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <pthread.h>
#include <cassert>
#include "core/cxxopts.h"
#include "core/get_time.h"

using namespace std;

#define sqr(x) ((x) * (x))
#define DEFAULT_NUMBER_OF_POINTS "1000000000"
#define DEFAULT_MAJOR_RADIUS "2"
#define DEFAULT_MINOR_RADIUS "1"
#define DEFAULT_RANDOM_SEED "1"

struct thread_data{
    int t_id;
    unsigned long n_this_thread;
    unsigned long count;
    double time;
    uint random_seed;
    float maj_radius;
    float min_radius;
};

uint c_const = (uint)RAND_MAX + (uint)1;
inline double get_random_coordinate(uint *random_seed) {
  return ((double)rand_r(random_seed)) / c_const;
}

unsigned long get_points_in_ellipse(unsigned long n, uint random_seed, float maj_radius, float min_radius)
{
  unsigned long ellipse_count = 0;
  double x_coord, y_coord;
  for (unsigned long i = 0; i < n; i++) {
    x_coord = maj_radius * ((2.0 * get_random_coordinate(&random_seed)) - 1.0);
    y_coord = min_radius * ((2.0 * get_random_coordinate(&random_seed)) - 1.0);
    if ((sqr(x_coord)/sqr(maj_radius) + sqr(y_coord)/sqr(min_radius)) <= 1.0)
      ellipse_count++;
  }
  return ellipse_count;
}
int main(int argc, char *argv[]) {
  // Initialize command line arguments

  MPI_Init(NULL,NULL);
  int world_rank;
  int world_size;
  //  Get the number of processors.
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  //  Get the rank of this processor.
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  cxxopts::Options options("Ellipse_area_calculation",
                           "Calculate area of an ellipse using serial and parallel execution");

  options.add_options(
      "custom",
      {
          {"nThreads", "Number of threads",
           cxxopts::value<int>()->default_value(DEFAULT_NUMBER_OF_THREADS)},
          {"nPoints", "Number of points",
           cxxopts::value<unsigned long>()->default_value(DEFAULT_NUMBER_OF_POINTS)},
          {"majorRad", "Major radius",
          cxxopts::value<float>()->default_value(DEFAULT_MAJOR_RADIUS)},
          {"minorRad", "Minor radius",
           cxxopts::value<float>()->default_value(DEFAULT_MINOR_RADIUS)},
          {"rSeed", "Random Seed",
           cxxopts::value<uint>()->default_value(DEFAULT_RANDOM_SEED)}
      });
  auto cl_options = options.parse(argc, argv);

  int n_threads = cl_options["nThreads"].as<int>();
  assert(n_threads > 0);

  unsigned long n_points = cl_options["nPoints"].as<unsigned long>();
  assert(n_points > 0);

  float maj_radius = cl_options["majorRad"].as<float>();
  assert (maj_radius > 0);

  float min_radius = cl_options["minorRad"].as<float>();
  assert (min_radius > 0);

  uint r_seed = cl_options["rSeed"].as<uint>();
  assert (r_seed > 0);
  //---------------------------------

  if(world_rank==0){
    printf("Number of processes : %d",world_size);
    printf("Number of points : %ul\n",n_points);
    printf("Major Radius : %f\n",maj_radius);
    printf("Minor Radius : %f\n",min_radius);
    printf("Random Seed : %ud\n",r_seed);

    // std::cout << "Number of points : " << n_points << std::endl;
    // cout << "Number of threads : "<< n_threads<<endl;
    // std::cout << "Major Radius : " << maj_radius << std::endl << "Minor Radius : " << min_radius << std::endl;
    // std::cout << "Random Seed : " << r_seed << std::endl;
  }
  timer global_timer;
  global_timer.start();
  // Dividing up n vertices on P processes.
  // Total number of processes is world_size. This process rank is world_rank
  unsigned long  min_points_per_process = n_points / world_size;
  int excess_points = n_points % world_size;
  unsigned long points_to_be_generated = min_points_per_process;

  if (world_rank < excess_points){
    unsigned long points_to_be_generated = min_points_per_process + 1;
  }
  else{
    unsigned long points_to_be_generated = min_points_per_process;
  }
  // Each process will work on points_to_be_generated and estimate ellipse_points.
  timer local_timer;
  local_timer.start();

  uint random_seed = random_seed + world_rank;
  unsigned long ellipse_count = get_points_in_ellipse(points_to_be_generated, random_seed, maj_radius, min_radius);
  local_timer.stop();

  unsigned long count = ellipse_count;

  if(world_rank == 0)
  {
    printf("rank, points_generated, ellipse_points, time_taken\n" );
    printf("%d, %ul, %ul, %f", world_rank, points_to_be_generated, ellipse_count, time);
    unsigned long local_count = 0;
    for(int j=1; j<world_size; j++)
    {
      MPI_Recv(&local_count, 1, MPI_UNSIGNED_LONG, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      count += local_count;
    }
    double total_time_taken = global_timer.stop();
    double area_value = (double)maj_radius * (double)min_radius * 4.0 * (double)global_count / (double)n_points;
    std::cout << "Total points generated : " << n_points << "\n";
    std::cout << "Total points in ellipse : " << global_ellpise_points << "\n";
    std::cout << "Result : " << std::setprecision(VAL_PRECISION) << area_value<< "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)<< total_time_taken << "\n";
  }
  else{
    MPI_Send(&ellipse_count, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD);
    printf("%d, %ul, %ul, %f", world_rank, points_to_be_generated, ellipse_count, time);
  }
  MPI_Finalize();
  return 0;
}
