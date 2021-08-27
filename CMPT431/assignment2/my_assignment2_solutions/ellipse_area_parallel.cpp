#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <pthread.h>
#include <cassert>


using namespace std;

#define sqr(x) ((x) * (x))
#define DEFAULT_NUMBER_OF_POINTS "1000000000"
#define DEFAULT_MAJOR_RADIUS "2"
#define DEFAULT_NUMBER_OF_THREADS "2"
#define DEFAULT_MINOR_RADIUS "1"
#define DEFAULT_RANDOM_SEED "1"


struct thread_data{
    int t_id;
    long total_points;
    pthread_mutex_t* mutex;
    unsigned long* g_points_ellipse;
    unsigned long l_points_ellipse;
    double time;
    uint random_seed;
    float maj_radius;
    float min_radius;
};

struct thread_data* thread_args;
pthread_t* threads_list;

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
void* thread_function(void* _arg){
    timer timer_;

    double time_taken = 0.0;
    timer_.start();
    
    struct thread_data* t_args;
    t_args = (struct thread_data*) _arg;

    unsigned long local_el_ps = get_points_in_ellipse(t_args->total_points, (t_args->random_seed + t_args->t_id), t_args->maj_radius, t_args->min_radius);
    
    //get mutex before calculating
    pthread_mutex_lock(t_args->mutex);
    *t_args->g_points_ellipse = *t_args->g_points_ellipse + local_el_ps;
    t_args->l_points_ellipse = local_el_ps;
    pthread_mutex_unlock(t_args->mutex);


    t_args->time = timer_.stop();

    //end threads
    pthread_exit(NULL);
}


void ellipse_area_parallel_calculation(int n_threads, unsigned long n, float maj_radius, float min_radius, uint r_seed, thread_data* thread_args, pthread_t* threads_list) {
  timer timer_;
  double total_time_taken = 0.0;
  uint random_seed = r_seed;
  //start timer
  timer_.start();
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);
  unsigned long global_ellpise_points = 0;
  
  int rc;
  long x= 0;
  for (int i = 0; i< n_threads; i++)
  {
    thread_args[i].t_id = i;
  
    thread_args[i].random_seed = r_seed;
    thread_args[i].mutex = &mutex;
    thread_args[i].maj_radius = maj_radius;
    thread_args[i].min_radius = min_radius;
    thread_args[i].g_points_ellipse = &global_ellpise_points;

    if (i == 0)
    {
        x = n/n_threads+n%n_threads;
        thread_args[i].total_points = x;
    }
    else
    {   x = n/n_threads;
        thread_args[i].total_points = x;
    }

    rc = pthread_create(&threads_list[i], NULL, thread_function, (void*) &thread_args[i]);
    if (rc)
    {
          cout << "Error creating thread" << i << "error:" << rc << endl;
    }
  }

  for (int i = 0; i< n_threads; i++)
  {
    rc = pthread_join(threads_list[i], NULL);
    if (rc)
    {
          cout << "Error joining thread" << i << "error:" << rc << endl;
    }

  }

  //get area
  double area_value = (double)maj_radius * (double)min_radius * 4.0 * (double)global_ellpise_points / (double)n;
  total_time_taken = timer_.stop(); //stop timer

  cout << "thread_id, points_generated, ellipse_points, time_taken\n";
  for (int i =0 ; i< n_threads; i++){
      cout << thread_args[i].t_id<< ", " << thread_args[i].total_points << ", "<< thread_args[i].l_points_ellipse << ", " << std::setprecision(TIME_PRECISION)<< thread_args[i].time << "\n";
  }

  std::cout << "Total points generated : " << n << "\n";
  std::cout << "Total points in ellipse : " << global_ellpise_points << "\n";
  std::cout << "Result : " << std::setprecision(VAL_PRECISION) << area_value<< "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)<< total_time_taken << "\n";
}

int main(int argc, char *argv[]) {
  // Initialize command line arguments
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
  
  std::cout << "Number of points : " << n_points << std::endl;
  cout << "Number of threads : "<< n_threads<<endl;
  std::cout << "Major Radius : " << maj_radius << std::endl << "Minor Radius : " << min_radius << std::endl;
  std::cout << "Random Seed : " << r_seed << std::endl;


  thread_args = new thread_data[n_threads];
  threads_list = new pthread_t[n_threads]; 

  ellipse_area_parallel_calculation(n_threads, n_points, maj_radius, min_radius, r_seed, thread_args, threads_list);

  delete[] thread_args;
  delete[] threads_list;

  return 0;
}