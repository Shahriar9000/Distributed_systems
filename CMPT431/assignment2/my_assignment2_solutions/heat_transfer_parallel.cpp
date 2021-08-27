//Shahriar
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>
#include <cassert>

using namespace std;

#define DEFAULT_GRID_SIZE "1000"
#define DEFAULT_NUMBER_OF_THREADS "1"
#define DEFAULT_CX "1"
#define DEFAULT_CY "1"
#define DEFAULT_TIME_STEPS "1000"
#define DEFAULT_MIDDLE_TEMP "600"

class TemperatureArray {

private:
  uint size;
  uint step;
  double Cx;
  double Cy;
  double *CurrArray;
  double *PrevArray;
  void assign(double *A, uint x, uint y, double newvalue) {
    A[x*size+y] = newvalue;
  };
  double read(double *A, uint x, uint y) {
    return A[x*size+y];
  }; 
public: 
  TemperatureArray(uint input_size, double iCx, double iCy, double init_temp) { // create array of dimension sizexsize
    size = input_size;
    Cx = iCx;
        Cy = iCy;
    step = 0;
    CurrArray = (double *)malloc(size*size*sizeof(double));
    PrevArray = (double *)malloc(size*size*sizeof(double));
    for (uint i = 0; i < size; i++)
      for (uint j = 0; j < size; j++) {
        if ((i > size/3) && (i < 2*size/3) && (j > size/3) && (j < 2*size/3)) {
          assign(PrevArray, i, j, init_temp); assign(CurrArray, i, j, init_temp);
        }
        else {
          assign(PrevArray, i, j, 0); assign (CurrArray, i, j, 0);
        } 
      }
  };
 
  ~TemperatureArray() {
    free (PrevArray);   free (CurrArray);
  };

  void IncrementStepCount() { step ++; };

  uint ReadStepCount() { return(step); };

  void ComputeNewTemp(uint x, uint y) {
    if ((x > 0) && (x < size-1) && (y > 0) && (y < size-1))
      assign(CurrArray, x, y , read(PrevArray,x,y)  
        + Cx * (read(PrevArray, x-1, y) + read(PrevArray, x+1, y) - 2*read(PrevArray, x, y)) 
        + Cy * (read(PrevArray, x, y-1) + read(PrevArray, x, y+1) - 2*read(PrevArray, x, y)));
  };

  void SwapArrays() {
    double *temp = PrevArray;
    PrevArray = CurrArray;
    CurrArray = temp;
  };  
 
  double temp(uint x, uint y) {
    return read(CurrArray, x, y);
  };
};

//using struct to hold thread local data
struct thread_data{
    int t_id;
    uint x_start;
    pthread_mutex_t* mutex;
    pthread_cond_t* cond_var;
    uint x_end;
    uint size;
    double *total_time_taken; 
    TemperatureArray* Temp_arr;
    uint steps;
    CustomBarrier* barrier;
};

struct thread_data* thread_args;
pthread_t* threads_list;

inline void heat_transfer_calculation(uint tid, uint size, uint start, uint end, double *time_taken, TemperatureArray* Temp_arr, uint steps, CustomBarrier* barrier, pthread_mutex_t* mutex, pthread_cond_t* cond_var) {
  timer t1;
  t1.start();
  uint stepcount;
  for (stepcount = 1; stepcount <= steps; stepcount ++) {
    for (uint x = start; x <= end; x++) {
      for (uint y = 0; y < size; y++) {
        Temp_arr->ComputeNewTemp(x, y);
      }
    }
    //threads shout wait
    barrier->wait();

    if (tid == 0) 
    {
        // thread 0 should swap arrays. This is the only thread in the serial version
        pthread_mutex_lock(mutex);

        Temp_arr->SwapArrays();
        Temp_arr->IncrementStepCount();

        pthread_cond_broadcast(cond_var);
        pthread_mutex_unlock(mutex);
        
    }
    else 
    {  
        pthread_mutex_lock(mutex);
        if (stepcount != Temp_arr->ReadStepCount())
        {
            pthread_cond_wait(cond_var ,mutex);
        }
        pthread_mutex_unlock(mutex);
    }
  }  // end of current step

  *time_taken = t1.stop();
}

void* thread_function (void* arg_){

    struct thread_data* t_args;
    t_args = (struct thread_data*) arg_;

    heat_transfer_calculation(t_args->t_id, t_args->size, t_args->x_start, t_args->x_end, t_args->total_time_taken, t_args->Temp_arr, t_args->steps, t_args->barrier, t_args->mutex, t_args->cond_var);
    
    //exit threads
    pthread_exit(NULL);
}


void heat_transfer_parallel_cal(uint size, uint number_of_threads, TemperatureArray* T, uint steps, thread_data* thread_args, pthread_t* threads_list) {
  timer serial_timer;
  double time_taken = 0.0;
  std::vector<uint> x_start(number_of_threads);
  std::vector<uint> x_end(number_of_threads);

  // The following code is used to determine start and end of each thread's share of the grid
  // Also used to determine which points to print out at the end of this function
  uint min_columns_for_each_thread = size /   number_of_threads;
  uint excess_columns = size % number_of_threads;
  uint curr_column = 0;

  for (uint i = 0; i < number_of_threads; i++) 
  {
    x_start[i] = curr_column;
    if (excess_columns > 0) 
    {
      x_end[i] = curr_column + min_columns_for_each_thread;
      excess_columns--;
    } 
    else 
    {
      x_end[i] = curr_column + min_columns_for_each_thread - 1;
    }

    curr_column = x_end[i]+1;
  }

  // end of code to determine start and end of each thread's share of the grid
  
  serial_timer.start();

  CustomBarrier* barrier = new CustomBarrier(number_of_threads);
  pthread_mutex_t mutex;
  pthread_cond_t cond_var;
  pthread_mutex_init (&mutex, NULL);
  pthread_cond_init(&cond_var, NULL);
  
  int rc;
  for (uint i = 0; i < number_of_threads; i++) {
    thread_args[i].t_id = i;
    thread_args[i].x_end = x_end[i];
    thread_args[i].x_start = x_start[i];
    thread_args[i].steps = steps;
    thread_args[i].total_time_taken = &time_taken;
    thread_args[i].Temp_arr = T;
    thread_args[i].size = size;
    thread_args[i].barrier = barrier;
    thread_args[i].mutex = &mutex;
    thread_args[i].cond_var = &cond_var;


    rc = pthread_create(&threads_list[i], NULL, thread_function, (void*) &thread_args[i]);
    if (rc)
    {
          cout << "Error creating thread" << i << "error:" << rc << endl;
    }
  }

  for (int i = 0; i< number_of_threads; i++)
  {
    rc = pthread_join(threads_list[i], NULL);
    if (rc)
    {
          cout << "Error joining thread" << i << "error:" << rc << endl;
    }
  }

  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&cond_var);
  delete barrier;

  
  std::cout << "thread_id, start_column, end_column, time_taken\n";
  // Print these statistics for each thread
  for (int i=0; i< number_of_threads; i++ ){
      cout<< thread_args[i].t_id<< ", ";
      cout<< thread_args[i].x_start << ", ";
      cout<< thread_args[i].x_end << ", ";
      cout<< std::setprecision(TIME_PRECISION)<<*thread_args[i].total_time_taken;
      cout<<endl;

  } 
  
  uint step = size/5;
  uint position = 0;
  for (uint x = 0; x < 5; x++) 
  {
      std::cout << "Temp[" << position << "," << position << "]=" << T->temp(position,position) << std::endl;
      position += step;
  } 
  //   // Print temparature at select boundary points;
  for (uint i = 0; i < number_of_threads; i++) {
      std::cout << "Temp[" << x_end[i] << "," << x_end[i] << "]=" << T->temp(x_end[i],x_end[i]) << std::endl;
  }

  time_taken = serial_timer.stop();

  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
          << time_taken << "\n";
}

int main(int argc, char *argv[]) {
  // Initialize command line arguments
  cxxopts::Options options("Heat_transfer_calculation",
                           "Model heat transfer in a grid using serial and parallel execution");
  options.add_options(
      "custom",
      {
          {"nThreads", "Number of threads",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_THREADS)},
          {"gSize", "Grid Size",         
           cxxopts::value<uint>()->default_value(DEFAULT_GRID_SIZE)},
          {"mTemp", "Temperature in middle of array",         
           cxxopts::value<double>()->default_value(DEFAULT_MIDDLE_TEMP)},
          {"iCX", "Coefficient of horizontal heat transfer",
           cxxopts::value<double>()->default_value(DEFAULT_CX)},
          {"iCY", "Coefficient of vertical heat transfer",
           cxxopts::value<double>()->default_value(DEFAULT_CY)},
          {"tSteps", "Time Steps",
           cxxopts::value<uint>()->default_value(DEFAULT_TIME_STEPS)}
      });
  auto cl_options = options.parse(argc, argv);
  uint n_threads = cl_options["nThreads"].as<uint>();
  uint grid_size = cl_options["gSize"].as<uint>();

  assert(n_threads > 0);
  assert(grid_size > 0);


  double init_temp = cl_options["mTemp"].as<double>();

  double Cx = cl_options["iCX"].as<double>();
  double Cy = cl_options["iCY"].as<double>();
//   if (Cx > 0.25 ||Cy > 0.25) {
//  std::cout << "CX and CY values must both be lower than 0.25 Terminating..." << std::endl;
//  return 1;
//   }
  uint steps = cl_options["tSteps"].as<uint>();
  assert(steps > 0);
  assert(n_threads < grid_size);

  std::cout << "Grid Size : " << grid_size << "x" << grid_size << std::endl;
  std::cout << "Number of threads : " << n_threads << std::endl;
  std::cout << "Cx : " << Cx << std::endl << "Cy : " << Cy << std::endl;
  std::cout << "Temperature in the middle of grid : " << init_temp << std::endl;
  std::cout << "Time Steps : " << steps << std::endl;
  
  std::cout << "Initializing Temperature Array..." << std::endl;
  TemperatureArray *T = new TemperatureArray(grid_size, Cx, Cy, init_temp);
  if (!T) {
      std::cout << "Cannot Initialize Temperature Array...Terminating" << std::endl;
      return 2;
  }

  thread_args = new thread_data [ n_threads];
  threads_list = new pthread_t[n_threads]; 

  heat_transfer_parallel_cal(grid_size, n_threads, T, steps, thread_args, threads_list);

  delete[] thread_args;
  delete[] threads_list;

  delete T;
  return 0;
}