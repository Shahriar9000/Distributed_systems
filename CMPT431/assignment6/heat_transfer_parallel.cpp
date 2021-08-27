//Shahriar
#include "core/utils.h"
#include <iomanip>
#include <stdlib.h>
#include <thread>
#include <mpi.h>
#include <iostream>
#include <cassert>

using namespace std;

#define DEFAULT_GRID_SIZE "1000"
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

	void write(uint x, uint y, double newvalue) {
		assign(CurrArray, x, y, newvalue);
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
    uint tsteps;
    CustomBarrier* barrier;
};

int main(int argc, char *argv[]) {
  // Initialize command line arguments
  cxxopts::Options options("Heat_transfer_calculation",
                           "Model heat transfer in a grid using serial and parallel execution");
  options.add_options(
      "custom",
      {
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
  uint grid_size = cl_options["gSize"].as<uint>();
  assert(grid_size > 0);

  double init_temp = cl_options["mTemp"].as<double>();
  double Cx = cl_options["iCX"].as<double>();
  double Cy = cl_options["iCY"].as<double>();
  uint steps = cl_options["tSteps"].as<uint>();
  assert(steps > 0);


  MPI_Init(NULL, NULL);
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  if (world_size < 2) {
    fprintf(stderr, "world size needs to be greater than 1");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  assert(world_size < grid_size);
  if (world_rank == 0){

    std::cout << "Number of processes : " << world_size << std::endl;
    std::cout << "Grid Size : " << grid_size << "x" << grid_size << std::endl;
    std::cout << "Cx : " << Cx << std::endl << "Cy : " << Cy << std::endl;
    std::cout << "Temperature in the middle of grid : " << init_temp << std::endl;
    std::cout << "Time Steps : " << steps << std::endl;

    std::cout << "Initializing Temperature Array..." << std::endl;
    std::cout << "rank, start_column, end_column, time_taken\n";
  }

  TemperatureArray *T = new TemperatureArray(grid_size, Cx, Cy, init_temp);
  if (!T) {
      std::cout << "Cannot Initialize Temperature Array...Terminating" << std::endl;
      return 2;
  }
  timer timer_for_each_process;
  timer global_timer;
  uint startx = 0;
  uint endx = grid_size - 1;
  timer_for_each_process.start();
  global_timer.start();

  uint min_columns_for_each_thread = grid_size / world_size;
  uint excess_columns = grid_size % world_size;
  if (world_rank < excess_columns){
    startx = (min_columns_for_each_thread + 1) * world_rank;
    endx = startx + min_columns_for_each_thread;
  }
  else {
    startx = (excess_columns * (min_columns_for_each_thread + 1)) + ((world_rank-excess_columns) * min_columns_for_each_thread);
    endx = startx + min_columns_for_each_thread - 1;
  }

  for (uint l_stepcount = 1; l_stepcount<= steps; l_stepcount++){
        for (uint x = startx; x <= endx; x++) {
            for (uint y = 0; y < grid_size; y++) {
                T->ComputeNewTemp(x, y);
            }
        }
        if (world_rank % 2 == 0){
            if (world_rank > 0){
                double receive_left_column[grid_size];
                double send_left_column[grid_size];
                MPI_Recv(receive_left_column, grid_size, MPI_DOUBLE, world_rank-1, l_stepcount, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                uint i = 0;
                while (i < grid_size) {
                  T->write(startx-1, i, (double)receive_left_column[i]);
                  i++;
                }
                i = 0;
                while (i < grid_size) {
                  send_left_column[i] = (double)T->temp(startx,i);
                  i++;
                }
                MPI_Send(send_left_column, grid_size, MPI_DOUBLE, world_rank-1 , l_stepcount, MPI_COMM_WORLD);
            }
            if (world_rank < world_size -1){
                double send_right_column[grid_size];
                double receive_right_column[grid_size];
                uint i = 0;
                while (i < grid_size) {
                  send_right_column[i] = (double)T->temp(endx,i);
                  i++;
                }
                MPI_Send(send_right_column, grid_size, MPI_DOUBLE, world_rank+1 , l_stepcount, MPI_COMM_WORLD);
                MPI_Recv(receive_right_column, grid_size, MPI_DOUBLE, world_rank+1, l_stepcount, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                i = 0;
                while (i < grid_size) {
                  T->write(endx+1, i, (double)receive_right_column[i]);
                  i++;
                }
            }
        }
        else{
            if (world_rank > 0){
                double receive_left_column[grid_size];
                double send_left_column[grid_size];
                MPI_Recv(receive_left_column, grid_size, MPI_DOUBLE, world_rank-1, l_stepcount, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                uint i = 0;
                while (i < grid_size) {
                  T->write(startx-1, i, (double)receive_left_column[i]);
                  i++;
                }
                i = 0;
                while (i < grid_size) {
                  send_left_column[i] = (double)T->temp(startx,i);
                  i++;
                }
                MPI_Send(send_left_column, grid_size, MPI_DOUBLE, world_rank-1 , l_stepcount, MPI_COMM_WORLD);
            }
            if (world_rank < world_size - 1){
                double send_right_column[grid_size];
                double receive_right_column[grid_size];
                uint i = 0;
                while (i < grid_size) {
                  send_right_column[i] = (double)T->temp(endx,i);
                  i++;
                }
                MPI_Send(send_right_column, grid_size, MPI_DOUBLE, world_rank+1 , l_stepcount, MPI_COMM_WORLD);
                MPI_Recv(receive_right_column, grid_size, MPI_DOUBLE, world_rank+1, l_stepcount, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                i = 0;
                while (i < grid_size) {
                  T->write(endx+1, i, (double)receive_right_column[i]);
                  i++;
                }
            }
        }
        T->SwapArrays();
        T->IncrementStepCount();
    }
  double time_taken = timer_for_each_process.stop();

	for (uint x = 0; x < 5; x++) {
   if (world_rank == x){
       std::cout << world_rank << ", " << startx << ", " << endx << ", " << time_taken << std::endl;
   }
   MPI_Barrier(MPI_COMM_WORLD);
 }
	if(world_rank == 0){
       std::cout << "Temp[" << 0 << "," << 0 << "]=" << T->temp(0,0) << "\n";
       for(int i= 1; i <5; i++){
         int index = i*(grid_size/5);
         if(index <= endx && index >=startx){
           std::cout << "Temp[" << index << "," << index << "]=" << T->temp(index, index) << "\n";
         }
       }
       std::cout << "Temp[" << endx << "," << endx << "]=" << T->temp(endx,endx) << "\n";

  }
  else{
      for(int i= 1; i <5; i++){
        int index = i*(grid_size/5);
        if(index <= endx && index >=startx){
          std::cout << "Temp[" << index << "," << index << "]=" << T->temp(index, index) << "\n";
        }
      }
      std::cout << "Temp[" << endx << "," << endx << "]=" << T->temp(endx,endx) << "\n";
   }

	MPI_Barrier(MPI_COMM_WORLD);
  double global_time = global_timer.stop();
  if (world_rank == 0){
      cout << "Time taken (in seconds) : " << global_time << endl;
  }

  delete T;
  MPI_Finalize();
  return 0;
}
