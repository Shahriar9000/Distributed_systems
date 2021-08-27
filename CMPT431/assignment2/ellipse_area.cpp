#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>

#define sqr(x) ((x) * (x))
#define DEFAULT_NUMBER_OF_POINTS "1000000000"
#define DEFAULT_MAJOR_RADIUS "2"
#define DEFAULT_MINOR_RADIUS "1"
#define DEFAULT_RANDOM_SEED "1"

uint c_const = (uint)RAND_MAX + (uint)1;
inline double get_random_coordinate(uint *random_seed) {
  return ((double)rand_r(random_seed)) / c_const;
}

unsigned long get_points_in_ellipse(unsigned long n, uint random_seed, float maj_radius, float min_radius) {
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

void ellipse_area_calculation_serial(unsigned long n, float maj_radius, float min_radius, uint r_seed) {
  timer serial_timer;
  double time_taken = 0.0;
  uint random_seed = r_seed;

  serial_timer.start();
 
  unsigned long ellipse_points = get_points_in_ellipse(n, r_seed, maj_radius, min_radius);
  
  double area_value =
      (double)maj_radius * (double)min_radius * 4.0 * (double)ellipse_points / (double)n;

  //*------------------------------------------------------------------------
  time_taken = serial_timer.stop();

  std::cout << "thread_id, points_generated, ellipse_points, time_taken\n";
  std::cout << "1, " << n << ", "
              << ellipse_points << ", " << std::setprecision(TIME_PRECISION)
              << time_taken << "\n";

  std::cout << "Total points generated : " << n << "\n";
  std::cout << "Total points in ellipse : " << ellipse_points << "\n";
  std::cout << "Result : " << std::setprecision(VAL_PRECISION) << area_value
            << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";
}

int main(int argc, char *argv[]) {
  // Initialize command line arguments
  cxxopts::Options options("Ellipse_area_calculation",
                           "Calculate area of an ellipse using serial and parallel execution");
  options.add_options(
      "custom",
      {
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
  unsigned long n_points = cl_options["nPoints"].as<unsigned long>();
  float maj_radius = cl_options["majorRad"].as<float>();
  float min_radius = cl_options["minorRad"].as<float>();
  uint r_seed = cl_options["rSeed"].as<uint>();
  std::cout << "Number of points : " << n_points << std::endl;
  std::cout << "Major Radius : " << maj_radius << std::endl << "Minor Radius : " << min_radius << std::endl;
  std::cout << "Random Seed : " << r_seed << std::endl;

  ellipse_area_calculation_serial(n_points, maj_radius, min_radius, r_seed);
  return 0;
}
