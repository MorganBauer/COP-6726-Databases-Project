#ifndef HIRESTIMER_H
#define HIRESTIMER_H

#include <iostream>
#include <ctime>

class HiResTimer
{
  timespec Real_time[2];
  timespec CPU_time[2];

public:

  void start()
  {
    clock_gettime(CLOCK_MONOTONIC, &Real_time[0]);
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &CPU_time[0]);
  }

  void stop()
  {
    clock_gettime(CLOCK_MONOTONIC, &Real_time[1]);
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &CPU_time[1]);
  }

  timespec diff(timespec const & startT, timespec const &  end) const
  {
    timespec temp;
    if ((end.tv_nsec-startT.tv_nsec)<0) {
      temp.tv_sec = end.tv_sec-startT.tv_sec-1;
      temp.tv_nsec = 1000000000+end.tv_nsec-startT.tv_nsec;
    } else {
      temp.tv_sec = end.tv_sec-startT.tv_sec;
      temp.tv_nsec = end.tv_nsec-startT.tv_nsec;
    }
    return temp;
  }

  void duration() const
  {
    timespec real = diff(Real_time[0],Real_time[1]);
    timespec cpu  = diff(CPU_time[0],CPU_time[1]);

    double r = ((double)real.tv_nsec)/((double)1000000000);
    r += real.tv_sec;
    double c = ((double)cpu.tv_nsec)/((double)1000000000);
    c += cpu.tv_sec;
    std::cout << r << " real time, " << c << " cpu time for a " << (c/r) << " speedup" << std::endl;
  }
};

#endif
