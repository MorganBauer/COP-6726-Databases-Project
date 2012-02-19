#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <vector>
#include <utility>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <functional>

using namespace std;

class BigQ {
 private:
  Pipe & in;
  Pipe & out;
  OrderMaker sortorder;
  int runlen;

  File partiallySortedFile;
  std::vector <pair <off_t, off_t> > runLocations;
  int pagesInserted;
  int runCount;

  pthread_t worker_thread;

  static void *thread_starter(void *context);
  void * WorkerThread(void);

  void PhaseOne(void);
  void sortRuns(std::vector<Record> & runlenrecords);
  void writeSortedRunToFile(std::vector<Record> & runlenrecords);

  void PhaseTwo(void);

  class Compare : public std::binary_function <Record &, Record &, bool>
    {
    private:
      OrderMaker & so;
    public:
    Compare(OrderMaker so) :so(so){}
      bool operator()(const Record & x, const Record & y) { ComparisonEngine comp;
        return  (comp.Compare(const_cast<Record *>(&x), const_cast<Record *>(&y), &so) < 0); }
    };

 public:
  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();


};

#endif
