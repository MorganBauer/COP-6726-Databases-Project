#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <vector>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {
 private:
  Pipe & in;
  Pipe & out;
  OrderMaker sortorder;
  int runlen;

  File partiallySortedFile;
  
  int pagesInserted;

  pthread_t worker_thread;

  static void *thread_starter(void *context);
  void * WorkerThread(void);
  void PhaseOne(void);
  void writeSortedRunToFile(std::vector<Record> & runlenrecords);
  void PhaseTwo(void);
 public:
  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();


};

#endif
