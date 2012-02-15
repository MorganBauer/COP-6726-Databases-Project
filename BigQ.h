#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
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

  pthread_t worker_thread;

  static void *thread_starter(void *context);
  void * WorkerThread(void);
  void PhaseOne(void);
  void PhaseTwo(void);
 public:
  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();


};

#endif
