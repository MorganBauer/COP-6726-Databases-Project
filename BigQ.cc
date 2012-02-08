#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
  // read data from in pipe sort them into runlen pages

  // proof of concept, simplest thing that could possibly work
  // for completely broken values of work
  Record temp;
  while (in.Remove(&temp))
    out.Insert(&temp);

  // construct priority queue over sorted runs and dump sorted data
  // into the out pipe

  // finally shut down the out pipe
  out.ShutDown ();
}

BigQ::~BigQ () {
}
