#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <utility>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <functional>
#include <cstdlib>

using namespace std;

class Run
{
 private:
  int runID;
  off_t start_offset;
  off_t end_offset;
  off_t cur_offset;
  Page p;
  Record r;
  bool empty;
  File * partiallySortedFile;
 public:
 Run(int rID, off_t start, off_t end, File * _partiallySortedFile) :
  runID(rID), start_offset(start), end_offset(end), cur_offset(start), p(), r(), empty(false), partiallySortedFile(_partiallySortedFile)
  { partiallySortedFile->GetPage(&p,start_offset); }
 Run(const Run & rn) :
  runID(rn.runID), start_offset(rn.start_offset), end_offset(rn.end_offset), cur_offset(rn.cur_offset), p(), r(), empty(rn.empty), partiallySortedFile(rn.partiallySortedFile)
    {
      partiallySortedFile->AddPage(const_cast<Page *>(&rn.p),cur_offset); // violating the const promise so hard right now
      partiallySortedFile->GetPage(&p,cur_offset);
    }
  Run & operator= (const Run & rn)
    {
      runID = rn.runID;
      start_offset = rn.start_offset;
      end_offset = rn.end_offset;
      cur_offset = rn.cur_offset;
      empty = rn.empty;
      rn.partiallySortedFile->AddPage(const_cast<Page *>(&rn.p),cur_offset); // violating the const promise so hard right now
      partiallySortedFile = rn.partiallySortedFile;
      partiallySortedFile->GetPage(&p,cur_offset);
      return * this;
    }
  ~Run () {}
  // returns true if a page was returned.
  bool getNextRecord(Record & ret)
  {
    // cout << "R.GNR of " << runID << endl;
    if( 1 == p.GetFirst(&ret) )
      { return true; }
    else
      { // page was empty, get new page
        cur_offset++;
        // check if out of bounds
        if (cur_offset < end_offset)
          {
            partiallySortedFile->GetPage(&p,cur_offset);
            if( 1 == p.GetFirst(&ret) )
              { // page had something in it, have a record now.
                return true;
              }
            else{ // error
              exit(-1);}
          }
        else{ return false;}
      }
  }
  void print()
  {std::cout << "Run " << runID << ", starting at " << start_offset << " and running to " << end_offset << endl;}
};

class TaggedRecord
{
 private:
  int run;
 public:
  Record r;
  int getRun(){return run;}
 TaggedRecord(const Record _r, int _run) : run(_run), r(_r)
  { }
};

class TaggedRecordCompare : public std::binary_function <TaggedRecord &, TaggedRecord &, bool>
{
 private:
  OrderMaker & so;
 public:
 TaggedRecordCompare(OrderMaker _so) :so(_so){}
  bool operator()(const TaggedRecord & x, const TaggedRecord & y) const { ComparisonEngine comp;
    bool ret = (comp.Compare(const_cast<Record *>(&(x.r)), const_cast<Record *>(&(y.r)), &so) > 0);
    return ret; }
};

class BigQ {
 private:
  Pipe & in;
  Pipe & out;
  OrderMaker sortorder;
  int runlen;

  int pagesInserted;
  int totalRecords;
  int runCount;
  File partiallySortedFile;
  std::vector <pair <off_t, off_t> > runLocations;

  pthread_t worker_thread;

  static void *thread_starter(void *context);
  void * WorkerThread(void);

  void PhaseOne(void);
  void sortRuns(std::vector<Record> & runlenrecords);
  int writeSortedRunToFile(std::vector<Record> & runlenrecords);

  void PhaseTwoLinearScan(void);
  void PhaseTwoPriorityQueue(void);

  class Compare : public std::binary_function <Record &, Record &, bool>
    {
    private:
      OrderMaker & so;
    public:
    Compare(OrderMaker _so) :so(_so){}
      bool operator()(const Record & x, const Record & y) { ComparisonEngine comp;
        return  (comp.Compare(const_cast<Record *>(&x), const_cast<Record *>(&y), &so) < 0); }
    };

 public:
  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();
};

#endif
