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
  /* class CompareMin : public std::binary_function <Record &, Record &, bool> */
  /*   { */
  /*   private: */
  /*     OrderMaker & so; */
  /*   public: */
  /*   CompareMin(OrderMaker so) :so(so){} */
  /*     bool operator()(Record & x, Record & y) */
  /*     { */
  /*       ComparisonEngine comp; */
  /*       return  (comp.Compare((&x), (&y), &so) < 0); */
  /*     } */
  /*   } cm; */
  void getNextPage(Page & p,int i)
  {
  }
 public:
  // Run(int rID, off_t start, off_t end, OrderMaker & sortorder): runID(rID), start_offset(start), end_offset(end), cm(sortorder) { empty = false;}
 Run(int rID, off_t start, off_t end, File * partiallySortedFile):
  runID(rID), start_offset(start), end_offset(end), cur_offset(start), empty(false)
    { this->partiallySortedFile = partiallySortedFile;
      partiallySortedFile->GetPage(&p,start_offset);
      cout << "Run constructor called" << partiallySortedFile->GetLength() << endl;}
  Run(const Run & rn)
    {
      cout << "copy constructor called" << endl;
      runID = rn.runID;
      start_offset = rn.start_offset;
      end_offset = rn.end_offset;
      cur_offset = rn.cur_offset;
      empty = rn.empty;
      // rn.partiallySortedFile->AddPage(&rn.p,rn.cur_offset);
      rn.partiallySortedFile->AddPage(const_cast<Page *>(&rn.p),cur_offset); // violating the const promise so hard right now
      partiallySortedFile = rn.partiallySortedFile;
      partiallySortedFile->GetPage(&p,cur_offset);
    }
  Run & operator= (const Run & rn)
    {
      cout << "assignment operator called" << endl;
      runID = rn.runID;
      start_offset = rn.start_offset;
      end_offset = rn.end_offset;
      cur_offset = rn.cur_offset;
      empty = rn.empty;
      rn.partiallySortedFile->AddPage(const_cast<Page *>(&rn.p),cur_offset); // violating the const promise so hard right now
      partiallySortedFile = rn.partiallySortedFile;
      partiallySortedFile->GetPage(&p,cur_offset);
    }
  ~Run ()
    {cout << "Run destructor called" << endl;
      cout << "destroyed run " << runID << endl;}
  // returns true if a page was returned.
  bool getNextRecord(Record & ret)
  {
    // cout << "R.GNR of " << runID << endl;
    if( 1 == p.GetFirst(&r) )
      { // page had something in it, have a record now.
        ret.Consume(&r);
        return true;
      }
    else
      { // page was empty, get new page
        cur_offset++;
        cout << cur_offset << endl;
        // check if out of bounds
        if (cur_offset < end_offset)
          {
            partiallySortedFile->GetPage(&p,cur_offset);
            if( 1 == p.GetFirst(&r) )
              { // page had something in it, have a record now.
                ret.Consume(&r);
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
  // bool operator< (Run & rhs)  { return empty ? false : cm(r,rhs.r);}
};


class BigQ {
 private:
  Pipe & in;
  Pipe & out;
  OrderMaker sortorder;
  int runlen;

  File partiallySortedFile;
  std::vector <pair <off_t, off_t> > runLocations;
  int pagesInserted;
  int totalRecords;
  int runCount;

  pthread_t worker_thread;

  static void *thread_starter(void *context);
  void * WorkerThread(void);

  void PhaseOne(void);
  void sortRuns(std::vector<Record> & runlenrecords);
  void writeSortedRunToFile(std::vector<Record> & runlenrecords);

  void PhaseTwo(void);
  void ReadModifyWrite(void);

  class Compare : public std::binary_function <Record &, Record &, bool>
    {
    private:
      OrderMaker & so;
    public:
    Compare(OrderMaker so) :so(so){}
      bool operator()(const Record & x, const Record & y) { ComparisonEngine comp;
        return  (comp.Compare(const_cast<Record *>(&x), const_cast<Record *>(&y), &so) < 0); }
    };

  class PQ
  {
    bool empty;
    vector<Run> runs;
    class CompareMin : public std::binary_function <Record &, Record &, bool>
      {
      private:
        OrderMaker & so;
      public:
      CompareMin(OrderMaker so) :so(so){}
        bool operator()(const Record & x, const Record & y)
        {
          ComparisonEngine comp;
          //if (x.bitsNull
          return  (comp.Compare(const_cast<Record *>(&x), const_cast<Record *>(&y), &so) < 0);
        }
      } cm;

  public:
  PQ(int runCount, OrderMaker & sortorder): cm(sortorder) { empty = false; runs.reserve(runCount);}
    bool Empty(void)
    {return empty;}
    void getMinimum(Record & r)
    {
      for (vector<Run>::iterator it = runs.begin(); it < runs.end() - 1; it++)
        {
          // compare it,it+1
        }
    }
  };

  class TaggedRecord
  {
  private:
    int run;
    Record r;
  public:

  };

 public:
  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();


};



#endif
