#include "BigQ.h"
#include <boost/thread.hpp>
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>

struct sorter : public std::binary_function<Record *, Record *, bool>
{
  OrderMaker * _so;
public:
  sorter(OrderMaker so) {this->_so = &so;}
  bool operator()(Record & _x, Record & _y) { ComparisonEngine comp;
    return  (comp.Compare(&_x, &_y, _so) < 0) ? true : false; }
  bool operator()(const Record & _x, const Record & _y) { ComparisonEngine comp;
    return  (comp.Compare(const_cast<Record *>(&_x), const_cast<Record *>(&_y), _so) < 0); }
  bool operator()(Record * _x, Record * _y) { ComparisonEngine comp;
    return  (comp.Compare((_x), (_y), _so) < 0) ? true : false; }
};

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
  : in(in),out(out),sortorder(sortorder),runlen(runlen)
{
  pthread_create (&worker_thread, NULL, &BigQ::thread_starter, this);
}

// don't declare static
// http://cplusplus.syntaxerrors.info/index.php?title=Cannot_declare_member_function_%E2%80%98static_int_Foo::bar%28%29%E2%80%99_to_have_static_linkage
void * BigQ :: thread_starter(void *context)
{
  return reinterpret_cast<BigQ*> (context)->WorkerThread();
}

void * BigQ :: WorkerThread(void) {
  PhaseOne();
  // in pipe should be dead now.
  
  // SECOND PHASE
  PhaseTwo();
  
  // Cleanup
  
  // finally shut down the out pipe
  // this lets the consumer thread know that there will not be anything else put into the pipe
  out.ShutDown ();
  pthread_exit(NULL); // make our worker thread go away
}

void BigQ::PhaseOne(void)
{
  // ComparisonEngine comp;
  // comp.Compare(&temp,&temp,&sortorder);
  size_t vecsize = runlen; //good first guess
  // FIRST PHASE
  // read data from in pipe sort them into runlen pages
  vector<Record> runlenrecords;
  runlenrecords.reserve(vecsize);
  // proof of concept, simplest thing that could possibly work
  // for completely broken values of work
  // this will need (runlen+1 pages)+1 record of memory,
  //  because we hang on to stuff after shoving a copy in a vector
  Page p;
  Record tempRecord;
  int pageCounter = 0;
  while (1 == in.Remove(&tempRecord)) // while we can take records out of the pipe, do so.
    {
      Record copy; // Append consumes the record, so I need to make a copy.
      copy.Copy(&tempRecord); // make the copy
      if (1 == p.Append(&tempRecord)) // page has room
        {
          runlenrecords.push_back(copy); // put it in our runlen sized buffer.
        }
      else // page is full
        {
          pageCounter++; // increment page count, because we have a new page that we just filled.
          // remember to add the page we got, but could not place in a page
          p.EmptyItOut(); // erase the page contents.
          { // put the record in the fresh page.
            int pageFit = p.Append(&tempRecord); // put our page we couldn't fit in.
            if (0 == pageFit)
              {
                exit(-1);
              }
          }
          if ( pageCounter == runlen ) // if it's larger than runlen, we need to stop.
            {
              // update probable max vector size to avoid copying in future iterations.
              if (vecsize < runlenrecords.size())
                {vecsize = runlenrecords.size();}

              sorter s = sorter(sortorder);
              // sort the records we have in the runlen buffer.
              // cout << "sorting run " << endl;
              std::sort(runlenrecords.begin(),
                        runlenrecords.end(),
                        sorter(sortorder));
              // cout << "run sorted " << endl;
              for (vector<Record>::iterator it = runlenrecords.begin(); it < runlenrecords.end(); it++)
                { //cout << "inserted" << endl;
                  out.Insert(&(*it));
                }
              // write them out to disk. temp file somewhere?
              pageCounter = 0;// reset page counter
            }
          runlenrecords.clear(); // reset the temp vector buffer thing
          runlenrecords.push_back(copy);// put the new record in
        }
    }

  if (0 < runlenrecords.size())
    {
      if (vecsize < runlenrecords.size())
        {vecsize = runlenrecords.size();}

      sorter s = sorter(sortorder);
      // sort the records we have in the runlen buffer.
      // cout << "sorting run " << endl;
      std::sort(runlenrecords.begin(),
                runlenrecords.end(),
                sorter(sortorder));
      // cout << "run sorted " << endl;
      for (vector<Record>::iterator it = runlenrecords.begin(); it < runlenrecords.end(); it++)
        { //cout << "inserted" << endl;
          out.Insert(&(*it));
        }

    }

  // we've taken all the records out of the pipe
  // do one last internal sort, on the the buffer that we have

  {
    cout << "maximum vector size needed was " << vecsize << endl;
  }

}

void BigQ::PhaseTwo(void)
{
  // construct priority queue over sorted runs and dump sorted data
  // into the out pipe
  File f;
  // out.Insert(&temp);

}

BigQ::~BigQ () {
}
