#include "BigQ.h"
// #include <boost/thread.hpp>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <utility>
#include <algorithm>
#include <cassert>

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
  : in(in),out(out),sortorder(sortorder),runlen(runlen), pagesInserted(0)
{

  pthread_create (&worker_thread, NULL, &BigQ::thread_starter, this);
}

// don't declare static
// http://cplusplus.syntaxerrors.info/index.php?title=Cannot_declare_member_function_%E2%80%98static_int_Foo::bar%28%29%E2%80%99_to_have_static_linkage
void * BigQ :: thread_starter(void *context)
{
  return reinterpret_cast<BigQ*>(context)->WorkerThread();
}

void * BigQ :: WorkerThread(void) {
  char * partiallySortedFileTempFileName = "/tmp/zzzpartiallysorted"; // maybe set this per instance to a random filename
  partiallySortedFile.Open(0, partiallySortedFileTempFileName);
  // FIRST PHASE
  PhaseOne();
  // in pipe should be dead now.

  // SECOND PHASE
  PhaseTwo();
  cout << "cleanup" << endl;
  partiallySortedFile.Close();
  // Cleanup
  // remove(partiallySortedFileTempFileName); // XXX TODO UNCOMMENT THIS IN FINAL VERSION
  // finally shut down the out pipe
  // this lets the consumer thread know that there will not be anything else put into the pipe
  out.ShutDown ();
  pthread_exit(NULL); // make our worker thread go away
}

void BigQ::PhaseOne(void)
{
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
  int pageReadCounter = 0;

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
          // deal with page being full
          pageReadCounter++;  // increment page count, because we have a new page that we just filled.
          p.EmptyItOut(); // erase the page contents.
          // remember to add the page we got, but could not place in a page
          if (0 == p.Append(&tempRecord)) // put the record in the fresh page.
            {
              exit(-1); // if we can't fit it after emptying it out, something is wrong.
            }

          if ( pageReadCounter == runlen ) // if it's larger than runlen, we need to stop.
            {
              cout << "finished getting a run, now to sort it" << endl;
              runCount++;
              // update probable max vector size to avoid copying in future iterations.
              if (vecsize < runlenrecords.size()) {vecsize = runlenrecords.size();}

              sortRuns(runlenrecords);
              cout << "run size " << runlenrecords.size() << endl;
              // cout << "run sorted " << endl;
              writeSortedRunToFile(runlenrecords);

              pageReadCounter = 0;// reset page counter
              runlenrecords.clear(); // reset the temp vector buffer thing
            }
          runlenrecords.push_back(copy);// put the new record in
        }
    }

  // we've taken all the records out of the pipe
  // do one last internal sort, on the the buffer that we have
  if (0 < runlenrecords.size())
    {
      if (vecsize < runlenrecords.size()) {vecsize = runlenrecords.size();}
      runCount++;
      sortRuns(runlenrecords);
      cout << "last run sorted " << endl;
      writeSortedRunToFile(runlenrecords);
    }

  runlenrecords.clear();
  cout << "maximum vector size needed was " << vecsize << endl;
}

void BigQ :: sortRuns(vector<Record> & runlenrecords)
{
  // sort the records we have in the runlen buffer.
  // cout << "sorting run " << endl;
  std::sort(runlenrecords.begin(),
            runlenrecords.end(),
            Compare(sortorder));
  cout << "run size " << runlenrecords.size() << endl;
}

void BigQ :: writeSortedRunToFile(vector<Record> & runlenrecords)
{
  // cout << "enter write sorted run to file"<< endl;
  off_t pageStart = pagesInserted;
  // now to write sorted records out to a file, first, we must fill a page ...
  Page tp;
  for (vector<Record>::iterator it = runlenrecords.begin(); it < runlenrecords.end(); it++)
    {
      Record trp;
      trp.Consume(&(*it));
      if(0 == tp.Append(&trp))
        {
          partiallySortedFile.AddPage(&tp,pagesInserted++);
          tp.EmptyItOut();
          tp.Append(&trp);
        }
    }
  partiallySortedFile.AddPage(&tp,pagesInserted++);

  off_t pageEnd = pagesInserted;
  cout << "inserted " <<  pageEnd - pageStart << " pages" << endl;
  runLocations.push_back(make_pair(pageStart,pageEnd));
}

void BigQ::PhaseTwo(void)
{
  cout << "merging sorted runs" << endl; 
  // construct priority queue over sorted runs and dump sorted data
  // into the out pipe

  // for each record we need from a specific page
  // read specific page
  // get record from page
  // write modified page back

  /* OR */

  // read page into buffer of pages, when it is empty, get next page from that sequence if it is available, and write that out.

  // iterate through pages putting them all in the pipe directly
  off_t lastPage = partiallySortedFile.GetLength() - 1;
  for(off_t curPage = 0;  curPage < lastPage; curPage++)
    {
      Page tp;
      partiallySortedFile.GetPage(&tp,curPage);
      Record temp;
      while(1 == tp.GetFirst(&temp))
        {
          out.Insert(&temp);
          // cout << "put a record in the pipe" << endl;
        }
    }
  cout << "phase two complete" << endl;
}

BigQ::~BigQ () {
  runLocations.clear();
}
