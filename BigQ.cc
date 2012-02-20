#include "BigQ.h"
// #include <boost/thread.hpp>
#include <vector>
#include <queue>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <utility>
#include <algorithm>
#include <cassert>
#include <iterator>
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
  : in(in),out(out),sortorder(sortorder),runlen(runlen), pagesInserted(0), runCount(0), totalRecords(0)
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
  cout << totalRecords << " Records written to file" << endl;
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
      totalRecords++;
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
  cout << endl << endl << "merging sorted runs" << endl;
  cout << runCount << " runs in " << partiallySortedFile.GetLength() << " total pages" << endl;
  for (std::vector < std::pair <off_t,off_t> >::iterator it = runLocations.begin(); it < runLocations.end(); it++)
    {
      cout << "from " << (*it).first << " to " << (*it).second << endl;
    }

  { // Linear scan of minimums
    // initialize page containers
    /*{// alternate version of manual page management
      vector<vector<Record > > pages;
      for (int i; i < runCount; i++)
      {
      Page tp;
      Record tr;

      }
      }*/

    // Run(1,runLocations[1].first,runLocations[1].second, & partiallySortedFile);




    vector<Run> runs;
    runs.reserve(runCount);
    cout << "initializing runs" << endl;
    for (int i = 0; i < runCount; i++)
      {
        cout << "Run " << i;
        runs.push_back(Run(i,runLocations[i].first,runLocations[i].second, &partiallySortedFile));
        cout << " initialized" << endl;
        runs[i].print();
      }

    for (int i = 0; i < runCount; i++)
      {
        runs[i].print();
      }
    
    vector<Record> minimums;
    // initialize minimums
    // for each run, get the first guy.
    cout << "initializing minimums" << endl;
    minimums.reserve(runCount);
    for (int i = 0; i < runCount; i++)
      {
        cout << "minimum " << i;
        Record tr;
        runs[i].getNextRecord(tr);
        minimums.push_back(tr);
        cout << "initialized " << endl;
      }
    // now find the minimum guy and put it in the pipe
    // do this totalRecords times
    cout << "putting stuff in the pipe" << endl;
    Compare c = Compare(sortorder);
    int runsLeft = runCount;
    for (int r = totalRecords ; r > 0; r--)
      {
        vector<Record>::iterator min = std::min_element(minimums.begin(), minimums.end(),Compare(sortorder));//Compare(sortorder));
        vector<Record>::iterator::difference_type run = std::distance( minimums.begin(), min);
        // cout << "record from run " << ((int)run) << " was chosen" << endl;

        std::vector<Record>::iterator result = std::min_element(minimums.begin(), minimums.end(),Compare(sortorder));
        // std::cout << "min element at: " << std::distance(minimums.begin(), result) << endl ;
        long int d = std::distance(minimums.begin(), result) ;
        vector<Record>::iterator::difference_type d2 = std::distance(minimums.begin(), result) ;
        // cout << d << "&"<< d2 << endl;
        
        Record tr;
        tr.Consume(&(minimums[run]));
        out.Insert(&tr);
        bool valid = runs[run].getNextRecord(tr);
        if (valid)
          {
            minimums[run].Consume(&tr);
          }
        else
          {
            cout << "run empty, got to get rid of it" << endl;
            runsLeft--;
            minimums.erase(minimums.begin() + run);
            runs.erase(runs.begin() + run);
            // need to get rid of run and shift everything over
          }
      }
    cout << minimums.size() << runs.size() << endl;
    cout << "runs left = "<< runsLeft << endl;
    assert (0 == runsLeft);

  }

  // construct priority queue over sorted runs and dump sorted data
  // into the out pipe

  // Page tp;
  // Record tr;

  // partiallySortedFile.GetPage(&tp,0);
  // tp.GetFirst(&tr);
  // priority_queue<Record, vector<Record>, Compare > prioQueueOverRuns (Compare(sortorder));
  // prioQueueOverRuns.push(tp);

  // get first page of each run, put into extra vector
  // git first record of each page, put into queue

  // while (not empty)/(not finished)
  // pull out minimum
  // put into pipe.

  /*
    { // priority queue stuff, figure out later, if even worthwhile.
    Record temp;
    PQ pq(runCount, sortorder);
    while( !pq.Empty() )
    {
    Record temp;
    pq.getMinimum(temp);
    out.Insert(&temp);
    }
    }
  */

  // for each record we need from a specific page
  // read specific page
  // get record from page
  // write modified page back
  ReadModifyWrite();

  /* OR */

  // read page into buffer of pages, when it is empty, get next page from that sequence if it is available, and write that out.

  // iterate through pages putting them all in the pipe directly
  /*
  int totalRecordsOut = 0;
  off_t lastPage = partiallySortedFile.GetLength() - 1;
  for(off_t curPage = 0;  curPage < lastPage; curPage++)
    {
      Page tp;
      partiallySortedFile.GetPage(&tp,curPage);
      Record temp;
      while(1 == tp.GetFirst(&temp))
        {
          totalRecordsOut++;
          out.Insert(&temp);
          // cout << "put a record in the pipe" << endl;
        }
    }
  cout << totalRecordsOut << " Records written to pipe" << endl;
  assert(totalRecordsOut == totalRecords);
  */
  cout << runCount << " runs in " << partiallySortedFile.GetLength() << " total pages" << endl;
  cout << "runlen of " << runlen << endl;
  cout << "phase two complete" << endl;
}

void BigQ::ReadModifyWrite(void)
{
  cout << "RMW" << endl;
}

BigQ::~BigQ () {
  runLocations.clear();
}
