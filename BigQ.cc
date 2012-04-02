#include "BigQ.h"
#include <vector>
#include <queue>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <utility>
#include <algorithm>
#include <cassert>
#include <iterator>
#include <omp.h>

/* Morgan Bauer */

BigQ :: BigQ (Pipe & _in, Pipe & _out, OrderMaker & _sortorder, int _runlen)
  : in(_in),out(_out),sortorder(_sortorder),runlen(_runlen), pagesInserted(0), totalRecords(0), runCount(0), partiallySortedFile(), runLocations(), worker_thread()
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
  // cout << endl << "BIGQ STARTED" << endl;
  char partiallySortedFileTempFileName[] = "/tmp/partiallysortedXXXXXX"; // maybe set this per instance to a random filename
  partiallySortedFile.TempOpen(partiallySortedFileTempFileName);
  clog << "using tempfile in " << partiallySortedFileTempFileName << endl;
  // FIRST PHASE
  clog << "sorting with " << runlen << " pages of memory" << endl;
  PhaseOne();
  // in pipe should be dead now.
  // SECOND PHASE
  static const int runThreshold = 200;
  clog << "sorting " <<runCount << " runs with " << runlen << " pages of memory" << endl;
  if (runThreshold >= runCount)
    {
      PhaseTwoLinearScan();
    }
  else
    {
      PhaseTwoPriorityQueue();
    }
  out.ShutDown ();
  clog << endl << "MERGE COMPLETE" << endl;
  // Cleanup
  partiallySortedFile.Close();
  clog << "deleting tempfile in " << partiallySortedFileTempFileName << endl;
  int ret = remove(partiallySortedFileTempFileName);
  if (ret)
    perror ("The following error occurred");
  // finally shut down the out pipe
  // this lets the consumer thread know that there will not be anything else put into the pipe
  pthread_exit(NULL); // make our worker thread go away
}

void BigQ::PhaseOne(void)
{
  size_t vecsize = runlen; //good first guess
  // FIRST PHASE
  // read data from in pipe sort them into runlen pages
  vector<Record> runlenrecords;
  runlenrecords.reserve(vecsize);
  Record tempRecord;
  static size_t const maxSize = PAGE_SIZE * runlen;
  size_t curSize = 0;
  while (1 == in.Remove(&tempRecord)) // while we can take records out of the pipe, do so.
    {
      curSize += tempRecord.GetSize();
      if (curSize <= maxSize) // have more room in the buffer
        {
          runlenrecords.push_back(tempRecord); // put it in our runlen sized buffer.
        }
      else // have runlen pages of records
        {
          runCount++;
          // update probable max vector size to avoid copying in future iterations.
          if (vecsize < runlenrecords.size()) {vecsize = runlenrecords.size();}

          sortRuns(runlenrecords);
          writeSortedRunToFile(runlenrecords);
          runlenrecords.clear(); // reset the temp vector buffer thing
          curSize = 0;
          runlenrecords.push_back(tempRecord);// put the new record in
        }
    }
  // we've taken all the records out of the pipe
  // do one last internal sort, on the the buffer that we have
  if (0 < runlenrecords.size())
    {
      if (vecsize < runlenrecords.size()) {vecsize = runlenrecords.size();}
      runCount++;
      sortRuns(runlenrecords);
      // cout << "last run sorted " << endl;
      writeSortedRunToFile(runlenrecords);
    }
}

void BigQ :: sortRuns(vector<Record> & runlenrecords)
{
  // sort the records we have in the runlen buffer.
  Compare c = Compare(sortorder);
  // if (!std::is_sorted(runlenrecords.begin(),
  //                     runlenrecords.end(),
  //                     c))
  {
    std::sort(runlenrecords.begin(),
              runlenrecords.end(),
              c);
  }
}

int BigQ :: writeSortedRunToFile(vector<Record> & runlenrecords)
{
  off_t pageStart = pagesInserted;
  // now to write sorted records out to a file, first, we must fill a page ...
  Page tp;
  Record trp;
  for (vector<Record>::iterator it = runlenrecords.begin(); it < runlenrecords.end(); it++)
    {
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
  // cout << "inserted " <<  pageEnd - pageStart << " pages" << endl;
  runLocations.push_back(make_pair(pageStart,pageEnd));
  return (int)(pageEnd-pageStart);
}

void BigQ::PhaseTwoLinearScan(void)
{
  clog << endl << endl << "Linear Scan Merge of sorted runs" << endl;
  {
    vector<Run> runs;
    runs.reserve(runCount);
    //initializing runs
    for (int i = 0; i < runCount; i++)
      {
        runs.push_back(Run(i,runLocations[i].first,runLocations[i].second, &partiallySortedFile)); // maybe try enplace from c++11
      }

    vector<Record> minimums;
    // initialize minimums
    // for each run, get the first guy.
    minimums.reserve(runCount); // we know how many runs we are merging, so reserve that much space from the beginning to avoid the vector reallocation
    for (int i = 0; i < runCount; i++)
      {
        Record tr;
        runs[i].getNextRecord(tr);
        minimums.push_back(tr);
      }
    // now find the minimum guy and put it in the pipe
    // do this totalRecords times
    Compare c = Compare(sortorder);
    {
      int runsLeft = runCount;
      int recordsOut = 0;
      for (int r = totalRecords ; r > 0; r--) // I know exactly how many records I have, so iterate that many times.
        {
          vector<Record>::iterator::difference_type run =
            std::distance( minimums.begin(),
                           std::min_element(minimums.begin(), minimums.end(), c));

          out.Insert(&(minimums[run]));

          if (!runs[run].getNextRecord((minimums[run]))) // run empty, got to get rid of it
            { // need to get rid of run and shift everything over
              runsLeft--;
              minimums.erase(minimums.begin() + run);
              runs.erase(runs.begin() + run);
            }
          recordsOut++;
          if (0 == recordsOut % 10000)
            {
              clog << recordsOut/10000 << " ";
            }
        }
      assert(recordsOut == totalRecords);
      assert (0 == runsLeft);
    }
  }
}

void BigQ::PhaseTwoPriorityQueue(void)
{
  cout << endl << endl << "Priority Queue Merge of sorted runs" << endl;
  {
    vector<Run> runs;
    runs.reserve(runCount);
    // cout << "initializing runs" << endl;
    for (int i = 0; i < runCount; i++)
      {
        // cout << "Run " << i;
        runs.push_back(Run(i,runLocations[i].first,runLocations[i].second, &partiallySortedFile));
        // cout << " initialized" << endl;
      }

    for (int i = 0; i < runCount; i++)
      {
        // runs[i].print();
      }

    std::priority_queue<TaggedRecord, vector<TaggedRecord>, TaggedRecordCompare> mins (sortorder);

    // initialize minimums
    // for each run, get the first guy.
    // cout << "initializing minimums" << endl;
    // minimums.reserve(runCount);
    for (int i = 0; i < runCount; i++)
      {
        // cout << "minimum " << i;
        Record tr;
        runs[i].getNextRecord(tr);
        // minimums.push_back(tr);
        // cout << "push" << endl;
        mins.push(TaggedRecord(tr,i));
        // cout << "initialized " << endl;
      }
    // now find the minimum guy and put it in the pipe
    // do this totalRecords times
    // cout << "putting stuff in the pipe" << endl;
    // Compare c = Compare(sortorder);
    {
      int runsLeft = runCount;
      int recordsOut = 0;
      for (int r = totalRecords ; r > 0; r--)
        {
          TaggedRecord TRtr(mins.top());
          Record tr(TRtr.r);

          int run = TRtr.getRun();

          recordsOut++;
          out.Insert(&tr);
          mins.pop();
          bool valid = runs[run].getNextRecord(tr);
          if (valid)
            {
              mins.push(TaggedRecord(tr,run));
            }
          else
            {
              // cout << "run empty, got to get rid of it" << endl;
              runsLeft--;
            }
        }
      assert(recordsOut == totalRecords);
      // cout << "runs left = "<< runsLeft << endl;
      assert (0 == runsLeft);
    }
  }
  // cout << runCount << " runs in " << partiallySortedFile.GetLength() << " total pages" << endl;
  // cout << "runlen of " << runlen << endl;
  // cout << "phase two complete" << endl;
}

BigQ::~BigQ () {}
