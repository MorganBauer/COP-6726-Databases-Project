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
  : in(_in),out(_out),sortorder(_sortorder),runlen(_runlen), pagesInserted(0), totalRecords(0), runCount(0), partiallySortedFile(), runLocations()
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
  // FIRST PHASE
  PhaseOne();
  // in pipe should be dead now.
  // cout << totalRecords << " Records written to file" << endl;
  // SECOND PHASE
  static const int runThreshold = 200;
  if (runThreshold >= runCount)
    {
      PhaseTwoLinearScan();
    }
  else
    {
      PhaseTwoPriorityQueue();
    }
  clog << endl << "MERGE COMPLETE" << endl;
  // cout << "cleanup" << endl;
  partiallySortedFile.Close();
  // Cleanup
  remove(partiallySortedFileTempFileName);
  // finally shut down the out pipe
  // this lets the consumer thread know that there will not be anything else put into the pipe
  out.ShutDown ();
  //cout << endl << "BIGQ FINISHED" << endl;
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
              // cout << "finished getting a run, now to sort it" << endl;
              runCount++;
              // update probable max vector size to avoid copying in future iterations.
              if (vecsize < runlenrecords.size()) {vecsize = runlenrecords.size();}

              sortRuns(runlenrecords);
              // cout << "run size " << runlenrecords.size() << endl;
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
      // cout << "last run sorted " << endl;
      writeSortedRunToFile(runlenrecords);
    }

  runlenrecords.clear();
  // cout << "maximum vector size needed was " << vecsize << endl;
}

void BigQ :: sortRuns(vector<Record> & runlenrecords)
{
  // sort the records we have in the runlen buffer.
  // cout << "sorting run " << endl;
  if (!std::is_sorted(runlenrecords.begin(),
                      runlenrecords.end(),
                      Compare(sortorder)))
    std::sort(runlenrecords.begin(),
              runlenrecords.end(),
              Compare(sortorder));
  // cout << "run size " << runlenrecords.size() << endl;
}

int BigQ :: writeSortedRunToFile(vector<Record> & runlenrecords)
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
  // cout << "inserted " <<  pageEnd - pageStart << " pages" << endl;
  runLocations.push_back(make_pair(pageStart,pageEnd));
  return (int)(pageEnd-pageStart);
}

void BigQ::PhaseTwoLinearScan(void)
{
  clog << endl << endl << "Linear Scan Merge of sorted runs" << endl;
  // cout << runCount << " runs in " << partiallySortedFile.GetLength() << " total pages" << endl;
  for (std::vector < std::pair <off_t,off_t> >::iterator it = runLocations.begin(); it < runLocations.end(); it++)
    {
      // cout << "from " << (*it).first << " to " << (*it).second << endl;
    }

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

    vector<Record> minimums;
    // initialize minimums
    // for each run, get the first guy.
    // cout << "initializing minimums" << endl;
    minimums.reserve(runCount);
    for (int i = 0; i < runCount; i++)
      {
        // cout << "minimum " << i;
        Record tr;
        runs[i].getNextRecord(tr);
        minimums.push_back(tr);
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
          vector<Record>::iterator min = std::min_element(minimums.begin(), minimums.end(),Compare(sortorder));
          vector<Record>::iterator::difference_type run = std::distance( minimums.begin(), min);

          Record tr;
          tr.Consume(&(minimums[run]));
          recordsOut++;
          if (0 == recordsOut % 10000)
            {
              clog << recordsOut << " ";
            }
          out.Insert(&tr);
          bool valid = runs[run].getNextRecord(tr);
          if (valid)
            {
              minimums[run].Consume(&tr);
            }
          else
            {
              // cout << "run empty, got to get rid of it" << endl;
              runsLeft--;
              minimums.erase(minimums.begin() + run);
              runs.erase(runs.begin() + run);
              // need to get rid of run and shift everything over
            }
        }
      assert(recordsOut == totalRecords);
      // cout << minimums.size() << runs.size() << endl;
      // cout << "runs left = "<< runsLeft << endl;
      assert (0 == runsLeft);
    }
  }
  // cout << runCount << " runs in " << partiallySortedFile.GetLength() << " total pages" << endl;
  // cout << "runlen of " << runlen << endl;
  // cout << "phase two complete" << endl;
}

void BigQ::PhaseTwoPriorityQueue(void)
{
  cout << endl << endl << "Priority Queue Merge of sorted runs" << endl;
  // cout << runCount << " runs in " << partiallySortedFile.GetLength() << " total pages" << endl;
  for (std::vector < std::pair <off_t,off_t> >::iterator it = runLocations.begin(); it < runLocations.end(); it++)
    {
      // cout << "from " << (*it).first << " to " << (*it).second << endl;
    }

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

BigQ::~BigQ () {
  runLocations.clear();
}
