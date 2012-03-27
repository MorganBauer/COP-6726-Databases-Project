#include "SortedDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <cstdlib>

/* Morgan Bauer */

SortedDBFile::SortedDBFile () : currentRWMode(reading), filepath(), f(), curPage(), curPageIndex(0), runLength(100), so(), toBigQ(NULL), fromBigQ(NULL), bq(NULL)
{}

int SortedDBFile::Open (char *f_path)
{
  int t;
  { // whole purpose is to set up t, really.
    string metafileName;
    metafileName.append(f_path);
    metafileName.append(".meta");
    ifstream metafile;
    metafile.open(metafileName.c_str());
    if(!metafile) return 1;

    metafile >> t;
    if(!metafile) return 1;

    metafile >> runLength;
    metafile >> so; // read in ordermaker
    // so.Print();

    fType dbfileType = (fType) t;
    metafile.close();
    // cout << "file type is " << dbfileType << endl;
  }

  filepath = f_path;
  f.Open(1, f_path);
  currentRWMode = reading;
  MoveFirst();
  cout << "file opened with " << f.GetLength() << " pages" << endl;
  return 0;
}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup)
{
  assert(sorted == f_type);
  // SortInfo si = *((SortInfo *)startup);
  filepath = f_path;
  f.Open(0,filepath.c_str());
  return 1;
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath)
{
  currentRWMode = writing;
  if (NULL == bq) // initialize pipes, and BigQ
    {
      toBigQ = new Pipe(pipeBufferSize);
      fromBigQ = new Pipe(pipeBufferSize);
      bq = new BigQ(*toBigQ,*fromBigQ,so, runLength );
    }
  FILE *tableFile = fopen (loadpath, "r");
  if (0 == tableFile)
    exit(-1);
  Record tempRecord;
  int recordCounter = 0; // counter for debug

  while (1 == tempRecord.SuckNextRecord (&f_schema, tableFile))
    { // there is another record available
      assert(recordCounter >= 0);
      recordCounter++;
      if (recordCounter % 10000 == 0) {
        cerr << recordCounter << "\n";
      }
      // use tempRecord, and put into tempPage. Later if page is full, write to file,
      toBigQ->Insert(&tempRecord);
    }
}

void SortedDBFile::MoveFirst ()
{
  if (writing == currentRWMode)
    {
      MergeDifferential();
    }
  // consider keeping an index value, rather than holding the page itself.
  curPageIndex = (off_t) 0;
  if (0 != f.GetLength())
    {
      f.GetPage(&curPage, curPageIndex);
    }
  else
    {
      curPage.EmptyItOut();
    }
}

int SortedDBFile::Close ()
{
  cout << "closing sorted DBFile" << endl
       << "f len is " << f.GetLength() << endl;
  if (0 == f.GetLength()) // file was new, so don't bother merging, just write from pipe to file.
    { // special case, we have a fresh file.
      if (writing == currentRWMode)
        {
          cout << "writing to empty file, stream outpipe to file directly" << endl;
          if (NULL != bq)
            {
              toBigQ->ShutDown();
              {// read from outPipe and write to file.
                Record tempRecord;
                Page tempPage;
                while (SUCCESS == fromBigQ->Remove(&tempRecord)) // while we can take records out of the pipe, do so.
                  {
                    if (FAILURE == tempPage.Append(&tempRecord)) // no more space in page, write to disk
                      { //
                        if (0 < f.GetLength())
                          {
                            f.AddPage(&tempPage,f.GetLength()-1); // new final page
                            tempPage.EmptyItOut();
                            tempPage.Append(&tempRecord);
                          }
                        else
                          {
                            f.AddPage(&tempPage,0);
                            tempPage.EmptyItOut();
                            tempPage.Append(&tempRecord);
                          }
                      }
                  }
                // finally write last thing to file
                if (0 < f.GetLength())
                  {
                    f.AddPage(&tempPage,f.GetLength()-1); // new final page
                    tempPage.EmptyItOut();
                  }
                else // we might have never written a page, so this might be the first still.
                  {
                    f.AddPage(&tempPage,0);
                    tempPage.EmptyItOut();
                  }
                cout << "flen = " << f.GetLength() << endl;
              }
              // close all things
              delete toBigQ;
              delete fromBigQ;
              delete bq;
              // null pointers
              toBigQ = NULL;
              fromBigQ = NULL;
              bq = NULL;
            }
        }
      int fsize = f.Close();
      cout << "at close we have " << fsize << " pages" << endl;
      if (fsize >= 0) // check that file size is positive. This is the only rational test I can come up with at this time.
        {
          return 1;
        }
      else
        {
          cout << "fsize was: " << fsize << endl;
          return 0; //failure, negative file size, or some other error.
        }
    }
  else // file needs to be merged
    {
      cout << "file is not 0 length" << endl;
      if (writing == currentRWMode)
        {
          cout << "mode is writing" << endl;
          MergeDifferential();
        }
      else if (reading == currentRWMode)
        {
          cout << "mode is reading" << endl;
        }
      else
        {
          cout << "unknown mode, crash" << endl;
          exit(-1);
        }
      int fsize = f.Close();
      cout << "at close we have " << fsize << " pages" << endl;
      if (fsize >= 0) // check that file size is positive. This is the only rational test I can come up with at this time.
        {
          return 1;
        }
      else
        {
          cout << "fsize was: " << fsize << endl;
          return 0; //failure, negative file size, or some other error.
        }
    }
}

void SortedDBFile::Add (Record &rec)
{
  // cout << "add";
  if (writing == currentRWMode) // we are in the correct RW mode.
    {
      toBigQ->Insert(&rec);
    }
  else if (reading == currentRWMode) // wrong RW mode, need to switch, and startup Q stuff.
    {
      currentRWMode = writing;
      if (NULL == bq) // initialize pipes, and BigQ
        {
          toBigQ = new Pipe(pipeBufferSize);
          fromBigQ = new Pipe(pipeBufferSize);
          bq = new BigQ(*toBigQ,*fromBigQ,so, runLength );
        }
      // insert first record
      toBigQ->Insert(&rec);
    }
  else
    {
      cout << "no known mode, inconsistent state, exiting" << endl;
      exit(-1);
    }
}

int SortedDBFile::GetNext (Record &fetchme)
{
  if (writing == currentRWMode)
    {
      MergeDifferential();
    }
  if(SUCCESS == curPage.GetFirst(&fetchme))
    { // page is not empty, return the next record.
      return 1;
    }
  else
    { // page is empty, get next page, if available, and return a record from it.
      ++curPageIndex;
      if(curPageIndex + 1 <= f.GetLength() - 1) // if there are still more pages to read.
        {
          f.GetPage(&curPage, curPageIndex);
          int ret = curPage.GetFirst(&fetchme);
          if (1 != ret)
            {
              cout << "gn flen = " << f.GetLength() << endl;
            }
          assert(1 == ret); // we can't now have fewer pages than we did four lines ago.
          return 1;
        }
      else // there are no more pages to read.
        {
          return 0;
        }
    }
  return 0;
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
  if (writing == currentRWMode)
    {
      MergeDifferential ();
    }
  // Compare sortorder ordermaker attributes, to cnf attributes
  // create new ordermaker based on this
  //
  // OrderMakers have this
  //   int numAtts;
  //   int whichAtts[MAX_ANDS];
  //   Type whichTypes[MAX_ANDS];
  //
  // CNFs have this
  //   Comparison orList[MAX_ANDS][MAX_ORS];
  //   int orLens[MAX_ANDS];
  //   int numAnds;

  // check if cached order maker is usable
  //    create new ordermaker if not
  OrderMaker query;
  // three conditions check (or maybe up above).

  ComparisonEngine comp;

  while(1 == GetNext(fetchme)) // there are more records
    {
      if (comp.Compare(&fetchme,&literal,&cnf)) // check the record
        {
          return 1;
        }
    }
  return 0;
}

void SortedDBFile :: MergeDifferential (void)
{
  currentRWMode = reading;
  // toBigQ->ShutDown(); // uncomment later
  StupidMergeDifferential();
  // merge differential file
  // two things to read from. bigq bq and already sorted file f.
  // write into new temp file, merging.

  // close all things
  delete toBigQ;
  delete fromBigQ;
  delete bq;
  // null pointers
  toBigQ = NULL;
  fromBigQ = NULL;
  bq = NULL;

  // move to beginning
  MoveFirst();
  cout << "need to write merge diff function, crashing" << endl;
  // exit(-1);
}

void SortedDBFile :: StupidMergeDifferential (void)
{
  cout << endl << "Doing stupidmerge" << endl<< endl;
  // two things to read from. bigq bq and already sorted file f.
  // Pipe in(100);
  // Pipe out(100);
  // BigQ stupid(in, out, so, runLength);

  // Pipe& diffPipe = *fromBigQ;
  // put the differential file into the pipe
  Record tempRecord;
  // while (SUCCESS == diffPipe.Remove(&tempRecord))
  //   {
  //     in.Insert(&tempRecord);
  //   }

  int origFileRecs = 0;
  // put the original file into the pipe.
  MoveFirst();
  cout << "original file had " << f.GetLength() << " pages" << endl;
  if (0 != f.GetLength()) // file was new, so don't bother merging, just write from pipe to file.
    {
      cout << "merge calling getnext" << endl;
      while (SUCCESS == GetNext(tempRecord))
        {
          origFileRecs++;
          toBigQ->Insert(&tempRecord);
        }
    }
  cout << "original file had " << origFileRecs << " records"<< endl;
  toBigQ->ShutDown();
  // everything should be in the pipe, close it now
  // in.ShutDown();
  // close the old file
  int fsize = f.Close();
  cout << "original file had " << fsize << " pages" << endl;
  // reopen file in create mode, starting from 0.
  cout << "REOPEN FILE" << endl;
  cout << "path is " << filepath << endl;
  f.Open(0,filepath.c_str());
  curPageIndex = 0;
  cout << "curPageIndex is" << curPageIndex << endl;
  cout << "new sorted merged file has " << f.GetLength() << " pages" << endl;
  // read everything from pipe and write to file.
  {
    int fromBigQrecs = 0;
    int fromBigQpgs = 0;
    curPage.EmptyItOut();
    while (SUCCESS == fromBigQ->Remove(&tempRecord))
      {
        fromBigQrecs++;
        if (FAILURE == curPage.Append(&tempRecord))
          { // page was full
            fromBigQpgs++;
            f.AddPage(&curPage,curPageIndex++); // add page to file
            curPage.EmptyItOut();
            curPage.Append(&tempRecord);
          }
      }
    cout << "BigQ had " << fromBigQrecs << " records"<< endl;
    f.AddPage(&curPage,curPageIndex++);
    fromBigQpgs++;
    curPage.EmptyItOut();
    cout << "took " << fromBigQpgs << " pages from bigq and put into file" << endl;
  }
  cout << "new sorted merged file has " << f.GetLength() << " pages" << endl;
  // everything should be done.
}
