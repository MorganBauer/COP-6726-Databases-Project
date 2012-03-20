#include "HeapDBFile.h"
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

HeapDBFile::HeapDBFile () : f(), curPage(), curPageIndex(0)
{}

int HeapDBFile::Open (char *f_path)
{
  f.Open(1, f_path);
  MoveFirst();
  return 0;
}

int HeapDBFile::Create (char *f_path, fType f_type, void *startup)
{
  assert(heap == f_type);
  f.Open(0,f_path);
  return 0;
}

void HeapDBFile:: Load (Schema &f_schema, char *loadpath)
{
  FILE *tableFile = fopen (loadpath, "r");
  if (0 == tableFile)
    exit(-1);
  Record tempRecord;
  Page tempPage;
  int recordCounter = 0; // counter for debug
  int pageCounter = 0; // counter for debug

  while (1 == tempRecord.SuckNextRecord (&f_schema, tableFile))
    { // there is another record available
      assert(pageCounter >= 0);
      assert(recordCounter >= 0);
      recordCounter++;
      if (recordCounter % 10000 == 0) {
        cerr << recordCounter << "\n";
      }
      // use tempRecord, and put into tempPage. Later if page is full, write to file,
      int full = tempPage.Append(&tempRecord);
      if (0 == full)
        {
          // page was full
          f.AddPage(&tempPage,pageCounter++);
          tempPage.EmptyItOut();
          tempPage.Append(&tempRecord);
        }
    }
  { // make sure to add the last page
    f.AddPage(&tempPage,pageCounter++);
    cout << "Read and converted " << recordCounter <<
      " records, into " << pageCounter << " pages." << endl;
  }
}

void HeapDBFile:: MoveFirst ()
{
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
void HeapDBFile:: Add (Record &rec)
{
  Page tempPage; //
  // cout << "getting page " << f.GetLength() << endl;
  if (0 != f.GetLength())
    {
      f.GetPage(&tempPage, f.GetLength() - 2 ); // get the last page with stuff in it.
      if (0 == tempPage.Append(&rec)) // if the page is full
        {
          // f.AddPage(&tempPage,f.GetLength()-1); // don't add page, it's already there.
          tempPage.EmptyItOut();
          tempPage.Append(&rec);
          f.AddPage(&tempPage,f.GetLength()-1); // new final page
        }
      else // the page is not full (this is probably the more common case and we should flip the if/else order
        {
          f.AddPage(&tempPage,f.GetLength()-2); // same final page
        }
    }
  else // special case, we have a fresh file.
    {
      if (1 == tempPage.Append(&rec))
        {
          f.AddPage(&tempPage,0); // new final page
        }
      else  // ought to have been a fresh page, if it's full, can't do anything anyway.
        {
          exit(-1);
        }
    }
}

int HeapDBFile:: GetNext (Record &fetchme)
{
  if(0 == curPage.GetFirst(&fetchme)) // 0 is empty
    { // page is empty, get next page, if available, and return a record from it.
      // cout << "page " << curPageIndex + 1 << " was depleted." << endl;
      ++curPageIndex;
      // clog << "attempting to read page " << curPageIndex + 1  << " out of "
      //      << (f.GetLength() - 1) << "... " << endl;
      if(curPageIndex + 1 <= f.GetLength() - 1) // if there are still more pages to read.
        {
          // cout << "successful" << endl;
          f.GetPage(&curPage, curPageIndex);
          int ret = curPage.GetFirst(&fetchme);
          assert(1 == ret); // we can't now have fewer pages than we did four lines ago.
          return 1;
        }
      else // there are no more pages to read.
        {
          // cout << "failed, end of file" << endl;
          return 0;
        }
    }
  else
    { // page is not empty, return the next record.
      return 1;
    }
  return 0;
}

int HeapDBFile:: GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
  ComparisonEngine comp;
  while(SUCCESS == GetNext(fetchme)) // there are more records
    {
      if (SUCCESS == comp.Compare(&fetchme,&literal,&cnf)) // check the record
        {
          return 1;
        }
      else {
        clog << "failed getnext cnf match" << endl;
      }
    }
  return 0;
}

int HeapDBFile:: Close ()
{
  int fsize = f.Close();
  if (fsize >= 0) // check that file size is positive. This is the only rational test I can come up with at this time.
    {
      return 1;
    }
  else
    {
      return 0; //failure, negative file size, or some other error.
    }
}
