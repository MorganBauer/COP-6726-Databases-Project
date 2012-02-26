#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapDBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <cstdlib>
/* Morgan Bauer */

HeapDBFile::HeapDBFile () : f(), curPage(), curPageIndex(0)
{}

int HeapDBFile::Open (char *fpath)
{
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

void HeapDBFile:: MoveFirst (){}
void HeapDBFile:: Add (Record &addme){}
int HeapDBFile:: GetNext (Record &fetchme){}
int HeapDBFile:: GetNext (Record &fetchme, CNF &cnf, Record &literal){}

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
