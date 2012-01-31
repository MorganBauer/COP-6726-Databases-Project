#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <cassert>
#include <cstdlib>
/* Morgan Bauer */

DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {

  f.Open(0,f_path); // open, with 0 to create, giving it the path.

  switch(f_type)
    {
    case heap:
      cout << "This is a heap file. Operating in heap mode." <<  endl;
      cout << "Writing metadata file as " << f_path <<".header" << endl;
      // make extra file with .header attached to tell us about this heap type db file
      break;
    case sorted: // fall through, not implemented
    case tree: // fall through, not implemented
    default:
      cout << "I don't know what type of file that is. Doing Nothing." <<  endl;
    }

  return 0;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
  // loadpath is path to '.tbl' file
  // we need to iterate thorugh the whole table writing it to the file.
  FILE *tableFile = fopen (loadpath, "r");
  if (tableFile == 0)
    exit(-1);
  Record tempRecord;
  Page tempPage;
  int recordCounter = 0; // counter for debug
  int pageCounter = 0; // counter for debug
  // bool addedToFile = false;

  while (tempRecord.SuckNextRecord (&f_schema, tableFile) == 1) {
    assert(pageCounter >= 0);
    assert(recordCounter >= 0);
    recordCounter++;
    if (recordCounter % 10000 == 0) {
      cerr << recordCounter << "\n";
    }
    // use tempRecord, and put into tempPage. Later if page is full, write to file,
    int full = tempPage.Append(&tempRecord);
    if (full == 0)
      {
        // cerr << "Page was full" << endl;
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

int DBFile::Open (char *f_path) {
  // TODO
  // remember switch statement and metadata file later.
  f.Open(1, f_path);
  return 0;
}

void DBFile::MoveFirst () {
  // consider keeping an index value, rather than holding the page itself.
  curPageIndex = (off_t) 0;
  f.GetPage(&curPage, curPageIndex);
}

int DBFile::Close () {
  // possibly also write out information to the metadata file before closing.

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

void DBFile::Add (Record &rec) {
  Page tempPage; // 
  //cout << "getting page " << f.GetLength() << endl;
  f.GetPage(&tempPage, f.GetLength() - 2 ); // get the last page with stuff in it.

  if (0 == tempPage.Append(&rec))
    {
      // f.AddPage(&tempPage,f.GetLength()-1); // don't add page, it's already there.
      tempPage.EmptyItOut();
      tempPage.Append(&rec);
      f.AddPage(&tempPage,f.GetLength()-1); // new final page
    }
  else
    {
      f.AddPage(&tempPage,f.GetLength()-2); // same final page
    }

}

int DBFile::GetNext (Record &fetchme) {
  /*
    The first version of GetNext simply gets the next record from the
    file and returns it to the user, where “next” is defined to be
    relative to the current location of the pointer. After the function
    call returns, the pointer into the file is incremented, so a
    subsequent call to GetNext won’t return the same record twice. The
    return value is an integer whose value is zero if and only if there is
    not a valid record returned from the function call (which will be the
    case, for example, if the last record in the file has already been
    returned).
  */

  //Record temp;

  if(0 == curPage.GetFirst(&fetchme)) // 0 is empty
    { // page is empty, get next page, if available, and return a record from it.
      // cout << "page " << curPageIndex + 1 << " was depleted." << endl;
      ++curPageIndex;
      // cout << "attempting to read page " << curPageIndex + 1  << " out of "
      // << (f.GetLength() - 1) << "... ";
      if(curPageIndex + 1 <= f.GetLength() - 1) // if there are still more pages to read.
        {
          // cout << "successful" << endl;
          f.GetPage(&curPage, curPageIndex);
          int ret = curPage.GetFirst(&fetchme)
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

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  // nick says I might need a temp Record for this to test the comparison
  // it may or may not be a smart idea. ~quoth the Nick. NEVERMORE.
  ComparisonEngine comp;

  while(1 == GetNext(fetchme))
    {
      if (comp.Compare(&fetchme,&literal,&cnf))
        {
          return 1;
        }
    }
  return 0;
}
