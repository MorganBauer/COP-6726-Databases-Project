#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

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
  Record temp;

  int counter = 0;

  while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
    // from example 'main' program
    // counter for debug
    counter++;
    if (counter % 10000 == 0) {
      cerr << counter << "\n";
    }
    // use temp, and put into page p, just do one for each record, for now. Later if page is full, write to file,
    int full = p.Append(&temp);
    if (full == 0)
      {
        cout << "Page was full" << endl;
        f.AddPage(&p,f.GetLength());
        p.EmptyItOut();
      }
  }
  // make sure to add the last page
  f.AddPage(&p,f.GetLength());

}

int DBFile::Open (char *f_path) {
  return 0;
}

void DBFile::MoveFirst () {

}

int DBFile::Close () {
  // possibly also write out information to the metadata file before closing.

  int fsize = f.Close();
  if (fsize >= 0) // check that file size is positive. This is the only rational test I can come up with at this time.
    {
      return 1;
    }
  else {
    return 0; //failure, negative file size, or some other error.
  }
}

void DBFile::Add (Record &rec) {

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
    returned).  */
  return 0;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  return 0;
}
