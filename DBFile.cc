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

  f.Open(0,f_path);

  switch(f_type)
    {
    case heap:
      cout << "This is a heap file. Operating in heap mode." <<  endl;
      cout << "Writing metadata file as " << f_path <<".header" << endl;
      // make extra file with .header attached to tell us about this heap type db file
      break;
    case sorted:
    case tree:
    default:
      cout << "I don't know what type of file that is. Doing Nothing." <<  endl;
    }

  return 0;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
}

int DBFile::Open (char *f_path) {
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
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
