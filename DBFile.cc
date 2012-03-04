#include "DBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <cstdlib>
/* Morgan Bauer */

DBFile::DBFile () : dbf(NULL)
{}

int DBFile::Open (char *f_path)
{
  // TODO
  // read metadata file later
  // switch statement dispatch of appropriate underneath type
  //
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
    fType dbfileType = (fType) t;
    metafile.close();
    cout << "file type is " << dbfileType << endl;
  }

  switch(t)
    {
    case heap:
      dbf = new HeapDBFile();
      break;
    case sorted: // fall through, not implemented
      cout << "open a sorted dbfile" << endl;
      dbf = new SortedDBFile();
      // cout << "crash on purpose in dbfile open" << endl;
      // exit(-1);
      break;
    case tree: // fall through, not implemented
      cout << "open a b-plus tree dbfile" << endl;
      exit(-1);
    default:
      cout << "I don't know what type of file that is. Doing Nothing." <<  endl;
      exit(-1);
    }

  return dbf->Open(f_path);
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
  {
    string metafileName;
    metafileName.append(f_path);
    metafileName.append(".meta");
    ofstream metafile;
    metafile.open(metafileName.c_str());
    if(!metafile) return 1;
    metafile << f_type << endl; // write db type
    if(sorted == f_type)
      {
        SortInfo si = *((SortInfo *)startup);
        metafile << si.runLength << endl;
        OrderMaker om = *(OrderMaker *)si.myOrder;
        metafile << om; // write 

        om.Print();
      }
    if(!metafile) return 1;
    metafile.close();
    cout << "file type is " << f_type << endl;
  }

  switch(f_type)
    {
    case heap:
      cout << "This is a heap file. Operating in heap mode." <<  endl;
      cout << "Writing metadata file as " << f_path <<".meta" << endl;
      dbf = new HeapDBFile();
      break;
    case sorted: // fall through, not implemented
      cout << "create a sorted dbfile" << endl;
      dbf = new SortedDBFile();
      break;
    case tree: // fall through, not implemented
      cout << "create a b-plus tree dbfile" << endl;
      exit(-1);
    default:
      cout << "I don't know what type of file that is. Doing Nothing." <<  endl;
      exit(-1);
    }
  return dbf->Create(f_path, f_type, startup);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
  dbf->Load(f_schema,loadpath);
}

void DBFile::MoveFirst () {
  dbf->MoveFirst();
}

int DBFile::Close ()
{
  return dbf->Close();
}

void DBFile::Add (Record &rec) {
  dbf->Add(rec);
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
  return dbf->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  // nick says I might need a temp Record for this to test the comparison
  // it may or may not be a smart idea. ~quoth the Nick. NEVERMORE.
  return dbf->GetNext(fetchme, cnf, literal);
}
