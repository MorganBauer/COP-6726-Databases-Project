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


SortedDBFile::SortedDBFile () : f(), curPage(), curPageIndex(0), runLength(0), so(), bq(NULL)
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
    metafile >> so;
    
    
    fType dbfileType = (fType) t;
    metafile.close();
    cout << "file type is " << dbfileType << endl;
  }

  // figure out how to get the order maker from the file.
  f.Open(1, f_path);
  return 0;
}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup)
{
  assert(sorted == f_type);
  // SortInfo si = *((SortInfo *)startup);
  f.Open(0,f_path);
  return 1;
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath)
{
}

void SortedDBFile::MoveFirst ()
{
}

int SortedDBFile::Close ()
{
  int fsize = f.Close();
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

void SortedDBFile::Add (Record &rec)
{
}

int SortedDBFile::GetNext (Record &fetchme)
{
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
}
