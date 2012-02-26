#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <cstdlib>

/* Morgan Bauer */


SortedDBFile::SortedDBFile ()
{}

int SortedDBFile::Open (char *f_path)
{
}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup)
{
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath)
{
}

void SortedDBFile::MoveFirst ()
{
}

int SortedDBFile::Close ()
{
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
