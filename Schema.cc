#include "Schema.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

int Schema :: Find (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		if (!strcmp (attName, myAtts[i].name)) {
			return i;
		}
	}

	// if we made it here, the attribute was not found
	return -1;
}

Type Schema :: FindType (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		if (!strcmp (attName, myAtts[i].name)) {
			return myAtts[i].myType;
		}
	}

	// if we made it here, the attribute was not found
	return Int;
}

int Schema :: GetNumAtts () {
	return numAtts;
}

Attribute *Schema :: GetAtts () {
	return myAtts;
}

Schema :: Schema (char *fpath, int num_atts, Attribute *atts) : numAtts(num_atts), myAtts(0), fileName(0) {
	fileName = strdup (fpath);
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {
		if (atts[i].myType == Int) {
			myAtts[i].myType = Int;
		}
		else if (atts[i].myType == Double) {
			myAtts[i].myType = Double;
		}
		else if (atts[i].myType == String) {
			myAtts[i].myType = String;
		}
		else {
			cout << "Bad attribute type for " << atts[i].myType << "\n";
			delete [] myAtts;
			exit (1);
		}
		myAtts[i].name = strdup (atts[i].name);
	}
}

Schema :: Schema (char *fName, const char * relName) : numAtts(0), myAtts(0), fileName(0)
{
	FILE *foo = fopen (fName, "r");

	// this is enough space to hold any tokens
	char space[200];

	fscanf (foo, "%s", space);
	int totscans = 1;

	// see if the file starts with the correct keyword
	if (strcmp (space, "BEGIN")) {
		cout << "Unfortunately, this does not seem to be a schema file.\n";
		exit (1);
	}

	while (1) {

		// check to see if this is the one we want
		fscanf (foo, "%s", space);
		totscans++;
		if (strcmp (space, relName)) {

			// it is not, so suck up everything to past the BEGIN
			while (1) {

				// suck up another token
				if (fscanf (foo, "%s", space) == EOF) {
					cerr << "Could not find the schema for the specified relation.\n";
					exit (1);
				}

				totscans++;
				if (!strcmp (space, "BEGIN")) {
					break;
				}
			}

		// otherwise, got the correct file!!
		} else {
			break;
		}
	}

	// suck in the file name
	fscanf (foo, "%s", space);
	totscans++;
	fileName = strdup (space);

	// count the number of attributes specified
	numAtts = 0;
	while (1) {
		fscanf (foo, "%s", space);
		if (!strcmp (space, "END")) {
			break;
		} else {
			fscanf (foo, "%s", space);
			numAtts++;
		}
	}

	// now actually load up the schema
	fclose (foo);
	foo = fopen (fName, "r");

	// go past any un-needed info
	for (int i = 0; i < totscans; i++) {
		fscanf (foo, "%s", space);
	}

	// and load up the schema
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {

		// read in the attribute name
		fscanf (foo, "%s", space);
		myAtts[i].name = strdup (space);

		// read in the attribute type
		fscanf (foo, "%s", space);
		if (!strcmp (space, "Int")) {
			myAtts[i].myType = Int;
		} else if (!strcmp (space, "Double")) {
			myAtts[i].myType = Double;
		} else if (!strcmp (space, "String")) {
			myAtts[i].myType = String;
		} else {
			cout << "Bad attribute type for " << myAtts[i].name << "\n";
			exit (1);
		}
	}

	fclose (foo);
}

Schema::Schema(const Schema& s) : fileName(0)
{
  if (0 != s.fileName)
    fileName = strdup(s.fileName);
  numAtts = s.numAtts;
  myAtts = new Attribute[numAtts];
  for (int i = 0; i < numAtts; i++ )
    {
      myAtts[i] = s.myAtts[i];
      myAtts[i].name = strdup(myAtts[i].name);
    }
}

Schema& Schema::operator=(const Schema& s)
{
  if (0 != s.fileName)
    fileName = strdup(s.fileName);
  numAtts = s.numAtts;
  myAtts = new Attribute[numAtts];
  for (int i = 0; i < numAtts; i++ )
    {
      myAtts[i] = s.myAtts[i];
      myAtts[i].name = strdup(myAtts[i].name);
    }
  return *this;
}

Schema :: ~Schema () {
  free(fileName);
  for (int i = 0; i < numAtts; i++ )
    {
      free(myAtts[i].name);
    }
	delete [] myAtts;
	myAtts = 0;
}

void Schema :: Reseat(string prefix)
{
 for (int i = 0; i < numAtts; i++ )
    {
      string oldName (myAtts[i].name);
      free(myAtts[i].name);
      string newName (prefix + "." + oldName);
      myAtts[i].name = strdup(newName.c_str());
      clog << "att number " << i << "was" << oldName << " and is now " << myAtts[i].name << endl;
    }
}

void Schema :: Print()
{
  clog << "printing schema with " << numAtts << " attributes" << endl;
  for (int i = 0; i < numAtts; i++ )
    {
      clog << "att number " << i << " is " << myAtts[i].name << " of type " << myAtts[i].myType << endl;
    }
}
