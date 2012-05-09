
#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdio.h>
#include <vector>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

struct att_pair {
	char *name;
	Type type;
};
struct Attribute {

	char *name;
	Type myType;
};

class OrderMaker;
class Schema {
 private:
  /* Schema operator=(const Schema&); */

	// gives the attributes in the schema
	int numAtts;
	Attribute *myAtts;

	// gives the physical location of the binary file storing the relation
	char *fileName;

	friend class Record;

public:
        Schema(const Schema&);
        Schema& operator=(const Schema&);
	// gets the set of attributes, but be careful with this, since it leads
	// to aliasing!!!
	Attribute *GetAtts ();

	// returns the number of attributes
	int GetNumAtts () const;

	// this finds the position of the specified attribute in the schema
	// returns a -1 if the attribute is not present in the schema
	int Find (const char *attName);

	// this finds the type of the given attribute
	Type FindType (char *attName);

	// this reads the specification for the schema in from a file
	Schema (char *fName, const char * relName);

	// this composes a schema instance in-memory
	Schema (const char *fName, int num_atts, Attribute *atts);

        // merges two schemas
        Schema(const Schema& left, const Schema& right);
        //
        Schema(const Schema& s, std::vector<int> indexesToKeep);

	// this constructs a sort order structure that can be used to
	// place a lexicographic ordering on the records using this type of schema
	int GetSortOrder (OrderMaker &order);
        void Reseat(std::string prefix);
        void Print();
 Schema() : numAtts(0), myAtts(0), fileName(0) { fileName = 0;}
	~Schema ();

};

#endif
