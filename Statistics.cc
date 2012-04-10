#include "Statistics.h"
#include <cassert>
#include <string>
#include <iostream>

Statistics::Statistics() : relations(), relationInformation()
{
}

Statistics::Statistics(Statistics &copyMe) : relations(copyMe.relations), relationInformation(copyMe.relationInformation)
{
  assert(0);
}

Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
  // This operation adds another base relation into the structure. The
  // parameter set tells the statistics object what the name and size
  // of the new relation is (size is given in terms of the number of
  // tuples).

  // Note that AddRel can be called more than one time for the same
  // relation or attribute. If this happens, then you simply update
  // the number of tuples or number of distinct values for the
  // specified attribute or relation.
  using std::string;
  // look up in map, insert
  string s(relName);
  relations[s] = numTuples;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
  // This operation adds an attribute to one of the base relations in
  // the structure. The parameter set tells the Statistics object what
  // the name of the attribute is, what relation the attribute is
  // attached to, and the number of distinct values that the relation
  // has for that particular attribute. If numDistincts is initially
  // passed in as a –1, then the number of distincts is assumed to be
  // equal to the number of tuples in the associated relation.

  // Note that AddAtt can be called more than one time for the same
  // relation or attribute. If this happens, then you simply update
  // the number of tuples or number of distinct values for the
  // specified attribute or relation.
  using std::string;
  string rel(relName);
  string att(attName);
  if (-1 == numDistincts)
    {
      relationInformation[rel] = make_pair(att, relations[rel]);
    }
  else
    {
      relationInformation[rel] = make_pair(att, numDistincts);
    }
}

void Statistics::CopyRel(char *oldName, char *newName)
{
  assert(0);
}

void Statistics::Read(char *fromWhere)
{
  assert(0);
}

void Statistics::Write(char *fromWhere)
{
  assert(0);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  assert(0);
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
  // check if the numdistincts in relation information is different
  // from the number of tuples in relations
  using std::cout; using std::endl;
  //OrList * lor = A
  char * op =  parseTree->left->left->left->value;
  cout << "op is" << op << endl;
  cout << relNames[0] << endl;
  assert(0);
}

