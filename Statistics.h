#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <tr1/unordered_map>
#include <set>
#include <tr1/unordered_set>
#include <string>
#include <utility>
#include <vector>
#include <iostream>

class RelationInformation
{
  typedef unsigned long long tupleCount;
  tupleCount numberOfTuples;
  std::map<std::string, tupleCount> attributeInformation;
  // Schema sch;
  std::string originalName;
 public:
  RelationInformation () : numberOfTuples(0), attributeInformation(), originalName() {}
  explicit RelationInformation (tupleCount tuples, std::string origNme) : numberOfTuples(tuples), attributeInformation(), /* sch(), */ originalName(origNme)
  {/* char * cat = "catalog"; sch = Schema(cat, origNme.c_str()); */}
  std::map<std::string, tupleCount> const GetAtts()
    {
      return attributeInformation;
    }
  void CopyAtts (RelationInformation const & otherRel)
  {
    attributeInformation.insert(otherRel.attributeInformation.begin(), otherRel.attributeInformation.end());
  }
  tupleCount GetDistinct (std::string attr)
  {
    return attributeInformation[attr];
  }
  void AddAtt (std::string attr, tupleCount numDistinct)
  {
    attributeInformation[attr] = numDistinct;
  }
  tupleCount NumTuples() {return numberOfTuples;}
  void print()
  {
    std::clog << numberOfTuples << std::endl;
    for (std::map<std::string, tupleCount>::iterator it=attributeInformation.begin() ; it != attributeInformation.end(); it++ )
      {std::clog << (*it).first << " => " << (*it).second << std::endl;}
  }

  friend std::ostream& operator<<(std::ostream& os, const RelationInformation & RI)
    {
      using std::endl;
      os << RI.numberOfTuples << endl;
      std::map<std::string, tupleCount>::const_iterator it;
      os << RI.attributeInformation.size() << endl;
      for (it = RI.attributeInformation.begin(); it != RI.attributeInformation.end(); it++ )
        {
          os << (*it).first << endl << (*it).second << endl;
        }
      return os;
    }

  friend std::istream& operator>>(std::istream& is, RelationInformation & RI)
    {
      using std::clog; using std::endl;
      clog << "reading relation" << endl;
      tupleCount numTups;
      is >> numTups;
      clog << "there are " << numTups << " tuples in this relation." << endl;
      RI.numberOfTuples = numTups;
      tupleCount numMappings;
      is >> numMappings;
      clog << "there are " << numMappings << " attributes in this relation." << endl;
      for (unsigned i = 0; i < numMappings; i++)
        {
          std::string attr;
          tupleCount distinct;
          is >> attr;
          is >> distinct;
          RI.attributeInformation[attr] = distinct;
        }
      return is;
    }
};

class Statistics
{
 private:
  typedef long long unsigned int tupleCount;

  std::map < std::string, RelationInformation > rels;
  std::map < std::string, std::string> extantAttrs; // if the attr exists, and what relation it is in. <k,v> is <attr, relation-attr-is-in>
  std::map < std::string, std::string> mergedRelations; // <k,v> is <original-relation-name, merged-relation-name>

  void Check (struct AndList *parseTree, char *relNames[], int numToJoin);
  void CheckRelations(char *relNames[], int numToJoin);
  std::vector<std::string> CheckParseTree(struct AndList *pAnd);
  double CalculateEstimate(struct AndList *pAnd);
  bool HasJoin(AndList *pAnd);
 public:
  Statistics();
  Statistics(Statistics &copyMe); // Performs deep copy
  ~Statistics();


  void AddRel(char *relName, int numTuples);
  void AddAtt(char *relName, char *attName, int numDistincts);
  void CopyRel(char *oldName, char *newName);

  void Read(char *fromWhere);
  void Write(char *fromWhere);

  void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
  double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

  std::string getAttrHomeTable(std::string a)
    {
      using std::clog;
      using std::endl;
      using std::string;
      clog << "called get attr home table" << endl;
      // std::string a(attr);
      clog << a << a.size() << endl;
      clog << "found " << extantAttrs.count(a);
      if (1 == extantAttrs.count(a))
        {
          clog << extantAttrs[a] << endl;
          string tbl(extantAttrs[a]);
          clog << tbl << endl;
          return tbl;
        }
      return "";
    }

  void print()
  {
    using std::clog; using std::endl;
    {
      std::map < std::string, RelationInformation >::iterator it;
      for (it = rels.begin(); it != rels.end(); it++ )
        {
          clog << (*it).first << " relation has information " << endl << (*it).second << endl;
        }
    }
    {
      std::map < std::string, std::string>::iterator it;
      for (it = extantAttrs.begin(); it != extantAttrs.end(); it++ )
        {
          clog << (*it).first << " is in relation " << (*it).second << endl;
        }
    }
    { // merged relations
      clog << "Merged relations are " << endl;
      std::map < std::string, std::string>::iterator it;
      for (it = mergedRelations.begin(); it != mergedRelations.end(); it++ )
        {
          clog << (*it).first << " is in relation " << (*it).second << endl;

        }
    }
  }
};

#endif
