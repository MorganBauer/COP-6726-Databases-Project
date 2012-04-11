#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <iostream>

class RelationInformation
{
  typedef unsigned long long tupleCount;

  tupleCount numberOfTuples;
  std::map<std::string, tupleCount> attributeInformation;
 public:
  RelationInformation () : numberOfTuples(0), attributeInformation() {}
  RelationInformation (tupleCount tuples) : numberOfTuples(tuples) {}
  void CopyAtts (RelationInformation & otherRel)
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
};

class Statistics
{
 private:
  std::map < std::string, int> relations;
  std::map < std::string, std::pair<std::string, int> > relationInformation;
  std::map < std::string, RelationInformation > rels;
  std::map < std::string, std::string> extantAttrs;

  void Check (struct AndList *parseTree, char *relNames[], int numToJoin);
  void CheckRelations(char *relNames[], int numToJoin);
  std::vector<std::string> CheckParseTree(struct AndList *pAnd);
  double CalculateEstimate(struct AndList *pAnd);
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
