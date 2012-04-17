#include "Statistics.h"
#include <cassert>
#include <string>
#include <iostream>
#include <cstdlib>
#include <vector>
#include <set>
#include <tr1/unordered_set>
#include <algorithm>
#include <fstream>

using std::string;
using std::vector;
using std::clog; using std::endl;

Statistics::Statistics() : // relations(), relationInformation(),
  rels(), extantAttrs()
{
}

Statistics::Statistics(Statistics &copyMe) // : relations(copyMe.relations), relationInformation(copyMe.relationInformation)
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
  const RelationInformation newRelation (numTuples);
  rels[s] = newRelation;
  rels.insert( make_pair(s,newRelation));
  rels[s].print();
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
  string const rel(relName);
  string const att(attName);
  if (-1 == numDistincts)
    {
      // relationInformation[rel] = make_pair(att, relations[rel]);
      rels[rel].AddAtt(att,rels[rel].NumTuples());
      extantAttrs.insert(make_pair(att,rel));
    }
  else
    {
      // relationInformation[rel] = make_pair(att, numDistincts);
      rels[rel].AddAtt(att,numDistincts);
      std::clog << "before set insert with numdistincts given" << std::endl;
      std::clog << "attempting to insert atter \"" << att << "\"" << std::endl;
      extantAttrs.insert(make_pair(att,rel));
      std::clog << "after set insert with numdistincts given" << std::endl;
    }
  rels[rel].print();
}

void Statistics::CopyRel(char *oldName, char *newName)
{
  std::string oldN(oldName);
  std::string newN(newName);
  std::map < std::string, tupleCount > const oldAttrs = rels[oldN].GetAtts();

  RelationInformation newR(rels[oldN].NumTuples());

  std::map < std::string, tupleCount >::const_iterator it;
  for (it = oldAttrs.begin(); it != oldAttrs.end(); it++ )
    {
      newR.AddAtt((newN+"."+(*it).first), (*it).second); // add modified attr to new relation
      extantAttrs[(newN+"."+(*it).first)] = newN; // know where these modified attrs are
    }

  rels[newN] = newR; // put new relation in
  newR.print();
}

void Statistics::Read(char *fromWhere)
{
  clog << endl;
  using std::ifstream;
  ifstream statFile(fromWhere);

  if (!statFile.good())
    {
      return;
    }

  unsigned iters;
  statFile >> iters;
  for(unsigned i = 0; i < iters; i++)
    {
      string relation;
      RelationInformation RI;
      statFile >> relation;
      statFile >> RI;
      rels[relation] = RI;
    }
  statFile >> iters;

  for(unsigned i = 0; i < iters; i++)
    {
      string attr;
      string relation;
      statFile >> attr >> relation;
      extantAttrs[attr] = relation;
    }
  statFile >> iters;
  for(unsigned i = 0; i < iters; i++)
    {
      string relation;
      string mergedrelation;
      statFile >> relation >> mergedrelation;
      mergedRelations[relation] = mergedrelation;
    }
}

void Statistics::Write(char *fromWhere)
{
  using std::ofstream;
  ofstream statFile(fromWhere);

  statFile << rels.size() << endl;
  {
    std::map < std::string, RelationInformation >::iterator it;
    for (it = rels.begin(); it != rels.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }
  statFile << extantAttrs.size() << endl;
  {
    std::map < std::string, std::string>::iterator it;
    for (it = extantAttrs.begin(); it != extantAttrs.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }
  statFile << mergedRelations.size() << endl;
  {
    std::map < std::string, std::string>::const_iterator it;
    for (it = mergedRelations.begin(); it != mergedRelations.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }

  statFile.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  // This operation takes a bit of explanation. Internally within the
  // Statistics object, the various relations are partitioned into a set of
  // subsets or partitions, where each and every relation is contained within
  // exactly one subset (initially, each relation is in its very own singleton
  // subset). When two or more relations are within the same subset, it means
  // that they have been “joined” and they do not exist independently
  // anymore. The Apply operation uses the statistics stored by the Statistics
  // class to simulate a join of all of the relations listed in the relNames
  // parameter. This join is performed using the predicates listed in the
  // parameter parseTree.

  // Of course, the operation does not actually perform a join (actually
  // performing a join will be the job of the various relational operations),
  // but what it does is to figure out what might happen if all of the
  // relations listed in relNames were joined, in terms of what it would do to
  // the important statistics associated with the result of the join. To
  // figure this out, the Statistics object estimates the number of tuples
  // that would exist in the resulting relation, as well as the number of
  // distinct values for each attribute in the resulting relation. How exactly
  // it performs this estimation will be a topic of significant discussion in
  // class. After this estimation is performed, all of the relations in
  // relNames then become part of the same partition (or resulting joined
  // relation) and no longer exist on their own.

  // Note that there are a few constraints on the parameters that are input to
  // this function. For completeness, you should probably check for violations
  // of these constraints, because when you write your optimizer using the
  // Statistics class, it will be very useful to have good error checking.

  // First, parseTree can only list attributes that actually belong to the
  // relations named in relNames. If any other attributes are listed, then you
  // should probably catch this, print out an error message, and exit the
  // program.
  //
  // For this, look up the numToJoin relNames in our internal relation objects
  //           look up all the attributes in parseTree.
  // now make sure that all the attributes are present in the internal relation object.

  CheckRelations(relNames, numToJoin);
  std::vector <std::string> attrs = CheckParseTree(parseTree);
  // if we actually return, the parse tree is good
  std::clog << "*** GOOD PARSE TREE!!! ***" << std::endl;
  double estimate = 0;
  if (0 == parseTree and 2 >= numToJoin)
    {
      double accumulator = 1.0l;
      for (unsigned i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          accumulator *= rels[rel].NumTuples();
        }
      estimate = accumulator;
    }
  else
    {
      estimate = CalculateEstimate(parseTree);
    }
  // make new name for joined relation.
  string newRelation;
  if(HasJoin(parseTree))
    {
      for (unsigned i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          newRelation += rel;
        }
      clog << "new relation is " << newRelation << endl;
      // new map, to have both relations merged into it.
      RelationInformation merged(estimate); // new relation with estimated

      for (int i = 0; i < numToJoin ; i++)
        {
          merged.CopyAtts(rels[relNames[i]]);
        }
      clog << "printing merged relations " << endl << endl;
      merged.print();
      rels[newRelation] = merged;

      { // get rid of information about old relations
      std::vector<std::string> attrsInParseTree  = CheckParseTree(parseTree);
      // create a set of old relations to steal attributes from.
      std::set<string> oldRels;
      for(vector<string>::iterator it = attrsInParseTree.begin(); it < attrsInParseTree.end(); ++it)
        {
          oldRels.insert(extantAttrs[(*it)]);
        }

      for(std::set<string>::const_iterator it = oldRels.begin(); it != oldRels.end(); ++it)
        {
          merged.CopyAtts(rels[*it]);
          rels.erase((*it));
        }
      for (int i = 0; i < numToJoin ; i++)
        {
          rels.erase(relNames[i]); //
          mergedRelations[relNames[i]] = newRelation;
        }
      }

      std::map<std::string, tupleCount> mergedAtts = merged.GetAtts();

      std::map < std::string, tupleCount>::const_iterator it;
      for (it = mergedAtts.begin(); it != mergedAtts.end(); it++ )
        {
          extantAttrs[(*it).first] = newRelation;
          clog << (*it).first << "now belongs to " << newRelation << endl;
        }
    }


  // Second, the relations in relNames must contain exactly the set of
  // relations in one or more of the current partitions in the Statistics
  // object. In other words, the join specified by the set of relations in
  // relNames must make sense. For example, imagine that there are five
  // relations: A, B, C, D, and E, and the three current subsets maintained by
  // the Statistics objects are {A, B}, {C, D}, and {E} (meaning that A and B
  // have been joined, and C and D have been joined, and E is still by
  // itself). In this case, it makes no sense if relNames contains {A, B, C},
  // because this set contains a subset of one of the existing joins. However,
  // relNames could contain {A, B, C, D}, or it could contain {A, B, E}, or it
  // could contain {C, D, E}, or it could contain {A, B}, or any similar
  // mixture of the current partitions. These are all valid, because they
  // contain exactly those relations in one or more of the current
  // partitions. Note that if it just contained {A, B}, then effectively we
  // are simulating a selection.

  // Also note that if parseTree is empty (that is, null), then it is assumed
  // that there is no selection predicate; this either has no effect on the
  // Statistics object (in the case where relNames gives exactly those
  // relations in an existing partition) or else it specifies a pure cross
  // product in the case that relNames combines two or more partitions.

  // Finally, note that you will never be asked to write or to read from disk
  // a Statistics object for which Apply has been called. That is, you will
  // always write or read an object having only singleton relations.
  clog << endl << endl << "****************" << estimate << "******************" << endl << endl;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
  if (0 == parseTree and 2 >= numToJoin)
    {
      double accumulator = 1.0l;
      for (unsigned i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          accumulator *= rels[rel].NumTuples();
        }
      return accumulator;
    }

  using std::clog; using std::cout; using std::endl;
  CheckRelations(relNames, numToJoin);
  std::vector <std::string> attrs = CheckParseTree(parseTree);
  // if we actually return, the parse tree is good
  std::clog << "*** GOOD PARSE TREE!!! ***" << std::endl;

  clog << attrs.size() << endl;

  for (unsigned int i = 0; i < attrs.size(); i++)
    {
      clog << attrs[i] << endl;
    }

  // new map, to have both relations merged into it.
  // RelationInformation merged;
  // for (int i = 0; i < numToJoin ; i++)
  //   {
  //     merged.CopyAtts(rels[relNames[i]]);
  //   }
  // merged.print();

  // check if the numdistincts in relation information is different
  // from the number of tuples in relations

  double result = CalculateEstimate(parseTree);
  clog << "estimated result is " << result << endl;
  return result;
}

void Statistics :: Check (struct AndList *parseTree, char *relNames[], int numToJoin)
{
  CheckRelations(relNames, numToJoin);
  CheckParseTree(parseTree);
  // if we actually return, the parse tree is good
  std::clog << "*** GOOD PARSE TREE!!! ***" << std::endl;
  return;
}

void Statistics :: CheckRelations(char *relNames[], int numToJoin)
{
  using std::string;
  using std::clog;
  using std::endl;

  for (int i = 0; i < numToJoin; i++)
    {
      string rel(relNames[i]);
      // try for a singleton relation &
      // try for a merged relation
      clog << "looking for rel " << rel << endl;
      clog << "single rel count" << rels.count(rel) << endl;
      clog << "merged rel count" << mergedRelations.count(rel) << endl;
      if (0 == rels.count(rel) and 0 == mergedRelations.count(rel))
        {
          clog << "relation " << rel << " not found in internal relation tracker" << endl;
          exit(-1);
        }
    }
  clog << "found all relations, .... now checking for all attrs in parsetree" << endl;
}

// returns a vector of the attrs
std::vector<std::string> Statistics :: CheckParseTree(struct AndList *pAnd)
{
  std::vector < std::string > attrs;
  attrs.reserve(100);
  while (pAnd)
    {
      struct OrList *pOr = pAnd->left;
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL && (4 == pOperand->code))
                  {
                    // check left operand
                    std::string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        std::cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        exit(-1);
                      }
                    attrs.push_back(attr);
                  }
              }
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL && (4 == pOperand->code))
                  {
                    // check right operand
                    std::string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        std::cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        exit(-1);
                      }
                    attrs.push_back(attr);
                  }
              }
            }
          pOr = pOr->rightOr;
        }
      pAnd = pAnd->rightAnd;
    }
  return attrs; // return by copy
}

double Statistics :: CalculateEstimate(AndList *pAnd)
{
  double result = 1.0l;
  bool seenJoin = false;
  double selectOnlySize = 0.0l;
  while (pAnd)
    {
      OrList *pOr = pAnd->left;
      bool independentORs = true; // assume independence
      bool singleOR = false;
      clog << "singleOr is " << singleOR << endl;
      { // but check
        std::set <string> ors;
        unsigned count = 0;
        while (pOr) // traverse with counter.
          {
            ComparisonOp *pCom = pOr->left;
            if (pCom!=NULL)
              {
                clog << count;
                count++;
                string attr(pOr->left->left->value);
                clog << "orattr is " << attr << endl;
                clog << "or.size is " << ors.size() << endl;
                ors.insert(attr);
              }
            pOr = pOr->rightOr;
          }
        if (ors.size() != count)
          {independentORs = false;}
        if (1 == count)
          {independentORs = false; clog << "singleOr is " << singleOR << endl; singleOR = true; clog << "singleOr is " << singleOR << endl; clog << "THERE IS A SINGLE OR" << endl; clog << "singleOr is " << singleOR << endl;}
        clog << " ors are ";
        if(independentORs)
          clog << "independent" << endl;
        else
          clog << "dependent" << endl;
      }
      clog << "singleOr is " << singleOR << endl;
      pOr = pAnd->left; // reset pointer
      double tempOrValue = 0.0l; // each or is calculated separately, and then multiplied in at the end.
      if(independentORs)
        {tempOrValue = 1.0l;}
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              // pcom has left and right, as well as the operand that
              // details what it is an equality signals either a join
              // or a selection, geq (>=) or leq (<=) are both
              // selections (or really dumb joins that I am not going
              // to cover)
              Operand *lOperand = pCom->left;
              Operand *rOperand = pCom->right;
              switch(pCom->code)
                {
                case EQUALS: // maybe selection or maybe join
                  {
                    if ((0 != lOperand and (4 == lOperand->code)) and
                        (0 != rOperand and (4 == rOperand->code)))
                      {// this is a join, because both the left and right are attribute names
                        clog << endl << "join case estimation" << endl << endl;
                        seenJoin = true;
                        string const lattr(lOperand->value);
                        string const rattr(rOperand->value);
                        // look up which relation l attr is in
                        string const lrel = extantAttrs[lattr];
                        // get size of l relation
                        tupleCount const lRelSize = rels[lrel].NumTuples();
                        // get number of Distinct values of L attr
                        int const lDistinct = rels[lrel].GetDistinct(lattr);
                        // look up which relation r attr is in
                        string const rrel = extantAttrs[rattr];
                        // get size of r relation
                        tupleCount const rRelSize = rels[rrel].NumTuples();
                        // get number of Distinct values of R attr
                        int const rDistinct = rels[rrel].GetDistinct(rattr);

                        clog << "lr = " << lRelSize << " rr = " << rRelSize << endl;
                        clog << "product is " << ((double)lRelSize * (double)rRelSize) << endl;

                        double numerator   = lRelSize * rRelSize;
                        double denominator = std::max(lDistinct,rDistinct);

                        clog << "lattr of " << lattr << " with " << lDistinct <<" distinct values is "
                             << "found in rel " << lrel << " of size " << lRelSize << endl;
                        clog << "rattr of " << rattr << " with " << rDistinct <<" distinct values is "
                             << "Found in rel " << rrel << " of size " << rRelSize << endl;
                        tempOrValue += (numerator/denominator);
                        clog << "numerator is " << numerator
                             << " denominator is " << denominator
                             << " with final result of " << tempOrValue << endl << endl;
                      }
                    else
                      { // this is a selection // maybe fall through?
                        clog << endl <<  "*** EQUALITY SELECTION" << endl;
                        Operand *opnd = 0;
                        Operand *constant = 0;
                        if (4 == lOperand->code)
                          {opnd = lOperand; constant = rOperand; }
                        else if (4 == rOperand->code)
                          {opnd = rOperand; constant = lOperand;}
                        assert(0 != opnd); // something was assigned
                        assert(0 != constant); // something was assigned

                        string const attr(opnd->value);
                        string const relation = extantAttrs[attr];
                        tupleCount const relationSize = rels[relation].NumTuples();
                        tupleCount const distinct = rels[relation].GetDistinct(attr);
                        double const numerator   = relationSize;
                        double const denominator = distinct;
                        clog << "singleOr is " << singleOR << endl;
                        if (singleOR)
                          {
                            double const calculation = (1.0l/distinct);// (numerator/denominator);

                            clog << "single value is " << calculation << endl;
                            tempOrValue += calculation;
                          }
                        else
                          {
                            if(independentORs) // independent ORs
                              {
                                double const calculation = (1.0l - (1.0l/distinct));
                                clog << "indep, value is " << calculation << endl;
                                tempOrValue *= calculation;
                              }
                            else // dependent ORs
                              {
                                // else
                                {
                                  double const calculation = (1.0l/distinct);
                                  clog << "dep, value is " << calculation << endl;
                                  tempOrValue += calculation;
                                }
                              }
                          }
                        clog <<  "*** EQUALITY SELECTION end with result " << endl << endl;
                      }
                    break;
                  }
                case LESS_THAN: // selection
                  //break;
                case GREATER_THAN: // selection
                  // break;
                  clog << "not equal selection fall through" << endl;
                  // we are in a selection now.
                  // so either of our operands could be a literal value rather than an attribute
                  Operand *opnd = 0;
                  Operand *constant = 0;
                  if (4 == lOperand->code)
                    {opnd = lOperand; constant = rOperand; }
                  else if (4 == rOperand->code)
                    {opnd = rOperand; constant = lOperand;}
                  assert(0 != opnd); // something was assigned
                  assert(0 != constant); // something was assigned

                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  tupleCount const relationSize = rels[relation].NumTuples();
                  tupleCount const distinct = rels[relation].GetDistinct(attr);

                  if(independentORs) // independent ORs
                    {
                      double const calculation = 1.0l - (1.0l)/(3.0l);;
                      clog << "indep, value is " << calculation << endl;
                      tempOrValue *= calculation;
                    }
                  else // dependent ORs
                    {
                      double const calculation = (1.0l)/(3.0l);
                      clog << "dep, value is " << calculation << endl;
                      tempOrValue += calculation;
                    }
                  break;
                }
              if (!seenJoin)
                {
                  Operand *opnd = 0;
                  if (4 == lOperand->code)
                    {opnd = lOperand;}
                  else if (4 == rOperand->code)
                    {opnd = rOperand;}
                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  tupleCount const relationSize = rels[relation].NumTuples();
                  selectOnlySize = relationSize;
                }
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL and (4 == pOperand->code))
                  {
                    // check left operand
                    std::string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        std::cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        exit(-1);
                      }
                  }
              }
              // operator
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL and (4 == pOperand->code))
                  {
                    // check right operand
                    std::string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        std::cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        exit(-1);
                      }
                  }
              }
            }
          pOr = pOr->rightOr; // go to next or
        }
      clog << "putting ors into and estimate" << endl;
      if (independentORs)
        {
          clog << "independent ors" << endl;
          clog << "before, result was " << result << endl;
          result *= (1 - tempOrValue);
          clog << "after, result was " << result << endl;
        }
      else
        {
          clog << "dependent ors" << endl;
          clog << "before, result was " << result << endl;
          result *= tempOrValue;
          clog << "after, result was " << result << endl;
        }
      pAnd = pAnd->rightAnd; // go to next and
    }
  if (!seenJoin)
    {
      result *= selectOnlySize;
    }
  return result;
}

bool Statistics :: HasJoin(AndList *pAnd)
{
  double result = 1.0l;
  bool seenJoin = false;
  double selectOnlySize = 0.0l;
  while (pAnd)
    {
      OrList *pOr = pAnd->left;
      bool independentORs = true; // assume independence
      bool singleOR = false;
      clog << "singleOr is " << singleOR << endl;
      { // but check
        std::set <string> ors;
        unsigned count = 0;
        while (pOr) // traverse with counter.
          {
            ComparisonOp *pCom = pOr->left;
            if (pCom!=NULL)
              {
                clog << count;
                count++;
                string attr(pOr->left->left->value);
                clog << "orattr is " << attr << endl;
                clog << "or.size is " << ors.size() << endl;
                ors.insert(attr);
              }
            pOr = pOr->rightOr;
          }
        if (ors.size() != count)
          {independentORs = false;}
        if (1 == count)
          {independentORs = false; clog << "singleOr is " << singleOR << endl; singleOR = true; clog << "singleOr is " << singleOR << endl; clog << "THERE IS A SINGLE OR" << endl; clog << "singleOr is " << singleOR << endl;}
        clog << " ors are ";
        if(independentORs)
          clog << "independent" << endl;
        else
          clog << "dependent" << endl;
      }
      clog << "singleOr is " << singleOR << endl;
      pOr = pAnd->left; // reset pointer
      double tempOrValue = 0.0l; // each or is calculated separately, and then multiplied in at the end.
      if(independentORs)
        {tempOrValue = 1.0l;}
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              // pcom has left and right, as well as the operand that
              // details what it is an equality signals either a join
              // or a selection, geq (>=) or leq (<=) are both
              // selections (or really dumb joins that I am not going
              // to cover)
              Operand *lOperand = pCom->left;
              Operand *rOperand = pCom->right;
              switch(pCom->code)
                {
                case EQUALS: // maybe selection or maybe join
                  {
                    if ((0 != lOperand and (4 == lOperand->code)) and
                        (0 != rOperand and (4 == rOperand->code)))
                      {// this is a join, because both the left and right are attribute names
                        clog << endl << "join case estimation" << endl << endl;
                        seenJoin = true;
                        string const lattr(lOperand->value);
                        string const rattr(rOperand->value);
                        // look up which relation l attr is in
                        string const lrel = extantAttrs[lattr];
                        // get size of l relation
                        tupleCount const lRelSize = rels[lrel].NumTuples();
                        // get number of Distinct values of L attr
                        int const lDistinct = rels[lrel].GetDistinct(lattr);
                        // look up which relation r attr is in
                        string const rrel = extantAttrs[rattr];
                        // get size of r relation
                        tupleCount const rRelSize = rels[rrel].NumTuples();
                        // get number of Distinct values of R attr
                        int const rDistinct = rels[rrel].GetDistinct(rattr);

                        clog << "lr = " << lRelSize << " rr = " << rRelSize << endl;
                        clog << "product is " << ((double)lRelSize * (double)rRelSize) << endl;

                        double numerator   = lRelSize * rRelSize;
                        double denominator = std::max(lDistinct,rDistinct);

                        clog << "lattr of " << lattr << " with " << lDistinct <<" distinct values is "
                             << "found in rel " << lrel << " of size " << lRelSize << endl;
                        clog << "rattr of " << rattr << " with " << rDistinct <<" distinct values is "
                             << "Found in rel " << rrel << " of size " << rRelSize << endl;
                        tempOrValue += (numerator/denominator);
                        clog << "numerator is " << numerator
                             << " denominator is " << denominator
                             << " with final result of " << tempOrValue << endl << endl;
                      }
                    else
                      { // this is a selection // maybe fall through?
                        clog << endl <<  "*** EQUALITY SELECTION" << endl;
                        Operand *opnd = 0;
                        Operand *constant = 0;
                        if (4 == lOperand->code)
                          {opnd = lOperand; constant = rOperand; }
                        else if (4 == rOperand->code)
                          {opnd = rOperand; constant = lOperand;}
                        assert(0 != opnd); // something was assigned
                        assert(0 != constant); // something was assigned

                        string const attr(opnd->value);
                        string const relation = extantAttrs[attr];
                        tupleCount const relationSize = rels[relation].NumTuples();
                        tupleCount const distinct = rels[relation].GetDistinct(attr);
                        double const numerator   = relationSize;
                        double const denominator = distinct;
                        clog << "singleOr is " << singleOR << endl;
                        if (singleOR)
                          {
                            double const calculation = (1.0l/distinct);// (numerator/denominator);

                            clog << "single value is " << calculation << endl;
                            tempOrValue += calculation;
                          }
                        else
                          {
                            if(independentORs) // independent ORs
                              {
                                double const calculation = (1.0l - (1.0l/distinct));
                                clog << "indep, value is " << calculation << endl;
                                tempOrValue *= calculation;
                              }
                            else // dependent ORs
                              {
                                // else
                                {
                                  double const calculation = (1.0l/distinct);
                                  clog << "dep, value is " << calculation << endl;
                                  tempOrValue += calculation;
                                }
                              }
                          }
                        clog <<  "*** EQUALITY SELECTION end with result " << endl << endl;
                      }
                    break;
                  }
                case LESS_THAN: // selection
                  //break;
                case GREATER_THAN: // selection
                  // break;
                  clog << "not equal selection fall through" << endl;
                  // we are in a selection now.
                  // so either of our operands could be a literal value rather than an attribute
                  Operand *opnd = 0;
                  Operand *constant = 0;
                  if (4 == lOperand->code)
                    {opnd = lOperand; constant = rOperand; }
                  else if (4 == rOperand->code)
                    {opnd = rOperand; constant = lOperand;}
                  assert(0 != opnd); // something was assigned
                  assert(0 != constant); // something was assigned

                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  tupleCount const relationSize = rels[relation].NumTuples();
                  tupleCount const distinct = rels[relation].GetDistinct(attr);

                  if(independentORs) // independent ORs
                    {
                      double const calculation = 1.0l - (1.0l)/(3.0l);;
                      clog << "indep, value is " << calculation << endl;
                      tempOrValue *= calculation;
                    }
                  else // dependent ORs
                    {
                      double const calculation = (1.0l)/(3.0l);
                      clog << "dep, value is " << calculation << endl;
                      tempOrValue += calculation;
                    }
                  break;
                }
              if (!seenJoin)
                {
                  Operand *opnd = 0;
                  if (4 == lOperand->code)
                    {opnd = lOperand;}
                  else if (4 == rOperand->code)
                    {opnd = rOperand;}
                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  tupleCount const relationSize = rels[relation].NumTuples();
                  selectOnlySize = relationSize;
                }
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL and (4 == pOperand->code))
                  {
                    // check left operand
                    std::string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        std::cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        exit(-1);
                      }
                  }
              }
              // operator
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL and (4 == pOperand->code))
                  {
                    // check right operand
                    std::string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        std::cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        exit(-1);
                      }
                  }
              }
            }
          pOr = pOr->rightOr; // go to next or
        }
      clog << "putting ors into and estimate" << endl;
      if (independentORs)
        {
          clog << "independent ors" << endl;
          clog << "before, result was " << result << endl;
          result *= (1 - tempOrValue);
          clog << "after, result was " << result << endl;
        }
      else
        {
          clog << "dependent ors" << endl;
          clog << "before, result was " << result << endl;
          result *= tempOrValue;
          clog << "after, result was " << result << endl;
        }
      pAnd = pAnd->rightAnd; // go to next and
    }
  if (!seenJoin)
    {
      result *= selectOnlySize;
    }
  return seenJoin;
}
