#include "Statistics.h"
#include <cassert>
#include <string>
#include <iostream>
#include <cstdlib>
#include <vector>
#include <algorithm>

using std::string;
using std::vector;
using std::clog; using std::endl;

Statistics::Statistics() : relations(), relationInformation(), rels(), extantAttrs()
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
  const RelationInformation newRelation (numTuples);
  relations[s] = numTuples;
  rels[s] = newRelation;
  // rels.insert( pair<string,RelationInformation>(s,newRelation));
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
      relationInformation[rel] = make_pair(att, relations[rel]);
      rels[rel].AddAtt(att,rels[rel].NumTuples());
      extantAttrs.insert(make_pair(att,rel));
    }
  else
    {
      relationInformation[rel] = make_pair(att, numDistincts);
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
  assert(0);
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
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
  RelationInformation merged;
  for (int i = 0; i < numToJoin ; i++)
    {
      merged.CopyAtts(rels[relNames[i]]);
    }
  merged.print();

  // check if the numdistincts in relation information is different
  // from the number of tuples in relations

  double result = CalculateEstimate(parseTree);
  clog << "estimated result is " << result << endl;
  assert("estimate kill");
  return 0.0l;
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
      if (0 == rels.count(rel))
        {
          clog << "relation " << rel << " not found in internal relation tracker" << endl;
          exit(-1);
        }
    }
  clog << "found all relations, .... now checking for all attrs in parsetree" << endl;
}

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

double Statistics :: CalculateEstimate(struct AndList *pAnd)
{
  double result = 0.0l;
  while (pAnd)
    {
      struct OrList *pOr = pAnd->left;
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
              switch(pCom->code)
                {
                case EQUALS: // maybe selection or maybe join
                  {
                    struct Operand *lOperand = pCom->left;
                    struct Operand *rOperand = pCom->right;
                    if ((lOperand!=NULL and (4 == lOperand->code)) and (rOperand!=NULL and (4 == rOperand->code)))
                      {// this is a join, because both the left and right are attribute names
                        clog << endl << "join case estimation" << endl << endl;
                        string const lattr(lOperand->value);
                        string const rattr(rOperand->value);
                        // look up which relation l attr is in
                        string const lrel = extantAttrs[lattr];
                        // get size of l relation
                        unsigned long long const lRelSize = rels[lrel].NumTuples();
                        // get number of Distinct values of L attr
                        int const lDistinct = rels[lrel].GetDistinct(lattr);
                        // look up which relation r attr is in
                        string const rrel = extantAttrs[rattr];
                        // get size of r relation
                        unsigned long long const rRelSize = rels[rrel].NumTuples();
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
                        result += (numerator/denominator);
                        clog << "numerator is " << numerator << " denominator is " << denominator << " with final result of " << result << endl << endl;
                        break;
                      }
                    else
                      {
                        // this is a selection // maybe fall through?
                      }
                  }
                case LESS_THAN: // selection
                  //break;
                case GREATER_THAN: // selection
                  // break;
                  clog << "selection fall through" << endl;
                  // we be in a selection now.
                  break;
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
          pOr = pOr->rightOr;
        }
      pAnd = pAnd->rightAnd;
    }
  return result;
}
