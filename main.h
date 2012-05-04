#include "HiResTimer.h"
#include "BigQ.h"
#include "RelOp.h"
#include <pthread.h>

static const int pAtts = 9;
static const int psAtts = 5;
static const int liAtts = 16;
static const int oAtts = 9;
static const int sAtts = 7;
static const int cAtts = 8;
static const int nAtts = 4;
static const int rAtts = 3;

static const int pipesz = 100; // buffer sz allowed for each pipe
static const int buffsz = 100; // pages of memory allowed for operations

static const Attribute IA = {"int", Int};
static const Attribute SA = {"string", String};
static const Attribute DA = {"double", Double};

SelectFile SF_ps, SF_p,  SF_s,  SF_o,  SF_li,  SF_c,  SF_r,  SF_n;
DBFile    dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c, dbf_r, dbf_n;
