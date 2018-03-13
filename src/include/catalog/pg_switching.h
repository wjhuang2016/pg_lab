#ifndef PG_SWITCHING_H
#define PG_SWITCHING_H

#include "catalog/genbki.h"

#define SwitchingId 9003

CATALOG(pg_switching,9003) BKI_WITHOUT_OIDS
{
  Oid  DeputyClassOid; 
  int32 AttributeNumber;
  int32 ExpressionNumber;
  text Expression;
} FormData_pg_switching;

typedef FormData_pg_switching *Form_pg_switching;

#define Natts_pg_switching 4
#define Anum_pg_switching_DeputyClassOid 1
#define Anum_pg_switching_AttributeNumber 2
#define Anum_pg_switching_ExpressionNumber 3
#define Anum_pg_switching_Expression 4

#endif

