#ifndef PG_DEPUTYTYPE_H
#define PG_DEPUTYTYPE_H

#include "catalog/genbki.h"

#define DeputyTypeId 9002

CATALOG(pg_deputytype,9002) BKI_WITHOUT_OIDS
{
  NameData DeputyClassName;
  Oid   DeputyClassOid;
  text  DeputyQuery;
} FormData_pg_deputytype;

typedef FormData_pg_deputytype *Form_pg_deputytype;

#define Natts_pg_deputytype 3
#define Anum_pg_deputytype_DeputyClassName 1
#define Anum_pg_deputytype_DeputyClassOid 2
#define Anum_pg_deputytype_DeputyQuery 3

#endif
