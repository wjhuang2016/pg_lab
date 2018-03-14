#ifndef PG_BIPOINTER_H
#define PG_BIPOINTER_H

#include "catalog/genbki.h"

#define BipointerId 9001

CATALOG(pg_bipointer,9001) BKI_WITHOUT_OIDS
{
  Oid   DeputyClassOid;
  Oid   DeputyObjectOid;
  Oid   SourceClassOid;
  Oid   SourceObjectOid;
} FormData_pg_bipointer;

typedef FormData_pg_bipointer *Form_pg_bipointer;

#define Natts_pg_bipointer 4
#define Anum_pg_bipointer_DeputyClassOid 1
#define Anum_pg_bipointer_DeputyObjectOid 2
#define Anum_pg_bipointer_SourceClassOid 3
#define Anum_pg_bipointer_SourceObjectOid 4

#endif
