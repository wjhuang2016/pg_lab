#ifndef PG_DEPUTY_H
#define PG_DEPUTY_H

#include "catalog/genbki.h"

#define DeputyId 9000

CATALOG(pg_deputy,9000) BKI_WITHOUT_OIDS
{
  Oid DeputyClassOid;
  Oid SourceClassOid;
} FormData_pg_deputy;

typedef FormData_pg_deputy *Form_pg_deputy;

#define Natts_pg_deputy 2
#define Anum_pg_deputy_DeputyClassOid 1
#define Anum_pg_deputy_SourceClassOid 2


#endif
