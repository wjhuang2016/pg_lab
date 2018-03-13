#ifndef PG_MAPPING_H
#define PG_MAPPING_H

#include "catalog/genbki.h"

#define MappingId 9004

CATALOG(pg_mapping,9004) BKI_WITHOUT_OIDS
{
  Oid DeputyClassOid; 
  Oid DeputyObjectOid;
  int8 Address;
} FormData_pg_mapping;

typedef FormData_pg_mapping *Form_pg_mapping;

#define Natts_pg_mapping 3
#define Anum_pg_mapping_DeputyClassOid 1
#define Anum_pg_mapping_DeputyObjectOid 2
#define Anum_pg_mapping_Address 3

#endif

