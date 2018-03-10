#ifndef CLASSX_H
#define CLASSX_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern void InsertPgBipointerTuple(Relation pg_bipointer_rel,
					   Form_pg_bipointer new_bipointer,
					   CatalogIndexState indstate);

extern static void AddNewBipointerTuples(Form_pg_bipointer attr);

extern void InsertPgSwitchingTuple(Relation pg_switching_rel,
					   Form_pg_switching new_switching,
					   CatalogIndexState indstate);

extern static void AddNewSwitchingTuples(Form_pg_switching attr);

extern ObjectAddress DefineClass(CreateClassStmt *stmt, Oid ownerId,
            ObjectAddress *typaddress, const char *queryString);

#endif