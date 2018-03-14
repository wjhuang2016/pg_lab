#ifndef DEPUTY_H
#define DEPUTY_H

#include "catalog/pg_bipointer.h"
#include "catalog/pg_switching.h"
#include "catalog/pg_deputy.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "parser/parse_node.h"

extern void InsertPgDeputyTuple(Relation pg_deputy_rel,
					Form_pg_deputy new_deputy,
					CatalogIndexState indstate);

extern void InsertPgBipointerTuple(Relation pg_bipointer_rel,
					   Form_pg_bipointer new_bipointer,
					   CatalogIndexState indstate);

extern void InsertPgSwitchingTuple(Relation pg_switching_rel,
					   Form_pg_switching new_switching,
					   CatalogIndexState indstate);

extern ObjectAddress DefineSelectDeputy(CreateClassStmt *stmt, Oid ownerId,
			   ObjectAddress *typaddress, const char *queryString);

#endif
