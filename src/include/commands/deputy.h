#ifndef DEPUTY_H
#define DEPUTY_H

#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "parser/parse_node.h"

extern void InsertPgDeputyTuple(Relation pg_deputy_rel,
					Form_pg_deputy new_deputy,
					CatalogIndexState indstate);

extern void AddNewPgDeputyTuple(Oid class_oid, Oid deputy_oid);

extern ObjectAddress DefineSelectDeputy(CreateClassStmt *stmt, Oid ownerId,
			   ObjectAddress *typaddress, const char *queryString);

#endif
