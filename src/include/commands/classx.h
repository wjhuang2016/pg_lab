#ifndef CLASSX_H
#define CLASSX_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress DefineClass(CreateClassStmt *stmt, Oid ownerId,
            ObjectAddress *typaddress, const char *queryString);

#endif