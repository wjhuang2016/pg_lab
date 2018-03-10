//
// Created by ShadowMov on 2017/12/8.
//

#include "postgres.h"
#include "commands/tablecmds.h"
#include "commands/classx.h"
#include "catalog/pg_class.h"

ObjectAddress
DefineClass(CreateClassStmt *stmt, Oid ownerId,
            ObjectAddress *typaddress, const char *queryString)
{
    return DefineRelation((CreateStmt *) (stmt->createstmt), RELKIND_CLASSX ,ownerId, typaddress, queryString);
}