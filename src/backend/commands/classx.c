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
    CreateStmt *createStmt;
    createStmt = (CreateStmt *)palloc0(sizeof(CreateStmt));
    createStmt->relation = stmt->classname;
    createStmt->tableElts = stmt->attrs;
    
    return DefineRelation(createStmt, RELKIND_CLASSX ,ownerId, typaddress, queryString);
}