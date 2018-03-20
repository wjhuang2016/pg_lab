/* ----------------------------------------------------------------
 *		function for deputy class
 *				Creates a new deputy class.
 * 
 *      Created by ejq, edited by ShadowMov
 *
 * stmt carries parsetree information from an ordinary CREATE SELECTDEPUTYCLASSX statement.
 * The other arguments are used to extend the behavior for other cases:
 * ownerId: if not InvalidOid, use this as the new relation's owner.
 * typaddress: if not null, it's set to the pg_type entry's address.
 *
 * Note that permissions checks are done against current user regardless of
 * ownerId.  A nonzero ownerId is used when someone is creating a relation
 * "on behalf of" someone else, so we still want to see that the current user
 * has permissions to do it.
 *
 * If successful, returns the address of the new relation.
 * ----------------------------------------------------------------
 */
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tupconvert.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/partition.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "catalog/toasting.h"
#include "commands/cluster.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/policy.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/predtest.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"
#include "pgstat.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"

#include "commands/deputy.h"
#include "catalog/pg_deputy.h"

/*
 * InsertPgBipointerTuple
 */
void
InsertPgBipointerTuple(Relation pg_bipointer_rel,
					   Form_pg_bipointer new_bipointer,
					   CatalogIndexState indstate)
{
	Datum		values[Natts_pg_bipointer];
	bool		nulls[Natts_pg_bipointer];
	HeapTuple	tup;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_bipointer_DeputyClassOid - 1] = ObjectIdGetDatum(new_bipointer->DeputyClassOid);
	values[Anum_pg_bipointer_DeputyObjectOid - 1] = ObjectIdGetDatum(new_bipointer->DeputyObjectOid);
	values[Anum_pg_bipointer_SourceClassOid - 1] = ObjectIdGetDatum(new_bipointer->SourceClassOid);
	values[Anum_pg_bipointer_SourceObjectOid - 1] = ObjectIdGetDatum(new_bipointer->SourceObjectOid);	

	tup = heap_form_tuple(RelationGetDescr(pg_bipointer_rel), values, nulls);

	/* finally insert the new tuple, update the indexes, and clean up */
	if (indstate != NULL)
		CatalogTupleInsertWithInfo(pg_bipointer_rel, tup, indstate);
	else
		CatalogTupleInsert(pg_bipointer_rel, tup);

	heap_freetuple(tup);

}

static void
AddNewBipointerTuples(Form_pg_bipointer attr)
{
	Relation	rel;
	CatalogIndexState indstate;

	rel = heap_open(BipointerId, RowExclusiveLock);
	indstate = CatalogOpenIndexes(rel);
	InsertPgBipointerTuple(rel, attr, indstate);
	CatalogCloseIndexes(indstate);
	heap_close(rel, RowExclusiveLock);
}

/*
 * InsertPgSwitchingTuple
 */
void
InsertPgSwitchingTuple(Relation pg_switching_rel,
					   Form_pg_switching new_switching,
					   CatalogIndexState indstate)
{
	Datum		values[Natts_pg_switching];
	bool		nulls[Natts_pg_switching];
	HeapTuple	tup;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_switching_DeputyClassOid - 1] = ObjectIdGetDatum(new_switching->DeputyClassOid);
	values[Anum_pg_switching_AttributeNumber - 1] = Int32GetDatum(new_switching->AttributeNumber);
	values[Anum_pg_switching_ExpressionNumber - 1] = Int32GetDatum(new_switching->ExpressionNumber);
        values[Anum_pg_switching_Expression - 1] = CStringGetTextDatum(&new_switching->Expression);

	tup = heap_form_tuple(RelationGetDescr(pg_switching_rel), values, nulls);

	/* finally insert the new tuple, update the indexes, and clean up */
	if (indstate != NULL)
		CatalogTupleInsertWithInfo(pg_switching_rel, tup, indstate);
	else
		CatalogTupleInsert(pg_switching_rel, tup);

	heap_freetuple(tup);

}


static void
AddNewSwitchingTuples(Form_pg_switching attr)
{
	Relation	rel;
	CatalogIndexState indstate;

	rel = heap_open(SwitchingId, RowExclusiveLock);
	indstate = CatalogOpenIndexes(rel);
	InsertPgSwitchingTuple(rel, attr, indstate);
	CatalogCloseIndexes(indstate);
	heap_close(rel, RowExclusiveLock);
}

void
InsertPgDeputyTuple(Relation pg_deputy_rel,
					Form_pg_deputy new_deputy,
					CatalogIndexState indstate)
{
	Datum		values[Natts_pg_deputy];
	bool		nulls[Natts_pg_deputy];
	HeapTuple	tup;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_deputy_DeputyClassOid-1] = ObjectIdGetDatum(new_deputy->DeputyClassOid);
	values[Anum_pg_deputy_SourceClassOid-1] = ObjectIdGetDatum(new_deputy->SourceClassOid);

	tup = heap_form_tuple(RelationGetDescr(pg_deputy_rel), values, nulls);

	/* finally insert the new tuple, update the indexes, and clean up */
	if (indstate != NULL)
		CatalogTupleInsertWithInfo(pg_deputy_rel, tup, indstate);
	else
		CatalogTupleInsert(pg_deputy_rel, tup);

	heap_freetuple(tup);
}

void
AddNewPgDeputyTuple(Oid class_oid, Oid deputy_oid)
{
	Form_pg_deputy deputy;
	int			i;
	Relation	rel;
	CatalogIndexState indstate;
	ObjectAddress myself,
				referenced;

	/*
	 * open pg_deputy and its indexes.
	 */
	rel = heap_open(DeputyId, RowExclusiveLock);

	indstate = CatalogOpenIndexes(rel);

	deputy->DeputyClassOid = deputy_oid;
	deputy->SourceClassOid = class_oid;

	InsertPgDeputyTuple(rel, deputy, indstate);

	/*
	 * clean up
	 */
	CatalogCloseIndexes(indstate);

	heap_close(rel, RowExclusiveLock);
}

ObjectAddress
DefineSelectDeputy(CreateClassStmt *stmt, Oid ownerId,
			   ObjectAddress *typaddress, const char *queryString)
{
	char		relname[NAMEDATALEN];
	Oid			namespaceId;
	Oid			relationId;
	Oid			tablespaceId;
	Relation	rel;
	TupleDesc	descriptor;
	List	   *inheritOids;
	List	   *old_constraints;
	bool		localHasOids;
	int			parentOidCount;
	List	   *rawDefaults;
	List	   *cookedDefaults;
	Datum		reloptions;
	ListCell   *listptr;
	AttrNumber	attnum;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	Oid			ofTypeId;
        CreateStmt  *createStmt;
	ObjectAddress address;
        
        createStmt = (CreateStmt *)palloc0(sizeof(CreateStmt));
        createStmt->relation = stmt->classname;
        createStmt->tableElts = stmt->attrs;

	/*
	 * Truncate relname to appropriate length (probably a waste of time, as
	 * parser should have done this already).
	 */
	StrNCpy(relname, createStmt->relation->relname, NAMEDATALEN);

	/*
	 * Look up the namespace in which we are supposed to create the relation,
	 * check we have permission to create there, lock it against concurrent
	 * drop, and mark stmt->relation as RELPERSISTENCE_TEMP if a temporary
	 * namespace is selected.
	 */
	namespaceId =
		RangeVarGetAndCheckCreationNamespace(createStmt->relation, NoLock, NULL);

	/*
	 * Security check: disallow creating temp tables from security-restricted
	 * code.  This is needed because calling code might not expect untrusted
	 * tables to appear in pg_temp at the front of its search path.
	 */
	if (createStmt->relation->relpersistence == RELPERSISTENCE_TEMP
		&& InSecurityRestrictedOperation())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("cannot create temporary table within security-restricted operation")));

	/*
	 * Use default tablespace
	 */
        tablespaceId = GetDefaultTablespace(createStmt->relation->relpersistence);
        /* note InvalidOid is OK in this case */

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tablespaceId));
	}

	/* In all cases disallow placing user relations in pg_global */
	if (tablespaceId == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only shared relations can be placed in pg_global tablespace")));

	/* Identify user ID that will own the table */
	if (!OidIsValid(ownerId))
		ownerId = GetUserId();

	/*
	 * Parse and validate reloptions, if any.
	 */
	reloptions = transformRelOptions((Datum) 0, createStmt->options, NULL, validnsps,
									 true, false);

        ofTypeId = InvalidOid;

	/*
	 * Create a tuple descriptor from the relation schema.  Note that this
	 * deals with column names, types, and NOT NULL constraints, but not
	 * default values or CHECK constraints; we handle those below.
	 */
	descriptor = BuildDescForRelation(createStmt->tableElts);

	/*
	 * For deputy class force hasoid == true 
	 */
        localHasOids = true;
	descriptor->tdhasoid = true;

	/*
	 * Find columns with default values and prepare for insertion of the
	 * defaults.  Pre-cooked (that is, inherited) defaults go into a list of
	 * CookedConstraint structs that we'll pass to heap_create_with_catalog,
	 * while raw defaults go into a list of RawColumnDefault structs that will
	 * be processed by AddRelationNewConstraints.  (We can't deal with raw
	 * expressions until we can do transformExpr.)
	 *
	 * We can set the atthasdef flags now in the tuple descriptor; this just
	 * saves StoreAttrDefault from having to do an immediate update of the
	 * pg_attribute rows.
	 */
	rawDefaults = NIL;
	cookedDefaults = NIL;
	attnum = 0;

	foreach(listptr, createStmt->tableElts)
	{
		ColumnDef  *colDef = lfirst(listptr);

		attnum++;

		if (colDef->raw_default != NULL)
		{
			RawColumnDefault *rawEnt;

			Assert(colDef->cooked_default == NULL);

			rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
			rawEnt->attnum = attnum;
			rawEnt->raw_default = colDef->raw_default;
			rawDefaults = lappend(rawDefaults, rawEnt);
			descriptor->attrs[attnum - 1]->atthasdef = true;
		}
		else if (colDef->cooked_default != NULL)
		{
			CookedConstraint *cooked;

			cooked = (CookedConstraint *) palloc(sizeof(CookedConstraint));
			cooked->contype = CONSTR_DEFAULT;
			cooked->conoid = InvalidOid;	/* until created */
			cooked->name = NULL;
			cooked->attnum = attnum;
			cooked->expr = colDef->cooked_default;
			cooked->skip_validation = false;
			cooked->is_local = true;	/* not used for defaults */
			cooked->inhcount = 0;	/* ditto */
			cooked->is_no_inherit = false;
			cookedDefaults = lappend(cookedDefaults, cooked);
			descriptor->attrs[attnum - 1]->atthasdef = true;
		}

		if (colDef->identity)
			descriptor->attrs[attnum - 1]->attidentity = colDef->identity;
	}
	
	//TODO: Find deputy attribute and non-deputy attribute

	/*
	 * Create the relation.  Inherited defaults and constraints are passed in
	 * for immediate handling --- since they don't need parsing, they can be
	 * stored immediately.
	 */
	relationId = heap_create_with_catalog(relname,
										  namespaceId,
										  tablespaceId,
										  InvalidOid,
										  InvalidOid,
										  ofTypeId,
										  ownerId,
										  descriptor,
										  list_concat(cookedDefaults,
													  old_constraints),
										  RELKIND_CLASSX,
										  createStmt->relation->relpersistence,
										  false,
										  false,
										  localHasOids,
										  parentOidCount,
										  createStmt->oncommit,
										  reloptions,
										  true,
										  allowSystemTableMods,
										  false,
										  typaddress);

	/* Process deputy relations */
	//AddNewPgDeputyTuple(class_oid, relationId);

	/*
	 * We must bump the command counter to make the newly-created relation
	 * tuple visible for opening.
	 */
	CommandCounterIncrement();

	/*
	 * Open the new relation and acquire exclusive lock on it.  This isn't
	 * really necessary for locking out other backends (since they can't see
	 * the new rel anyway until we commit), but it keeps the lock manager from
	 * complaining about deadlock risks.
	 */
	rel = relation_open(relationId, AccessExclusiveLock);

	/*
	 * Now add any newly specified column default values and CHECK constraints
	 * to the new relation.  These are passed to us in the form of raw
	 * parsetrees; we need to transform them to executable expression trees
	 * before they can be added. The most convenient way to do that is to
	 * apply the parser's transformExpr routine, but transformExpr doesn't
	 * work unless we have a pre-existing relation. So, the transformation has
	 * to be postponed to this final step of CREATE TABLE.
	 */
	if (rawDefaults)
		AddRelationNewConstraints(rel, rawDefaults, createStmt->constraints,
								  true, true, false);

	ObjectAddressSet(address, RelationRelationId, relationId);

	/*
	 * Clean up.  We keep lock on new relation (although it shouldn't be
	 * visible to anyone else anyway, until commit).
	 */
	relation_close(rel, NoLock);

	return address;
}

/*
 * 这个函数用于在创建代理类之后，初始化代理对象
 * 实现方式是：遍历源类的所有对象，通过切换表达式检查源类对象是否满足条件，如果满足条件，将源对象的Oid插入到bipointer表中
 */
void InitSelectDeputy(Oid sourceClassOid, Oid deputyClassOid, Node* switchingExpr) {
	// Step. 1 从pg_mapping表中获取sourceClassOid所有的对象Oid
	// (这里应该是个list吧)
	// Step. 2 逐一检查取出的对象Oid是否满足switchingExpr
	//--> Step. 2.5 如果满足条件，则插入到pg_bipointer
}