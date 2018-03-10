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

	values[Anum_pg_bipointer_sourceclassid - 1] = ObjectIdGetDatum(new_bipointer->sourceclassid);
	values[Anum_pg_bipointer_deputyclassid - 1] = NameGetDatum(&new_bipointer->deputyclassid);
	values[Anum_pg_bipointer_deputyobjid - 1] = ObjectIdGetDatum(new_bipointer->deputyobjid);
	values[Anum_pg_bipointer_sourceobjid - 1] = Int32GetDatum(new_bipointer->sourceobjid);	

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

/*
#define Natts_pg_switching					3
#define Anum_pg_switching_deputyClassName	1
#define Anum_pg_switching_deputyType		2
#define Anum_pg_switching_deputyQuery		3
*/

	values[Anum_pg_switching_deputyClassName - 1] = NameDataGetDatum(new_bipointer->deputyClassName);
	values[Anum_pg_switching_deputyType - 1] = CharDatum(&new_bipointer->deputyType);
	values[Anum_pg_switching_deputyQuery - 1] = TextGetDatum(new_bipointer->deputyQuery);

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

	values[Anum_pg_deputy_deputyseqno-1] = Int4GetDatum(new_deputy->deputyseqno);
	values[Anum_pg_deputy_deputyclassid-1] = ObjectIdGetDatum(new_deputy->deputyclassid);
	values[Anum_pg_deputy_sourceclassid-1] = ObjectIdGetDatum(new_deputy->sourceclassid);

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
	int			natts = tupdesc->natts;
	ObjectAddress myself,
				referenced;

	/*
	 * open pg_deputy and its indexes.
	 */
	rel = heap_open(DeputyId, RowExclusiveLock);

	indstate = CatalogOpenIndexes(rel);

	deputy->deputyclassid = deputy_oid;
	deputy->sourceclassid = class_oid;
	deputy->deputyseqno = 0;

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
	ObjectAddress address;

	/*
	 * Truncate relname to appropriate length (probably a waste of time, as
	 * parser should have done this already).
	 */
	StrNCpy(relname, stmt->relation->relname, NAMEDATALEN);

	/*
	 * Check consistency of arguments
	 */
	if (stmt->oncommit != ONCOMMIT_NOOP
		&& stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("ON COMMIT can only be used on temporary tables")));

	/*
	 * Look up the namespace in which we are supposed to create the relation,
	 * check we have permission to create there, lock it against concurrent
	 * drop, and mark stmt->relation as RELPERSISTENCE_TEMP if a temporary
	 * namespace is selected.
	 */
	namespaceId =
		RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock, NULL);

	/*
	 * Security check: disallow creating temp tables from security-restricted
	 * code.  This is needed because calling code might not expect untrusted
	 * tables to appear in pg_temp at the front of its search path.
	 */
	if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP
		&& InSecurityRestrictedOperation())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("cannot create temporary table within security-restricted operation")));

	/*
	 * Select tablespace to use.  If not specified, use default tablespace
	 * (which may in turn default to database's default).
	 */
	if (stmt->tablespacename)
	{
		tablespaceId = get_tablespace_oid(stmt->tablespacename, false);
	}
	else
	{
		tablespaceId = GetDefaultTablespace(stmt->relation->relpersistence);
		/* note InvalidOid is OK in this case */
	}

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
	reloptions = transformRelOptions((Datum) 0, stmt->options, NULL, validnsps,
									 true, false);

	if (stmt->ofTypename)
	{
		AclResult	aclresult;

		ofTypeId = typenameTypeId(NULL, stmt->ofTypename);

		aclresult = pg_type_aclcheck(ofTypeId, GetUserId(), ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error_type(aclresult, ofTypeId);
	}
	else
		ofTypeId = InvalidOid;

	/*
	 * Look up inheritance ancestors and generate relation schema, including
	 * inherited attributes.  (Note that stmt->tableElts is destructively
	 * modified by MergeAttributes.)
	 */
	stmt->tableElts =
		MergeAttributes(stmt->tableElts, stmt->inhRelations,
						stmt->relation->relpersistence,
						stmt->partbound != NULL,
						&inheritOids, &old_constraints, &parentOidCount);

	/*
	 * Create a tuple descriptor from the relation schema.  Note that this
	 * deals with column names, types, and NOT NULL constraints, but not
	 * default values or CHECK constraints; we handle those below.
	 */
	descriptor = BuildDescForRelation(stmt->tableElts);

	/*
	 * Notice that we allow OIDs here only for plain tables and partitioned
	 * tables, even though some other relkinds can support them.  This is
	 * necessary because the default_with_oids GUC must apply only to plain
	 * tables and not any other relkind; doing otherwise would break existing
	 * pg_dump files.  We could allow explicit "WITH OIDS" while not allowing
	 * default_with_oids to affect other relkinds, but it would complicate
	 * interpretOidsOption().
	 */
	localHasOids = interpretOidsOption(stmt->options, true);
	descriptor->tdhasoid = (localHasOids || parentOidCount > 0);

	/*
	 * If a partitioned table doesn't have the system OID column, then none of
	 * its partitions should have it.
	 */
	if (stmt->partbound && parentOidCount == 0 && localHasOids)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot create table with OIDs as partition of table without OIDs")));

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

	foreach(listptr, stmt->tableElts)
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
										  stmt->relation->relpersistence,
										  false,
										  false,
										  localHasOids,
										  parentOidCount,
										  stmt->oncommit,
										  reloptions,
										  true,
										  allowSystemTableMods,
										  false,
										  typaddress);

	/* Store inheritance information for new rel. */
	StoreCatalogInheritance(relationId, inheritOids, stmt->partbound != NULL);

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
	 * Process the partitioning specification (if any) and store the partition
	 * key information into the catalog.
	 */
	if (stmt->partspec)
	{
		char		strategy;
		int			partnatts;
		AttrNumber	partattrs[PARTITION_MAX_KEYS];
		Oid			partopclass[PARTITION_MAX_KEYS];
		Oid			partcollation[PARTITION_MAX_KEYS];
		List	   *partexprs = NIL;

		partnatts = list_length(stmt->partspec->partParams);

		/* Protect fixed-size arrays here and in executor */
		if (partnatts > PARTITION_MAX_KEYS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("cannot partition using more than %d columns",
							PARTITION_MAX_KEYS)));

		/*
		 * We need to transform the raw parsetrees corresponding to partition
		 * expressions into executable expression trees.  Like column defaults
		 * and CHECK constraints, we could not have done the transformation
		 * earlier.
		 */
		stmt->partspec = transformPartitionSpec(rel, stmt->partspec,
												&strategy);

		ComputePartitionAttrs(rel, stmt->partspec->partParams,
							  partattrs, &partexprs, partopclass,
							  partcollation);

		StorePartitionKey(rel, strategy, partnatts, partattrs, partexprs,
						  partopclass, partcollation);
	}

	/*
	 * Now add any newly specified column default values and CHECK constraints
	 * to the new relation.  These are passed to us in the form of raw
	 * parsetrees; we need to transform them to executable expression trees
	 * before they can be added. The most convenient way to do that is to
	 * apply the parser's transformExpr routine, but transformExpr doesn't
	 * work unless we have a pre-existing relation. So, the transformation has
	 * to be postponed to this final step of CREATE TABLE.
	 */
	if (rawDefaults || stmt->constraints)
		AddRelationNewConstraints(rel, rawDefaults, stmt->constraints,
								  true, true, false);

	ObjectAddressSet(address, RelationRelationId, relationId);

	/*
	 * Clean up.  We keep lock on new relation (although it shouldn't be
	 * visible to anyone else anyway, until commit).
	 */
	relation_close(rel, NoLock);

	return address;
}
