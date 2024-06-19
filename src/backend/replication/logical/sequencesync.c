/*-------------------------------------------------------------------------
 * sequencesync.c
 *	  PostgreSQL logical replication: initial sequence synchronization
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/sequencesync.c
 *
 * NOTES
 *	  This file contains code for sequence synchronization for
 *	  logical replication.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/sequence.h"
#include "pgstat.h"
#include "replication/logicalworker.h"
#include "replication/worker_internal.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rls.h"
#include "utils/usercontext.h"

/*
 * Fetch sequence data (current state) from the remote node, including the
 * page LSN.
 */
static int64
fetch_sequence_data(WalReceiverConn *conn, Oid remoteid, XLogRecPtr *lsn)
{
	WalRcvExecResult *res;
	StringInfoData cmd;
	TupleTableSlot *slot;
	Oid			tableRow[2] = {INT8OID, LSNOID};
	int64		value = (Datum) 0;

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "SELECT (last_value + log_cnt), page_lsn "
					 "FROM pg_sequence_state(%d)", remoteid);

	res = walrcv_exec(conn, cmd.data, 2, tableRow);
	pfree(cmd.data);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				(errmsg("could not receive sequence list from the publisher: %s",
						res->err)));

	/* Process the sequence. */
	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
	{
		bool		isnull;

		value = DatumGetInt64(slot_getattr(slot, 1, &isnull));
		Assert(!isnull);

		*lsn = DatumGetInt64(slot_getattr(slot, 2, &isnull));
		Assert(!isnull);
	}

	ExecDropSingleTupleTableSlot(slot);

	walrcv_clear_result(res);

	return value;
}

/*
 * Copy existing data of a sequence from publisher.
 *
 * Caller is responsible for locking the local relation.
 */
static XLogRecPtr
copy_sequence(WalReceiverConn *conn, Relation rel)
{
	StringInfoData cmd;
	int64		sequence_value;
	XLogRecPtr	lsn = InvalidXLogRecPtr;
	WalRcvExecResult *res;
	Oid			tableRow[] = {OIDOID, CHAROID};
	TupleTableSlot *slot;
	LogicalRepRelId remoteid;	/* unique id of the relation */
	char			relkind PG_USED_FOR_ASSERTS_ONLY;
	bool		isnull;
	char *nspname = get_namespace_name(RelationGetNamespace(rel));
	char *relname = RelationGetRelationName(rel);

	/* Fetch Oid. */
	initStringInfo(&cmd);
	appendStringInfo(&cmd, "SELECT c.oid, c.relkind"
					 "  FROM pg_catalog.pg_class c"
					 "  INNER JOIN pg_catalog.pg_namespace n"
					 "        ON (c.relnamespace = n.oid)"
					 " WHERE n.nspname = %s"
					 "   AND c.relname = %s",
					 quote_literal_cstr(nspname),
					 quote_literal_cstr(relname));
	res = walrcv_exec(conn, cmd.data,
					  lengthof(tableRow), tableRow);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not fetch sequence info for table \"%s.%s\" from publisher: %s",
						nspname, RelationGetRelationName(rel), res->err)));

	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("sequence \"%s.%s\" not found on publisher",
						nspname, relname)));

	remoteid = DatumGetObjectId(slot_getattr(slot, 1, &isnull));
	Assert(!isnull);
	relkind = DatumGetChar(slot_getattr(slot, 2, &isnull));
	Assert(!isnull);

	ExecDropSingleTupleTableSlot(slot);
	walrcv_clear_result(res);

	Assert(relkind == RELKIND_SEQUENCE);

	/*
	 * Logical replication of sequences is based on decoding WAL records,
	 * describing the "next" state of the sequence the current state in the
	 * relfilenode is yet to reach. But during the initial sync we read the
	 * current state, so we need to reconstruct the WAL record logged when we
	 * started the current batch of sequence values.
	 *
	 * Otherwise we might get duplicate values (on subscriber) if we failed
	 * over right after the sync.
	 */
	sequence_value = fetch_sequence_data(conn, remoteid, &lsn);

	/* sets the sequences in non-transactional way */
	SetSequence(RelationGetRelid(rel), sequence_value);

	/* return the LSN when the sequence state was set */
	return lsn;
}

/*
 * Start syncing the sequences in the sync worker.
 */
static void
LogicalRepSyncSeqeunces()
{
	char	   *err;
	bool		must_use_password;
	List *sequences;
	char	   slotname[NAMEDATALEN];
	AclResult	aclresult;
	UserContext ucxt;
	bool		run_as_owner;
	ListCell *lc;
	int 		currseq = 0;
	Oid			subid = MyLogicalRepWorker->subid;

#define MAX_SEQUENCES_SYNC_PER_BATCH 100

	/* Get the sequences that should be synchronized. */
	StartTransactionCommand();
	sequences = GetSubscriptionSequences(subid,
										 SUBREL_STATE_INIT);
	CommitTransactionCommand();

	/* Is the use of a password mandatory? */
	must_use_password = MySubscription->passwordrequired &&
		!MySubscription->ownersuperuser;

	snprintf(slotname, NAMEDATALEN, "pg_%u_sync_sequences_" UINT64_FORMAT,
			 subid, GetSystemIdentifier());

	/*
	 * Here we use the slot name instead of the subscription name as the
	 * application_name, so that it is different from the leader apply worker,
	 * so that synchronous replication can distinguish them.
	 */
	LogRepWorkerWalRcvConn =
		walrcv_connect(MySubscription->conninfo, true, true,
					   must_use_password,
					   slotname, &err);
	if (LogRepWorkerWalRcvConn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the publisher: %s", err)));


	foreach(lc, sequences)
	{
		SubscriptionRelState *seqinfo = (SubscriptionRelState *) lfirst(lc);
		Relation	sequencerel;
		XLogRecPtr	sequence_lsn;

		CHECK_FOR_INTERRUPTS();

		if (currseq % MAX_SEQUENCES_SYNC_PER_BATCH == 0)
			StartTransactionCommand();

		sequencerel = table_open(seqinfo->relid, RowExclusiveLock);\

		/*
		 * Make sure that the copy command runs as the sequence owner, unless the
		 * user has opted out of that behaviour.
		 */
		run_as_owner = MySubscription->runasowner;
		if (!run_as_owner)
			SwitchToUntrustedUser(sequencerel->rd_rel->relowner, &ucxt);

		/*
		 * Check that our sequence sync worker has permission to insert into the
		 * target sequence.
		 */
		aclresult = pg_class_aclcheck(RelationGetRelid(sequencerel), GetUserId(),
									ACL_INSERT);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult,
						get_relkind_objtype(sequencerel->rd_rel->relkind),
						RelationGetRelationName(sequencerel));

		/*
		 * COPY FROM does not honor RLS policies.  That is not a problem for
		 * subscriptions owned by roles with BYPASSRLS privilege (or superuser,
		 * who has it implicitly), but other roles should not be able to
		 * circumvent RLS.  Disallow logical replication into RLS enabled
		 * relations for such roles.
		 */
		if (check_enable_rls(RelationGetRelid(sequencerel), InvalidOid, false) == RLS_ENABLED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("user \"%s\" cannot replicate into relation with row-level security enabled: \"%s\"",
							GetUserNameFromId(GetUserId(), true),
							RelationGetRelationName(sequencerel))));

		sequence_lsn = copy_sequence(LogRepWorkerWalRcvConn, sequencerel);

		UpdateSubscriptionRelState(subid, seqinfo->relid, SUBREL_STATE_READY,
								   sequence_lsn);
		ereport(LOG,
				errmsg("logical replication synchronization for subscription \"%s\", sequence \"%s\" has finished",
					   get_subscription_name(subid, false), RelationGetRelationName(sequencerel)));
		table_close(sequencerel, NoLock);

		currseq++;

		if (currseq % MAX_SEQUENCES_SYNC_PER_BATCH == 0 || currseq == list_length(sequences))
			CommitTransactionCommand();
	}

	if (!run_as_owner)
		RestoreUserContext(&ucxt);
}

/*
 * Execute the initial sync with error handling. Disable the subscription,
 * if it's required.
 *
 * Allocate the slot name in long-lived context on return. Note that we don't
 * handle FATAL errors which are probably because of system resource error and
 * are not repeatable.
 */
static void
start_sequence_sync()
{
	Assert(am_sequencesync_worker());

	PG_TRY();
	{
		/* Call initial sync. */
		LogicalRepSyncSeqeunces();
	}
	PG_CATCH();
	{
		if (MySubscription->disableonerr)
			DisableSubscriptionAndExit();
		else
		{
			/*
			 * Report the worker failed during sequence synchronization. Abort
			 * the current transaction so that the stats message is sent in an
			 * idle state.
			 */
			AbortOutOfAnyTransaction();
			pgstat_report_subscription_error(MySubscription->oid, false);

			PG_RE_THROW();
		}
	}
	PG_END_TRY();
}

/* Logical Replication Sequencesync worker entry point */
void
SequencesyncWorkerMain(Datum main_arg)
{
	int			worker_slot = DatumGetInt32(main_arg);

	SetupApplyOrSyncWorker(worker_slot);

	start_sequence_sync();

	finish_sync_worker(false);
}
