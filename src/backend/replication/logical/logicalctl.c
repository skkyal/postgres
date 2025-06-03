/*-------------------------------------------------------------------------
 * logicalctl.c
 *		Functionality to control logical decoding status online.
 *
 * This module enables dynamic control of logical decoding availability.
 * Logical decoding becomes active under two conditions: when the wal_level
 * parameter is set to 'logical', or when at least one logical replication
 * slot exists with wal_level set to 'replica'. The system disables logical
 * decoding when neither condition is met. Therefore, the dynamic control
 * of logical decoding availability is required only when wal_level is set
 * to 'replica'. With 'logical' WAL level, logical decoding is always enabled
 * whereas with 'minimal' WAL level, it's always disabled.
 *
 * The module maintains separate controls of two aspects: writing information
 * required by logical decoding to WAL records and utilizing logical decoding
 * itself, controlled by LogicalDecodingCtl->xlog_logical_info and
 * ->logical_decoding_enabled fields respectively. The activation process of
 * logical decoding  involves several steps, beginning with maintaining logical
 * decoding in a disabled state while incrementing the effective WAL level to
 * its 'logical' equivalent. This change is reflected in the read-only
 * effective_wal_level parameter. The process includes necessary synchronization
 * to ensure all processes adapt to the new effective WAL level before logical
 * decoding is fully enabled. Deactivation follows a similarly careful,
 * multi-step process in the reverse order.
 *
 * During recovery, both fields are updated together solely by replaying
 * XLOG_LOGICAL_DECODING_STATUS_CHANGE records since no coordination is
 * needed. And local wal_level setting has no effect during this time -
 * standbys inherit their logical decoding behavior from its upstream server
 * through the replayed WAL. At promotion, we update logical decoding
 * status based on local conditions: wal_level value and presence of logical
 * slots.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 	  src/backend/replication/logical/logicalctl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "storage/condition_variable.h"
#include "storage/procarray.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "replication/logicalctl.h"
#include "replication/slot.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"
#include "utils/wait_event_types.h"

/*
 * Struct for controlling the logical decoding status.
 *
 * This struct is protected by LogicalDecodingControlLock.
 */
typedef struct LogicalDecodingCtlData
{
	/*
	 * This is the authoritative value used by all processes to determine
	 * whether to write additional information required by logical decoding to
	 * WAL. Since this information could be checked frequently, each process
	 * caches this value in XLogLogicalInfo for better performance.
	 */
	bool		xlog_logical_info;

	/* True if logical decoding is available in the system */
	bool		logical_decoding_enabled;

	/* True while the logical decoding status is being changed */
	bool		transition_in_progress;

	/*
	 * This flag is set to true by the startup process during recovery, to
	 * delay any logical decoding status change attempts until the recovery
	 * actually completes. See comments in
	 * start_logical_decoding_status_change() for details.
	 */
	bool		delay_status_change;

	/* Condition variable signaled when a transition completes */
	ConditionVariable transition_cv;
} LogicalDecodingCtlData;

static LogicalDecodingCtlData *LogicalDecodingCtl = NULL;

/*
 * A process local cache of LogicalDecodingCtl->xlog_logical_info. This is
 * initialized at process startup time, and could be updated when absorbing
 * the process barrier signal in ProcessBarrierUpdateXLogLogicalInfo().
 */
bool		XLogLogicalInfo = false;

static void update_xlog_logical_info(void);
static void abort_logical_decoding_activation(int code, Datum arg);
static bool start_logical_decoding_status_change(bool new_status);

Size
LogicalDecodingCtlShmemSize(void)
{
	return sizeof(LogicalDecodingCtlData);
}

void
LogicalDecodingCtlShmemInit(void)
{
	bool		found;

	LogicalDecodingCtl = ShmemInitStruct("Logical information control",
										 LogicalDecodingCtlShmemSize(),
										 &found);

	if (!found)
	{
		LogicalDecodingCtl->xlog_logical_info = false;
		LogicalDecodingCtl->logical_decoding_enabled = false;
		LogicalDecodingCtl->transition_in_progress = false;
		LogicalDecodingCtl->delay_status_change = false;
		ConditionVariableInit(&LogicalDecodingCtl->transition_cv);
	}
}

/*
 * Initialize logical decoding status on shmem at server startup. This
 * must be called ONCE during postmaster or standalone-backend startup.
 */
void
StartupLogicalDecodingStatus(bool last_status)
{
	/* Logical decoding is always disabled when 'minimal' WAL level */
	if (wal_level == WAL_LEVEL_MINIMAL)
		return;

	/*
	 * Set the initial logical decoding status based on the last status. If
	 * logical decoding was enabled before the last shutdown, it remains
	 * enabled as we might have set wal_level='logical' or have a few logical
	 * slots.
	 */
	if (last_status)
	{
		LogicalDecodingCtl->xlog_logical_info = true;
		LogicalDecodingCtl->logical_decoding_enabled = true;
	}
}

/*
 * Update the XLogLogicalInfo cache.
 */
static inline void
update_xlog_logical_info(void)
{
	XLogLogicalInfo = IsXLogLogicalInfoEnabled();
}

/*
 * Initialize XLogLogicalInfo backend-private cache. This routine is called
 * during process initialization.
 */
void
InitializeProcessXLogLogicalInfo(void)
{
	update_xlog_logical_info();
}

/*
 * This routine is called when we are ordered to update XLogLogicalInfo
 * by a ProcSignalBarrier.
 */
bool
ProcessBarrierUpdateXLogLogicalInfo(void)
{
	update_xlog_logical_info();
	return true;
}

/*
 * Check the shared memory state and return true if logical decoding is
 * enabled on the system.
 */
bool
IsLogicalDecodingEnabled(void)
{
	bool		enabled;

	LWLockAcquire(LogicalDecodingControlLock, LW_SHARED);
	enabled = LogicalDecodingCtl->logical_decoding_enabled;
	LWLockRelease(LogicalDecodingControlLock);

	return enabled;
}

/*
 * Check the shared memory state and return true if logical information WAL
 * logging is enabled.
 */
bool
IsXLogLogicalInfoEnabled(void)
{
	bool		xlog_logical_info;

	LWLockAcquire(LogicalDecodingControlLock, LW_SHARED);
	xlog_logical_info = LogicalDecodingCtl->xlog_logical_info;
	LWLockRelease(LogicalDecodingControlLock);

	return xlog_logical_info;
}

/*
 * Enable or disable both status of logical info WAL logging and logical decoding
 * on shared memory.

 * Note that this function updates the global flags without the state transition
 * process. EnsureLogicalDecodingEnabled() and DisableLogicalDecodingIfNecessary()
 * should be used instead if there could be concurrent processes doing writes
 * or logical decoding.
 */
void
UpdateLogicalDecodingStatus(bool new_status, bool need_lock)
{
	if (need_lock)
		LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);

	LogicalDecodingCtl->xlog_logical_info = new_status;
	LogicalDecodingCtl->logical_decoding_enabled = new_status;

	if (need_lock)
		LWLockRelease(LogicalDecodingControlLock);

	elog(DEBUG1, "update logical decoding status to %d", new_status);
}

/*
 * A PG_ENSURE_ERROR_CLEANUP callback for activating logical decoding, resetting
 * the shared flags to revert the logical decoding activation process.
 */
static void
abort_logical_decoding_activation(int code, Datum arg)
{
	Assert(LogicalDecodingCtl->transition_in_progress);

	elog(DEBUG1, "aborting logical decoding activation process");

	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->xlog_logical_info = false;
	LWLockRelease(LogicalDecodingControlLock);

	/*
	 * Some processes might have already started logical info WAL logging, so
	 * order all running processes to update their caches. We don't need to
	 * wait for all processes to disable xlog_logical_info locally as it's
	 * always safe to write logical information to WAL records, even when not
	 * strictly required.
	 */
	EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO);

	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->transition_in_progress = false;
	LWLockRelease(LogicalDecodingControlLock);

	/* Let waiters know the WAL level change completed */
	ConditionVariableBroadcast(&LogicalDecodingCtl->transition_cv);
}

/*
 * This function does several kinds of preparation works required to start
 * the process of logical decoding status change. If the status change is
 * required, it ensures we can change logical decoding status, setting
 * LogicalDecodingCtl->transition_in_progress on, and returns true.
 * Otherwise, if it's not required or not allowed (e.g., during recovery
 * or wal_level = 'logical'), it returns false.
  */
static bool
start_logical_decoding_status_change(bool new_status)
{
	if (RecoveryInProgress())
	{
		bool		delay_status_change;

		/*
		 * During recovery, there is a race condition with the startup
		 * process's end-of-recovery action; after the startup process updates
		 * logical decoding status at the end of recovery, it's possible that
		 * other processes try to enable or disable logical decoding status
		 * before the recovery completes but are unable to write WAL records.
		 * Therefore, if the startup process has done its end-of-recovery
		 * work, we need to wait for the recovery to actually finish.
		 */
		LWLockAcquire(LogicalDecodingControlLock, LW_SHARED);
		delay_status_change = LogicalDecodingCtl->delay_status_change;
		LWLockRelease(LogicalDecodingControlLock);

		/*
		 * Return if the startup process is applying WAL records. We cannot
		 * change logical replication status during recovery by other than
		 * replaying a XLOG_LOGICAL_DECODING_STATUS_CHANGE record.
		 */
		if (!delay_status_change)
			return false;

		elog(DEBUG1,
			 "waiting for recovery completion to change logical decoding status");

		/*
		 * The startup process already updated logical decoding status at the
		 * end of recovery but it might not be allowed to write WAL records
		 * yet. Wait for the recovery to complete and check the status again.
		 */
		while (RecoveryInProgress())
		{
			pgstat_report_wait_start(WAIT_EVENT_LOGICAL_DECODING_STATUS_CHANGE_DELAY);
			pg_usleep(100000L); /* wait for 100 msec */
			pgstat_report_wait_end();
		}
	}

retry:
	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);

	/*
	 * When attempting to disable logical decoding, if there is at least one
	 * logical slots we cannot disable it.
	 */
	if (!new_status && CheckLogicalSlotExists())
	{
		LWLockRelease(LogicalDecodingControlLock);
		return false;
	}

	/*
	 * We check the current status to see if we need to change it. If a status
	 * change is in-progress, we need to wait for completion.
	 */
	if (LogicalDecodingCtl->transition_in_progress)
	{
		/* Release the lock and wait for someone to complete the transition */
		LWLockRelease(LogicalDecodingControlLock);
		ConditionVariableSleep(&LogicalDecodingCtl->transition_cv,
							   WAIT_EVENT_LOGICAL_DECODING_STATUS_CHANGE);

		goto retry;
	}

	/* Return if we don't need to change the status */
	if (LogicalDecodingCtl->logical_decoding_enabled == new_status)
	{
		LWLockRelease(LogicalDecodingControlLock);
		return false;
	}

	/* Mark the state transition is in-progress */
	LogicalDecodingCtl->transition_in_progress = true;

	LWLockRelease(LogicalDecodingControlLock);

	return true;
}

/*
 * Enable logical decoding if disabled.
 *
 * Note that there is no interlock between logical decoding activation
 * and slot creation. To ensure enabling logical decoding the caller
 * needs to call this function after creating a logical slot without
 * initializing its logical decoding context.
 */
void
EnsureLogicalDecodingEnabled(void)
{
	Assert(MyReplicationSlot);

	if (wal_level != WAL_LEVEL_REPLICA)
		return;

	/* Prepare and start the activation process if it's disabled */
	if (!start_logical_decoding_status_change(true))
		return;

	/*
	 * Ensure we reset the activation process if we cancelled or errored out
	 * below
	 */
	PG_ENSURE_ERROR_CLEANUP(abort_logical_decoding_activation, (Datum) 0);
	{
		RunningTransactions running;

		/*
		 * Set logical info WAL logging on the shmem. All process starts after
		 * this point will include the information required by logical
		 * decoding to WAL records.
		 */
		LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
		LogicalDecodingCtl->xlog_logical_info = true;
		LWLockRelease(LogicalDecodingControlLock);

		/*
		 * Order all running processes to reflect the xlog_logical_info
		 * update, and wait. This ensures that all running processes have
		 * enabled logical information WAL logging.
		 */
		WaitForProcSignalBarrier(
								 EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO));

		/*
		 * While all processes are using the new status, there could be some
		 * transactions that might have started with the old status. So wait
		 * for the running transactions to complete so that logical decoding
		 * doesn't include transactions that wrote WAL with insufficient
		 * information.
		 */
		running = GetRunningTransactionData();
		LWLockRelease(ProcArrayLock);
		LWLockRelease(XidGenLock);

		elog(DEBUG1, "waiting for %d transactions to complete", running->xcnt);

		for (int i = 0; i < running->xcnt; i++)
		{
			TransactionId xid = running->xids[i];

			if (TransactionIdIsCurrentTransactionId(xid))
				continue;

			XactLockTableWait(xid, NULL, NULL, XLTW_None);
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(abort_logical_decoding_activation, (Datum) 0);

	START_CRIT_SECTION();

	/*
	 * Here, we can ensure that all running transactions are using the new
	 * xlog_logical_info value, writing logical information to WAL records. So
	 * now enable logical decoding globally.
	 *
	 * It's always safe to write logical information to WAL records even when
	 * not strictly required. So we first enable it and write the WAL record
	 * below.
	 */
	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->logical_decoding_enabled = true;
	LogicalDecodingCtl->transition_in_progress = false;
	LWLockRelease(LogicalDecodingControlLock);

	{
		XLogRecPtr	recptr;
		bool		logical_decoding = true;

		XLogBeginInsert();
		XLogRegisterData(&logical_decoding, sizeof(bool));
		recptr = XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
		XLogFlush(recptr);
	}

	END_CRIT_SECTION();

	ereport(LOG,
			(errmsg("logical decoding is enabled upon creating a new logical replication slot")));

	/* Let waiters know the work finished */
	ConditionVariableBroadcast(&LogicalDecodingCtl->transition_cv);
}

/*
 * Disable logical decoding if enabled.
 *
 * This function expects to be called after dropping a possibly-last logical
 * replication slot. It disable logical decoding only if it was the last
 * remaining logical slot and wal_level = 'replica'. Otherwise, it performs
 * no action.
 */
void
DisableLogicalDecodingIfNecessary(void)
{
	if (wal_level != WAL_LEVEL_REPLICA)
		return;

	/* Prepare and start the deactivation process if it's enabled */
	if (!start_logical_decoding_status_change(false))
		return;

	/*
	 * We don't need PG_ENSURE_ERROR_CLEANUP() to abort the deactivation
	 * process since we can expect all operations below not to throw ERROR or
	 * FATAL.
	 */

	START_CRIT_SECTION();

	/*
	 * When disabling logical decoding, we need to disable logical decoding
	 * first and disable logical information WAL logging in order to ensure
	 * that no logical decoding processes WAL records with insufficient
	 * information.
	 */
	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->logical_decoding_enabled = false;
	LWLockRelease(LogicalDecodingControlLock);

	/* Write the WAL to disable logical decoding on standbys too */
	if (XLogStandbyInfoActive())
	{
		bool		logical_decoding = false;
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterData(&logical_decoding, sizeof(bool));
		recptr = XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
		XLogFlush(recptr);
	}

	/* Now disable logical information WAL logging */
	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->xlog_logical_info = false;
	LWLockRelease(LogicalDecodingControlLock);

	/*
	 * Order all running processes to reflect the xlog_logical_info update.
	 * Unlike when enabling logical decoding, we don't need to wait for all
	 * processes to complete it in this case. We already disabled logical
	 * decoding and it's always safe to write logical information to WAL
	 * records, even when not strictly required. Therefore, we don't need to
	 * wait for all running transactions to finish either.
	 */
	EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO);

	/* Complete the transition */
	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->transition_in_progress = false;
	LWLockRelease(LogicalDecodingControlLock);

	END_CRIT_SECTION();

	ereport(LOG,
			(errmsg("logical decoding is disabled because all logical replication slots are removed")));

	/* Let waiters know the work finished */
	ConditionVariableBroadcast(&LogicalDecodingCtl->transition_cv);
}

/*
 * Update logical decoding status at end of the recovery. This function
 * must be called before accepting writes.
 */
void
UpdateLogicalDecodingStatusEndOfRecovery(void)
{
	bool		new_status = false;
	bool		need_wal = false;

	Assert(RecoveryInProgress());
	Assert(!LogicalDecodingCtl->transition_in_progress);

	/* With 'minimal' WAL level, logical decoding is always disabled */
	if (wal_level == WAL_LEVEL_MINIMAL)
		return;

	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);

	/*
	 * We can use logical decoding if we're using 'logical' WAL level or there
	 * is at least one logical replication slot.
	 */
	if (wal_level == WAL_LEVEL_LOGICAL || CheckLogicalSlotExists())
		new_status = true;

	if (LogicalDecodingCtl->logical_decoding_enabled != new_status)
		need_wal = true;

	/*
	 * Update shmem flags. We don't need to care about the order of setting
	 * global flag and writing the WAL record writes are not allowed yet.
	 */
	UpdateLogicalDecodingStatus(new_status, false);

	/*
	 * We disallow any logical decoding status change until we actually
	 * completes the recovery, i.e., RecoveryInProgress() returns false. This
	 * is necessary to deal with the race condition that could happen after
	 * this point; processes are able to create or drop logical replication
	 * slots and tries to enable or disable logical decoding accordingly, but
	 * they are not allowed to write any WAL records until the recovery
	 * completes.
	 */
	LogicalDecodingCtl->delay_status_change = true;

	LWLockRelease(LogicalDecodingControlLock);

	if (need_wal)
	{
		XLogRecPtr	recptr;

		Assert(XLogStandbyInfoActive());

		XLogBeginInsert();
		XLogRegisterData(&new_status, sizeof(bool));
		recptr = XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
		XLogFlush(recptr);
	}

	/*
	 * Ensure all running processes have the updated status. We don't need to
	 * wait for running transactions to finish as we don't accept any writes
	 * yet. We need the wait even if we've not updated the status above as the
	 * status have been turned on and off during recovery, having running
	 * processes have different status on their local caches.
	 */
	if (IsUnderPostmaster)
		WaitForProcSignalBarrier(
								 EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO));

	INJECTION_POINT("startup-logical-decoding-status-change-end-of-recovery", NULL);
}
