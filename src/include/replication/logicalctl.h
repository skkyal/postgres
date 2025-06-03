/*-------------------------------------------------------------------------
 *
 * logicalctl.h
 *		Definitions for logical decoding status control facility.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/replication/logicalctl.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALCTL_H
#define LOGICALCTL_H

extern Size LogicalDecodingCtlShmemSize(void);
extern void LogicalDecodingCtlShmemInit(void);
extern void StartupLogicalDecodingStatus(bool status_in_control_file);
extern void InitializeProcessXLogLogicalInfo(void);
extern bool ProcessBarrierUpdateXLogLogicalInfo(void);
extern bool IsLogicalDecodingEnabled(void);
extern bool IsXLogLogicalInfoEnabled(void);
extern void EnsureLogicalDecodingEnabled(void);
extern void DisableLogicalDecodingIfNecessary(void);
extern void UpdateLogicalDecodingStatus(bool new_status, bool need_lock);
extern void UpdateLogicalDecodingStatusEndOfRecovery(void);

#endif
