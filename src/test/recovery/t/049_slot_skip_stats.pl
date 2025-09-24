# Copyright (c) 2024-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $primary = PostgreSQL::Test::Cluster->new('publisher');
$primary->init(
	allows_streaming => 'logical',
	auth_extra => [ '--create-role' => 'repl_role' ]);
$primary->append_conf(
	'postgresql.conf', qq{
autovacuum = off
max_prepared_transactions = 1
});
$primary->start;

$primary->safe_psql('postgres', q(CREATE EXTENSION injection_points));

my $backup_name = 'backup';
$primary->backup($backup_name);

my $standby1 = PostgreSQL::Test::Cluster->new('standby1');
$standby1->init_from_backup(
	$primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);

my $connstr_1 = $primary->connstr;
$standby1->append_conf(
	'postgresql.conf', qq(
hot_standby_feedback = on
primary_slot_name = 'sb1_slot'
primary_conninfo = '$connstr_1 dbname=postgres'
log_min_messages = 'debug2'
));
$primary->psql('postgres',
	q{SELECT pg_create_physical_replication_slot('sb1_slot');});

$standby1->start;

$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('slot_sync', 'test_decoding', false, false, true)"
);
$primary->wait_for_replay_catchup($standby1);

$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

my $result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason
	 FROM pg_replication_slots
	 WHERE slot_name = 'slot_sync'
	   AND synced"
);
is($result, 'none', "slot sync reason is none");


# Change pg_hba.conf so that standby cannot connect to primary
unlink($primary->data_dir . '/pg_hba.conf');
$primary->append_conf('pg_hba.conf',
	qq{local   all             all                                     trust}
);
$primary->restart;

# Advance the failover slot so that confirmed flush LSN of remote slot become
# ahead of standby's flushed LSN
$primary->safe_psql(
	'postgres', qq(
	CREATE TABLE t1(a int);
	INSERT INTO t1 values(1);
));
$primary->safe_psql('postgres',
	"SELECT pg_replication_slot_advance('slot_sync', pg_current_wal_lsn());");

$standby1->psql('postgres', "SELECT pg_sync_replication_slots();");

# Confirm that standby is behind
$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason
	 FROM pg_replication_slots
	 WHERE slot_name = 'slot_sync'
	   AND synced"
);
is($result, 'missing_wal_record', "slot sync skip when standby is behind");

$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_count
	 FROM pg_stat_replication_slots
	 WHERE slot_name = 'slot_sync'"
);
is($result, '1', "check slot sync skip count");

# Repeat pg_sync_replication_slots to check slot_sync_skip_count is advancing
$standby1->psql('postgres', "SELECT pg_sync_replication_slots();");

$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason
	 FROM pg_replication_slots
	 WHERE slot_name = 'slot_sync'
	   AND synced"
);
is($result, 'missing_wal_record', "slot sync skip when standby is behind");

$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_count
	 FROM pg_stat_replication_slots
	 WHERE slot_name = 'slot_sync'"
);
is($result, '2', "check slot sync skip count");

# Restore the connect between primary and standby
unlink($primary->data_dir . '/pg_hba.conf');
$primary->append_conf(
	'pg_hba.conf',
	qq{
local   all             all                                     trust
local   replication     all                                     trust
});
$primary->restart;

# Cleanup
$primary->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('slot_sync')");
$primary->wait_for_replay_catchup($standby1);

$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Create a new logical slot on primary
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('slot_sync', 'test_decoding', false, false, true)"
);

# Attach injection point
my $standby_psql = $standby1->background_psql('postgres');
$standby_psql->query_safe(
	q(select injection_points_attach('slot-sync-skip','wait')));

# initiate sync the failover slots
$standby_psql->query_until(
	qr/slot_sync/,
	q(
\echo slot_sync
select pg_sync_replication_slots();
));

$standby1->wait_for_event('client backend', 'slot-sync-skip');

# the logical slot is in temporary state and the sync will skip as remote is
# behind the freashly created slot
$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason
	 FROM pg_replication_slots
	 WHERE slot_name = 'slot_sync'
	   AND synced"
);
is($result, 'remote_behind', "slot sync skip as remote is behind");

$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_count
	 FROM pg_stat_replication_slots
	 WHERE slot_name = 'slot_sync'"
);
is($result, '1', "check slot sync skip cout");

$standby1->safe_psql('postgres',
	q{select injection_points_detach('slot-sync-skip')});
$standby1->safe_psql('postgres',
	q{select injection_points_wakeup('slot-sync-skip')});

done_testing();
