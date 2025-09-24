# Copyright (c) 2024-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Skip all tests if injection points are not supported in this build
if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize the primary cluster
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

# Load the injection_points extension
$primary->safe_psql('postgres', q(CREATE EXTENSION injection_points));

# Take a backup of the primary for standby initialization
my $backup_name = 'backup';
$primary->backup($backup_name);

# Initialize standby from primary backup
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
));

# Create a physical replication slot on primary for standby
$primary->psql('postgres',
	q{SELECT pg_create_physical_replication_slot('sb1_slot');});

$standby1->start;

# Create a logical replication slot on primary for testing
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('slot_sync', 'test_decoding', false, false, true)"
);

# Wait for standby to catch up
$primary->wait_for_replay_catchup($standby1);

# Initial sync of replication slots
$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Verify that initially there is no skip reason
my $result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason FROM pg_replication_slots
     WHERE slot_name = 'slot_sync' AND synced"
);
is($result, 'none', "slot sync reason is none");

# Simulate standby connection failure by modifying pg_hba.conf
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
    INSERT INTO t1 VALUES(1);
));
$primary->safe_psql('postgres',
	"SELECT pg_replication_slot_advance('slot_sync', pg_current_wal_lsn());");

# Attempt to sync replication slots while standby is behind
$standby1->psql('postgres', "SELECT pg_sync_replication_slots();");

# Check skip reason and count when standby is behind
$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason FROM pg_replication_slots
     WHERE slot_name = 'slot_sync' AND synced AND NOT temporary"
);
is($result, 'missing_wal_record', "slot sync skip when standby is behind");

$result = $standby1->safe_psql('postgres',
	"SELECT slot_sync_skip_count FROM pg_stat_replication_slots WHERE slot_name = 'slot_sync'"
);
is($result, '1', "check slot sync skip count");

# Repeat sync to ensure skip count increments
$standby1->psql('postgres', "SELECT pg_sync_replication_slots();");

$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason FROM pg_replication_slots
     WHERE slot_name = 'slot_sync' AND synced AND NOT temporary"
);
is($result, 'missing_wal_record', "slot sync skip when standby is behind");

$result = $standby1->safe_psql('postgres',
	"SELECT slot_sync_skip_count FROM pg_stat_replication_slots WHERE slot_name = 'slot_sync'"
);
is($result, '2', "check slot sync skip count");

# Restore connectivity between primary and standby
unlink($primary->data_dir . '/pg_hba.conf');
$primary->append_conf(
	'pg_hba.conf',
	qq{
local   all             all                                     trust
local   replication     all                                     trust
});
$primary->restart;

# Cleanup: drop the logical slot and ensure standby catches up
$primary->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('slot_sync')");
$primary->wait_for_replay_catchup($standby1);

$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Create a new logical slot for testing injection point
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('slot_sync', 'test_decoding', false, false, true)"
);

# Attach injection point to simulate wait
my $standby_psql = $standby1->background_psql('postgres');
$standby_psql->query_safe(
	q(select injection_points_attach('slot-sync-skip','wait')));

# Initiate sync of failover slots
$standby_psql->query_until(
	qr/slot_sync/,
	q(
\echo slot_sync
select pg_sync_replication_slots();
));

# Wait for backend to reach injection point
$standby1->wait_for_event('client backend', 'slot-sync-skip');

# Logical slot is temporary and sync will skip because remote is behind
$result = $standby1->safe_psql(
	'postgres',
	"SELECT slot_sync_skip_reason FROM pg_replication_slots
     WHERE slot_name = 'slot_sync' AND synced AND temporary"
);
is($result, 'remote_behind', "slot sync skip as remote is behind");

$result = $standby1->safe_psql('postgres',
	"SELECT slot_sync_skip_count FROM pg_stat_replication_slots WHERE slot_name = 'slot_sync'"
);
is($result, '1', "check slot sync skip count");

# Detach injection point
$standby1->safe_psql(
	'postgres', q{
	SELECT injection_points_detach('slot-sync-skip');
	SELECT injection_points_wakeup('slot-sync-skip');
});

done_testing();
