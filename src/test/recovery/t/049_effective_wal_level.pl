
# Copyright (c) 2024-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Check both wal_level and effective_wal_level values on the given node
# are expected.
sub test_wal_level
{
	my ($node, $expected, $msg) = @_;

	is( $node->safe_psql(
			'postgres',
			qq[select current_setting('wal_level'), current_setting('effective_wal_level');]
		),
		"$expected",
		"$msg");
}

# Initialize the primary server with wal_level = 'replica'
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "log_min_messages = debug1");
$primary->start();

# Check both wal_level and effective_wal_level values.
test_wal_level($primary, "replica|replica",
	"wal_level and effective_wal_level starts with the same value 'replica'");

# Create a physical slot.
$primary->safe_psql('postgres',
	qq[select pg_create_physical_replication_slot('test_phy_slot', false, false)]
);
test_wal_level($primary, "replica|replica",
	"effective_wal_level doesn't change with a new physical slot");

# Create a new logical slot, enabling the logical decoding.
$primary->safe_psql('postgres',
	qq[select pg_create_logical_replication_slot('test_slot', 'pgoutput')]);

# effective_wal_level must be bumped to 'logical'
test_wal_level($primary, "replica|logical",
	"effective_wal_level bumped to logical upon logical slot creation");

# restart the server and check again.
$primary->restart();
test_wal_level($primary, "replica|logical",
	"effective_wal_level becomes logical during startup");

# Create and drop another logical slot, then check if effective_wal_level stays
# 'logical'.
$primary->safe_psql('postgres',
	qq[select pg_create_logical_replication_slot('test_slot2', 'pgoutput')]);
$primary->safe_psql('postgres',
	qq[select pg_drop_replication_slot('test_slot2')]);
test_wal_level($primary, "replica|logical",
	"effective_wal_level stays 'logical' as one slot remains");

# Cleanup all existing slots and start the concurrency test.
$primary->safe_psql('postgres',
		    qq[select pg_drop_replication_slot('test_slot')]);

# Start three psql sessions.
my $psql_tx = $primary->background_psql('postgres');
my $psql_create_slot_1 = $primary->background_psql('postgres');
my $psql_create_slot_2 = $primary->background_psql('postgres');

# Start a new transaction and stays in idle_in_transaction state.
$psql_tx->query_safe(qq[
begin;
select txid_current();
]);

# Start the logical decoding activation process upon creating the logical
# slot, but it needs to wait for the transaction to complete.
$psql_create_slot_1->query_until(
    qr/create_slot_cancelled/,
    q(\echo create_slot_cancelled
select pg_create_logical_replication_slot('slot_cancelled', 'pgoutput');
\q
));
$primary->wait_for_event('client backend', 'transactionid');

# Start another activation process but it needs to wait for the first
# activation process to complete.
$psql_create_slot_2->query_until(
    qr/create_slot_success/,
    q(\echo create_slot_success
select pg_create_logical_replication_slot('test_slot', 'pgoutput');
\q
));
$primary->wait_for_event('client backend', 'LogicalDecodingStatusChange');

# Cancel the backend initiated by $psql_create_slot_1, aborting its activation
# process, letting the second activation process proceed.
$primary->safe_psql('postgres',
		    qq[
select pg_cancel_backend(pid) from pg_stat_activity where query ~ 'slot_cancelled' and pid <> pg_backend_pid()]);

# Check if the backend aborted the activation process.
$primary->wait_for_log("aborting logical decoding activation process");

$psql_tx->query_safe(qq[commit;]);
$psql_tx->quit;

# Wait for the logical slot 'test_slot' has been created.
$primary->poll_query_until('postgres',
			   qq[select exists (select 1 from pg_replication_slots where slot_name = 'test_slot')]);

test_wal_level($primary, "replica|logical", "effective_wal_level bumped to 'logical'");

# Take backup during the effective_wal_level being 'logical'.
$primary->backup('my_backup');

# Initialize standby1 node from the backup 'my_backup'. Note that the
# backup was taken during the logical decoding being enabled on the
# primary because of one logical slot, but replication slots are not
# included in the backup.
my $standby1 = PostgreSQL::Test::Cluster->new('standby1');
$standby1->init_from_backup($primary, 'my_backup', has_streaming => 1);
$standby1->set_standby_mode();
$standby1->start;

# Check if the standby's effective_wal_level should be 'logical' in spite
# of wal_level being 'replica'.
test_wal_level($standby1, "replica|logical",
	"effective_wal_level='logical' on standby");

# Promote the standby1 node that doesn't have any logical slot. So
# the logical decoding must be disabled at promotion.
$standby1->promote;
test_wal_level($standby1, "replica|replica",
	"effective_wal_level got decrased to 'replica' during promotion");
$standby1->stop;

# Initialize standby2 node form the backup 'my_backup'.
my $standby2 = PostgreSQL::Test::Cluster->new('standby2');
$standby2->init_from_backup($primary, 'my_backup', has_streaming => 1);
$standby2->set_standby_mode();
$standby2->start;

# Create a logical slot on the standby, which should be succeeded
# as the primary enables it.
$standby2->create_logical_slot_on_standby($primary, 'standby2_slot',
	'postgres');

# Promote the standby2 node that has one logical slot. So the logical decoding
# keeps enabled even after the promotion.
$standby2->promote;
test_wal_level($standby2, "replica|logical",
	"effective_wal_level keeps 'logical' even after the promotion");

# Confirm if we can create a logical slot after the promotion.
$standby2->safe_psql('postgres',
	qq[select pg_create_logical_replication_slot('standby2_slot2', 'pgoutput')]
);
$standby2->stop;

# Initialize standby3 and starts it with wal_level = 'logical'.
my $standby3 = PostgreSQL::Test::Cluster->new('standby3');
$standby3->init_from_backup($primary, 'my_backup', has_streaming => 1);
$standby3->set_standby_mode();
$standby3->append_conf('postgresql.conf', qq[wal_level = 'logical']);
$standby3->start();
$standby3->backup('my_backup3');

# Initialize cascade standby and starts with wal_level = 'replica'.
my $cascade = PostgreSQL::Test::Cluster->new('cascade');
$cascade->init_from_backup($standby3, 'my_backup3', has_streaming => 1);
$cascade->adjust_conf('postgresql.conf', 'wal_level', 'replica');
$cascade->set_standby_mode();
$cascade->start();

# Regardless of their wal_level values, effective_wal_level values on the
# standby and the cascaded standby depend on the primary's value, 'logical'.
test_wal_level($standby3, "logical|logical",
	"check wal_level and effective_wal_level on standby");
test_wal_level($cascade, "replica|logical",
	"check wal_level and effective_wal_level on cascaded standby");

# Drop the primary's last logical slot, disabling the logical decoding on
# all nodes.
$primary->safe_psql('postgres',
	qq[select pg_drop_replication_slot('test_slot')]);

$primary->wait_for_replay_catchup($standby3);
$standby3->wait_for_replay_catchup($cascade, $primary);

test_wal_level($primary, "replica|replica",
	"effective_wal_level got decreased to 'replica' on primary");
test_wal_level($standby3, "logical|replica",
	"effective_wal_level got decreased to 'replica' on standby");
test_wal_level($cascade, "replica|replica",
	"effective_wal_level got decreased to 'replica' on standby");

# Promote standby3. It enables the logical decoding at promotion as it uses
# 'logical' WAL level.
$standby3->promote;
$standby3->wait_for_replay_catchup($cascade);

test_wal_level($cascade, "replica|logical",
	"effective_wal_level got increased to 'logical' on standby");

$standby3->stop;
$cascade->stop;

# Initialize standby4 and starts it with wal_level = 'logical'.
my $standby4 = PostgreSQL::Test::Cluster->new('standby4');
$standby4->init_from_backup($primary, 'my_backup', has_streaming => 1);
$standby4->set_standby_mode();
$standby4->append_conf('postgresql.conf', qq[wal_level = 'logical']);
$standby4->start;

$primary->wait_for_replay_catchup($standby4);

# Create logical slots on both nodes.
$primary->safe_psql('postgres',
	qq[select pg_create_logical_replication_slot('test_slot', 'pgoutput')]);
$standby4->create_logical_slot_on_standby($primary, 'standby4_slot',
	'postgres');

# Drop the logical slot from the primary, disabling the logical decoding on the
# primary. Which leads to invalidate the logical slot on the standby due to
# 'wal_level_insufficient'.
$primary->safe_psql('postgres',
	qq[select pg_drop_replication_slot('test_slot')]);
test_wal_level($primary, "replica|replica",
	"logical decoding is disabled on the primary");
$standby4->poll_query_until(
	'postgres', qq[
select invalidation_reason = 'wal_level_insufficient' from pg_replication_slots where slot_name = 'standby4_slot'
			    ]);

# Restart the server to check if the slot is successfully restored during
# startup.
$standby4->restart;

# Check if the logical decoding is not enabled on the standby4.
test_wal_level($standby4, "logical|replica",
	"standby's effective_wal_level got decreased to 'replica'");
$standby4->safe_psql('postgres',
	qq[select pg_drop_replication_slot('standby4_slot')]);

# Restart the primary with setting wal_level = 'logical' and create a new logical
# slot.
$primary->append_conf('postgresql.conf', qq[wal_level = 'logical']);
$primary->restart;
$primary->safe_psql('postgres',
	qq[select pg_create_logical_replication_slot('test_slot', 'pgoutput')]);

# The logical decoding should be enabled on both nodes.
$primary->wait_for_replay_catchup($standby4);
test_wal_level($primary, "logical|logical",
	"check WAL levels on the primary node");
test_wal_level($standby4, "logical|logical",
	"standby's effective_wal_level got increased to 'logical' again");

# Set wal_level to 'replica' and restart the primary. Since one logical slot
# is still present on the primary, the logical decoding is not disabled even
# if wal_level got decreased to 'replica'.
$primary->adjust_conf('postgresql.conf', 'wal_level', 'replica');
$primary->restart;
$primary->wait_for_replay_catchup($standby4);

# Check if the logical decoding is still enabled on the both nodes
test_wal_level($primary, "replica|logical",
	"logical decoding is still enabled on the primary");
test_wal_level($standby4, "logical|logical",
	"logical decoding is still enabled on the standby");

# Test the race condition at end of the recovery between the startup and logical
# decoding status change. This test requires injection points enabled.
if (   $ENV{enable_injection_points} eq 'yes'
	&& $primary->check_extension('injection_points'))
{
	# Change standby's wal_level to 'replica' and both the primary and standby4 are
	# using 'replica' WAL level.
	$standby4->adjust_conf('postgresql.conf', 'wal_level', 'replica');
	$standby4->restart;

	# Both servers have one logical slot.
	$standby4->create_logical_slot_on_standby($primary, 'standby4_slot',
		'postgres');

	# Enable and attach the injection point on the standby.
	$primary->safe_psql('postgres', 'create extension injection_points');
	$primary->wait_for_replay_catchup($standby4);
	$standby4->safe_psql('postgres',
		q{select injection_points_attach('startup-logical-decoding-status-change-end-of-recovery', 'wait');}
	);

	# Trigger promotion with no wait for the startup process to reach the
	# injection point.
	$standby4->safe_psql('postgres', qq[select pg_promote(false)]);
	note('promote the standby and waiting for injection_point');
	$standby4->wait_for_event('startup',
		'startup-logical-decoding-status-change-end-of-recovery');
	note('injection_point is reached');

	# Drop the logical slot in background. We can drop the logical replication slot
	# but have to wait for the recovery to complete before disabling logical decoding.
	my $psql = $standby4->background_psql('postgres');
	$psql->query_until(
		qr/drop_slot/,
		q(\echo drop_slot
select pg_drop_replication_slot('standby4_slot');
\q
));

	$standby4->safe_psql('postgres',
		q{select injection_points_wakeup('startup-logical-decoding-status-change-end-of-recovery')}
	);
	$psql->quit;

	# Check if logical decoding got disabled after the recovery.
	test_wal_level($standby4, "replica|replica",
		"effective_wal_level properly got decreased to 'replica'");
}

$standby4->stop;
$primary->stop;

done_testing();
