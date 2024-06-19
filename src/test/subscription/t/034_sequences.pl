
# Copyright (c) 2021, PostgreSQL Global Development Group

# This tests that sequences are synced correctly to the subscriber
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Create some preexisting content on publisher
my $ddl = qq(
	CREATE TABLE seq_test (v BIGINT);
	CREATE SEQUENCE s;
);

# Setup structure on the publisher
$node_publisher->safe_psql('postgres', $ddl);

# Create some the same structure on subscriber, and an extra sequence that
# we'll create on the publisher later
$ddl = qq(
	CREATE TABLE seq_test (v BIGINT);
	CREATE SEQUENCE s;
	CREATE SEQUENCE s2;
	CREATE SEQUENCE s3;
);

$node_subscriber->safe_psql('postgres', $ddl);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION seq_pub FOR ALL SEQUENCES");

# Insert initial test data
$node_publisher->safe_psql(
	'postgres', qq(
	-- generate a number of values using the sequence
	INSERT INTO seq_test SELECT nextval('s') FROM generate_series(1,100);
));

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION seq_sub CONNECTION '$publisher_connstr' PUBLICATION seq_pub"
);

# Wait for initial sync to finish as well
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check the data on subscriber
my $result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s;
));

is($result, '132|0|t', 'initial test data replicated');

# create a new sequence, it should be synced
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE s2;
	INSERT INTO seq_test SELECT nextval('s2') FROM generate_series(1,100);
));

# changes to existing sequences should not be synced
$node_publisher->safe_psql(
	'postgres', qq(
	INSERT INTO seq_test SELECT nextval('s') FROM generate_series(1,100);
));

# Refresh publication after create a new sequence and updating existing
# sequence.
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	ALTER SUBSCRIPTION seq_sub REFRESH PUBLICATION
));

$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check the data on subscriber
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s;
));

is($result, '132|0|t', 'initial test data replicated');

$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s2;
));

is($result, '132|0|t', 'initial test data replicated');

# Changes of both new and existing sequence should be synced after REFRESH
# PUBLICATION SEQUENCES.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE s3;
	INSERT INTO seq_test SELECT nextval('s3') FROM generate_series(1,100);

	-- Existing sequence
	INSERT INTO seq_test SELECT nextval('s2') FROM generate_series(1,100);
));

# Refresh publication sequences after create new sequence and updating existing
# sequence.
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	ALTER SUBSCRIPTION seq_sub REFRESH PUBLICATION SEQUENCES
));

$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check the data on subscriber
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s2;
));

is($result, '231|0|t', 'initial test data replicated');

$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT * FROM s3;
));

is($result, '132|0|t', 'initial test data replicated');

done_testing();
