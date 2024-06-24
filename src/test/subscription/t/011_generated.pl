
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Test generated columns
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# setup

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	"max_logical_replication_workers = 10");
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) STORED)"
);

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 22) STORED, c int)"
);

# publisher-side tab2 has generated col 'b' but subscriber-side tab2 has NON-generated col 'b'.
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab2 (a int, b int GENERATED ALWAYS AS (a * 2) STORED)"
);

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab2 (a int, b int)"
);

# publisher-side tab3 has generated col 'b' but subscriber-side tab2 has DIFFERENT COMPUTATION generated col 'b'.
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab3 (a int, b int GENERATED ALWAYS AS (a + 10) STORED)"
);

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab3 (a int, b int GENERATED ALWAYS AS (a + 20) STORED)"
);

# tab4: publisher-side generated col 'b' and 'c'  ==> subscriber-side non-generated col 'b', and generated-col 'c'
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab4 (a int , b int GENERATED ALWAYS AS (a * 2) STORED, c int GENERATED ALWAYS AS (a * 2) STORED)"
);

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab4 (a int, b int, c int GENERATED ALWAYS AS (a * 22) STORED)"
);

# tab5: publisher-side non-generated col 'b' --> subscriber-side non-generated col 'b'
$node_publisher->safe_psql('postgres', "CREATE TABLE tab5 (a int, b int)");

$node_subscriber->safe_psql('postgres',
 	"CREATE TABLE tab5 (a int, b int GENERATED ALWAYS AS (a * 22) STORED)");

# data for initial sync

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1 (a) VALUES (1), (2), (3)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab2 (a) VALUES (1), (2), (3)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab3 (a) VALUES (1), (2), (3)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab4 (a) VALUES (1), (2), (3)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab5 (a, b) VALUES (1, 1), (2, 2), (3, 3)");

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub1 FOR TABLE tab1");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub2 FOR TABLE tab2");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub3 FOR TABLE tab3");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub4 FOR TABLE tab4");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub5 FOR TABLE tab5");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1"
);

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub2 CONNECTION '$publisher_connstr' PUBLICATION pub2 WITH (include_generated_columns = true, copy_data = false)"
);

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub3 CONNECTION '$publisher_connstr' PUBLICATION pub3 WITH (include_generated_columns = true, copy_data = false)"
);

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub4 CONNECTION '$publisher_connstr' PUBLICATION pub4 WITH (include_generated_columns = true)"
);

# Wait for initial sync of all subscriptions
$node_subscriber->wait_for_subscription_sync;

my $result = $node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab1");
is( $result, qq(1|22
2|44
3|66), 'generated columns initial sync');

$result = $node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab2");
is($result, qq(), 'generated columns initial sync');

$result = $node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab3");
is($result, qq(), 'generated columns initial sync');

# data to replicate

$node_publisher->safe_psql('postgres', "INSERT INTO tab1 VALUES (4), (5)");

$node_publisher->safe_psql('postgres', "UPDATE tab1 SET a = 6 WHERE a = 5");

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres', "SELECT * FROM tab1");
is( $result, qq(1|22|
2|44|
3|66|
4|88|
6|132|), 'generated columns replicated');

$node_publisher->safe_psql('postgres', "INSERT INTO tab2 VALUES (4), (5)");

$node_publisher->wait_for_catchup('sub2');

# the column was NOT replicated because the result value of 'b'is the subscriber-side computed value
$result = $node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab2 ORDER BY a");
is( $result, qq(4|8
5|10),
	'confirm generated columns ARE replicated when the subscriber-side column is not generated'
);

$node_publisher->safe_psql('postgres', "INSERT INTO tab3 VALUES (4), (5)");

$node_publisher->wait_for_catchup('sub3');

$result = $node_subscriber->safe_psql('postgres', "SELECT a, b FROM tab3 ORDER BY a");
is( $result, qq(4|24
5|25),
	'confirm generated columns are NOT replicated when the subscriber-side column is also generated'
);

$node_publisher->safe_psql('postgres', "INSERT INTO tab4 VALUES (4), (5)");

$node_publisher->wait_for_catchup('sub4');

# gen-col 'b' in publisher replicating to NOT gen-col 'b' on subscriber
# gen-col 'c' in publisher not replicating to gen-col 'c' on subscriber
$result =
  $node_subscriber->safe_psql('postgres', "SELECT * FROM tab4 ORDER BY a");
is( $result, qq(1|2|22
2|4|44
3|6|66
4|8|88
5|10|110), 'replicate generated column with initial sync');

# NOT gen-col 'b' in publisher not replicating to gen-col 'b' on subscriber
my $offset = -s $node_subscriber->logfile;

# sub5 will cause table sync worker to restart repetitively
# So SUBSCRIPTION sub5 is created separately
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub5 CONNECTION '$publisher_connstr' PUBLICATION pub5 WITH (include_generated_columns = true)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? logical replication target relation "public.tab5" has a generated column "b" but corresponding column on source relation is not a generated column/,
	$offset);

$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub5");

# try it with a subscriber-side trigger

$node_subscriber->safe_psql(
	'postgres', q{
CREATE FUNCTION tab1_trigger_func() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.c := NEW.a + 10;
  RETURN NEW;
END $$;

CREATE TRIGGER test1 BEFORE INSERT OR UPDATE ON tab1
  FOR EACH ROW
  EXECUTE PROCEDURE tab1_trigger_func();

ALTER TABLE tab1 ENABLE REPLICA TRIGGER test1;
});

$node_publisher->safe_psql('postgres', "INSERT INTO tab1 VALUES (7), (8)");

$node_publisher->safe_psql('postgres', "UPDATE tab1 SET a = 9 WHERE a = 7");

$node_publisher->wait_for_catchup('sub1');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT * FROM tab1 ORDER BY 1");
is( $result, qq(1|22|
2|44|
3|66|
4|88|
6|132|
8|176|18
9|198|19), 'generated columns replicated with trigger');

done_testing();
