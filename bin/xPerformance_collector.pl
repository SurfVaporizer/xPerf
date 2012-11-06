#!/usr/bin/perl 
# -- Created by Skribnik Evgeniy, Nvision Group
# -- Version 1.0

use lib "/var/www/html/xM/lib";
use lib "/var/www/cgi-bin";
use DBI;
use utf8;
use Data::Dumper;
use InCharge::session;
use threads; # -- should to upgrade to last version
use Thread::Queue; # -- should to upgrade to last version

require "xCommon_procedures.pl";
require "xMdb_procedures.pl";
require "xIonix_procedures.pl";

# -- load global configuration file
%confHash = ();
%confHash =  load_config("/var/www/cgi-bin/xM.conf");

our $debug = 1;

# -- DB Credentials
my $database = $confHash{'PERFORMANCE.dbName'};
my $hostname = $confHash{'PERFORMANCE.dbHost'};
my $port = $confHash{'PERFORMANCE.dbPort'};
my $dbType = $confHash{'PERFORMANCE.dbType'};

my $dbUser = $confHash{'PERFORMANCE.dbUser'};
my $dbPassword = $confHash{'PERFORMANCE.dbPass'};

my $collector_thread_number = $confHash{'PERFORMANCE.collector_thread_number'};
my $db_persister_thread_number = $confHash{'PERFORMANCE.db_persister_thread_number'};

# -- Ionix credentials
my $dmUsername = $confHash{'Access.dmUsername'};
my $dmPassword = $confHash{'Access.dmPassword'};
my $apmManager = $confHash{'IONIX.apmManager'};
my $samManager = $confHash{'IONIX.samManager'};

my $broker = $confHash{'IONIX.broker'};

# -- log file variables
our $log_file_name_template = $confHash{'PERFORMANCE.collector_log_file'};
our $log_dir = $confHash{'PERFORMANCE.log_dir'};
our $log_policy="both";

print_log("Info:", "Start collector with pid \"$$\"");

# -- calculate time from start
$script_start_time = time();

# -- connect to DB
if ($debug) { print_log("Info:", "Connecting to DB $database with credentials \"$dbUser, $dbPassword,\""); }
$dbh = dbConnect($dbUser, $dbPassword, $database, $hostname, $port, $dbType);

# -- get all checks
my $sql = "SELECT
instances.instance_class,
instances.instance_name,
nodes.sm_domain_name,
data_mapping.attribute_name,
data_mapping.instrumentation_key,
m_type.id AS m_type_id,
instances.instance_id
FROM
m_type
INNER JOIN data_mapping ON m_type.metric_id = data_mapping.id
INNER JOIN instances ON m_type.instance_id = instances.instance_id
INNER JOIN nodes ON instances.node_id = nodes.node_id";

# -- insert checks into queue
my $ref = get_table_hasharr_by_sql($sql);
$dbh->disconnect();
my %checks_hash = xArray2Hash($ref, "m_type_id");

# -- fill queue with hash keys
my $measurementsQ = Thread::Queue->new(keys %checks_hash);

# -- get count of all checks
my $checks_count = $measurementsQ->pending();

if ($checks_count == 0) {
	print_log("Info:", "Exit, no checks are defined in DB");
	exit;
}

# -- queue for handle processed data
my $db_persistQ = Thread::Queue->new();

# -- create threads for data collecting
for my $i (1 .. $collector_thread_number) {
	push @threads, threads->create(\&main, $measurementsQ, $i);
}

foreach my $thread (@threads) {
	$thread->join();
}

# -- Main
sub main {
	eval {
		if (&get_from_queue(@_) == -1) {
			if ($debug) { print_log("Info:", "Return from \"get_from_queue\" subroutine"); }
			foreach my $i (1 .. $db_persister_thread_number) {
				push @db_threads, threads->create(\&control_db, $db_persistQ, $i);
			}
			# -- run threads
			foreach my $thread (@db_threads) {
				$thread->join();
			}
		}
	} or do {
		print_log("Error:", "$@");
	}
}

sub get_from_queue {
	my ($q, $thr_num) = @_;
	$session = dmConnect($broker, $apmManager, $dmUsername, $dmPassword, "Connect by xPerformance");
	while (my $item = $q->dequeue()) {
		if ($debug) { print_log("Debug:", "Thread($thr_num): Get key \"$item\""); }
		my $left = $q->pending();
		# -- run collect sub function with number of check key and thread number
		&collect($item, $thr_num);
		if ($left == 0) {
			if ($debug) { print_log("Debug:", "Thread($thr_num): Queue is empty");}
			return -1;
		}
	}
}

sub collect {
	my ($k, $thr_num) = @_;
	my $time = time();
	my $found_i = 0;
	my $result = "";
	
	# -- get attributes from hash
	my $attribute_name = $checks_hash{$k}->{attribute_name};
	my $instrumentation_key = $checks_hash{$k}->{instrumentation_key};
	my $m_type_id = $checks_hash{$k}->{m_type_id};
	my $instance_class = $checks_hash{$k}->{instance_class};
	my $instance_id = $checks_hash{$k}->{instance_id};
	my $instance_name = $checks_hash{$k}->{instance_name};
	my $sm_domain_name = $checks_hash{$k}->{sm_domain_name};
	
	# $session = dmConnect($broker, $sm_domain_name, $dmUsername, $dmPassword, "Connect by xPerformance");
	
	# -- get object of instance by class and name
	my $obj = getSmObjByClass($session, $instance_class, $instance_name);
	return if (!$obj);
	
	# -- get attribute values
	if ($instrumentation_key ne "") {
		# -- get Attribute value from Instrumentation class
		my $instr = $obj->findInstrumentation($instrumentation_key);
		my $instrObj = getSmObjByName($session, $instr);
		if (!$instrObj) {
			print_log("Debug:", "Thread($thr_num): Can not get object to \"$instr\"");
			next;
		}
		my $value = getSmValue($instrObj, $attribute_name);
		if ($value ne "-1") {
			if ($debug) { print_log("Debug:", "Thread($thr_num): Value of attribute: \"$attribute_name\" for element \"$obj->{DisplayName} = \"$value\""); }
			$found_i = 1;
			$result = $value;
		}
		else {
			print_log("Debug:", "Thread($thr_num): Can not get value of \"$attribute_name\" from \"$instrObj->{DisplayName}\"");
		}
	}
	else {
		# -- get attribute from Top class
		my $value = getSmValue($obj, $attribute_name);
		return if ($value eq "-1");
		if ($debug) { print_log("Debug:", "Thread($thr_num): Value of attribute: \"$attribute_name\" for element \"$obj->{DisplayName} = \"$value\""); }
		$found_i = 1;
		$result = $value;
	}
	
	if ($found_i) {
		# -- insert sql hash to db persis queue
		my %results = ();
		$results{m_type_id} = $k;
		$results{value} = $result;
		$db_persistQ->enqueue(\%results);
	}
		
	my $finTime = time();
	my $take_time = $finTime - $time;
	
	if ($debug) { print_log("Debug:", "Thread($thr_num): Processed with $k"); }
	return;
}

sub control_db {
	eval {
		if (&write_db(@_) == -1) {
			if ($debug) { print_log("Info:", "Exit from program"); }
			my $script_finish_time = time();
			my $script_take_time = sec2human($script_finish_time - $script_start_time);
			
			print_log("Info:", "Script ran \"$script_take_time\" and pass \"$checks_count\" checks");
			exit;
		}
	} or do {
		print STDERR "$@";
	}
}

sub write_db {
	my ($q, $thr_num) = @_;
	$dbh = dbConnect($dbUser, $dbPassword, $database, $hostname, $port, $dbType);
	while (my $item = $q->dequeue()) {
		
		my $left = $q->pending();
		# -- write data to DB
		if ($debug) { print_log("Debug:", "DB_Thread($thr_num): Insert data for check \"$item->{m_type_id}\""); }
		 my $res = &xInsert_table_hash("measurements", $item);
		 if ($res != 1) {
		 	 print_log("Error:", "DB_Thread($thr_num): \"$res\"");
		 }
		 	 
		if ($left == 0) {
			if ($debug) { print_log("DB_Thread:", "Thread($thr_num): Queue is empty");}
			my $end_time = time();
			return -1;
		}
	}
}
