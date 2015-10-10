#!/usr/bin/perl

# Public Domain: Can be used, modified and distributed without any restriction
# Lubomir Rintel <lkundrak@v3.sk>, 2014, 2015

use strict;
use warnings;

use URI;
use URI::Escape;
use JSON;
use LWP::UserAgent;
use Database::DumpTruck;

my $this_year = 1900 + [localtime]->[5];
my $root = new URI 'http://old.digitalnemesto.sk/';
my $ua = new LWP::UserAgent;
my $dt = new Database::DumpTruck ({ dbname => 'data.sqlite' });

# We assign numerical ids to these so that we sort them into a fixed order
# for the purposes of keeping track of where we've left
my @tabs = (
	{ id => 0, name => 'invoicesd' },
	{ id => 1, name => 'invoiceso' },
	{ id => 2, name => 'orders' },
	{ id => 3, name => 'contracts' },
);

# Sorting helper, for record-tracking purposes
sub idsort { sort { $a->{id} <=> $b->{id} } @_; }

# JSON RPC
sub call
{
	my $call = shift;

	# query_form-formatted params are passed in path component
	my $params = new URI;
	$params->query_form (procedure => $call, @_);
	$params->opaque =~ /.(.*)/; # strip leading ? from query params
	my $uri = new URI ("/getjsondata/$1")->abs ($root);
	my $time = time;

	# Backend is known to return incomplete responses from time to time
	my ($response, $response2);
	my $retries = 10;
	while ($retries--) {
		if ($response) {
			warn "Retry: Inconsistent response for GET $uri";
			sleep 1;
		}

		# First try
		$uri->query_form (['dojo.preventCache' => $time++]);
		$response = $ua->get ($uri);

		# Verify
		$uri->query_form (['dojo.preventCache' => $time++]);
		$response2 = $ua->get ($uri);

		last if length $response->decoded_content == length $response2->decoded_content
			and length $response->decoded_content >= 14;
	}
	die $response->status_line unless $response->is_success;
	warn 'Out of tries' unless $retries;

	my $content = $response->decoded_content;

	# This resource used to return HTML with Content-Type: application/json:
	# http://www.digitalnemesto.sk/getjsondata/procedure=getinvoicesd&idCity=508250000&year=2013?dojo.preventCache=1406818754
	unless ($content =~ /^[\[{]/) {
		warn "Skipping: Not a JSON response for GET $uri";
		return ();
	}

	# https://rt.cpan.org/Ticket/Display.html?id=97558
	warn "Bad tabs for GET $uri" if $content =~ s/\t/ /g;

	# "nazov":"Zmluva na zabezpečenie pozície " supervízora výrob"
	# "nazov":"Dodatok č.2 k Zmluve o dielo "Keltská ul.""
	while ($content =~ s/"([^{}:,\[\\]*)"([^{}:,\[]*)"/"$1\\"$2"/g) {
		warn "Bad quoting for GET $uri";
	};

	return @{new JSON::XS->utf8->relaxed->decode ($content)->{items}};
}

# Format into database
sub fmt
{
	# Merge
	my %data = map { %$_ } @_;

	# Flatten
	foreach my $key (keys %data) {
		$data{$key} = $data{$key}{_value} if ref $data{$key} eq 'HASH';
		$data{$key} = join "\n", @{$data{$key}} if ref $data{$key} eq 'ARRAY';
		delete $data{$key} if ref $data{$key};
	}

	return \%data;
}

# Walk a single tab for given city/year. Resuming where we left.
sub dotab
{
	my $tab = shift;
	my $partner = shift;
	my $year = shift;

	my $last_var = "$year.$partner->{id}.$tab->{id}.last";
	my $last_id = eval { $dt->get_var ($last_var) } || 0;

	foreach my $item (idsort(call ("get$tab->{name}", idMesto => $partner->{id}, year => $year))) {

		# Already seen this
		next unless $item->{id} > $last_id;

		my ($details) = call ("get$tab->{name}detail", idMesto => $partner->{id}, id => $item->{id});
		my $entry = fmt ($item, $details, { mesto => $partner->{name},
			year => $year, @_ });

		$dt->insert($entry, $tab->{name});
		$dt->save_var ($last_var, $item->{id});
	}
}

# Start the ball rolling
foreach my $year (2013..$this_year) {
	foreach my $tab (idsort(@tabs)) {
		foreach my $partner (idsort(call ('getpartners'))) {
			dotab ($tab, $partner, $year);
		}
	}
}
