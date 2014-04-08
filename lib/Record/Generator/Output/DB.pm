#!/usr/bin/env perl

package Record::Generator::Output::DB;


sub setup
{
	my $dbh = shift;
	my $setup = sub
	{
		my $table = shift;
		my @header = map {
			$_->{'name'}
		} @_;
		
		$dbh->do(q{DROP TABLE IF EXISTS }.$table);
		$dbh->do(q{CREATE TABLE }.$table." (".join(', ', @header).')');
	};
	
	my $output = sub
	{
	    my $q = q{INSERT INTO }.$table.q{ (}.join(', ', @header).q{) VALUES (}.
				join(', ', map '?', @header).q{)};
	    my $rv = $dbh->do($q, undef, @record{@header});
	    warn $q."\n".$output->errstr unless defined $rv;
	};
	
	my $teardown = sub
	{
		return;
	};
	
	return($setup, $output, $teardown);
}

1;