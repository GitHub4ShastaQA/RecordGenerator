#!/usr/bin/env perl
package Record::Generator;

sub Const
{
	my $self = shift;
	
	return ref $_[0] ? @{$_[0]} : $_[0];
}


1;