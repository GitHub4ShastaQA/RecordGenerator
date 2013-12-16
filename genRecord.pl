#!/usr/bin/env perl

# Copyright 2010 - 2013 James Switzer <jswitzer@shastaqa.com>, ShastaQA
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

use Carp;
use Date::Calc qw{Delta_Days Add_Delta_Days Day_of_Week Today Date_to_Days};
use DBI;
use FindBin;
use Getopt::Long;
use List::Util qw{shuffle};
use Text::CSV;
use Text::Lorem::More qw(lorem);
use YAML qw{LoadFile};
use Data::GUID::Any 'guid_as_string';
use Math::Random::OO::Normal;
use 5.10.0;
use strict;

my %opts = (
    dbfile  => $FindBin::RealBin."/testData.sqlite3",
    number  => 30,
    output  => undef,
    config  => "./record.yaml",
	csv		=> 0,
	xml		=> 0,
	db		=> 1,
	append	=> 0,
);
GetOptions(
    "output=s"	=> \$opts{'output'},
    "config=s"	=> \$opts{'config'},
    "number=s"	=> \$opts{'number'},
    "database=s"=> \$opts{'dbfile'},
	"append"	=> \$opts{'append'},
    "format=s"	=> \&format,
    "help"	=> \&help,
);

my %record = ();
my ( $output, $table );
my $dbh = DBI->connect("dbi:SQLite:dbname=".$opts{'dbfile'},"","");
my $config = LoadFile($opts{'config'});
my @header = eval {map { $_->[0] } @$config } or die; 
@header = map {ref $_?@$_:$_} @header;
my %types = (
    Address	=> \&genAddress,
    Between	=> \&randBetween,
    BitVector	=> \&genBitVector,
    CrossJoinFile   => \&genCrossJoinFile,
    CrossJoinTable  => \&genCrossJoinTable,
    Cycle	=> \&genCycle,
    CycleSql	=> \&genCycleFromSql,
    Date	=> \&genDate,
    DateTime	=> \&genDateTime,
    Email	=> \&genEmail,
	FormatedNumber	=> \&genFormatedNumber,
	Gaussian	=> \&genGaussian,
	GUID	=> \&genGuid,
    List	=> \&genFromList,
    NestedList	=> \&genNestedList,
	NestedCycle => \&genNestedCycle,
    ListFile	=> \&genListFromFile,
    ListMinus	=> \&genListMinus,
    ListSql	=> \&genListFromSql,
    ListTable	=> \&genListFromTable,
    Name	=> \&genName,
    Phone	=> \&genPhone,
    Range	=> \&genRange,
	RelativeDate	=> \&genRelativeDate,
    Sequence	=> \&genSequence,
    SeqFile	=> \&genSequenceFromFile,
    SeqTable	=> \&genSeqFromTable,
    SSN		=> \&genSSN,
    Select	=> \&genSelect,
    Table	=> \&genFromTable,
    Text	=> \&genText,
    ShuffledTable   => \&genShuffledTable,
    Tree	=> \&genTree,
    TreeMF	=> \&genTreeMF,
    Compound	=> \&genCompound,
    Debug	=> \&genDebug,
);

initOutput();

my $state = {};
RECORD: for(1 .. $opts{'number'})
{
    %record = ();
    for(@$config)
    {
		my @args = @$_;
		my $name = shift @args or die "No Field Name";
		my $type = shift @args or die "No Field Type";
		die "Invalid type: $type" unless exists $types{$type};

		@args = map 
		    { 
				s/(?<!\\)\$(\w*)/$record{$1}/ge; 
				s/\\\$/\$/g;
				$_; 
		    } @args;

		if(ref $name)
		{
			@record{@$name} = $types{$type}->($state, @args);
		} else {
			( $record{$name} ) = $types{$type}->($state, @args);
		}
    }

	output(\%record);
    progress($_, $opts{'number'}) if $opts{'number'} > 100;
}

print "\n";
exit(0);

=head1 NAME

genRecord - Generate test data

=head1 SYNOPSIS

	genRecord -c configFile.yaml -o database.sqlite:tableName -n 50

=head1 DESCRIPTION

The config file is a yaml file that contains an array of field definitions. In 
general the field definitions consist of an array that contains a field name or
array of field names, a generator name, and arguments to the generator. The 
fields are generated in the order that they are specified. The results of 
earlier fields are accesible for use as arguments to later generators. They are
accessed using the $ notation where $fieldName is replaced with the value of 
the field.

=head2 Command Line Arguments

=over

=item B<-c>

The path to the config file to be used

=item B<-o>

The output file. For database output this should be the path to the database 
file followed by a colon and the table name. eg path/foo.sqlite:bar

=item B<-n>

The number of records to generate. Default 30.

=item B<-a>

Append records. If present then the table will not be dropped and recreated. 

=head2 Generators 

=over

=item B<Name>

	[Field, Name, Male]

This generator takes one optional argument for the type of name to 
generate. The default is to generate a surname. Passing "Male" or 
"Female" will generate personal names of the given gender. Passing
"Lorem" or "AN" will generate more adbstract names

=cut

sub genName
{
    my $static = shift;
    return genFromTable($static, 'Surname')	unless defined $_[0];	#Surname
    return genFromTable($static, 'MaleName')	if $_[0] =~ /^M/i;	#Male
    return genFromTable($static, 'FemaleName')	if $_[0] =~ /^F/i;	#Female
    return lorem->name				if $_[0] =~ /^L/i;	#Lorem
    return ucfirst(genFromTable($static, "Adjective"))." ".
	    ucfirst(genFromTable($static, "Noun")) if $_[0] =~ /^AN|^Adj/;
    return genFromTable($static, 'Surname');
}

=item B<Table>

	[Field, Table, tableName, columnName]

Chose a random value from the default database

=cut

sub genFromTable
{
    my $static = shift;
    my $table = shift;
    my $column = shift || "*";
    croak "no table given" unless $table;

    my $tot = $dbh->selectall_arrayref(
	qq{select count(*) from $table})->[0][0];
    $tot = int(rand($tot));
    return $dbh->selectall_arrayref(
	qq{select $column from $table limit 1 offset $tot})->[0][0];
}

=item B<Address>

	[[Field1, Field2,...], Address, [City, State,...], Yes]
	[Field, Address, LongState, Yes]
	[Field, Address, Zip]

This generator can generate multiple fields. You can pass multiple field names
along with the types they should be filled with. Valid types are "Street",
"City", "State", "LongState", and "Zip". State is a two character abbreviation 
and LongState is the full state name. If the second argument is present a new
Address is generated, otherwise the cached address is used. The second form 
is deprecated.

=cut

sub genAddress
{
	my $s = shift;
	my $type = shift;
	if($_[0])
	{
		$s->{'Address'}{'Street'} = int(rand(10_000)+1) ." ".
		genFromTable($s, "StreetName");

		my $tot = $dbh->selectall_arrayref(
			qq{select count(*) from CityStateZip})->[0][0];
		$tot = int(rand($tot));
		my @stuff = @{ $dbh->selectall_arrayref(qq{
			SELECT City, LongState, Zip, State 
			FROM CityStateZip LIMIT 1 OFFSET $tot
			})->[0] };
		@{$s->{'Address'}}{qw{City LongState Zip State}} = @stuff;
	}
	
	if(ref $type)
	{
		return map {$s->{'Address'}{$_}} @$type;
	} else {
		return $s->{'Address'}{$type};
	}
}

=item B<List>

	[Field, List, Value, Value,...]

Returns a random value from the given list of values.

=cut

sub genFromList
{
    my $static = shift;
    my $tot = scalar(@_);
    return "" unless $tot;
    my $res = $_[int(rand($tot))];
    if(ref $res)
    {
	my @args = @$res; 
	my $type = shift @args or die "No Nested Field Type";
	@args= map {
		s/(?<!\\)\$(\w*)/$record{$1}/ge;
		s/\\\$/\$/g;
		$_; 
	    } @args;
	return $types{$type}->($static, @args);
    } else {
	return $res;
    }
}

=item B<NestedList>

	[[Field1, Field2,...], NestedList, [value, value,...], [value, value,...],...]

=cut

sub genNestedList
{
    my $static = shift;
    my $tot = scalar(@_);
    return "" unless $tot;
    my $res = $_[int(rand($tot))];

    if(ref $res)
    {
	return @$res;
    } else {
       return $res;
    }
}

=item B<Phone>

	[field, Phone]

Generates a phone number of the form (###) ###-####

=cut

sub genPhone
{
	shift;
	return sprintf("(%3d) %3d-%04d", int(rand(900)+100), int(rand(900)+100),
			int(rand(10_000)));
}

# US	(1-)?###-###-####
# US	(###) ###-####
# US	### ### ####
# US	##########
# US	1? (###) ### ####
# US	[2-9][0-8]\d [2-9]\d\d \d{4}
# AU	(##) #### ####
# AU	#### ### ###
# CA	(###) ###-####
# CN	(###)########
# CN	(####)########
# CN	#### ####
# DE	###/#######
# GB	### #### ####
# RU	(###) ###-##-##

=item B<FormatedNumber>

	[field, FormatedNumber, ###-###-####, ###-####,...]

Given a list of format strings this generator will pick one and replace all the
#'s with random digits

=cut

# [field, FormatedNumber, ###-###-####, ###-####,...]
sub genFormatedNumber
{
	shift;
	my $format = $_[int(rand(scalar(@_)))];
	$format =~ s/#/int(rand(10))/eg;
	return $format;
}

=item B<SSN>

	[Field, SSN]

Generates a Social Security Number

=cut 

sub genSSN
{
    my $static = shift;
    my $res;
    do
    {
	#$res = sprintf("%03d-%02d-%04d", int(rand(899)+1), int(rand(99)+1),
	#    int(rand(9_999)+1));
	$res = sprintf("%03d-%02d-%04d", int(rand(699)+1), int(rand(99)+1),
            int(rand(9_999)+1));
	redo if( $res =~ /^666-/ );
    };
    return $res;
}

=item B<Date>

	[Field, Date, mm/dd/yyyy, mm/dd/yyyy]
	[Field, Date, mm/dd/yyyy, mm/dd/yyyy, True]

This will generate dates of the form mm/dd/yyyy between the given dates. If
the optional third argument is passed and is true then the date is checked to
ensure it is a weekday. This may cause dates to be slightly out of range if the
end date is on a weekend.

=cut

sub genDate
{
    my $static = shift;
    my $start = shift;
    my $end = shift;
    my $weekday = shift;
    my $delta = Delta_Days((split /\//, $start)[2,0,1], (split /\//, $end)[2,0,1]);
    $delta = randBetween($static, 0,$delta+1);
    my @date = Add_Delta_Days((split /\//, $start)[2,0,1], $delta);
    if($weekday)
    {
        @date = Add_Delta_Days(@date, 2) if(Day_of_Week(@date) > 5);
    }
    return sprintf(q{%d/%d/%d}, $date[1], $date[2], $date[0] );
}

=item B<DateTime>

	[Field, Date, mm/dd/yyyy, mm/dd/yyyy]

This will generate dates of the form yyyy-mm-dd-00-00-00 between the given dates.

=cut

sub genDateTime
{
    my $static = shift;
    my $start = shift;
    my $end = shift;
    die "Invalid date format: $start" unless($start =~ m!^\d+/\d+/\d+$!); 
    die "Invalid date format: $end" unless($end =~ m!^\d+/\d+/\d+$!);
    my $delta = Delta_Days((split /\//, $start)[2,0,1], (split /\//, $end)[2,0,1]);
    $delta = randBetween($static, 0, $delta+1);
    my @date = Add_Delta_Days((split /\//, $start)[2,0,1], $delta);

    # 0=>year, 1=>month, 2=>day
    return sprintf(q{%02d-%02d-%02d-00-00-00}, $date[0], $date[1], $date[2] );
}

=item B<RelativeDate>

	[Field, RelativeDate, Date, MinOffset, MaxOffset, Weekday?]

If date is not in the form mm/dd/yyyy it is assumed to be today's date. Min 
and max offset are a range of days relative from the given date that the
returned date will be in. Negative numbers of days can be used. If Weekday is
present and true then the date returned will be "rounded up" to the next 
weekday if it is on a weekend. This may put it slightly out of range.

=cut 

sub genRelativeDate
{
	my $s = shift;
	my $date = shift;
	my $min = shift;
	my $max = shift;
	my $weekday = shift;
	$date =~ s/"//g;
	
	my @date = Today();
	@date = (split /\//, $date)[2,0,1] if($date =~ m!^\d+/\d+/\d+$!);
    
    if($max eq "today")
    {
        $max = Delta_Days( @date, Today() );
    }
    
	my $delta = randBetween($s, $min, $max);
	
	@date = Add_Delta_Days(@date, $delta);
    if($weekday)
    {
        @date = Add_Delta_Days(@date, 2) if(Day_of_Week(@date) > 5);
    }
    return sprintf(q{%d/%d/%d}, $date[1], $date[2], $date[0] );
}

=item B<Sequence>

	[Field, Sequence, name, start]

The sequence generator returns an asending numeric sequence of values. Both 
name and start are optional. Name is used to identify sequences if you have
multiple independent sequences in a record. If it is not specified the default
sequence is used. Start is the starting value of the sequence. See 
L<Auto-increment|perlop/"Auto-increment and Auto-decrement"> for details on how
sequences are generated.

=cut

sub genSequence
{
    my $static = shift;
    my $sequence = shift || 'Default';
    my $start = shift || 1;
    $static->{'sequence'}{$sequence} = $start 
	    unless exists $static->{'sequence'}{$sequence};

    return $static->{'sequence'}{$sequence}++;
}

=item B<SequenceFromFile>

	[Field, SequenceFromFile, File, Column, True]

This returns the given column from each row of a csv file. File is of course 
the name of the csv file. The third optional argument if true causes a new row
to be fetched from the file.

=cut

sub genSequenceFromFile
{
    my $static = shift;
	state $s;
    my $file = shift;
    my $col = shift;

    unless(exists $s->{$file}) # open the file
    {
	$s->{$file}{'csv'} = Text::CSV->new();
	open $s->{$file}{'fh'}, '<', $file or die "could not open file: $!";
	$s->{$file}{'header'} = $s->{$file}{'csv'}->getline($s->{$file}{'fh'});
    }

    if($_[0]) # get a new line
    {
	$s->{$file}{'csv'}->column_names($s->{$file}{'header'});
	$s->{$file}{'line'} = $s->{$file}{'csv'}->getline_hr($s->{$file}{'fh'});
	exit(1) if $s->{$file}{'csv'}->eof();
	exit(1) unless defined $s->{$file}{'line'}{'ID'} and $s->{$file}{'line'}{'ID'};
    }

    return $s->{$file}{'line'}{$col};
}

=item B<ListFromFile>

	[[Field, Field,...], ListFromFile, File, Column, Column,...]

Returns a set of columns from a random row from a csv file. The file argument 
is the name of the csv file. The remaining arguments are column names.

=cut

sub genListFromFile
{
    my $static = shift;
	state $s;
    my $file = shift;
    my @col = @_;

    unless(exists $s->{$file}) # open the file
    {
	my $csv = Text::CSV->new();
	open $s->{$file}{'fh'}, '<', $file or die "could not open file: $!";
	$s->{$file}{'header'} = $csv->getline($s->{$file}{'fh'});
	$csv->column_names($s->{$file}{'header'});

	until($csv->eof())
	{
	    my $line = $csv->getline_hr($s->{$file}{'fh'});
	    #next unless defined $line->{'ID'} and $line->{'ID'};
	    next unless (ref $line) =~ /HASH/;
	    push @{$s->{$file}{'list'}}, $line;
	}
    }
    
    my $tot = int(rand(scalar(@{$s->{$file}{'list'}})));
	my %row = %{$s->{$file}{'list'}[$tot]};
    return @row{@col};
}

=item B<Range>

	[Field, Range, Start, End]

Returns a random element from the list of items between Start and End. This 
uses the perl '..' operator to generate the list.

=cut

sub genRange
{
    my $static = shift;
    my $begin = shift;
    my $end = shift;

    genFromList($static, $begin .. $end);
}

=item B<Between>

	[Field, Between, Start, End]

Returns a random integer between the start and end values.

=cut

sub randBetween
{
    my $static = shift;
    my $min = shift;
    die "no min value given" unless defined $min;
    my $max = shift;
    die "no max value given" unless defined $max;

    return int(rand($max-$min+1)+$min);
}

=item B<Gaussian>

	[Field, Gaussian, mean, stddev]

Returns a random numbers that follow the given Gaussian distribution.

=cut

sub genGaussian
{
	my $static = shift;
	my $mean = shift // 0;
	my $stddev = shift // 1;
	my $gen = Math::Random::OO::Normal->new($mean, $stddev);
	
	return $gen->next();
}

=item B<Tree>

	[[field, ...], Tree, [field, ...], [root, ...], num, ...

This generator is used to generate trees of data in the form of adjacency 
lists. The first set of fields is the names of the parent fields. This is
followed by a list of values to for the parent values at the top level of the
tree. This is folowed by a lit of integers that specify how many nodes to put
in the given level of the tree. Once the specified levels are filled the
remaining records are put in the bottom level. 

=cut

sub genTree
{
	my $state = shift;
	state $s = {};
	my $field = shift or die;
	my $root = shift;
	
	unless(exists $s->{$field}{'PrevLevel'})
	{
		$s->{$field}{'PrevLevel'} = [$root];
	}
	
	$s->{$field}{'Count'}++;
	if( defined $_[($s->{$field}{'Level'} || 0)] and 
			($s->{$field}{'Count'} || 0) > $_[($s->{$field}{'Level'} || 0)])
	{
		$s->{$field}{'Count'} = 1;
		$s->{$field}{'Level'}++;
		$s->{$field}{'PrevLevel'} = $s->{$field}{'ThisLevel'};
		$s->{$field}{'ThisLevel'} = [];
	}
	
	if(ref $field)
	{
		push @{$s->{$field}{'ThisLevel'}}, [ @record{@$field} ];
	} else {
		push @{$s->{$field}{'ThisLevel'}}, $record{$field};
	}
	
	my $val = int(rand(@{$s->{$field}{'PrevLevel'}}));
	$val = $s->{$field}{'PrevLevel'}[$val];
	return ref($val) ? @$val : $val;
}

=item B<TreeMF>

don't use.

=cut

sub genTreeMF
{
    my $s = shift;
    my $parentField = shift or die;
    my $rootValue = shift;
    my $new = shift;
    $new = 0 if($new =~ /false/i);

    unless(exists $s->{'TreeMF'}{'PrevLevel'}{$parentField})
    { 
	$s->{'TreeMF'}{'PrevLevel'}{$parentField} = [$rootValue]; 
    }
    
    unless(exists $s->{'TreeMF'}{'LevelSize'})
    {
	@{$s->{'TreeMF'}{'LevelSize'}} = @_; 
    }
    
    if($new)
    {
	$s->{'TreeMF'}{'Count'}++;
	if( defined $s->{'TreeMF'}{'LevelSize'}[$s->{'TreeMF'}{'Level'}] and
		$s->{'TreeMF'}{'Count'} > 
			$s->{'TreeMF'}{'LevelSize'}[$s->{'TreeMF'}{'Level'}])
	{
	    $s->{'TreeMF'}{'Count'} = 1;
	    $s->{'TreeMF'}{'Level'}++;
	    $s->{'TreeMF'}{'PrevLevel'} = $s->{'TreeMF'}{'ThisLevel'};
	    $s->{'TreeMF'}{'ThisLevel'} = {};
	}
    }
    
    push @{$s->{'TreeMF'}{'ThisLevel'}{$parentField}}, $record{$parentField};

    if($new)
    { 
	$s->{'TreeMF'}{'Rand'} = int(rand(scalar(
		    @{$s->{'TreeMF'}{'PrevLevel'}{$parentField}})));
    }

    return $s->{'TreeMF'}{'PrevLevel'}{$parentField}[$s->{'TreeMF'}{'Rand'}];
}

=item B<Compound>

	[field, Compound, format, [gen, arg, arg], ...

This is a thin wrapper around sprintf. See its documentation for details of the
format string. The remainder of the arguments are lists containing generators
and their arguments. The results of these are passed in to be formatted.

=cut

sub genCompound
{
    my $static = shift;
    my $format = shift;
    
    return sprintf $format, map {
	if(ref $_)
	{
	    my @args = @$_; 
	    my $type = shift @args or die "No Nested Field Type";
	    @args= map {
		    s/(?<!\\)\$(\w*)/$record{$1}/ge;
		    s/\\\$/\$/g;
		    $_; 
		} @args;
	    $types{$type}->($static, @args);
	} else {
	    $_;
	}
    } @_;
}

=item B<Select>

	[Field, Select, FieldName, {Key: Value, Key: [Gen, Arg, Arg],...}]

FieldName is the name of a prevously generated field. The value of that field
is used to lookup either a fixed value or a generator and its arguments from
the hash. Fixed values are simply returned. Generators are run and their
results are returned.

=cut

sub genSelect
{
	my $static = shift;
	my $field = shift;
    my $gen;
    if ( exists $_[0]->{$record{$field}} )
	{
        $gen = $_[0]->{$record{$field}};
    }
    else
    {
        $gen = $_[0]->{'default'};
    }
	if(ref $gen)
	{
		my @args = @$gen; 
	    my $type = shift @args or die "No Nested Field Type";
	    @args= map {
		    s/(?<!\\)\$(\w*)/$record{$1}/ge;
		    s/\\\$/\$/g;
		    $_; 
		} @args;
	    return $types{$type}->($static, @args);
	} else {
		return $gen
	}
}

=item B<BitVector>

	[Field, BitVector]
	[Field, BitVector, bits]
	[Field, BitVector, bits, none, one, some, most, all]

Returns a random string of ones and zeros. The length is specified by bits, the
default is one. The function is weighted so that strings of all ones or zeros
and strings with a single one or zero come up more often then is statically 
likely. You can specify the relative weights of the special cases.

=cut

sub genBitVector
{
    my $static = shift;
    my $width = shift || 1;
    my @weight = (5,5,80,5,5);
    @weight = @_ if(defined $_[0] && defined $_[1] && defined $_[2] && defined $_[3] && defined $_[4]);

    my $total = 0;
    $total += $_ foreach @weight;
    
    my @code;

    push @code, sub # none
    { return "0" x $width; };

    push @code, sub # one
    { 
	my $string = "0" x $width;
	substr($string, int(rand($width)), 1, "1"); 
	return $string;
    };

    push @code, sub # some
    {
    my $string;
	while(1)
	{
	    $string = join '', map { rand 1 > .5 ? 1 : 0 } (1 .. $width);
	    last if $width < 4; 
	    next if $string =~ /^0*$/;
	    next if $string =~ /^0*10*$/;
	    next if $string =~ /^1*01*$/;
	    next if $string =~ /^1*$/;
	    last;
    }
    return $string;
    };

    push @code, sub # most
    { 
	my $string = "1" x $width;
	substr($string, int(rand($width)), 1, "0"); 
	return $string;
    };

    push @code, sub # all
    { return "1" x $width; };

    my $rand = rand $total;
    foreach(0 .. $#code)
    {
	$rand -= $weight[$_];
	return $code[$_]->() if($rand < 0);
    }
}

=item B<Email>

	[Field, Email]

Returns something that looks like an email.

=cut

sub genEmail
{
    return lorem->email;
}

=item B<Text>

	[Field, Text, num, type]

Returns Lorem Ipsum text. Type can be one of Alpha characters, Numeric Characters, Words, Sentences, Paragraphs, or
URLs. Num is the number of things to generate.

=cut

sub genText
{
    my $static = shift;
    my $num = shift;
    my $type = shift // "";

    given($type)
    {
	when(/^W/i) { return lorem->words($num); }
	when(/^S/i) { return lorem->sentences($num); }
	when(/^P/i) { return lorem->paragraphs($num); }
	when(/^U/i) { return lorem->httpurl; }
	when(/^AN|Alphanum/i) { return alnum($num); }
    when (/^alphaLower/i) { return alphaLower($num); }
    when (/^alphaUpper/i) { return alphaUpper($num); }
   	when(/^alpha/i) { return alphaMixed($num); }
	default { return lorem->paragraph; }
    }
}

sub alnum
{
	my $num = shift;
	my $ret = "";
	
	foreach(1..$num)
	{
		$ret .= ('A'..'Z','a'..'z',0..9)[int(rand(62))];
	}
	return $ret;
}

sub alphaMixed
{
	my $num = shift;
	my $ret = "";
	
	foreach(1..$num)
	{
		$ret .= ('A'..'Z','a'..'z')[int(rand(52))];
	}
	return $ret;
}

sub alphaLower
{
	my $num = shift;
	my $ret = "";
	
	foreach(1..$num)
	{
		$ret .= ('a'..'z')[int(rand(26))];
	}
	return $ret;
}

sub alphaUpper
{
	my $num = shift;
	my $ret = "";
	
	foreach(1..$num)
	{
		$ret .= ('A'..'Z')[int(rand(26))];
	}
	return $ret;
}


=item B<Cycle>

	[field, Cycle, [thing1, thing2...]]

Returns the items in the given list in turn. When the end is reached it starts
over.

=cut

sub genCycle
{
    my $static = shift;
    state $s;
    my $list = shift;
    $s->{$list} = $list unless exists $s->{$list};

    my $value = shift @{$s->{$list}};
    push( @{$s->{$list}}, $value );

    return $value;
}

=item B<NestedCycle>

	[[f1, f2...], NestedCycle, [v1, v2], [[f2v1, f3v1], [f2v2, f3v2]...]...]

FIXME explain complicated spinny bits

=cut

sub genNestedCycle
{
	shift;
	state $s;
	my $carry = 1;
	my @return;
	
	for(reverse(@_))
	{
		unless(exists($s->{$_}))
		{ 
			$s->{$_} = 0; 
			$carry = 0; 
		}
		
		$s->{$_}++ if($carry);
		
		if($s->{$_} >= scalar(@$_))
		{
			$s->{$_} = 0;
			$carry = 1;
		} 
		else 
		{ $carry = 0; }
		
		if(ref $_->[$s->{$_}])
		{
			unshift @return, @{ $_->[$s->{$_}] };
		}
		else
		{
			unshift @return, $_->[$s->{$_}];
		}
	}
	
	return @return;
}

=item B<Cross>

	[[field1, field2...], Cross, [file.csv, field, field], []...]

Create a cartesian product of the given files are return it a row at a time.

=cut

sub genCrossJoinFile
{
    my $static = shift;
    state $s;
    
    unless(exists $s->{$_[0]})
    {
	my @rows;
	foreach my $file (@_)
	{
	    push @rows, getRows(@$file);
	}

	$s->{$_[0]} = [ crossJoin([], @rows) ];
    }

    #my $val = shift @{$s->{$_[0]}};
    my $val = splice @{$s->{$_[0]}}, int(rand(scalar(@{$s->{$_[0]}}))), 1;

    return ref $val ? @$val : $val ;
}

sub getRows
{
    my $file = shift @_;
    my $fh;
    my $list;
    my $csv = Text::CSV->new();
    open $fh, '<', $file or die "could not open file: $!";
    $csv->column_names($csv->getline($fh));

    until($csv->eof())
    {
	my $line = $csv->getline_hr($fh);
	next unless (ref $line) =~ /HASH/;
	push @{$list}, [ map {$line->{$_}} @_ ];
    }
    return $list;
}

sub crossJoin
{
    my $prefix = shift;
    my $row = shift;
    my @rows = @_;
    my @results;

    return $prefix unless(ref $row);

    foreach(@$row)
    {
	push @results, crossJoin([@$prefix, @$_], @rows); 
    }

    return @results;
}

=item B<ListTable>

	[[Field], ListTable, foo.sqlite, table, field, field...]

Returns fields from random row of the specified table. The database may be 
specified as '-' in which case the output database will be used.

=cut

sub genListFromTable
{
    my $static = shift;
    state $s;
    my $db = shift;
    my $table = shift;

    unless(exists $s->{$db})
    {
	$s->{$db}{'dbh'} = $output if($db eq '-');
	$s->{$db}{'dbh'} = DBI->connect("dbi:SQLite:dbname=".$db,"","") unless exists $s->{$db}{'dbh'};
    }

    return $s->{$db}{'dbh'}->selectrow_array(
		"select ".join(', ', @_)." from ".$table." ORDER BY RANDOM() LIMIT 1");
}

=item B<SeqTable>

	[[Field], SeqTable, foo.sqlite, table, field,...]

Returns the fields of each row in order from the given table. If the database 
is '-' then the output database will be used.

=cut

sub genSeqFromTable
{
    my $static = shift;
    state $s;
    my $db = shift;
    my $table = shift;

    unless(exists $s->{$db})
    {
	$s->{$db}{'dbh'} = $output if($db eq '-');
	$s->{$db}{'dbh'} = DBI->connect("dbi:SQLite:dbname=".$db,"","") unless exists $s->{$db}{'dbh'};
    }

    unless(exists $s->{$db}{$table}{'count'})
    {
	$s->{$db}{$table}{'count'} = -1;
    }

    $s->{$db}{$table}{'count'}++;
    return $s->{$db}{'dbh'}->selectrow_array( 
		"select ".join(', ', @_)." from ".$table." limit 1 offset ".$s->{$db}{$table}{'count'});
}

=item B<ShuffledTable>

	[[Field], ShuffledTable, foo.sqlite, table, field...]

Returns the given fields from the specified table in a random order. If the 
database is given as '-' then the output database is used. When all rows of the
table have been returned then new rows will be returned compleatly randomly.

=cut

sub genShuffledTable
{
    my $static = shift;
    state $s;
    my $db = shift;
    my $table = shift;

    unless(exists $s->{$db})
    {
	$s->{$db}{'dbh'} = $output if($db eq '-');
	$s->{$db}{'dbh'} = DBI->connect("dbi:SQLite:dbname=".$db,"","") unless exists $s->{$db}{'dbh'};
    }

    unless(exists $s->{$db}{$table})
    {
	my $tot = $s->{$db}{'dbh'}->selectrow_array(qq{select count(*) from $table});
	$s->{$db}{$table} = [ shuffle( 0 .. ($tot-1) ) ];
    }

    my $row = shift @{ $s->{$db}{$table} };
    $static->{'Debug'} = $row;
    # return unless defined $row;
    return genListFromTable($static, $db, $table, @_) unless defined $row;
    return $s->{$db}{'dbh'}->selectrow_array(
		"select ".join(', ', @_)." from ".$table." limit 1 offset ".$row);
}

=item B<ListMinus>

	[Field, ListMinus, remove, thing, thing...]

Returns a value from the list that does not match the removed value. This is
useful with variable substituion to remove the current value of a field when
you need to give it a new different value.

=cut

sub genListMinus
{
    my $static = shift;
    my $remove = shift;
    my @list = grep { $_ ne $remove } @_;
    my $tot = scalar(@list);
    return "" unless $tot;
    return $list[int(rand($tot))];
}

=item B<CrossJoinTable>

	[[Field, Field], CrossJoinTable, db.sqlite, [table, field...], [table, field...]...]

Returns the cartesian product of the given tables. If the database is '-' then
the ouput database is used.

=cut

sub genCrossJoinTable
{
    my $static = shift;
    state $s;
    my $db = shift;
    my @table;
    my @field;
    my $query;

    unless(exists $s->{$db})
    {
	$s->{$db}{'dbh'} = $output if($db eq '-');
	$s->{$db}{'dbh'} = DBI->connect("dbi:SQLite:dbname=".$db,"","") unless exists $s->{$db}{'dbh'};
    }

    unless(exists $s->{$_[0]})
    {
	foreach(@_)
	{
	    my @arg = @$_;
	    my $table = shift @arg;
	    push @table, $table;
	    push @field, map { $table.".".$_ } @arg;
	}
	
	$query = "SELECT ".join(', ', @field)." FROM ".join(', ', @table)." ORDER BY RANDOM()";
	$s->{$_[0]} = $s->{$db}{'dbh'}->selectall_arrayref($query);
    }

    return @{ shift @{ $s->{$_[0]} } };
}

=item B<ListSql>

	[[Field, ...], ListSql, db.sqlite, sql query, (Ignore, Redo, Die)]

Returns a random row of results from the given query. If the database is '-' 
then the ouput database is used.

=cut

sub genListFromSql
{
    my $static = shift;
    state $s;
    my $db = shift;
    my $query = shift;
    my $error = shift || "Ignore";

    unless(exists $s->{$db})
    {
	$s->{$db}{'dbh'} = $output if($db eq '-');
	$s->{$db}{'dbh'} = DBI->connect("dbi:SQLite:dbname=".$db,"","") unless exists $s->{$db}{'dbh'};
    }

    my $res = $s->{$db}{'dbh'}->selectall_arrayref($query);
    my $tot = @$res;
    return () if( not $tot and $error =~ /^I/i ); #REFACTOR ME
    if( not $tot and $error =~ /^R/i )
    {
	print "No rows returned: retring record ".$record{'ID'}."\n";
	redo RECORD;
    }
    die "No Data" if ( not $tot and $error =~ /^D/i );
    return @{ $res->[int(rand($tot))] };
}

=item B<CycleSql>

	[[Field, ...], CycleSql, db.sqlite, sql query, (Ignore, Redo, Die)]



=cut

sub genCycleFromSql
{
    my $static = shift;
    state $s;
    my $db = shift;
    my $query = shift;
    my $error = shift || "Ignore";

    unless(exists $s->{$db})
    {
	$s->{$db}{'dbh'} = $output if($db eq '-');
	$s->{$db}{'dbh'} = DBI->connect("dbi:SQLite:dbname=".$db,"","") unless exists $s->{$db}{'dbh'};
    }

    unless(exists $s->{$query}{'results'})
    {
    	$s->{$query}{'results'} = $s->{$db}{'dbh'}->selectall_arrayref($query);
    	$s->{$query}{'tot'} = @{$s->{$query}{'results'}};
    }
    
    return () if( not $s->{$query}{'tot'} and $error =~ /^I/i ); #REFACTOR ME
    if( not $s->{$query}{'tot'} and $error =~ /^R/i )
    {
	print "No rows returned: retring record ".$record{'ID'}."\n";
	redo RECORD;
    }
    die "No Data" if ( not $s->{$query}{'tot'} and $error =~ /^D/i );
    
    my $value = shift @{$s->{$query}{'results'}};
    push(@{$s->{$query}{'results'}}, $value);

    return @$value;
}

=item B<GUID>

	[Field, GUID, (prefix)]

Returns an GUID. If specified the give prefix is prepended.

=cut

sub genGuid
{
	my $s = shift;
	my $prefix = shift // "";
	return ($prefix?$prefix."-":"").guid_as_string();
}

sub genDebug
{
    my $static = shift;
    my $debug = $static->{'Debug'};
    $static->{'Debug'} = "";
    return $debug;
}

=back

=head1 TODO

=over

=item Unify the L</Date>, L</DateTime>, L<RelativeDate>, and other date related generators

=item Group and unify inline, db, and file generators

=back

=cut

sub help
{
	@ARGV = ('-F', $0);
	my $rc = eval {
		require Pod::Perldoc;
		Pod::Perldoc->run() || 0
	} || 0;
	print STDERR "\n", $@ if length($@);
	exit( $rc );
}

sub format
{
	my $name = shift;
	my $format = shift;
	given($format) {
		when (/^csv/i) { $opts{'csv'} = 1; $opts{'xml'} = $opts{'db'}  = 0; }
		when (/^db/i) { $opts{'db'}  = 1; $opts{'xml'} = $opts{'csv'} = 0; }
		when (/^database/i) { $opts{'db'}  = 1; $opts{'xml'} = $opts{'csv'} = 0; }
		when (/^x/i) { $opts{'xml'} = 1; $opts{'csv'} = $opts{'db'}  = 0; }
		default { warn "unknown format"; }
	}
}

sub progress
{
    my $num = shift;
    return if( $num % 10 );
    print STDERR ".";
    return if( $num % 200 );
    print STDERR " ".$num." of ".$_[0]." records generated\n";
}


sub initOutput
{
	if($opts{'csv'})
	{
		if( $opts{'output'} )
		{
		    open $output, ">", $opts{'output'} or die "can't open file: $!";
		    #select OUT;
		}
		print $output join( ",", @header);
	}
	
	if($opts{'xml'})
	{}
	
	if($opts{'db'})
	{
		my($db);
		if( $opts{'output'} )
		{
		    $opts{'output'} =~ /^(.+\.sqlite)(?::(.+))?$/; # fix for C:\paths...
		    ($db, $table) = ($1,$2);
		    #($db, $table) = split /:/, $opts{'output'};
		} else {
			$db = "output.sqlite";
		}
		
		unless( defined $table )
		{
			($table) = $opts{'config'} =~ /([a-zA-Z0-9.]+).yaml/;
		 	$table ||= "data";
		}
		
		$output = DBI->connect("dbi:SQLite:dbname=".$db,"","");
		#die "!".$table."!\n";
		if($opts{'append'})
		{
			warn "Current schema must be a subset of existing schema";
		} else {
			$output->do(q{DROP TABLE IF EXISTS }.$table);
			$output->do(q{CREATE TABLE }.$table." (".join(', ', @header).')');
		}
	}
}

sub output
{
	my %record = %{$_[0]};
	if($opts{'csv'})
	{
		print $output "\n", join( ",", @record{@header} );
	}
	
	if($opts{'xml'})
	{}
	
	if($opts{'db'})
	{
	    my $q = q{INSERT INTO }.$table.q{ (}.join(', ', @header).q{) VALUES (}.
				join(', ', map '?', @header).q{)};
	    my $rv = $output->do($q, undef, @record{@header});
	    warn $q."\n".$output->errstr unless defined $rv;
	}
}
