package Record::Generator;
use strict;
use warnings;

=head1 Name

Record::Generator - Generate complex structured test data for data driven testing

=head1 Synopsis

	use Record::Generator;
	
	$table = Record::Generator->new(
			no_setup    => 0,
			no_teardown => 0,
			name        => 'stuff',
			output      => [db=>$dbh],
		);
	
	$table->name('TheTableName');
	print $table->name(), "\n";
	
	$table->output(db     => $dbh);
	$table->output(csv    => $fh);
	$tabel->output(custom => [sub {}, sub {}, sub {}]); # setup, output, teardown
	
	$table	->add_column('ColumnName', 'List', 'arg', 'arg', 'more args')
			->add_column('Column2', &Record::Generator::Cycle, 'thing', 'stuff', 'narf')
			->add_column('ColumnThree, sub { ... }, @args );
	
	$table->add_column(['City','State','Zip'], &Record::Generator::Address, 
		'City', 'LongState', 'ZipCode');
	$table->add_column(['ShippingStreet', 'ShippingCity', 'ShippingState], 
		'Address', 'Street', 'City', 'ShortState');
	
	$table->add_subtable( Record::Generator->new() 
			->name('SubTable')
			->add_column('thing', ...)
			->add_column('stuff', ...),
		0, 42);
	
	$table->generate(10);

=head1 Description



=head2 Methods

=head3 new([%args])

Constructs a new Record::Generator object

=cut

sub new
{
	my $class = shift || __PACKAGE__;
	
	my $self = {
		state       => undef,
		
		no_setup    => 0,
		no_teardown => 0,
		
		setup       => sub {},
		teardown    => sub {},
		output      => sub {},
		
		name        => undef,
		columns     => [],
		subtables   => [],
		
		parent      => undef,
		@_
	};
	
	return bless $self, $class;
}

=head3 name([$name])

Gets or sets the name of the table

=cut

sub name
{
	my $self = shift;
	my $arg = shift;
	
	$self->{'name'} = $arg if $arg;
	
	return $self->{'name'};
}

=head3 output()

sets the output method

=cut

sub output
{
	my $self = shift;
	my $type = shift;
	
	if($type eq 'custom')
	{
		my($setup, $output, $teardown) = @{$_[0]};
		die unless ref $setup eq 'CODE';
		die unless ref $output eq 'CODE';
		die unless ref $teardown eq 'CODE';
		
		$self->{'setup'}    = $setup;
		$self->{'output'}   = $output;
		$self->{'teardown'} = $teardown;
	} else {
		
		#FIXME configure output subs
		
	}
	return $self;
}

=head3 add_column($name, $generator, @args)

...

=cut

sub add_column
{
	my $self = shift;
	my $column = { state=>{}, meta => [] };
	$column->{'name'} = shift;
	$column->{'generator'} = shift;
	unless(ref $column->{'generator'} eq 'CODE')
	{
		#FIXME add magic here
	}
	$column->{'args'} = [@_];	
	
	push @{$self->{'columns'}}, $column;
	
	return $self;
}

=head3 add_subtable($table, $min[, $max])

create a subtable with between $min and $max rows for each row in the parent 
table.

=cut

sub add_subtable
{
	my $self = shift;
	my $subtable = {};
	$subtable->{'table'} = shift;
	$subtable->{'min'} = shift || die;
	$subtable->{'max'} = shift || $subtable->{'min'};
	
	push @{$self->{'subtables'}}, $subtable;
	
	return $self;
}

=head3 value([$name])

The result of calling this is only defined in the output callback.

Returns the value of column $name in the the current table or an ancestor 
table. If not specified it returns the contents of the current row as a 
hashref.

=cut

sub value
{
	my $self = shift;
	my $arg = shift;
	
	return $self->{'record'} unless defined $arg;
	return $self->{'record'}{$arg} if exists $self->{'record'}{$arg};
	return $self->{'parent'}->value($arg) if exists $self->{'parent'};
	return '$'.$arg;
}

=head3 generate($min[, $max])

Generates between $min and $max rows of data. If $max is ommited it generates 
exactly $min rows of data. What is returned depends on the output method.

=cut

sub generate
{
	my $self = shift;
	my $min = shift || die;
	my $max = shift || $min;
	
	_setup($self);
	
	my $ret = _generate($self, int(rand($max-$min+1)+$min));
	
	$ret = $self->{'teardown'}->($ret);
	
	return $ret;
}

sub _generate
{
	my $self = shift;
	my $count = shift;
	my @output;
	
	for(my $i=0; $i<$count; $i++ )
	{
		$self->{'record'} = {};
		
		foreach(@{$self->{'columns'}})
		{
			my @args = $_->{'args'};
			my $name = $_->{'name'};
			@args = map 
		    { 
				s/(?<!\\)\$(\w*)/$self->value($1)/ge; 
				s/\\\$/\$/g;
				$_; 
		    } @args;
			
			$self->{'state'} = $_->{'state'};
			
			if(ref $name)
			{
				@{$self->{'record'}}{@$name} = $_->{'generator'}($self, @args);
			} else {
				( $self->{'record'}{$name} ) = $_->{'generator'}($self, @args);
			}
			
			$_->{'state'} = $self->{'state'}; #FIXME $_ after function call
		}
		
		foreach(@{$self->{'subtables'}})
		{
			my $name = $_->{'name'};
			my $ret = _generate($_->{'table'}, int(rand($_->{'max'}-$_->{'min'}+1)+$_->{'min'}));
			$self->{'record'}{$name} = $ret if $ret;
		}
		
		my $ret = $self->{'output'}->($self->{'record'});
		push @output, $ret if $ret;
	}
	
	return \@output if @output;
}

sub _setup {
	my $self = shift;
	my $setup = shift || $self->{'setup'};
	
	return if $self->{'no_setup'};
	$setup->($self->name, @{$self->{'columns'}});
	
	foreach(@{$self->{'subtables'}})
	{
		_setup($_->{'table'}, $setup);
	}
}


1;
