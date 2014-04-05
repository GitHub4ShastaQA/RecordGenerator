#usr/bin/env perl

use strict;
use warnings;

use Test::More;

use Record::Generator;


# isa_ok(Record::Generator->new, 'Record::Generator')
new_ok('Record::Generator');

done_testing();