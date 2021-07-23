use strict;
use warnings;
use ExtUtils::Installed;

my $inst = ExtUtils::Installed->new();
print join "\n" ,$inst->modules();
