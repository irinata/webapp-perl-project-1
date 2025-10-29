package My::Exceptions;

use strict;
use warnings;

use Exception::Class ('DatabaseConnectionError');

use Exporter 'import';

our @EXPORT_OK = qw(My::Exceptions::DatabaseConnectionError);
our %EXPORT_TAGS = (all => \@EXPORT_OK);


sub DatabaseConnectionError::as_string {
  my $self = shift;
  return ref($self) . ": " . $self->error;
}

1;