package My::DB::Init;

use strict;
use warnings;
use feature qw(signatures);

use Try::Tiny;
use Path::Tiny;
use FindBin;

use Exporter 'import';

our @EXPORT_OK = qw(createSchema);
our %EXPORT_TAGS = (all => \@EXPORT_OK);


sub getSqlCommands($file) {
  my @commands = ();

  try {
    my $content = path($file)->slurp;
    $content =~ s/\n+$//;
    @commands = split(/;/, $content);
  } catch {
    warn("Error reading $file: $!");
  };

  return \@commands;
}

sub doCommandsOf($dbh, $file) {
  my $commands = getSqlCommands($file);
  return if !@$commands;

  $dbh->{AutoCommit} = 0;

  try {
    foreach my $command (@$commands) {
      $dbh->do($command);
    }
    $dbh->commit;
  } catch {
    warn("Transaction aborted: $_");
    eval { $dbh->rollback };
  };

  $dbh->{AutoCommit} = 1;
}

sub createSchema($dbh) {
  my $filename = 'init.sql';

  my $rootDir = path($FindBin::Bin)->parent;
  my $schemaFile = $rootDir->child('schema', $filename);

  doCommandsOf($dbh, $schemaFile);
}

1;