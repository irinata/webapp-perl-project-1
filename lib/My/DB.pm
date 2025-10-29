package My::DB;

use strict;
use warnings;
no warnings 'exiting';
use feature qw(say signatures);

use DBI;
use Try::Tiny;

use My::Exceptions qw(My::Exceptions::DatabaseConnectionError);

use Exporter 'import';

our @EXPORT_OK = qw(
      connectWithRetry
      connectOnce
);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

use constant DB_CONNECT_RETRY_SEC => 5;


sub connectWithRetry($attempts, $params) {
  my $dbh;

  for my $attempt (1..$attempts) {
    say("Connecting to database, attempt $attempt");

    try {
      $dbh = DBI->connect(
        "dbi:Pg:dbname=$params->{Database};host=$params->{Host};port=$params->{Port}",
        $params->{UserName},
        $params->{Password},
        {
          RaiseError => $params->{RaiseError} // 1,
          PrintError => $params->{PrintError} // 1,
          AutoCommit => $params->{AutoCommit} // 1,
        }
      );

      say "Done!";
      last;
    }
    catch {
      warn "Connection failed: $_";

      if ($attempt == $attempts) {
        DatabaseConnectionError->throw("Failed to connect to database by timeout: $_\n");
      }

      say("Waiting for a while and retrying...");
      sleep(DB_CONNECT_RETRY_SEC);
    };
  }

  return $dbh;
}

sub connectOnce($params) {
  return connectWithRetry(1, $params);
}

1;