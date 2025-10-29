package MyApp;

use strict;
use warnings;

use Template;
use Dancer2;
use Dotenv -load => '.env.app';
use Try::Tiny;

use My::DB qw(connectWithRetry);
use My::DB::Init qw(createSchema);

use constant LIMIT   => 100;
use constant RETRIES => 3;


my $params = {
  Database => $ENV{APP_DB_DATABASE},
  Host     => $ENV{APP_DB_HOST},
  Port     => $ENV{APP_DB_PORT},
  UserName => $ENV{APP_DB_USER},
  Password => $ENV{APP_DB_PASSWORD},
};

my $dbh = connectWithRetry(RETRIES, $params);

createSchema($dbh);

get '/' => sub {
    template 'index' => { 'title' => 'MyApp' };
};

post '/process' => sub {
  my $address = body_parameters->get('email-input');

  my @bind_values = ($address, LIMIT + 1);

  my $statement = "
    SELECT l.created AS timestamp, l.str AS \"строка лога\"
    FROM log AS l
    LEFT JOIN message AS m ON l.int_id = m.int_id
    WHERE l.address = ?
    ORDER BY m.int_id, m.created
    LIMIT  ?";

  try {
    my $result = $dbh->selectall_arrayref($statement, { Slice => {} }, @bind_values);

    my $exceedsLimit = @$result > LIMIT;
    pop @$result if $exceedsLimit;

    template 'result', {
      log => $result,
      address => $address,
      exceedsLimit => $exceedsLimit,
      limit => LIMIT,
    };
  }
  catch {
    template 'error', {};
  };
};

END {
  $dbh->disconnect if $dbh;
}

1;
