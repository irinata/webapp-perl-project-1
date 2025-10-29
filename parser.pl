#!/usr/bin/perl

use strict;
use warnings;
use feature qw(say signatures);
use lib 'lib';

use Pod::Usage;
use Getopt::Long qw(GetOptions :config no_ignore_case no_auto_abbrev);
use Dotenv -load => '.env.app';

use My::DB qw(connectOnce);


my ($help, $verbose);

GetOptions('help|h'    => \$help,
           'verbose|v' => \$verbose,
) || pod2usage(2);

pod2usage(1) if $help;
pod2usage(-verbose => 2) if $verbose;

my $dbh = connectOnce({
  Database   => $ENV{APP_DB_DATABASE},
  Host       => $ENV{APP_DB_HOST},
  Port       => $ENV{APP_DB_PORT},
  UserName   => $ENV{APP_DB_USER},
  Password   => $ENV{APP_DB_PASSWORD},
});

my $idRe    = qr/id=([^\s]+)/;
my $emailRe = qr/^([\w.%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}):?\s+/;
my $flagRe  = qr/^(?:=>|->|\*\*|==)$/;

sub getHeader($line) {
  return if !$line;
  my ($date, $time, $intId, $flag, @rest) = (split(/ /, $line));

  return {
    timestamp => $date . ' ' . $time,
    intId     => $intId,
    flag      => $flag,
    rest      => join(' ', @rest),
  }
}

sub parseLine($line) {
  my $header = getHeader($line);
  my %result = ();

  if ($header->{flag} eq '<=') {
    my ($id) = $header->{rest} =~ $idRe;
    return if !$id;
    $result{id} = $id;
  }
  elsif ($header->{flag} =~ $flagRe) {
    my ($email) = $header->{rest} =~ $emailRe;
    return if !$email;
    $result{address} = $email;
  }
  else {
    return;
  }

  return { %result,
    created => $header->{timestamp},
    int_id  => $header->{intId},
    flag    => $header->{flag},
    str     => join(' ', ($header->{intId}, $header->{flag}, $header->{rest})),
  };
}

sub insertMessageRecords($recRef) {
  my $placeholders = join(',', ('(?, ?, ?, ?, NULL)') x @$recRef);
  my $query = "INSERT INTO message (created, id, int_id, str, status) VALUES $placeholders";

  my $sth = $dbh->prepare($query);
  my @bindValues;

  foreach my $info (@$recRef) {
    push(@bindValues, $info->{created}, $info->{id}, $info->{int_id}, $info->{str});
  }

  $sth->execute(@bindValues);
}

sub insertLogRecords($recRef) {
  my $placeholders = join(',', ('(?, ?, ?, ?)') x @$recRef);
  my $query = "INSERT INTO log (created, int_id, str, address) VALUES $placeholders";

  my $sth = $dbh->prepare($query);
  my @bindValues;

  foreach my $info (@$recRef) {
    push(@bindValues, $info->{created}, $info->{int_id}, $info->{str}, $info->{address});
  }

  $sth->execute(@bindValues);
}

my $recordsCount = 0;
my $batchSize = 100;
my (@msgRecords, @logRecords);

while (my $line = <>) {
  chomp($line);
  my $info = parseLine($line);
  next if !$info;

  if ($info->{flag} eq '<=') {
    push @msgRecords, $info;

    if (@msgRecords == $batchSize) {
      insertMessageRecords(\@msgRecords);
      @msgRecords = ();
      $recordsCount += $batchSize;
    }
  } else {
    push @logRecords, $info;

    if (@logRecords == $batchSize) {
      insertLogRecords(\@logRecords);
      @logRecords = ();
      $recordsCount += $batchSize;
    }
  }
}

if (@msgRecords) {
  insertMessageRecords(\@msgRecords);
  $recordsCount += @msgRecords;
}

if (@logRecords) {
  insertLogRecords(\@logRecords);
  $recordsCount += @logRecords;
}

say "$recordsCount records inserted into database.";

$dbh->disconnect;


__END__

=encoding utf8

=head1 NAME

parser.pl - Читает и парсит лог файл из STDIN и добавляет записи в БД

=head1 SYNOPSIS

parser.pl input_file

=head1 OPTIONS

=over 4

=item B<--help>, B<-h>

Показать краткую справку

=item B<--verbose>, B<-v>

Показать подробную справку

=back

=head1 EXAMPLES

=over 4

=item parser.pl input_file

=item parser.pl < input_file

=item cat input_file | parser.pl

=back

=cut