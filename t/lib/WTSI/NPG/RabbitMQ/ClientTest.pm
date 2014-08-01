use utf8;

package WTSI::NPG::RabbitMQ::ClientTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 9;
use Test::Exception;

use Data::Dumper;
use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::RabbitMQ::Client'); }

use WTSI::NPG::RabbitMQ::Client;

our @credentials = (host  => 'localhost',
                    port  => 5672,
                    vhost => '/',
                    user  => 'guest',
                    pass  => 'guest');

sub require : Test(1) {
  require_ok('WTSI::NPG::RabbitMQ::Client');
}

sub constructor : Test(1) {
  new_ok('WTSI::NPG::RabbitMQ::Client', []);
}

sub connect : Test(2) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;

  my $cv = AnyEvent->condvar;
  ok($client->connect(@credentials, cond => $cv)->is_open, 'Can connect');
  $cv->recv;

  $cv = AnyEvent->condvar;
  ok(!$client->disconnect(cond => $cv)->is_open, 'Can disconnect');
  $cv->recv;
}

sub open_channel : Test(4) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;

  my $cv = AnyEvent->condvar;
  ok($client->connect(@credentials, cond => $cv));
  $cv->recv;
  ok($client->is_open, "Client connected");

  my $channel_name = 'channel.' . $$;

  $cv = AnyEvent->condvar;
  ok($client->open_channel(name => $channel_name,
                           cond => $cv), 'Can open channel');
  $cv->recv;

  ok($client->channel($channel_name), 'Channel exists');

  $cv = AnyEvent->condvar;
  $client->disconnect(cond => $cv);
  $cv->recv;

  #$cv = AnyEvent->condvar;
  #ok($client->channel($channel_name)->is_open, 'Channel is open');
  #$cv->recv;
}


1;
