use utf8;

package WTSI::NPG::RabbitMQ::ClientTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 27;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::RabbitMQ::Client'); }

use WTSI::NPG::RabbitMQ::Client;

our @credentials = (host  => 'localhost',
                    port  => 5672,
                    vhost => '/',
                    user  => 'guest',
                    pass  => 'guest');

sub make_fixture : Test(setup) {

}

sub require : Test(1) {
  require_ok('WTSI::NPG::RabbitMQ::Client');
}

sub constructor : Test(1) {
  new_ok('WTSI::NPG::RabbitMQ::Client', []);
}

sub connect_disconnect : Test(2) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;

  my $cv = AnyEvent->condvar;
  ok($client->connect(@credentials, cond => $cv)->is_open, 'Can connect');
  $cv->recv;

  $cv = AnyEvent->condvar;
  ok($client->disconnect(cond => $cv), 'Can disconnect');
  $cv->recv;
}

sub open_close_channel : Test(7) {
  my $client = WTSI::NPG::RabbitMQ::Client->new(verbose => 1);
  my $channel_name = 'channel.' . $$;

  ok($client->connect(@credentials));
  ok($client->is_open, "Client connected");
  ok($client->open_channel(name => $channel_name), 'Can open channel');
  ok($client->channel($channel_name), 'Channel exists');
  ok($client->channel($channel_name)->is_open, 'Channel is open');
  ok($client->close_channel(name => $channel_name));
  ok(!$client->channel($channel_name)->is_open, 'Channel is closed');

  $client->disconnect;
}

sub declare_delete_exchange : Test(2) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;
  my $channel_name  = 'channel.' . $$;
  my $exchange_name = 'exchange.' . $$;

  $client->connect(@credentials);
  $client->open_channel(name => $channel_name);
  ok($client->declare_exchange(name    => $exchange_name,
                               channel => $channel_name), 'Exchange declared');

  ok($client->delete_exchange(name    => $exchange_name,
                              channel => $channel_name), 'Exchange deleted');
  $client->close_channel(name => $channel_name);
  $client->disconnect;
}

sub declare_delete_queue : Test(2) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;
  my $channel_name = 'channel.' . $$;
  my $queue_name   = 'queue.' . $$;

  $client->connect(@credentials);
  $client->open_channel(name => $channel_name);

  ok($client->declare_queue(name    => $queue_name,
                            channel => $channel_name), 'Queue declared');
  ok($client->delete_queue(name    => $queue_name,
                           channel => $channel_name), 'Queue deleted');

  $client->close_channel(name => $channel_name);
  $client->disconnect;
}

sub bind_unbind_queue : Test(2) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;
  my $channel_name  = 'channel.' . $$;
  my $exchange_name = 'exchange.' . $$;
  my $queue_name    = 'queue.' . $$;
  my $routing_key   = 'route.' . $$;

  $client->connect(@credentials);
  $client->open_channel(name => $channel_name);
  $client->declare_exchange(name    => $exchange_name,
                            channel => $channel_name);
  $client->declare_queue(name    => $queue_name,
                         channel => $channel_name);

  ok($client->bind_queue(name     => $queue_name,
                         route    => $routing_key,
                         exchange => $exchange_name,
                         channel  => $channel_name), 'Queue bound');

  ok($client->unbind_queue(name     => $queue_name,
                           route    => $routing_key,
                           exchange => $exchange_name,
                           channel  => $channel_name), 'Queue unbound');

  $client->delete_queue(name    => $queue_name,
                        channel => $channel_name);
  $client->delete_exchange(name    => $exchange_name,
                           channel => $channel_name);

  $client->close_channel(name => $channel_name);
  $client->disconnect;
}

sub publish_consume : Test(2) {
  my $publisher = WTSI::NPG::RabbitMQ::Client->new;
  my $channel_name  = 'channel.' . $$;
  my $exchange_name = 'exchange.' . $$;
  my $queue_name    = 'queue.' . $$;
  my $routing_key   = 'route.' . $$;

  $publisher->connect(@credentials);
  $publisher->open_channel(name => $channel_name);
  $publisher->declare_exchange(name    => $exchange_name,
                               channel => $channel_name);
  $publisher->declare_queue(name    => $queue_name,
                            channel => $channel_name);
  $publisher->bind_queue(name     => $queue_name,
                         route    => $routing_key,
                         exchange => $exchange_name,
                         channel  => $channel_name);

  my $num_published = 0;
  my $num_consumed  = 0;

  my $cv = AnyEvent::CondVar->new;
  foreach my $i (0 .. 99) {
    $publisher->publish(channel   => $channel_name,
                        exchange  => $exchange_name,
                        route     => $routing_key,
                        body      => "Hello $i",
                        mandatory => 1);

    # Count the messages out
    $num_published++;
    $cv->begin;
  }

  my $consumer = WTSI::NPG::RabbitMQ::Client->new
    (consume_handler => sub {
       my ($payload) = @_;

       # Count the messages in
       $num_consumed++;
       $cv->end;
     });

  $consumer->connect(@credentials);
  $consumer->open_channel(name => $channel_name);

  $consumer->consume(channel => $channel_name,
                     queue   => $queue_name);

  # Wait until all the published messages are consumed.
  $cv->recv;
  cmp_ok($num_published, '==', 100, 'Number published');
  cmp_ok($num_consumed,  '==', 100, 'Number consumed');

  $publisher->unbind_queue(name     => $queue_name,
                           route    => $routing_key,
                           exchange => $exchange_name,
                           channel  => $channel_name);
  $publisher->delete_queue(name    => $queue_name,
                           channel => $channel_name);
  $publisher->delete_exchange(name    => $exchange_name,
                              channel => $channel_name);

  $publisher->close_channel(name => $channel_name);
  $publisher->disconnect;
}

sub use_caller_condvar : Test(7) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;
  my $channel_name = 'channel.' . $$;

  my $cv = AnyEvent->condvar;
  ok($client->connect(@credentials, cond => $cv));
  $cv->recv;
  ok($client->is_open, "Client connected");

  $cv = AnyEvent->condvar;
  ok($client->open_channel(name => $channel_name,
                           cond => $cv), 'Can open channel');
  $cv->recv;

  ok($client->channel($channel_name), 'Channel exists');
  ok($client->channel($channel_name)->is_open, 'Channel is open');

  $cv = AnyEvent->condvar;
  ok($client->close_channel(name => $channel_name,
                            cond => $cv));
  $cv->recv;

  ok(!$client->channel($channel_name)->is_open, 'Channel is closed');

  $cv = AnyEvent->condvar;
  $client->disconnect(cond => $cv);
  $cv->recv;
}

1;
