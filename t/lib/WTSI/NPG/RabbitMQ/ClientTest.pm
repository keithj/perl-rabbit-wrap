use utf8;

package WTSI::NPG::RabbitMQ::ClientTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 33;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::RabbitMQ::Client'); }

use WTSI::NPG::RabbitMQ::Client;

our @credentials = (host  => 'localhost',
                    port  => 5672,
                    vhost => '/test',
                    user  => 'npg',
                    pass  => 'npg');

sub make_fixture : Test(setup) {

}

sub require : Test(1) {
  require_ok('WTSI::NPG::RabbitMQ::Client');
}

sub constructor : Test(1) {
  new_ok('WTSI::NPG::RabbitMQ::Client', []);
}

sub connect_disconnect : Test(4) {
  my $connect_calledback    = 0;
  my $disconnect_calledback = 0;

  my $client = WTSI::NPG::RabbitMQ::Client->new
    (connect_handler    => sub { $connect_calledback++ },
     disconnect_handler => sub { $disconnect_calledback++ });

  ok($client->connect(@credentials)->is_open, 'Can connect');
  ok($connect_calledback, 'Connect callback fired');

  ok($client->disconnect, 'Can disconnect');
  ok($disconnect_calledback, 'Disconnect callback fired');
}

sub open_close_channel : Test(9) {
  my $open_calledback  = 0;
  my $close_calledback = 0;

  my $client = WTSI::NPG::RabbitMQ::Client->new
    (open_channel_handler  => sub { $open_calledback++ },
     close_channel_handler => sub { $close_calledback++ });
  my $channel_name = 'channel.' . $$;

  ok($client->connect(@credentials));
  ok($client->is_open, 'Client connected');
  ok($client->open_channel(name => $channel_name), 'Can open channel');
  ok($client->channel($channel_name), 'Channel exists');
  ok($client->channel($channel_name)->is_open, 'Channel is open');
  ok($open_calledback, 'Open callback fired');

  ok($client->close_channel(name => $channel_name));
  ok(!$client->channel($channel_name)->is_open, 'Channel is closed');
  ok($close_calledback, 'Close callback fired');
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

sub declare_delete_queue : Test(4) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;
  my $channel_name = 'channel.' . $$;
  my $queue_name   = 'queue.' . $$;

  $client->connect(@credentials);
  $client->open_channel(name => $channel_name);

  is($client->declare_queue(name    => $queue_name,
                            channel => $channel_name),
     $queue_name, 'Queue declared');
  ok($client->delete_queue(name    => $queue_name,
                           channel => $channel_name), 'Queue deleted');

  my $anon_queue = $client->declare_queue(channel => $channel_name);
  ok($anon_queue, 'Anonymous queue declared');
  ok($client->delete_queue(name    => $anon_queue,
                           channel => $channel_name),
     'Anonymouse queue deleted');

  $client->close_channel(name => $channel_name);
  $client->disconnect;
}

sub bind_unbind_queue : Test(2) {
  my $client = WTSI::NPG::RabbitMQ::Client->new;
  my $channel_name  = 'channel.' . $$;
  my $exchange_name = 'exchange.' . $$;
  my $queue_name    = 'queue.' . $$;
  my $routing_key   = 'bind_unbind_queue_test.' . $$;

  $client->connect(@credentials);
  $client->open_channel(name => $channel_name);
  $client->declare_exchange(name    => $exchange_name,
                            type    => 'direct',
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
  my $routing_key   = 'publish_consume_test.' . $$;

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

  # Publish $total messages with one client and then consume them with
  # another
  my $total = 100;
  my $num_published = 0;
  my $num_consumed  = 0;

  # Timeout after 10 seconds
  my $timeout = 10;
  my $cv = AnyEvent->condvar;
  my $timer = AnyEvent->timer(after => $timeout, cb => $cv);

  foreach my $i (0 .. $total - 1) {
    $publisher->publish(channel   => $channel_name,
                        exchange  => $exchange_name,
                        route     => $routing_key,
                        body      => "Hello $i",
                        mandatory => 1);
    # Count the messages out
    $num_published++;
    $cv->begin;
  }

  # Provide a consume_handler callback that counts the messages with
  # both an integer and an AnyEvent begin/end pair watcher
  my $consumer = WTSI::NPG::RabbitMQ::Client->new
    (consume_handler => sub {
       my ($payload) = @_;

       # Count the messages in
       $num_consumed++;
       $cv->end;
     });

  $consumer->connect(@credentials);
  $consumer->open_channel(name => $channel_name);

  # Begin consuming the messages
  $consumer->consume(channel => $channel_name,
                     queue   => $queue_name);

  # Wait until all the messages are consumed (or timeout).
  $cv->recv;
  cmp_ok($num_published, '==', $total, 'Number published');
  cmp_ok($num_consumed,  '==', $total, 'Number consumed');

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
