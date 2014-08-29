use utf8;

package WTSI::NPG::RabbitMQ::Client;

use AnyEvent::RabbitMQ;
use AnyEvent::Strict;
use Moose;

with 'WTSI::NPG::Loggable';

our @HANDLED_BROKER_METHODS = qw(is_open server_properties verbose);

# Named arguments used in this API
our $BODY_ARG         = 'body';
our $CHANNEL_ARG      = 'channel';
our $CONDVAR_ARG      = 'cond';
our $CONSUMER_TAG_ARG = 'consumer_tag';
our $DURABLE_ARG      = 'durable';
our $EXCHANGE_ARG     = 'exchange';
our $EXCLUSIVE_ARG    = 'exclusive';
our $IMMEDIATE_ARG    = 'immediate';
our $MANDATORY_ARG    = 'mandatory';
our $NAME_ARG         = 'name';
our $NO_ACK_ARG       = 'no_ack';
our $PASSIVE_ARG      = 'passive';
our $QUEUE_ARG        = 'queue';
our $ROUTING_KEY_ARG  = 'routing_key';
our $TYPE_ARG         = 'type';

has 'broker' =>
  (is       => 'rw',
   isa      => 'Maybe[AnyEvent::RabbitMQ]',
   required => 1,
   lazy     => 1,
   default  => sub { return AnyEvent::RabbitMQ->new->load_xml_spec },
   handles  => [@HANDLED_BROKER_METHODS]);

has 'channels' =>
  (is       => 'ro',
   isa      => 'HashRef[AnyEvent::RabbitMQ::Channel]',
   required => 1,
   lazy     => 1,
   default  => sub { return {} },
   init_arg => undef);

has 'fully_asynchronous' =>
  (is       => 'rw',
   isa      => 'Bool',
   required => 1,
   default  => 0);

has 'connect_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'connect_failure_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'disconnect_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'open_channel_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'close_channel_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'publish_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'consume_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'consume_cancel_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

has 'error_handler' =>
  (is       => 'rw',
   isa      => 'CodeRef',
   required => 1,
   lazy     => 1,
   builder  => '_make_default_handler');

=head2 channel

  Arg [1] : An channel name

  Example :    my $c = $client->channel('my_channel');
  Description: Return the named channel. Raise an error if the named channel
               does not exist.
  Returntype : AnyEvent::RabbitMQ::Channel

=cut

sub channel {
  my ($self, $name) = @_;

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");
  exists $self->channels->{$name} or
    $self->logconfess("No channel named '$name' exists");

  return $self->channels->{$name};
}

=head2 connect

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               host    => <server host name>,
               port    => <server port>,
               vhost   => <server vhost>,
               user    => <user name>,
               pass    => <user password>,
               timeout => <connection timeout>,
               tls     => <TLS flag, see AnyEvent::RabbitMQ>,
               tune    => <Tuning arguments, see AnyEvent::RabbitMQ>,
               cond    => <AnyEvent::condvar on which to synchronize>

  Example :    my $c = $client->connect(host    => 'localhost',
                                        port    => 5672,
                                        vhost   => '/test',
                                        user    => 'guest',
                                        pass    => $pass,
                                        timeout => 1);

  Description: Connect to a RabbitMQ server. Call the connect_handler on
               success, the connect_failure_handler on failure and the
               error_handler in response to read_failure, return or close
               events.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'connect' => sub { _maybe_sync('connect', @_) };

sub connect {
  my ($self, %args) = @_;
  my $host    = $args{host};
  my $port    = $args{port};
  my $vhost   = $args{vhost};
  my $user    = $args{user};
  my $pass    = $args{pass};
  my $timeout = $args{timeout};
  my $tls     = $args{tls};
  my $tune    = $args{tune};
  my $cv      = $args{cond};

  defined $host or $self->logconfess("The host argument was undefined");
  $host or $self->logconfess("The host argument was empty");

  defined $port or $self->logconfess("The port argument was undefined");
  $port or $self->logconfess("The port argument was empty");

  defined $vhost or $self->logconfess("The vhost argument was undefined");
  $vhost or $self->logconfess("The vhost argument was empty");

  defined $user or $self->logconfess("The user argument was undefined");
  $user or $self->logconfess("The user argument was empty");

  defined $pass or $self->logconfess("The pass argument was undefined");

  $tune ||= {};

  unless ($self->fully_asynchronous) {
    _is_condvar($cv) or
      $self->logconfess("The cv argument was not an AnyEvent::CondVar");
  }

  $self->debug("Connecting to $host:$port$vhost as $user");

  $self->broker->connect
    (host    => $host,
     port    => $port,
     vhost   => $vhost,
     user    => $user,
     pass    => $pass,
     timeout => $timeout,
     tls     => $tls,
     tune    => $tune,
     on_success      => sub { $self->call_connect_handler($cv, @_) },
     on_failure      => sub { $self->call_connect_failure_handler($cv, @_) },
     on_read_failure => sub { $self->call_error_handler($cv, @_) },
     on_return       => sub { $self->call_error_handler($cv, @_) },
     on_close        => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

=head2 disconnect

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               cond => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->disconnect;

  Description: Disconnect from a RabbitMQ server. Call the disconnect_handler
               on success or the error_handler on failure.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'disconnect' => sub { _maybe_sync('disconnect', @_) };

sub disconnect {
  my ($self, %args) = @_;
  my $cv = $args{cond};

  $self->broker->close
    (on_success => sub { $self->call_disconnect_handler($cv) },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

=head2 open_channel

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name => <channel name>,
               cond => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->open_channel(name => 'test');

  Description: Open a new channel on a RabbitMQ server. Call the
               open_channel_handler on success, the error_handler on
               failure and the close_channel_handler on a close event.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'open_channel' => sub { _maybe_sync('open_channel', @_) };

sub open_channel {
  my ($self, %args) = @_;
  my $name = $args{$NAME_ARG};
  my $cv   = $args{$CONDVAR_ARG};

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");
  exists $self->channels->{$name} and
    $self->logconfess("A channel named '$name' exists already");

  $self->broker->open_channel
    (on_success => sub { $self->call_open_channel_handler($name, $cv, @_) },
     on_failure => sub { $self->call_error_handler($cv, @_) },
     on_close   => sub { $self->call_close_channel_handler($name, $cv) });

  return $self;
}

=head2 close_channel

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name => <channel name>,
               cond => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->close_channel(name => 'test');

  Description: Close a channel on a RabbitMQ server. Call the
               close_channel_handler on success or the error_handler on
               failure.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'close_channel' => sub { _maybe_sync('close_channel', @_) };

sub close_channel {
  my ($self, %args) = @_;
  my $name = $args{$NAME_ARG};
  my $cv   = $args{$CONDVAR_ARG};

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");
  exists $self->channels->{$name} or
    $self->logconfess("No channel named '$name' exists");

  $self->channels->{$name}->close
    (on_success => sub { $self->call_close_channel_handler($name, $cv) },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

=head2 declare_exchange

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name    => <exchange name>,
               channel => <channel name>,
               type    => <exchange type name, see AnyEvent::RabbitMQ>,
               durable => <durability flag, see AnyEvent::RabbitMQ>,
               passive => <passivity flag, see AnyEvent::RabbitMQ>,
               cond    => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->declare_exchange(name    => 'test',
                                                 channel => 'test');

  Description: Declare an exchange on a RabbitMQ server. Call the
               error_handler on failure.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'declare_exchange' => sub {

  _maybe_sync('declare_exchange', @_)

};

sub declare_exchange {
  my ($self, %args) = @_;
  my $name    = $args{$NAME_ARG};
  my $cname   = $args{$CHANNEL_ARG};
  my $type    = $args{$TYPE_ARG};
  my $durable = $args{$DURABLE_ARG};
  my $passive = $args{$PASSIVE_ARG};
  my $cv      = $args{$CONDVAR_ARG};

  defined $name or $self->logconfess("The $NAME_ARG argument was undefined");
  $name or $self->logconfess("The $NAME_ARG argument was empty");

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  $type    ||= 'direct';
  $durable ||= 0;
  $passive ||= 0;

  my $channel = $self->channel($cname);
  $channel->declare_exchange
    (exchange   => $name,
     type       => $type,
     durable    => $durable,
     passive    => $passive,
     on_success => sub {
       $self->debug("Declared exchange '$name' on channel '$cname'");
       $cv->send($self);
     },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

=head2 delete_exchange

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name    => <exchange name>,
               channel => <channel name>,
               cond    => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->delete_exchange(name    => 'test',
                                                channel => 'test');

  Description: Delete an exchange on a RabbitMQ server. Call the
               error_handler on failure.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'delete_exchange' => sub { _maybe_sync('delete_exchange', @_) };

sub delete_exchange {
  my ($self, %args) = @_;
  my $name  = $args{$NAME_ARG};
  my $cname = $args{$CHANNEL_ARG};
  my $cv    = $args{$CONDVAR_ARG};

  defined $name or $self->logconfess("The $NAME_ARG argument was undefined");
  $name or $self->logconfess("The $NAME_ARG argument was empty");

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  $self->channel($cname)->delete_exchange
    (exchange   => $name,
     on_success => sub {
       $self->debug("Deleted exchange '$name' on channel '$cname'");
       $cv->send($self);
     },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

=head2 declare_queue

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name      => <queue name>,
               channel   => <channel name>,
               durable   => <durability flag, see AnyEvent::RabbitMQ>,
               exclusive => <exclusivity flag, see AnyEvent::RabbitMQ>,
               passive   => <passivity flag, see AnyEvent::RabbitMQ>,
               cond      => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->declare_queue(name    => 'test',
                                              channel => 'test');

  Description: Declare a queue on a RabbitMQ server. Call the
               error_handler on failure.
  Returntype : Str queue name

=cut

around 'declare_queue' => sub { _maybe_sync('declare_queue', @_) };

sub declare_queue {
  my ($self, %args) = @_;
  my $name     = $args{$NAME_ARG};
  my $cname    = $args{$CHANNEL_ARG};
  my $durable  = $args{$DURABLE_ARG};
  my $passive  = $args{$PASSIVE_ARG};
  my $excusive = $args{$EXCLUSIVE_ARG};
  my $cv       = $args{$CONDVAR_ARG};

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  $name    ||= '';
  $durable ||= 0;
  $passive ||= 0;

  $self->channel($cname)->declare_queue
    (queue      => $name,
     durable    => $durable,
     on_success => sub {
       my ($response) = @_;
       my $frame = $response->method_frame;
       my $msg = sprintf("Declared queue '%s' consumer count: %d, " .
                         "message count: %d on channel '$cname'",
                         $frame->queue, $frame->consumer_count,
                         $frame->message_count);
       $self->debug($msg);
       $cv->send($frame->queue);
     },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $name;
}

=head2 delete_queue

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name    => <queue name>,
               channel => <channel name>,
               cond    => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->delete_queue(name    => 'test',
                                             channel => 'test');

  Description: Delete a queue on a RabbitMQ server. Call the
               error_handler on failure.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'delete_queue' => sub { _maybe_sync('delete_queue', @_) };

sub delete_queue {
  my ($self, %args) = @_;
  my $name  = $args{$NAME_ARG};
  my $cname = $args{$CHANNEL_ARG};
  my $cv    = $args{$CONDVAR_ARG};

  defined $name or $self->logconfess("The $NAME_ARG argument was undefined");
  $name or $self->logconfess("The $NAME_ARG argument was empty");

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  $self->channel($cname)->delete_queue
    (queue      => $name,
     on_success => sub {
       $self->debug("Deleted queue '$name' on channel '$cname'");
       $cv->send($self);
     },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

=head2 bind_queue

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name        => <queue name>,
               routing_key => <routing key>,
               exchange    => <exchange name>,
               channel     => <channel name>,
               cond        => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->bind_queue(name        => 'test',
                                           routing_key => 'test',
                                           exchange    => '',
                                           channel     => 'test');

  Description: Bind a queue on a RabbitMQ server. Call the
               error_handler on failure.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'bind_queue' => sub { _maybe_sync('bind_queue', @_) };

sub bind_queue {
  my ($self, %args) = @_;
  my $name  = $args{$NAME_ARG};
  my $route = $args{$ROUTING_KEY_ARG};
  my $ename = $args{$EXCHANGE_ARG};
  my $cname = $args{$CHANNEL_ARG};
  my $cv    = $args{$CONDVAR_ARG};

  defined $name or $self->logconfess("The $NAME_ARG argument was undefined");
  $name or $self->logconfess("The $NAME_ARG argument was empty");

  defined $route or
    $self->logconfess("The $ROUTING_KEY_ARG argument was undefined");
  $route or $self->logconfess("The $ROUTING_KEY_ARG argument was empty");

  defined $ename or
    $self->logconfess("The $EXCHANGE_ARG argument was undefined");

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  $ename ||= '';

  $self->channel($cname)->bind_queue
    (queue       => $name,
     exchange    => $ename,
     routing_key => $route,
     on_success => sub {
       $self->debug("Bound queue '$name' to exchange '$ename' with ",
                    "routing key '$route' on channel '$cname'");
       $cv->send($self);
     },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

=head2 unbind_queue

  Arg [1] :    Arguments hash. Valid key/value pairs are:

               name        => <queue name>,
               routing_key => <routing key>,
               exchange    => <exchange name>,
               channel     => <channel name>,
               cond        => <AnyEvent::CondVar on which to synchronize>

  Example :    my $c = $client->unbind_queue(name        => 'test',
                                             routing_key => 'test',
                                             exchange    => '',
                                             channel     => 'test');

  Description: Unbind a queue on a RabbitMQ server. Call the
               error_handler on failure.
  Returntype : WTSI::NPG::RabbitMQ::Client

=cut

around 'unbind_queue' => sub { _maybe_sync('unbind_queue', @_) };

sub unbind_queue {
  my ($self, %args) = @_;
  my $name  = $args{$NAME_ARG};
  my $route = $args{$ROUTING_KEY_ARG};
  my $ename = $args{$EXCHANGE_ARG};
  my $cname = $args{$CHANNEL_ARG};
  my $cv    = $args{$CONDVAR_ARG};

  defined $name or $self->logconfess("The $NAME_ARG argument was undefined");
  $name or $self->logconfess("The $NAME_ARG argument was empty");

  defined $route or
    $self->logconfess("The $ROUTING_KEY_ARG argument was undefined");
  $route or $self->logconfess("The $ROUTING_KEY_ARG argument was empty");

  defined $ename or
    $self->logconfess("The $EXCHANGE_ARG argument was undefined");

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  $ename ||= '';

  $self->channel($cname)->unbind_queue
    (queue       => $name,
     exchange    => $ename,
     routing_key => $route,
     on_success => sub {
       $self->debug("Unbound queue '$name' from exchange '$ename' with ",
                    "routing key '$route' on channel '$cname'");
       $cv->send($self);
     },
     on_failure => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

around 'publish' => sub { _maybe_sync('publish', @_) };

sub publish {
  my ($self, %args) = @_;
  my $route     = $args{$ROUTING_KEY_ARG};
  my $ename     = $args{$EXCHANGE_ARG};
  my $cname     = $args{$CHANNEL_ARG};
  my $body      = $args{$BODY_ARG};
  my $immediate = $args{$IMMEDIATE_ARG};
  my $mandatory = $args{$MANDATORY_ARG};
  my $cv        = $args{$CONDVAR_ARG};

  defined $route or
    $self->logconfess("The $ROUTING_KEY_ARG argument was undefined");
  $route or $self->logconfess("The $ROUTING_KEY_ARG argument was empty");

  defined $ename or
    $self->logconfess("The $EXCHANGE_ARG argument was undefined");

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  defined $body or $self->logconfess("The $BODY_ARG argument was undefined");

  $self->channel($cname)->publish
    (exchange    => $ename,
     routing_key => $route,
     body        => $body,
     immediate   => $immediate,
     mandatory   => $mandatory);
  $self->call_publish_handler($body, $route, $cv);

  return $self;
}

around 'consume' => sub { _maybe_sync('consume', @_) };

sub consume {
  my ($self, %args) = @_;
  my $queue        = $args{$QUEUE_ARG};
  my $cname        = $args{$CHANNEL_ARG};
  my $no_ack       = $args{$NO_ACK_ARG};
  my $consumer_tag = $args{$CONSUMER_TAG_ARG};
  my $cv           = $args{$CONDVAR_ARG};

  defined $queue or $self->logconfess("The $QUEUE_ARG argument was undefined");
  $queue or $self->logconfess("The $QUEUE_ARG argument was empty");

  defined $cname or
    $self->logconfess("The $CHANNEL_ARG argument was undefined");
  $cname or $self->logconfess("The $CHANNEL_ARG argument was empty");

  $no_ack ||= 0;

  $self->channel($cname)->consume
    (queue        => $queue,
     no_ack       => $no_ack,
     consumer_tag => $consumer_tag,
     on_consume   => sub { $self->call_consume_handler($cname, $cv, @_) },
     on_cancel    => sub {
       $self->call_consume_cancel_handler($cname, $cv, @_)
     },
     on_failure   => sub { $self->call_error_handler($cv, @_) });

  return $self;
}

sub call_connect_handler {
  my ($self, $cv) = @_;

  $self->connect_handler->($self);
}

after 'call_connect_handler' => sub {
  my ($self, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Called connect_handler");
};

sub call_connect_failure_handler {
  my ($self, $cv, @args) = @_;
  my ($iohandle, $code, $message) = @args;
  $self->connect_failure_handler->($self, $iohandle, $code, $message);
}

after 'call_connect_failure_handler' => sub {
  my ($self, $cv, @args) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Called connect_failure_handler");
};

sub call_disconnect_handler {
  my ($self, $cv) = @_;

  $self->disconnect_handler->($self);
}

after 'call_disconnect_handler' => sub {
  my ($self, $cv) = @_;

  $self->broker(undef);

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Called disconnect_handler");
};

sub call_open_channel_handler {
  my ($self, $channel_name, $cv, @args) = @_;
  my ($channel) = @args;

  $self->open_channel_handler->($self, $channel, $channel_name);
}

after 'call_open_channel_handler' => sub {
  my ($self, $channel_name, $cv, @args) = @_;
  my ($channel) = @args;

  $self->channels->{$channel_name} = $channel;

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Called open_channel for '$channel_name'");
};

sub call_close_channel_handler {
  my ($self, $channel_name, $cv) = @_;

  $self->close_channel_handler->($self, $channel_name);
}

after 'call_close_channel_handler' => sub {
  my ($self, $channel_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Handled close_channel for '$channel_name'");
};

sub call_publish_handler {
  my ($self, $message, $route, $cv) = @_;

  $self->publish_handler->($self, $message, $route);
}

after 'call_publish_handler' => sub {
  my ($self, $body, $route, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Called publish_handler for body '$body' with routing key ",
               "'$route'");
};

sub call_consume_handler {
  my ($self, $channel_name, $cv, @args) = @_;
  my ($response) = @args;

  my $payload = $response->{body}->to_raw_payload;
  $self->consume_handler->($payload);
}

after 'call_consume_handler' => sub {
  my ($self, $channel_name, $cv, @args) = @_;
  my ($response) = @args;

  defined $cv or $self->logconfess("The cv argument was not defined");
  my $dtag = $response->{deliver}->method_frame->delivery_tag;
  $self->channel($channel_name)->ack(delivery_tag => $dtag);

  my $payload = $response->{body}->to_raw_payload;
  $self->debug("Received payload '$payload'");

  $cv->send($self);
  $self->debug("Called consume_handler from '$channel_name'");
};

sub call_consume_cancel_handler {
  my ($self, $channel_name, $cv, @args) = @_;
  my ($response) = @args;

  $self->consume_cancel_handler->($response);
}

after 'call_consume_cancel_handler' => sub {
  my ($self, $channel_name, $cv, @args) = @_;
  my ($response) = @args;

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Called consume_cancel_handler from '$channel_name'");
};

sub call_error_handler {
  my ($self, $cv, @args) = @_;
  my ($response) = @args;

  $self->error_handler->($response);
}

after 'call_error_handler' => sub {
  my ($self, $cv, @args) = @_;
  my ($response) = @args;

  if (ref $response) {
    my $method_frame = $response->method_frame;
    $self->error($method_frame->reply_code, ": ", $method_frame->reply_text);
  }
  else {
    $self->error($response);
  }

  defined $cv or $self->logconfess("The cv argument was not defined");
  $cv->send($self);
  $self->debug("Called error_handler");
};

sub _make_default_handler {
  my ($self, @args) = @_;

  return sub { return 1 };
}

# Call a wrapped method
sub _call_with_sync {
  my ($self, $name, $method, %args) = @_;

  $self->debug("Calling method '$name' with a supplied AnyEvent::CondVar");
  return $self->$method(%args);
}

# This function exists to wait for the completion of a method that
# would otherwise return immediately because it does its work
# asynchronously. It does this by creating a new AnyEvent::CondVar (if
# none was supplied in the arguments then calling the method and
# finally calling the recv function on the CondVar.
sub _maybe_sync {
  my ($name, $orig, $self, %args) = @_;

  $self->debug("Calling wrapped method for $name");

  # If this flag is on, the API user has assumed responsibility for
  # wiring up all the callbacks before any methods have been
  # called. The flag indicates that we are fully event-driven.
  if ($self->fully_asynchronous) {
    $self->$orig(%args);
  }
  else {
    if (!defined $args{$CONDVAR_ARG}) {
      $self->debug("Creating a new AnyEvent::CondVar for $name");
      $args{$CONDVAR_ARG} = AnyEvent->condvar;
    }

    $self->$orig(%args);
    # This return value propagates any asynchronously created value
    # (from AnyEvent::CondVar->send calls) back to the caller.
    $args{$CONDVAR_ARG}->recv;
  }
}

sub _is_condvar {
  my ($arg) = @_;

  return defined $arg && ref $arg && (ref $arg eq 'AnyEvent::CondVar');
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::RabbitMQ::Client

=head1 DESCRIPTION

WTSI::NPG::RabbitMQ::Client is a convenience wrapper around
AnyEvent::RabbitMQ which provides these features:

 - Sets up default callbacks for events fired while performing common
   messaging tasks, such as connecting and disconnecting from the
   server, declaring and deleting exchanges and queues, binding and
   unbinding queues and publishing and consuming messages. The
   callbacks for these operations are stored as Moose attributes and
   may be customised.

 - Provides an option to create automatically and use AnyEvent::CondVar
   objects where operations are required to block.

 - Adds argument checking and logging using Log4perl.


=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
