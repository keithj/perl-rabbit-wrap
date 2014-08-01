use utf8;

package WTSI::NPG::RabbitMQ::Client;

use AnyEvent::RabbitMQ;
use AnyEvent::Strict;
use Moose;

with 'WTSI::NPG::Loggable';

our @HANDLED_BROKER_METHODS = qw(is_open server_properties verbose);

has 'broker' =>
  (is       => 'rw',
   isa      => 'AnyEvent::RabbitMQ',
   lazy     => 1,
   required => 1,
   default  => sub { return AnyEvent::RabbitMQ->new->load_xml_spec },
   handles  => [@HANDLED_BROKER_METHODS]);

has 'channels' =>
  (is       => 'ro',
   isa      => 'HashRef[AnyEvent::RabbitMQ::Channel]',
   required => 1,
   default  => sub { return {} },
   init_arg => undef);

has 'fully_asynchronous' =>
  (is       => 'rw',
   isa      => 'Bool',
   required => 1,
   default  => 0);

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

  unless ($self->fully_asynchronous) {
    _condvar($cv) or
      $self->logconfess("The cv argument was not an AnyEvent::CondVar");
  }

  $self->debug("Connecting to $host:$port$vhost as $user");

  $self->broker->connect
    (host       => $host,
     port       => $port,
     vhost      => $vhost,
     user       => $user,
     pass       => $pass,
     timeout    => $timeout,
     tls        => $tls,
     on_success      => sub { $self->handle_connect(@_, $cv) },
     on_failure      => sub { $self->handle_failure(@_, $cv) },
     on_read_failure => sub { $self->handle_failure(@_, $cv) },
     on_return       => sub { $self->handle_error_response(@_) },
     on_close        => sub { $self->handle_error_response(@_) });

  return $self;
}

around 'disconnect' => sub { _maybe_sync('disconnect', @_) };

sub disconnect {
  my ($self, %args) = @_;
  my $cv = $args{cond};

  $self->broker->close
    (on_success => sub { $self->handle_disconnect(@_, $cv) },
     on_failure => sub { $self->handle_failure(@_, $cv) });

  return $self;
}

around 'open_channel' => sub { _maybe_sync('open_channel', @_) };

sub open_channel {
  my ($self, %args) = @_;
  my $name = $args{name};
  my $cv   = $args{cond};

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");
  exists $self->channels->{$name} and
    $self->logconfess("A channel named '$name' exists already");

  $self->debug("Requesting open channel '$name'");

  $self->broker->open_channel
    (on_success => sub { $self->handle_open_channel(@_, $name, $cv) },
     on_failure => sub { $self->handle_failure(@_, $cv) },
     on_close   => sub { $self->handle_close_channel($name, $cv) });

  return $self;
}

around 'close_channel' => sub { _maybe_sync('close_channel', @_) };

sub close_channel {
  my ($self, %args) = @_;
  my $name = $args{name};
  my $cv   = $args{cond};

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");
  exists $self->channels->{$name} or
    $self->logconfess("No channel named '$name' exists");

  $self->channels->{$name}->close
    (on_success => sub { $self->handle_close_channel($name, $cv) },
     on_failure => sub { $self->handle_failure(@_, $cv) });

  return $self;
}

around 'declare_exchange' => sub { _maybe_sync('declare_exchange', @_) };

sub declare_exchange {
  my ($self, %args) = @_;
  my $name  = $args{name};
  my $cname = $args{channel};
  my $cv    = $args{cond};

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");

  defined $cname or $self->logconfess("The channel argument was undefined");
  $cname or $self->logconfess("The channel argument was empty");

  my $channel = $self->channel($cname);
  $channel->declare_exchange
    (exchange   => $name,
     on_success => sub { $self->handle_declare_exchange(@_, $name, $cv) },
     on_failure => sub { $self->handle_failure(@_, $cv) });

  return $self;
}

# sub delete_exchange { }

around 'declare_queue' => sub { _maybe_sync('declare_queue', @_) };

sub declare_queue {
  my ($self, %args) = @_;
  my $name  = $args{name};
  my $cname = $args{channel};
  my $cv    = $args{cond};

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");

  defined $cname or $self->logconfess("The channel argument was undefined");
  $cname or $self->logconfess("The channel argument was empty");

  $self->channel($cname)->declare_queue
    (queue      => $name,
     on_success => sub { $self->handle_declare_queue(@_, $name, $cv) },
     on_failure => sub { $self->handle_failure(@_, $cv) });

  return $self;
}

# sub delete_queue { }

around 'bind_queue' => sub { _maybe_sync('bind_queue', @_) };

sub bind_queue {
  my ($self, %args) = @_;
  my $name  = $args{name};
  my $route = $args{route};
  my $ename = $args{exchange};
  my $cname = $args{channel};
  my $cv    = $args{cond};

  defined $name or $self->logconfess("The name argument was undefined");
  $name or $self->logconfess("The name argument was empty");

  defined $route or $self->logconfess("The route argument was undefined");
  $route or $self->logconfess("The route argument was empty");

  defined $ename or $self->logconfess("The exchange argument was undefined");

  defined $cname or $self->logconfess("The channel argument was undefined");
  $cname or $self->logconfess("The channel argument was empty");

  $self->channel($cname)->bind_queue
    (queue       => $name,
     exchange    => $ename,
     routing_key => $route,
     on_success => sub { $self->handle_bind_queue(@_, $name, $cv) },
     on_failure => sub { $self->handle_failure(@_, $cv) });

  return $self;
}

# sub unbind_queue { }

around 'publish' => sub { _maybe_sync('publish', @_) };

sub publish {
  my ($self, %args) = @_;
  my $route     = $args{route};
  my $ename     = $args{exchange};
  my $cname     = $args{channel};
  my $message   = $args{message};
  my $immediate = $args{immediate};
  my $mandatory = $args{mandatory};
  my $cv        = $args{cond};

  defined $route or $self->logconfess("The route argument was undefined");
  $route or $self->logconfess("The route argument was empty");

  defined $ename or $self->logconfess("The exchange argument was undefined");

  defined $cname or $self->logconfess("The channel argument was undefined");
  $cname or $self->logconfess("The channel argument was empty");

  $self->channel($cname)->publish
    (exchange    => $ename,
     routing_key => $route,
     body        => $message,
     immediate   => $immediate,
     mandatory   => $mandatory);
  $self->handle_publish($route, $cv);

  return $self;
}

around 'consume' => sub { _maybe_sync('consume', @_) };

sub consume {
  my ($self, %args) = @_;
  my $queue = $args{queue};
  my $cname = $args{channel};
  my $cv    = $args{cond};

  defined $queue or $self->logconfess("The queue argument was undefined");
  $queue or $self->logconfess("The queue argument was empty");

  defined $cname or $self->logconfess("The channel argument was undefined");
  $cname or $self->logconfess("The channel argument was empty");

  $self->channel($cname)->consume
    (queue      => $queue,
     no_ack     => 0,
     on_consume => sub { $self->handle_consume(@_, $cname, $cv) },
     on_failure => sub { $self->handle_failure(@_, $cv) });

  return $self;
}

sub handle_connect {
  my ($self, $broker, $cv) = @_;
  # Stub may be implemented by user
}

after 'handle_connect' => sub {
  my ($self, $broker, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  $cv->send;
  $self->debug("Handled connect with ", $cv);
};

sub handle_disconnect {
  # Stub may be implemented by user
}

after 'handle_disconnect' => sub {
  my ($self, $response, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  $cv->send;
  $self->debug("Handled disconnect with ", $cv);
};

sub handle_open_channel {
  # Stub may be implemented by user
}

after 'handle_open_channel' => sub {
  my ($self, $channel, $channel_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  $self->channels->{$channel_name} = $channel;
  $cv->send;
  $self->debug("Handled open_channel '$channel_name' with ", $cv);
};

sub handle_close_channel {
  # Stub may be implemented by user
}

after 'handle_close_channel' => sub {
  my ($self, $channel_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  if (exists $self->channels->{$channel_name}) {
    delete $self->channels->{$channel_name};
  }
  $cv->send;
  $self->debug("Handled close_channel '$channel_name' with ", $cv);
};

sub handle_declare_exchange {
  # Stub may be implemented by user
}

after 'handle_declare_exchange' => sub {
  my ($self, $response, $exchange_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  $cv->send;
  $self->debug("Handled declare_exchange '$exchange_name' with ", $cv);
};

sub handle_declare_queue {
  # Stub may be implemented by user
}

after 'handle_declare_queue' => sub {
  my ($self, $response, $queue_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  $cv->send;
  $self->debug("Handled declare_queue '$queue_name' with ", $cv);
};

sub handle_bind_queue {
  # Stub may be implemented by user
}

after 'handle_bind_queue' => sub {
  my ($self, $response, $queue_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  $cv->send;
  $self->debug("Handled bind_queue '$queue_name' with ", $cv);
};

sub handle_publish {
  # Stub may be implemented by user
}

after 'handle_publish' => sub {
  my ($self, $exchange_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  $cv->send;
  $self->debug("Handled publish '$exchange_name' with ", $cv);
};

sub handle_consume {
  my ($self, $response, $channel_name, $cv) = @_;

  # Stub may be implemented by user
}

after 'handle_consume' => sub {
  my ($self, $response, $channel_name, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  my $dtag = $response->{deliver}->method_frame->delivery_tag;
  $self->channel($channel_name)->ack(delivery_tag => $dtag);

  my $payload = $response->{body}->to_raw_payload;
  $self->debug("Received payload '$payload'");

  $cv->send;
  $self->debug("Handled consume '$channel_name' with ", $cv);
};

sub handle_error_response {
  # Stub may be implemented by user
}

after 'handle_error_response' => sub {
  my ($self, $response, $cv) = @_;

  defined $cv or $self->logconfess("The cv argument was not defined");

  if (ref $response) {
    my $method_frame = $response->method_frame;
    $self->error($method_frame->reply_code, ": ", $method_frame->reply_text);
  }
  else {
    $self->error($response);
  }

  $cv->send;
  $self->debug("Handled server error response with ", $cv);
};

sub handle_failure {
  # Stub may be implemented by user
}

after 'handle_failure' => sub {
  my ($self, $handle, $code, $message, $cv) = @_;

  unless ($self->fully_asynchronous) {
    _condvar($cv) or $self->logconfess("The cv argument was not defined");

    $self->warn($message, ": ", $code);

    $cv->send;
    $self->debug("Handled failure with ", $cv);
  }
};

sub _condvar {
  my ($arg) = @_;

  return defined $arg && ref $arg && (ref $arg eq 'AnyEvent::CondVar');
}

sub _condvar_or_undef {
  my ($arg) = @_;

  return !defined $arg || ref $arg && (ref $arg eq 'AnyEvent::CondVar');
}

sub _maybe_sync {
  my ($name, $orig, $self, %args) = @_;

  # If this flag is on, the API user has assumed responsibility for
  # wiring up all the callbacks before any methods have been
  # called. The flag indicates that we are fully event-driven.
  if ($self->fully_asynchronous) {
    $self->debug("Calling wrapped method for $name");
    $self->$orig(%args);
  }
  else {
    if (!defined $args{cond}) {
      $self->debug("Creating a new AnyEvent::CondVar for $name");
      $args{cond} = AnyEvent->condvar;
    }

    $self->debug("Calling wrapped method for $name");
    $self->$orig(%args);

    $args{cond}->recv;
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
