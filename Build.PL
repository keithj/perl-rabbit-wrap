use utf8;

use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't/lib'
# directory is added because the Test::Class tests are there.
use lib qw(. t/lib);

use Module::Build;
use Build;

my $build = Build->new
  (module_name => 'WTSI::NPG::RabbitMQ',
   dist_author => ['Keith James <kdj@sanger.ac.uk>'],
   dist_abstract => 'AnyEvent::RabbitMQ Convenience Library',
   dist_version => Build->git_tag,
   license => 'gpl',
   requires => {
                'AnyEvent::RabbitMQ' => '1.16',
                'Carp'               => 0,
                'JSON'               => 0,
                'Log::Log4perl'      => 0,
                'Moose'              => 0,
                'TAP::Harness'       => 0,
                'Test::Class'        => 0,
                'Test::More'         => 0,
                'Test::Exception'    => 0
               });

$build->create_build_script;
