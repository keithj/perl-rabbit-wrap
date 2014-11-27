
use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't/lib'
# directory is added because the Test::Class tests are there.
use lib qw(lib t/lib);

use WTSI::DNAP::Utilities::Build;

my $build = WTSI::DNAP::Utilities::Build->new
  (module_name   => 'WTSI::NPG::RabbitMQ',
   dist_author   => ['Keith James <kdj@sanger.ac.uk>'],
   dist_abstract => 'AnyEvent::RabbitMQ Convenience Library',
   dist_version  => WTSI::DNAP::Utilities::Build->report_version,
   license       => 'gpl',
   test_requires => {
                     'TAP::Harness'          => '>= 3.30',
                     'Test::Class'           => '>= 0.41',
                     'Test::More'            => '>= 0.98',
                     'Test::Exception'       => '>= 0.32'
                    },
   requires =>      {
                     'AnyEvent::RabbitMQ'    => '>= 1.18',
                     'Carp'                  => '>= 1.20',
                     'Data::Dump'            => '>= 1.21',
                     'Log::Log4perl'         => '>= 1.42',
                     'Moose'                 => '>= 2.1',
                     'WTSI::DNAP::Utilities' => '>= 0.4.1'
                    });

$build->create_build_script;
