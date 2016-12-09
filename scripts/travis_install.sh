#!/bin/bash

set -e -x

# The default build branch for all repositories. This defaults to
# TRAVIS_BRANCH unless set in the Travis build environment.
WTSI_NPG_BUILD_BRANCH=${WTSI_NPG_BUILD_BRANCH:=$TRAVIS_BRANCH}

# WTSI NPG Perl repo dependencies, only one at the moment
for repo in perl-dnap-utilities; do
    cd /tmp
    # Always clone master when using depth 1 to get current tag
    git clone --branch master --depth 1 ${WTSI_NPG_GITHUB_URL}/${repo}.git ${repo}.git
    cd /tmp/${repo}.git
    # Shift off master to appropriate branch (if possible)
    git ls-remote --heads --exit-code origin ${WTSI_NPG_BUILD_BRANCH} && git pull origin ${WTSI_NPG_BUILD_BRANCH} && echo "Switched to branch ${WTSI_NPG_BUILD_BRANCH}"
    repos=$repos" /tmp/${repo}.git"
done

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    cd $repo
    cpanm --quiet --notest --installdeps .
    ./Build install
done

cd $TRAVIS_BUILD_DIR

cpanm --quiet --notest --installdeps .
