#!/bin/bash

set -e -x

export TEST_AUTHOR=1

perl Build.PL
./Build clean
./Build test
