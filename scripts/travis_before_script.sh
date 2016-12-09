#!/bin/bash

set -e -x

sudo rabbitmqctl add_vhost /test
sudo rabbitmqctl add_user npg npg
sudo rabbitmqctl set_permissions -p /test npg '.*' '.*' '.*'
