#!/bin/bash

set -eux

microdnf install -y \
         hostname \
    ;

groupadd -g 1000 temporal
useradd -u 1000 -g temporal temporal
mkdir /etc/temporal/config
chown -R temporal:temporal /etc/temporal/config
