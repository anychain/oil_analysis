#!/bin/sh
 
# script to automate the load and export to CSV of an oracle dump
# This script assumes:
# * You have the Oracle VirtualBox image running locally
# ** ssh port-forwarding is configured for host port 2222 -> guess port 22.
# ** run `vagrant ssh-config` to check your ssh configuration to access the virutal box.
# ** For example:
#Host oracle
#  HostName 127.0.0.1
#  User oracle
#  Port 2222
#  UserKnownHostsFile /dev/null
#  StrictHostKeyChecking no
#  PasswordAuthentication no
#  IdentityFile /Users/gengjh/MyWorks/vagrant-ubuntu-oracle-xe/.vagrant/machines/default/virtualbox/private_key
#  IdentitiesOnly yes
#  LogLevel FATAL

set -e
set -x
 
SSH_PORT=2222
ORACLE_HOST=127.0.0.1
 
REMOTE_MACHINE="oracle"
 
SSH_ARGS=""
 
DMP_FILE=$1
if [ x"" == x"$DMP_FILE" ]; then
	echo "dump file is needed! "
	exit 1
fi


# Copy utility scripts
rsync -Pae "ssh $SSH_ARGS" create-db.sql export.sql dump2csv.sql $REMOTE_MACHINE:~/
# Copy database dump
rsync -Pae "ssh $SSH_ARGS" $DMP_FILE $REMOTE_MACHINE:~/
 
# create the tablespace, user, grants, and directory object
ssh $SSH_ARGS $REMOTE_MACHINE "sqlplus / as sysdba @create-db.sql"
# import the Oracle .dmp file
#ssh $SSH_ARGS $REMOTE_MACHINE "imp oil/passw0rd file=${DMP_FILE##*/} FROMUSER=PDQ_OWNR tables=og_well_completion INDEXES=NO"
 
# Create the PL/SQL procedure to export tables to CSV
ssh $SSH_ARGS $REMOTE_MACHINE "sqlplus oil/passw0rd @dump2csv.sql"
# Ensure that the target directory is created.
ssh $SSH_ARGS $REMOTE_MACHINE "rm -rf /tmp/exports; mkdir /tmp/exports"
 
# Run the script to export all the tables
ssh $SSH_ARGS $REMOTE_MACHINE "sqlplus oil/passw0rd @export.sql"
 
# Zip up the export
ssh $SSH_ARGS $REMOTE_MACHINE "cd /tmp; tar cjvf exports.tar.bz2 exports"
 
# Copy the export
rsync -Pae "ssh $SSH_ARGS" $REMOTE_MACHINE:/tmp/exports.tar.bz2 ./$(date +%Y-%m-%dT%H%M%S)-exports.tar.bz2

