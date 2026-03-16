#!/bin/sh

idx=$1
user=$2
shift
shift
java -DlogFilename=logs/node$idx -cp asdProj.jar Main -conf config.properties "$@" &> /proc/1/fd/1
chown $user logs/node$idx.log
