#!/bin/bash
if test x"$1" = "x"; then
	echo "You must provide a directory for sample management"
	echo
	exit 1
fi

# Create an "incoming" folder, where new analyses will land
mkdir -p "$1/incoming/"

# Populate sqlite3 db file
sqlite3 "$1/samples.sqlite" 'CREATE TABLE `samples` (`mwid` TEXT PRIMARY KEY NOT NULL,`mwpath` TEXT NOT NULL);'
