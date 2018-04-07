#!/bin/bash
if test x"$1" = "x"; then
	echo "You must provide a directory for sample management"
	echo
	exit 1
fi

# Create an "incoming" folder, where new analyses will land
mkdir -p "$1/incoming/"
