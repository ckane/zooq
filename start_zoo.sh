#!/bin/bash
if test x"$1" = "x"; then
	echo "You must provide a directory for sample management"
	echo
	exit 1
fi

"$(dirname $0)/zooq_main.py" -d "$1" -f
