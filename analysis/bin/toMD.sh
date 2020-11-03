#!/usr/bin/env bash

FILE=$1

# Add Leading and Trailing pipes for MD.
cat $FILE | sed 's/$/|/g' | sed 's/^/|/g' > $FILE.md


# Start Header
head -n 1 $FILE.md > $FILE.hdr

# Get the first line and build a header row from it.
cat $FILE.hdr | sed 's/\([a-z]\)//g' | sed 's/_//g' | sed 's/\|/\| /g' | sed 's/ $//g' | sed 's/ /:---/g' >> $FILE.hdr

tail -n +2 $FILE.md > $FILE.data

cat $FILE.hdr $FILE.data > $FILE.md
