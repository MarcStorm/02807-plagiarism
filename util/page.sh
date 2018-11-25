#!/bin/bash

dd skip=$1 count=$(($2-$1)) bs=1 if=enwiki-20181020-pages-articles-multistream.xml.bz2 | bzip2 -d > $3
