#!/bin/bash

for x in {0..122}
do
    x=$(printf %03d $x)
    dirname="GaiaOut${x}XX"
    echo "Now copying to directory $dirname"
    mkdir $dirname
    cp GaiaOut${x}* $dirname
done
