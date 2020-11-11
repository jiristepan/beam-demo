#!/bin/bash

# simple script for measuring and logging DirectRunner performance

original_file="./data/svejk.txt"

#echo "n,words,start,end,time" > ./stats.csv
for n in 1 2 5 10
do
    file="./data/svejk_$n.txt"
    [ -a "$file" ] && rm $file
    echo "Generating $file"

    for (( i=0 ; i<$n ; i++ ))
    do
        cat $original_file >> $file
    done
    words=$(wc -w $file | cut -d " " -f 1)
    start=$(date +%s)
    python3 wordcount_gcp.py --input $file --output ./output/$n
    end=$(date +%s)
    time=$(( end - start))
    echo "$n,$words,$start,$end,$time" >> ./stats.csv
done