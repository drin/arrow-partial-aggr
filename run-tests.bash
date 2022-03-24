#!/usr/bin/bash

test_fpath="aggr_timing.csv"
slice_counts=(1 2 3 4 5 6 7 8 9 10)
col_limits=(200 400 600 800 1000 1200 1400 1600 1800 2000)

for slice_count in "${slice_counts[@]}"; do
    for col_limit in "${col_limits[@]}"; do
        ./build-dir/simple "$PWD" vector ${slice_count} ${col_limit} >> "${test_fpath}"
    done
done
