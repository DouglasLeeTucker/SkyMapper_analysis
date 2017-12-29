for i in {0..318}; do 
    i_lo=$(( i*1000000 ))
    i_hi=$(( i_lo + 499999 ))
    outputFileName=`printf "master_table.%09d-%09d.csv" $i_lo $i_hi`
    echo $i, $i_lo, $i_hi, $outputFileName
    master_table_query.py --outputFile $outputFileName --object_id_lo $i_lo --object_id_hi $i_hi --verbose 2
done
