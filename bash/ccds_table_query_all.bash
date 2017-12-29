for ccd in {1..32}; do 
    echo $ccd
    ccds_table_query.py --outputFile ccds_table.$ccd.csv --ccd $ccd --verbose 2
done

echo "Finis!"

