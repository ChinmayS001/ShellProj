file1=$1
file2=$2
rm op
cat $file1 | tr ' ' '^' >temp
cat temp >$file1
cat $file2 | tr ' ' '^' >temp
cat temp >$file2
for line in $(cat $file1); do
    IFS='#'
    set -- $line
    
    if [[ -z $1 || -z $2 || -z $3 || -z $4 || -z $5 ]]; then
        continue
    fi
    
    v2=$(echo $2 | tr -dc '0-9');
    v3=$(echo $3 | tr '^' ' ' | tr -s ' ')
    v4=$(echo $4 | tr '^' ' ' | tr [a-z] [A-Z])
    cat temp2 >>temp
    rm temp2
    echo $5 | cut -d '|' -f1 >temp2
    echo $5 | cut -d '|' -f2 >>temp2
    echo $5 | cut -d '|' -f3 >>temp2
    if [ $(tail -n 1 temp2) = $(tail -n 2 temp2 | head -n 1) ]; then
        continue
    fi
    for ln in $(cat temp2); do
        echo $ln | cut -d ';' -f1 >t3
        echo $ln | cut -d ';' -f2 >t2
        echo $ln | cut -d ';' -f3 >t1
        
        #if [[ -z $v7 || -z $v8 ]]; then
        #  echo continue;
        #fi
        (
            echo $1","$v2","$v3","$v4
            echo $1","$v2","$v3","$v4
            echo $1","$v2","$v3","$v4
        ) >temp3
        paste -d "," temp3 t3 t2 t1 >>op
    done
done
unset IFS
for line in $(cat $file2); do
    
    IFS=',';set -- $line
    
    if [[ -z $1 || -z $2 || -z $3 || -z $4 || -z $5 ]]; then
        continue
    fi
    
    v2=$(echo $2 | tr -dc '0-9')
    v3=$(echo $3 | tr '^' ' ' | tr -s ' ')
    v4=$(echo $4 | tr [a-z] [A-Z])
    
    cat temp2 >>temp
    rm temp2
    echo $5 | cut -d '|' -f1 >temp2
    echo $5 | cut -d '|' -f2 >>temp2
    echo $5 | cut -d '|' -f3 >>temp2
    cat temp2
    
    if [ $(tail -n 1 temp2) = $(tail -n 2 temp2 | head -n 1) ]; then
        continue
    fi
    
    for ln in $(cat temp2); do
        echo $ln | cut -d ';' -f1 >t3
        echo $ln | cut -d ';' -f2 >t2
        echo $ln | cut -d ';' -f3 >t1
        #if [[ -z $v7 || -z $v8 ]]; then
        #   continue;
        #fi
        (
            echo $1","$v2","$v3","$v4
            echo $1","$v2","$v3","$v4
            echo $1","$v2","$v3","$v4
        ) >temp3
        paste -d "," temp3 t3 t2 t1 >>op
    done
done

rm t* temp*