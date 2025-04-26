# mkdir ForScript
# cd ForScript
# cp ../op ./


cat op|tr ' ' '^'>temp
cat temp>op2
unset IFS
for row in $(cat op2) ; do
    IFS=","
    set -- $row
    f=0
    # echo $1 $2 $3 $4 $5 $6 $7
    unset IFS
    for show in $(cat f2) ; do
        # echo $show $5 "Damn"
        if test $5 = $show ; then
            op=$(expr $(cat $show|cut -d ' ' -f1) + $7)
            cnt=$(expr $(cat $show|cut -d ' ' -f2) + 1)
            #  echo "The new value of" $show "is " $op "and count" $cnt
            (echo $op $cnt)|cat>$show
            #  echo "No I'm Came"
            f=1;break;
        fi
    done
    # echo $f
    if expr $f = 0 ; then
        #  echo "I'm Came"
        (echo $5)>>f2;
        (echo $7 "1")|cat>$5
    fi
done



cat ShowDetails|tr ' ' '^'>temp
grep -v "ShowID,Genre,Actors,Director,Release_Year,Synopsis" temp>ShowDetails
echo "0 1">M10
unset IFS
for row in $(cat ShowDetails) ; do
    IFS=",";set -- $row
    f=0
    # echo $1 $2 $3 $4 $5 $6
    unset IFS
    for genre in $(cat f1) ; do
        # echo $genre $2 "Damn"
        if test $2 = $genre ; then
            op=$(expr $(cat $genre|cut -d ' ' -f1) + $(cat $1|cut -d ' ' -f1))
            cnt=$(expr $(cat $genre|cut -d ' ' -f2) + $(cat $1|cut -d ' ' -f2))
            #    echo "The new value of" $genre "is " $op "and count" $cnt
            (echo $op $cnt)|cat>$genre
            #   echo "No I'm Came"
            f=1;break;
        fi
    done
    if expr $f = 0 ; then
        #echo "I'm Came"
        (echo $2)>>f1;
        (echo $(cat $1))|cat>$2
    fi
done
unset IFS
max=0
maxG=""
for genre in $(cat f1) ; do
    cnt=$(expr $(cat $genre|cut -d ' ' -f1) \* 100)
    avg=$(expr $cnt / $(cat $genre|cut -d ' ' -f2))
    if test $max -lt $avg  ; then
        max=$avg
        maxG=$genre
    fi
    echo $genre","$(cat $genre)","$(echo "scale=2;$avg / 100"|bc)>>Table
    rm $genre
done
echo $maxG
rm M*
rm f1
rm f2 temp
# cd ../
