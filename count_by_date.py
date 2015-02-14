#source='/opt/eodn/incoming/'
source='/data/prb/eodn/'
year='2014'
start=1
end=365
for i in $(seq -f "%03g" $start $end)
do
   count=`ls -la ${source}*${year}${i}*.zip 2>/dev/null | wc -l` 
   if [ ${count} -ne 0 ]; then
     echo "${year} day: ${i} ${count}" 
   fi
done
