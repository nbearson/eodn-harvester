#!/bin/bash
#
# Ingest files into AVDLN
# Upload exNodes into LoDN
# Produce scene list for GloVis  
#
# Global declarations
#
lodn="lodn://dlt.incntre.iu.edu:5000/eodn/"
#lodn="lodn://dresci.incntre.iu.edu:5000/eodn/"        # path to the LoDN server
#lodn="lodn://tvdlnet0.sfasu.edu:5000/eodn/"
#scenelist="glovis_scenelist_africa"                          # name of the Glovis Scenelist 
#scenelist="glovis_scenelist"                          # name of the Glovis Scenelist 
#scenelist="glovis_scenelist_texas"
#scenelist="glovis_scenelist_tennessee"
#scenelist="glovis_scenelist_wisconsin"
#scenelist="glovis_scenelist_louisana"
#scenelist="glovis_scenelist_indiana"
#scenelist="glovis_scenelist_kenya"
echo $lodn
#FILES=/mnt/l8/Landsat8/indiana/*.tar.gz
#FILES=/mnt/l8/Landsat8/kenya/*.tar.gz
#FILES=/mnt/l8/Landsat8/michigan/*.tar.gz

#FILES=/mnt/l8/Landsat8/tennessee/*.tar.gz
#FILES=/mnt/l8/Landsat8/texas/*.tar.gz
#FILES=/mnt/l8/Landsat8/wisconsin/*.tar.gz
FILES=/data/prb/eodn/*
#
# Function - accepts year and day-of-year
#            returns YYYY-MM-DD in global variable $ymd
#
function jul () 
  { 
	ymd=`date -d "$1-01-01 +$2 days -1 day" "+%Y-%m-%d"`
  }
#
# Remove old scenelist
#
#rm $scenelist
#
# Create base directory Structure
#
#lodn_mkdir $lodn
#
# Process through the files
#
for f in $FILES
do
  echo " "
  echo "Processing $f ..."
  #
  # extract parameters from filepath
  #
  for fullpath in "$f"                                # extract the base name from the path
    do
      filename="${fullpath##*/}"                      # Strip longest match of */ from start
      dir="${fullpath:0:${#fullpath} - ${#filename}}" # Substring from 0 thru pos of filename
      base="${filename%%.[^.]*}"                      # Strip shortest match of . plus at least one non-dot char from end
      ext="${filename:${#base} + 1}"                  # Substring from len of base thru end
      if [[ -z "$base" && -n "$ext" ]]; then          # Extension and no base, is really the base
        base=".$ext"
        ext=""
      fi
  done
  #
  # deconstruct the base name
  #
  sensor="${base:0:3}"
  path="${base:3:3}"
  row="${base:6:3}"
  year="${base:9:4}"
  doy="${base:13:3}"
  xnd=$base.$ext".xnd"
  jul $year $doy                                     # convert the day-of-year to yyyy-mm=dd
  if [ $sensor = "LC8" ]; then
    sensor="l8oli"
  fi
  sceneid="${sensor},${path},${row},${ymd}"
  #
  #  Debugging 
  #
  #echo -e "$fullpath:\n\tdir  = \"$dir\"\n\tbase = \"$base\"\n\text  = \"$ext\""
  #echo sensor = $sensor
  #echo path = $path
  #echo row = $row
  #echo year = $year
  #echo doy = $doy
  #echo date = $ymd
  echo xnd = $xnd
  #echo scene ID = $sceneid
  #
  # Build the exnode path while
  # Building the directory structure
  # if directory exists, just let it fail
  #
#  echo "Making directory structure"
  lodn_path="${lodn}${sensor}"
#  lodn_mkdir "${lodn_path}"
#echo $lodn_path
  lodn_path="${lodn_path}/p${path}"
#  lodn_mkdir "${lodn_path}"
#echo $lodn_path
  lodn_path="${lodn_path}/r${row}"
#  lodn_mkdir "${lodn_path}"
#echo $lodn_path
  lodn_path="${lodn_path}/y${year}"
#  lodn_mkdir "${lodn_path}"
# echo $lodn_path
  # 
  # Upload the file to local depot 
  #
 # lors_upload --duration=10h --copies=1 --depot-list -f $f
  #
  # upload exNode to LoDN
  #
  #lodn_path="${lodn_path}/${base}.${ext}"
  lodn_path="${lodn_path}/${base}"
  echo LoDN Path: "$lodn_path"
  # unlink old exnode if it exists
  lodn_unlink $lodn_path
done    # file processing loop
