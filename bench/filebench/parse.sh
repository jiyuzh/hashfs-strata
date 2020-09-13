declare -a benchmarks=(
  "varmail"
  "webproxy"
  "fileserver"
  "webserver"
);
declare -a confs=(
  "RADIX_TREES"
  "EXTENT_TREES"
  "HASHFS"
  "GLOBAL_CUCKOO_HASH"
  "LEVEL_HASH_TABLES"
);
echo -n "Benchmarks";
for j in "${confs[@]}"
do
  echo -n " $j";
done
echo "";
for k in "${benchmarks[@]}";
do
  echo -n "$k";
  #tail -n +2 $k/result.txt|awk '{printf " %.1f+-%.1f%",$2,100.0*$3/$2}';
  tail -n +2 $k/result.txt|awk 'BEGIN{a=0;}{if(a==0)a=$2;printf " %.1f",$2/a}';
  echo "";
done
