declare -a confs=(
    "NONE"
    "EXTENT_TREES"
    "GLOBAL_HASH_TABLE"
    "HASHFS"
    "GLOBAL_CUCKOO_HASH"
    "LEVEL_HASH_TABLES"
    "RADIX_TREES" 
);

prefix="/home/takh/git-repos/efes-strata";
suffix="bench/filebench"
benchmark_name="fileserver";
ITERATION=10

rm -rf $prefix/$suffix/$benchmark_name;
mkdir $prefix/$suffix/$benchmark_name;

echo "Index Throughput-avg Throughput-std-dev" > $prefix/$suffix/$benchmark_name/result.txt

for k in "${confs[@]}";
do
  echo $k;
  mkdir $prefix/$suffix/$benchmark_name/$k;
  for (( j=0; j<ITERATION; j++ ))
  do    
    # Format filesystem
    cd $prefix/libfs;
    sudo MLFS_IDX_STRUCT="$k" ./bin/mkfs.mlfs 1 &>> $prefix/$suffix/$benchmark_name/$k/mkfs1.txt
    sudo MLFS_IDX_STRUCT="$k" ./bin/mkfs.mlfs 4 &>> $prefix/$suffix/$benchmark_name/$k/mkfs4.txt
    # Run server
    cd $prefix/kernfs/tests;
    sudo MLFS_IDX_STRUCT="$k" ./run.sh kernfs >> $prefix/$suffix/$benchmark_name/$k/server.txt 2>&1 &
    # Run client
    cd $prefix/bench/filebench;
    sudo MLFS_IDX_STRUCT="$k" ./run_mine.sh $benchmark_name >> $prefix/$suffix/$benchmark_name/$k/client.txt 2>&1 &
    #read -p "Exit $k: "
    sleep 120
    echo "Exiting"
    pid=$(ps aux|grep kernfs|head -1|awk '{print $2}');
    for (( i=pid; i<pid+3;i++ ))
    do
      sudo kill -9 $i;
    done
    ps aux |grep mlfs|awk '{print $2}'|while read i; do sudo kill -9 $i; done
    ps aux |grep filereader|awk '{print $2}'|while read i; do sudo kill -9 $i; done
    sudo rm -rf /tmp/filebench-shm-*
  done
  rm -rf /tmp/tmp.txt
  grep 'IO Summary:' $prefix/$suffix/$benchmark_name/$k/client.txt > /tmp/tmp.txt
  echo -n "$k " >> $prefix/$suffix/$benchmark_name/result.txt
  awk 'BEGIN{a=0.0;b=0.0;c=0;}{a+=$6;b+=$6^2;c+=1}END{print a/c,sqrt(b/c-(a/c)^2)}' /tmp/tmp.txt >> $prefix/$suffix/$benchmark_name/result.txt
done
