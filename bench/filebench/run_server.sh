#! /bin/bash

declare -a confs=(
    #"NONE"
    "RADIX_TREES" 
    "EXTENT_TREES"
    #"GLOBAL_HASH_TABLE"
    "HASHFS"
    "GLOBAL_CUCKOO_HASH"
    "LEVEL_HASH_TABLES"
);

prefix="$(realpath ../../)"
suffix="bench/filebench"
benchmark_name="fileserver"; #"webserver"; #"varmail"; #"webproxy"; #"fileserver";
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
    MLFS_IDX_STRUCT="$k" MLFS_LAYOUT_SCORE='85'  ./bin/mkfs.mlfs 1 &>> $prefix/$suffix/$benchmark_name/$k/mkfs1.txt
    MLFS_IDX_STRUCT="$k" MLFS_LAYOUT_SCORE='85' ./bin/mkfs.mlfs 4 &>> $prefix/$suffix/$benchmark_name/$k/mkfs4.txt
    # Run server
    cd $prefix/kernfs/tests;
    MLFS_IDX_STRUCT="$k" MLFS_LAYOUT_SCORE='85'  taskset -c 0 ./run.sh kernfs >> $prefix/$suffix/$benchmark_name/$k/server.txt 2>&1 &
    sleep 5;
    # Run client
    cd $prefix/bench/filebench;
    MLFS_IDX_STRUCT="$k" MLFS_LAYOUT_SCORE='85' taskset -c 3 ./run_mine.sh $benchmark_name >> $prefix/$suffix/$benchmark_name/$k/client.txt 2>&1 &
    #read -p "Exit $k: "
    sleep 67
    echo "Killing"
    ps aux|grep kernfs|awk '{print $2}'|while read i; do kill -9 $i; done
    ps aux |grep mlfs|awk '{print $2}'|while read i; do kill -9 $i; done
    ps aux |grep filebench|awk '{print $2}'|while read i; do kill -9 $i; done
    ps aux |grep filereader|awk '{print $2}'|while read i; do kill -9 $i; done
    ps aux|grep $benchmark_name|awk '{print $2}'|while read i; do kill -9 $i; done
    ps aux|grep run_mine|awk '{print $2}'|while read i; do kill -9 $i; done
    rm -rf /tmp/filebench-shm-*
  done
  rm -rf /tmp/tmp.txt
  grep 'IO Summary:' $prefix/$suffix/$benchmark_name/$k/client.txt > /tmp/tmp.txt
  echo -n "$k " >> $prefix/$suffix/$benchmark_name/result.txt
  awk 'BEGIN{a=0.0;b=0.0;c=0;}{a+=$6;b+=$6^2;c+=1}END{print a/c,sqrt(b/c-(a/c)^2)}' /tmp/tmp.txt >> $prefix/$suffix/$benchmark_name/result.txt
done
