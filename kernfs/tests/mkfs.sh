#! /bin/bash

echo "Formatting DEV 1 (shared area)"
../../libfs/bin/mkfs.mlfs 1
#sudo ../../libfs/bin/mkfs.mlfs 2
#../../libfs/bin/mkfs.mlfs 3
echo "Formatting DEV 4 (per-app log)"
../../libfs/bin/mkfs.mlfs 4
echo "Formatting DEV 5 ('undo' log)"
../../libfs/bin/mkfs.mlfs 5
