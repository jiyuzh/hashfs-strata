#! /bin/bash

sudo cgdb -p `pgrep MTCC | tail -n 1`
