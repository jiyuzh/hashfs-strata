#! /bin/bash

sudo cgdb -p `pgrep ycsbc | tail -n 1`
