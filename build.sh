#!/bin/sh

#gcc -c lock_full_btree.c -c pager.c -c utils.c && \
#g++ -std=c++17 runner.cpp lock_full_btree.o pager.o utils.o ../benchmark/libbenchmark.a ../benchmark/libbenchmark_main.a -v -o runner

#g++ -std=c++17 runner.cpp -c lock_full_btree.c -c pager.c -c utils.c ../benchmark/libbenchmark.a ../benchmark/libbenchmark_main.a
# -o runner

#g++ -std=c++17 runner.o lock_full_btree.o pager.o utils.o ../benchmark/libbenchmark.a ../benchmark/libbenchmark_main.a -o runner

g++ -std=c++17 runner.cpp utils.cpp pager.cpp btree_base.cpp ../benchmark/libbenchmark.a ../benchmark/libbenchmark_main.a -o runner