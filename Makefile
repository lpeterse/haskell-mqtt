.PHONY: docs profbench test all

all:	install test bench docs


install:
	stack install

test:
	stack test

bench:
	stack bench

docs:
	stack haddock
	rm -rf docs
	cp -r .stack-work/install/x86_64-linux/lts-7.10/8.0.1/doc/mqtt-0.1.0.0 docs
	echo "mqtt.lpeterse.de" > docs/CNAME
	git add docs

# More exotic targets..

profbench:
	stack bench --library-profiling --executable-profiling --benchmark-arguments '+RTS -N -s -p'
