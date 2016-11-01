.PHONY: docs profbench test all

all:
	stack install

profbench:
	stack bench --library-profiling --executable-profiling --benchmark-arguments '+RTS -N -s -p'

test:
	stack test

docs:
	stack haddock
	rm -rf docs
	cp -r .stack-work/install/x86_64-linux/lts-7.2/8.0.1/doc/mqtt-0.1.0.0 docs
	echo "mqtt.lpeterse.de" > docs/CNAME
	git add docs
