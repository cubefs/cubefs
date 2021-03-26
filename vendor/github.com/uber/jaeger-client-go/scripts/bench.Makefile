# A small helper Makefile to build the benchmark binary so that it can be run on other machines.
#
# The binary can be run with
#     $ ./benchmark.bin -test.bench=BenchmarkTracer -test.run=BenchmarkTracer

build:
	go test -c -o=benchmark.bin .

build-linux:
	CGO_ENABLED=0 GOOS=linux go test -c -o=benchmark.bin .
