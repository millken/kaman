

BINDATA_IGNORE = $(shell git ls-files -io --exclude-standard $< | sed 's/^/-ignore=/;s/[.]/[.]/g')

usage:
	@echo ""
	@echo "Task                 : Description"
	@echo "-----------------    : -------------------"
	@echo "make setup           : Install all necessary dependencies"
	@echo "make test            : Run tests"
	@echo "make format          : formater code"
	@echo "make build           : Generate production build for current OS"
	@echo "make release         : Generate binaries for all supported OSes"
	@echo "make clean           : Remove all build files and reset assets"
	@echo ""

test:
	godep go test

format:	
	go fmt ./...
	
build: 
	godep go build

release: 
	gox -osarch="darwin/amd64 darwin/386 linux/amd64 linux/386 windows/amd64 windows/386" -output="./bin/kafkaman_{{.OS}}_{{.Arch}}"

setup:
	go get github.com/mitchellh/gox
	go get github.com/tools/godep
	godep restore

clean:
	rm -f ./bin/*

