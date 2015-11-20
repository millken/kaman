OK_COLOR=\033[32;01m
NO_COLOR=\033[0m

BINDATA_IGNORE = $(shell git ls-files -io --exclude-standard $< | sed 's/^/-ignore=/;s/[.]/[.]/g')

.PHONY: all

all: clean format deps build

help:
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
	godep go test -cover ./...

format:	
	go fmt ./...
	
archive:
	@echo "$(OK_COLOR)==> Building Tarball...$(NO_COLOR)"
	tar -cvzf dist/kaman.tar.gz config/production.ini config/staging.ini bin/kaman

deps:
	@echo "$(OK_COLOR)==> Installing dependencies$(NO_COLOR)"
	@go get -d -v ./...
	@go list -f '{{range .TestImports}}{{.}} {{end}}' ./... | xargs -n1 go get -d

build: 
	$(eval SHA := $(shell git rev-parse --short=5 HEAD))
	@echo "$(OK_COLOR)==> Compiling binary$(NO_COLOR)"	
	godep go build -ldflags "-X main.gitVersion='$(SHA)'" -o bin/kaman

release: 
	gox -osarch="darwin/amd64 darwin/386 linux/amd64 linux/386 windows/amd64 windows/386" -output="./bin/kaman_{{.OS}}_{{.Arch}}"

setup:
	go get github.com/mitchellh/gox
	go get github.com/tools/godep
	godep restore

clean:
	@rm -f ./bin/*
	@rm -f kaman
