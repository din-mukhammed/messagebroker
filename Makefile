.PHONY: test
test:
	LOGXI=* go test -v ./...

.PHONY: vendor
vendor:
	go mod tidy
	go mod vendor

%:
	@:
