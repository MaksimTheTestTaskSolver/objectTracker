.PHONY: run prepare-db start

prepare-db:
	docker run --name test-postgres -p 2345:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_USER=test -e POSTGRES_DB=test -d postgres

kill-db:
	docker stop test-postgres
	docker rm test-postgres

run:
	go run main.go
