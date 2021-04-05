package main

import (
	"net/http"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/MaksimTheTestTaskSolver/objectTracker/handler"
	"github.com/MaksimTheTestTaskSolver/objectTracker/job"
	"github.com/MaksimTheTestTaskSolver/objectTracker/repository"
	"github.com/MaksimTheTestTaskSolver/objectTracker/statusFetcher"
)

var schema = `
CREATE TABLE IF NOT EXISTS object (
    id bigint Primary Key,
    online boolean,
    status_fetched boolean NOT NULL DEFAULT false,
    received_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp
);
CREATE INDEX IF NOT EXISTS received_idx ON object (received_at);
CREATE INDEX IF NOT EXISTS updated_idx ON object (updated_at);
`

func main() {
	db, err := sqlx.Connect("postgres", "user=test dbname=test password=password port=2345 sslmode=disable")
	if err != nil {
		logrus.Fatalln(err)
	}

	db.MustExec(schema)

	objectRepo := repository.NewObjectRepository()
	sf := statusFetcher.NewStatusFetcher(db, objectRepo)
	h := handler.NewHandler(db, objectRepo, sf)

	http.HandleFunc("/callback", h.Callback)

	go job.NewDeleteJob(db, objectRepo).Run()
	go sf.Run()

	logrus.Info("start service on :9090 port")

	logrus.Info(http.ListenAndServe(":9090", nil))
}
