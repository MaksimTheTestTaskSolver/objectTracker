package statusFetcher

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/MaksimTheTestTaskSolver/objectTracker/repository"
)

func NewStatusFetcher(db *sqlx.DB, repository *repository.ObjectRepository) *StatusFetcher {
	return &StatusFetcher{start: make(chan struct{}, 1), db: db, repository: repository}
}

// StatusFetcher is a background process fetching the statuses for the objects stored in the database
// To start a process use StatusFetcher.Run method
type StatusFetcher struct {
	start chan struct{}

	db         *sqlx.DB
	repository *repository.ObjectRepository
}

func (sf *StatusFetcher) Start() {
	select {
	case sf.start <- struct{}{}:
	default:
	}
}

func (sf *StatusFetcher) Run() {
	for {
		<-sf.start

		logger := logrus.WithField("name", "statusFetcher")
		logger.Info("Start fetching statuses")

		for {
			objectIDs, err := sf.repository.GetObjectsForUpdate(sf.db, 100)
			if err != nil {
				logger.Errorf("Error during fetching objects for status update: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if len(objectIDs) == 0 {
				logger.Info("No objects to update")
				break
			}

			for _, objectID := range objectIDs {
				go sf.fetchStatus(logger, objectID)
			}
		}
	}
}

type Response struct {
	ObjectID int  `json:"id"`
	Online   bool `json:"online"`
}

func (sf *StatusFetcher) fetchStatus(logger *logrus.Entry, objectID int) {
	resp, err := http.Get("http://localhost:9010/objects/" + strconv.Itoa(objectID))
	if err != nil {
		logger.Errorf("Error during GET request: %s", err)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		logger.Warnf("Status code is not 200: %d", resp.StatusCode)
		return
	}

	var r Response
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		logger.Errorf("Can't decode the response: %s", err)
		return
	}

	if r.Online {
		if err := sf.repository.UpdateObject(sf.db, objectID, true); err != nil {
			logger.Errorf("Can't update the object status '%d': %s", objectID, err)
			return
		}
		return
	}

	if err := sf.repository.DeleteObject(sf.db, objectID); err != nil {
		logger.Errorf("Can't delete the offline object: %s", err)
		return
	}

	logger.Infof("Updated status for object %d", objectID)
}
