package job

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/MaksimTheTestTaskSolver/objectTracker/repository"
)

func NewDeleteJob(db *sqlx.DB, repository *repository.ObjectRepository) *DeleteJob {
	return &DeleteJob{db: db, repository: repository}
}

type DeleteJob struct {
	db         *sqlx.DB
	repository *repository.ObjectRepository
}

// DeleteJob deletes expired object records
func (j *DeleteJob) Run() {
	logger := logrus.WithField("name", "deleteJob")
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			amountDeleted, err := j.repository.DeleteExpiredObjects(j.db)
			if err != nil {
				logger.Errorf("Can't delete expired objects: %s", err)
				continue
			}
			logger.Infof("Deleted %d expired objects", amountDeleted)
		}
	}
}
