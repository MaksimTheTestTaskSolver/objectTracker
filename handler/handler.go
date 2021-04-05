package handler

import (
	"encoding/json"
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/MaksimTheTestTaskSolver/objectTracker/repository"
	"github.com/MaksimTheTestTaskSolver/objectTracker/statusFetcher"
)

func NewHandler(
	db *sqlx.DB,
	repository *repository.ObjectRepository,
	statusFetcher *statusFetcher.StatusFetcher,
) *Handler {
	return &Handler{db: db, repository: repository, statusFetcher: statusFetcher}
}

type Handler struct {
	db            *sqlx.DB
	repository    *repository.ObjectRepository
	statusFetcher *statusFetcher.StatusFetcher
}

type Request struct {
	ObjectIDs []int `json:"object_ids"`
}

// Callback stores the ids form the request in the database and starts statusFetcher.StatusFetcher to fetch the status of each object
func (h *Handler) Callback(w http.ResponseWriter, req *http.Request) {
	logger := logrus.WithField("name", "handler")
	logger.Info("Handler called")

	var r Request
	if err := json.NewDecoder(req.Body).Decode(&r); err != nil {
		logger.Warnf("Invalid json: %s", err)
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := h.repository.StoreObjectIDs(h.db, r.ObjectIDs); err != nil {
		logger.Errorf("Can't store object ids: %s", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.statusFetcher.Start()
}
