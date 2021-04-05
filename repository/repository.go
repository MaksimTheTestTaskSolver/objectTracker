package repository

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

func NewObjectRepository() *ObjectRepository {
	return &ObjectRepository{}
}

type ObjectRepository struct {
}

// StoreObjectIDs stores the object ids into a db for the further handling.
func (r *ObjectRepository) StoreObjectIDs(tx *sqlx.DB, objectIDs []int) error {
	if len(objectIDs) == 0 {
		return nil
	}

	// insert the ids into the db. In case if the id already in the db set it's status_fetched field to false
	// to trigger the status fetch again.
	queryTemplate := `INSERT INTO object (id) values %s ON CONFLICT (id) DO UPDATE SET received_at = now(), status_fetched = false;`

	var values strings.Builder

	args := make([]interface{}, 0, len(objectIDs))

	// to use "ON CONFLICT DO UPDATE" statement we need to be sure that objectIDs list contains only unique values
	dedup := make(map[int]struct{})

	for i, objectID := range objectIDs {
		if _, ok := dedup[objectID]; ok {
			// not unique value, skip
			continue
		}

		dedup[objectID] = struct{}{}

		args = append(args, objectID)
		if i != 0 {
			values.WriteString(", ")
		}
		values.WriteString(fmt.Sprintf("($%d)", len(args)))
	}

	query := fmt.Sprintf(queryTemplate, values.String())

	_, err := tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("can't insert object ids into the database: %w", err)
	}

	return nil
}

// GetObjectsForUpdate returns the list of objectIDs for which we need to updated statuses. If objectID from this list
// won't be updated in the next 10 seconds, it will be returned by the next call to GetObjectsForUpdate again for retry
func (r *ObjectRepository) GetObjectsForUpdate(tx *sqlx.DB, amount int) (objectIDs []int, err error) {
	query := fmt.Sprintf(`
		UPDATE object SET updated_at = NOW() WHERE id IN (
			SELECT id from object WHERE status_fetched = false AND (updated_at IS Null OR updated_at < NOW() - INTERVAL '10 sec')
				FOR UPDATE SKIP LOCKED LIMIT %d
			) RETURNING id
`, amount)
	err = tx.Select(&objectIDs, query)

	if err != nil {
		return nil, fmt.Errorf("can't get objects for update from database: %w", err)
	}

	return objectIDs, nil
}

var ErrNoObjectsToUpdate = fmt.Errorf("no objects to update")

func (r *ObjectRepository) UpdateObject(tx *sqlx.DB, objectID int, online bool) error {
	res, err := tx.Exec("UPDATE object SET online = $1, status_fetched = true, updated_at = NOW() WHERE id = $2", online, objectID)

	if err != nil {
		return fmt.Errorf("can't update object: %w", err)
	}

	amountUpdated, err := res.RowsAffected()

	if err != nil {
		return fmt.Errorf("can't count the amount of deleted rows: %w", err)
	}

	if amountUpdated == 0 {
		return ErrNoObjectsToUpdate
	}

	return nil
}

func (r *ObjectRepository) DeleteObject(tx *sqlx.DB, objectID int) error {
	_, err := tx.Exec("DELETE FROM object WHERE id = $1", objectID)

	if err != nil {
		return fmt.Errorf("can't delete object from database: %w", err)
	}

	return nil
}

func (r *ObjectRepository) DeleteExpiredObjects(tx *sqlx.DB) (amountDeleted int64, err error) {
	res, err := tx.Exec("DELETE FROM object WHERE received_at < now() - interval '30 second'")

	if err != nil {
		return 0, fmt.Errorf("can't delete object from database: %w", err)
	}

	amountDeleted, err = res.RowsAffected()

	if err != nil {
		return 0, fmt.Errorf("can't count the amount of deleted rows: %w", err)
	}

	return amountDeleted, nil
}
