package db

import (
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/buntdb"
	"strings"
	"time"
)

type DAL interface {
	GetNumberAndAddRequest(requestUUID, endpointUUID, userUUID *uuid.UUID) (*int, error)
}

type dal struct {
	db *buntdb.DB
}

func NewDAL(db *buntdb.DB) DAL {
	return &dal{
		db: db,
	}
}

func (d *dal) GetNumberAndAddRequest(requestUUID, endpointUUID, userUUID *uuid.UUID) (*int, error) {
	count := 0
	err := d.db.Update(func(tx *buntdb.Tx) error {
		newRequestUUIDs, err := d.getRequestsStillValid(tx, endpointUUID)
		if err != nil {
			return err
		}
		count = len(newRequestUUIDs)
		_, _, err = tx.Set(endpointUUID.String(),
			strings.Join(append(newRequestUUIDs, requestUUID.String()), ";"), nil)
		if err != nil {
			return err
		}
		_, _, err = tx.Set(requestUUID.String(), "", &buntdb.SetOptions{Expires:true, TTL:time.Second * 1000})
		if err != nil {
			// TODO if this fails, should we rollback?
		}
		return err
	})
	return &count, err
}

func (d *dal) getRequestsStillValid(tx *buntdb.Tx, endpointUUID *uuid.UUID) ([]string, error) {
	v, err := tx.Get(endpointUUID.String())
	if err == buntdb.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	requestUUIDs := strings.Split(v, ";")
	newRequestUUIDs := make([]string, 0)
	for _, requestUUID := range requestUUIDs {
		_, err := tx.Get(requestUUID)
		if err != nil {
			continue
		}
		newRequestUUIDs = append(newRequestUUIDs, requestUUID)
	}
	return newRequestUUIDs, nil
}
