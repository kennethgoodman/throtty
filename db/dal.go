package db

import (
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/buntdb"
	"strconv"
)

type DAL interface {
	GetNumberOfRequestsForEndpoint(endpointUUID uuid.UUID) (*int, error)
	GetNumberOfRequestsForUser(userUUID uuid.UUID) (*int, error)

	AddRequest(requestUUID, endpointUUID, userUUID *uuid.UUID) error
}

type dal struct {
	db *buntdb.DB
}

func NewDAL(db *buntdb.DB) DAL {
	return &dal{
		db: db,
	}
}

func (d *dal) GetNumberOfRequestsForEndpoint(endpointUUID uuid.UUID) (*int, error) {
	return d.getNumberOfRequestsForCol(&endpointUUID)
}

func (d *dal) GetNumberOfRequestsForUser(userUUID uuid.UUID) (*int, error) {
	return d.getNumberOfRequestsForCol(&userUUID)
}

func (d *dal) AddRequest(requestUUID, endpointUUID, userUUID *uuid.UUID) error {
	return d.incrementEndpointUUID(endpointUUID)
}

func (d *dal) getNumberOfRequestsForColWithTx(tx *buntdb.Tx, id *uuid.UUID) (*int, error) {
	val, err := tx.Get(id.String())
	zero := 0
	if err == buntdb.ErrNotFound {
		return &zero, nil
	}
	if err != nil {
		return nil, err
	}

	if val != "" {
		v, err := strconv.Atoi(val)
		return &v, err
	}
	return &zero, nil
}

func (d *dal) getNumberOfRequestsForCol(id *uuid.UUID) (*int, error) {
	currentCount := 0
	err := d.db.View(func(tx *buntdb.Tx) error {
		currentCountPtr, err := d.getNumberOfRequestsForColWithTx(tx, id)
		if err == nil && currentCountPtr != nil {
			currentCount = *currentCountPtr
		}
		return err
	})
	return &currentCount, err
}

func (d *dal) incrementEndpointUUID(endpointUUID *uuid.UUID) error {
	return d.changeCount(endpointUUID, 1)
}

func (d *dal) decrementEndpointUUID(endpointUUID *uuid.UUID) error {
	return d.changeCount(endpointUUID, -1)
}

func (d *dal) changeCount(key *uuid.UUID, countChangeBy int) error {
	return d.db.Update(func(tx *buntdb.Tx) error {
		currentCountPtr, err := d.getNumberOfRequestsForColWithTx(tx, key)
		if err != nil {
			return err
		}
		_, _, err = tx.Set(key.String(), strconv.Itoa(*currentCountPtr + countChangeBy), nil)
		return err
	})
}