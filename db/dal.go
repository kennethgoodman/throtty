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

// The bulk of the logic is here:
// We store the data in two keys:
//        endpointUUID -> list of request UUIDs that hit this endpoint
//        requestUUID ->  nil, but with a TTL of 1000 seconds (the period)
// we set the requestUUID with a TTL so we know whether or not we should count it
// when we loop over the list for the endpointUUID, we can look to see if requestUUID is still valid
// this is automatically handled by buntdb TTL
//  
// we store "list of request UUIDs that hit this endpoint" as a ";" seperated list because there are no lists/sets in buntdb
// We need to do string logic because of that
// Main logic:
//    Update and lock DB
//       valid requests = get requests for endpoint that are still valid
//       set current request to endpoint with "valid request" and current request
// 	 set TTL on request
//    return count
//
//
// "set current request to endpoint with "valid request" and current request" is so we don't loop store request we know are not valid
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


// Main logic:
//    get endpoint uuid list
//    split by ";"
//    return list = []
//    loop over each request
//         check if request exists (still has TTL)
//	   append request to "return list"
//    return "return list"
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
