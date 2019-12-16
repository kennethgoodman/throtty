package throttler

import (
	"github.com/kennethgoodman/throtty/db"
	uuid "github.com/satori/go.uuid"
	"log"
	"net/http"
)

const (
	maxAllowedCount = 5

	EndpointUUIDHeader = "endpointUUID"
	UserUUIDHeader = "userUUID"
)

type Throttler interface {
	IsValidRequest(*http.Request) (*bool, error)
}

type throttler struct {
	Logger *log.Logger
	dal db.DAL
}

func NewThrottler(logger *log.Logger, dal db.DAL) Throttler {
	return &throttler{
		Logger: logger,
		dal: dal,
	}
}

func (t *throttler) IsValidRequest(request *http.Request) (*bool, error) {
	endpointUUID, err := uuid.FromString(request.Header.Get(EndpointUUIDHeader))
	if err != nil {
		return nil, err
	}

	requestUUID, _ := uuid.NewV4()
	userUUID, err := uuid.FromString(request.Header.Get(UserUUIDHeader))
	var c *int
	if err != nil {
		c, err = t.dal.GetNumberAndAddRequest(&requestUUID, &endpointUUID, nil)
	} else {
		c, err = t.dal.GetNumberAndAddRequest(&requestUUID, &endpointUUID, &userUUID)
	}
	if err != nil {
		return nil, err
	}
	isValid := *c < maxAllowedCount
	return &isValid, nil
}
