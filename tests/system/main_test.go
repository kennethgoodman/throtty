package system

import (
	"testing"

	"github.com/kennethgoodman/throtty/throttler"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"net/http"
)

func sendRequest(endpointUUID uuid.UUID) *http.Response {
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://localhost:8080/", nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add(throttler.EndpointUUIDHeader, endpointUUID.String())
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

// Test5RequestsInOneSecond needs to have the main server running
func Test5RequestsInOneSecond(t *testing.T) {
	endpointUUID, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}

	for i := 0; i <= 4; i++ {
		resp := sendRequest(endpointUUID)
		assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	}
	resp := sendRequest(endpointUUID)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}