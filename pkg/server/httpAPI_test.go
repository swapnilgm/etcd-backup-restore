// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthCheckHandler(t *testing.T) {
	// HTTPHandler is implementation to handle HTTP API exposed by server
	healthyHandler := HTTPHandler{
		Health: http.StatusOK,
	}
	unhealthyHandler := HTTPHandler{
		Health: http.StatusInternalServerError,
	}
	if err := healthCheckTest(healthyHandler.serveHealthz, http.StatusOK, true); err != nil {
		t.Fatal(err)
	}
	if err := healthCheckTest(unhealthyHandler.serveHealthz, http.StatusInternalServerError, false); err != nil {
		t.Fatal(err)
	}
}

func healthCheckTest(handlerFunc http.HandlerFunc, expectedStatus int, expectedHealth bool) error {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("GET", "/healthz", nil)
	if err != nil {
		return err
	}
	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handlerFunc)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != expectedStatus {
		return fmt.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := fmt.Sprintf(`{"health":%v}`, expectedHealth)
	if rr.Body.String() != expected {
		return fmt.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
	return nil
}
