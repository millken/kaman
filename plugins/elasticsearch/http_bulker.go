package elasticsearch

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	//"sync"
	"time"
)

// A BulkIndexer is used to index documents in ElasticSearch
type BulkIndexer interface {
	// Index documents
	Index(body []byte) error
	// Check if a flush is needed
	CheckFlush(count int, length int) bool
}

// A HttpBulkIndexer uses the HTTP REST Bulk Api of ElasticSearch
// in order to index documents
type HttpBulkIndexer struct {
	// Protocol (http or https).
	Protocol string
	// Host name and port number (default to "localhost:9200").
	Domain string
	// Maximum number of documents.
	MaxCount int
	// Internal HTTP Client.
	client *http.Client
	// Optional username for HTTP authentication
	username string
	// Optional password for HTTP authentication
	password string
}

func NewHttpBulkIndexer(protocol string, domain string, maxCount int,
	username string, password string, httpTimeout uint32, httpDisableKeepalives bool,
	connectTimeout uint32, tlsConf *tls.Config) *HttpBulkIndexer {

	tr := &http.Transport{
		TLSClientConfig:   tlsConf,
		DisableKeepAlives: httpDisableKeepalives,
		Dial: func(network, address string) (net.Conn, error) {
			return net.DialTimeout(network, address, time.Duration(connectTimeout)*time.Millisecond)
		},
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   time.Duration(httpTimeout) * time.Millisecond,
	}
	return &HttpBulkIndexer{
		Protocol: protocol,
		Domain:   domain,
		MaxCount: maxCount,
		client:   client,
		username: username,
		password: password,
	}
}

func (h *HttpBulkIndexer) CheckFlush(count int, length int) bool {
	if count >= h.MaxCount {
		return true
	}
	return false
}

func (h *HttpBulkIndexer) Index(body []byte) error {
	var response_body []byte
	var response_body_json map[string]interface{}

	url := fmt.Sprintf("%s://%s%s", h.Protocol, h.Domain, "/_bulk")

	// Creating ElasticSearch Bulk HTTP request
	request, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("Can't create bulk request: %s", err.Error())
	}
	request.Header.Add("Accept", "application/json")
	if h.username != "" && h.password != "" {
		request.SetBasicAuth(h.username, h.password)
	}

	request_start_time := time.Now()
	response, err := h.client.Do(request)
	request_time := time.Since(request_start_time)
	if err != nil {
		if (h.client.Timeout > 0) && (request_time >= h.client.Timeout) &&
			(strings.Contains(err.Error(), "use of closed network connection")) {

			return fmt.Errorf("HTTP request was interrupted after timeout. It lasted %s",
				request_time.String())
		} else {
			return fmt.Errorf("HTTP request failed: %s", err.Error())
		}
	}
	if response != nil {
		defer response.Body.Close()
		if response.StatusCode > 304 {
			return fmt.Errorf("HTTP response error status: %s", response.Status)
		}
		if response_body, err = ioutil.ReadAll(response.Body); err != nil {
			return fmt.Errorf("Can't read HTTP response body: %s", err.Error())
		}
		err = json.Unmarshal(response_body, &response_body_json)
		if err != nil {
			return fmt.Errorf("HTTP response didn't contain valid JSON. Body: %s",
				string(response_body))
		}
		json_errors, ok := response_body_json["errors"].(bool)
		if ok && json_errors {
			return fmt.Errorf("ElasticSearch server reported error within JSON: %s",
				string(response_body))
		}
	}
	return nil
}
