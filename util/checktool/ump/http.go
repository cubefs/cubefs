package ump

import (
	"io"
	"net/http"
	"strings"
)

func doPost(url string, body []byte, headers map[string]string) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		return make([]byte, 0), err
	}
	for key, header := range headers {
		req.Header.Set(key, header)
	}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	resultBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return make([]byte, 0), err
	}
	return resultBody, nil
}
