// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

import (
	"bytes"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormRequest(t *testing.T) {
	b := bytes.NewBuffer(nil)
	w := multipart.NewWriter(b)
	w.WriteField("key", "object.txt")
	w.CreateFormFile("file", "file.data")
	w.Close()

	// 1. request is not POST or invalid Content-Type
	req, _ := http.NewRequest("PUT", "https://cubefs.com/bucket", b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	formReq := NewFormRequest(req)
	err := formReq.ParseMultipartForm()
	require.ErrorContains(t, err, "POST")
	req, _ = http.NewRequest("POST", "https://cubefs.com/bucket", b)
	formReq = NewFormRequest(req)
	err = formReq.ParseMultipartForm()
	require.ErrorContains(t, err, "multipart/form-data")

	// 2. missing file
	b.Reset()
	w = multipart.NewWriter(b)
	w.WriteField("key", "object.txt")
	w.Close()
	req, _ = http.NewRequest("POST", "https://cubefs.com/bucket", b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	formReq = NewFormRequest(req)
	err = formReq.ParseMultipartForm()
	require.Equal(t, http.ErrMissingFile, err)

	// 3. called multiple times
	b.Reset()
	w = multipart.NewWriter(b)
	w.WriteField("key", "object.txt")
	w.CreateFormFile("file", "file.data")
	w.Close()
	req, _ = http.NewRequest("POST", "https://cubefs.com/bucket", b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	formReq = NewFormRequest(req)
	err = formReq.ParseMultipartForm()
	require.NoError(t, err)
	formReq = NewFormRequest(req)
	err = formReq.ParseMultipartForm()
	require.NoError(t, err)

	// 4. in memory
	fileData := "small file data"
	b.Reset()
	w = multipart.NewWriter(b)
	w.WriteField("key", "object.txt")
	fw, _ := w.CreateFormFile("file", "file-test.txt")
	fw.Write([]byte(fileData))
	w.Close()
	req, _ = http.NewRequest("POST", "https://cubefs.com/bucket", b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	formReq = NewFormRequest(req)
	err = formReq.ParseMultipartForm()
	require.NoError(t, err)
	require.Equal(t, "object.txt", formReq.MultipartFormValue("key"))
	require.Equal(t, "file-test.txt", formReq.FileName())
	f, size, err := formReq.FormFile(1024)
	require.NoError(t, err)
	defer f.Close()
	require.Equal(t, int64(len(fileData)), size)
	fb, _ := ioutil.ReadAll(f)
	require.Equal(t, fileData, string(fb))

	// 5. on disk
	fileData = "big file data"
	b.Reset()
	w = multipart.NewWriter(b)
	w.WriteField("key", "object.txt")
	fw, _ = w.CreateFormFile("file", "file-test.txt")
	fw.Write([]byte(fileData))
	w.Close()
	req, _ = http.NewRequest("POST", "https://cubefs.com/bucket", b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	formReq = NewFormRequest(req)
	err = formReq.ParseMultipartForm()
	require.NoError(t, err)
	require.Equal(t, "object.txt", formReq.MultipartFormValue("key"))
	require.Equal(t, "file-test.txt", formReq.FileName())
	f, size, err = formReq.FormFile(5)
	require.NoError(t, err)
	defer f.Close()
	require.Equal(t, int64(len(fileData)), size)
	fb, _ = ioutil.ReadAll(f)
	require.Equal(t, fileData, string(fb))
}
