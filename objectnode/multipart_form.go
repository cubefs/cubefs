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
	"errors"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type FormRequest struct {
	*http.Request
}

func NewFormRequest(r *http.Request) *FormRequest {
	return &FormRequest{r}
}

func (r *FormRequest) multipartReader() (*formReader, error) {
	v := r.Header.Get("Content-Type")
	if v == "" {
		return nil, http.ErrNotMultipart
	}
	d, params, err := mime.ParseMediaType(v)
	if err != nil || d != "multipart/form-data" {
		return nil, http.ErrNotMultipart
	}
	boundary, ok := params["boundary"]
	if !ok {
		return nil, http.ErrMissingBoundary
	}
	return &formReader{multipart.NewReader(r.Body, boundary)}, nil
}

// ParseMultipartForm parses a request body as multipart/form-data.
// Except for the data of file, the values of other form keys will be parsed into memory,
// and the data of file can be read through FormFile.
// After one call to ParseMultipartForm, subsequent calls have no effect.
func (r *FormRequest) ParseMultipartForm() error {
	if r.Method != "POST" || !strings.Contains(r.Header.Get(ContentType), ValueMultipartFormData) {
		return errors.New("request should be POST multipart/form-data")
	}
	if r.Form == nil {
		if err := r.ParseForm(); err != nil {
			return err
		}
	}
	if r.MultipartForm != nil {
		return nil
	}

	mr, err := r.multipartReader()
	if err != nil {
		return err
	}

	file, mf, err := mr.readForm()
	if err != nil {
		return err
	}

	if r.PostForm == nil {
		r.PostForm = make(url.Values)
	}
	for k, v := range mf.Value {
		r.Form[k] = append(r.Form[k], v...)
		r.PostForm[k] = append(r.PostForm[k], v...)
	}

	r.MultipartForm = mf
	r.Body = file

	return nil
}

// MultipartFormValue returns the first value for the named component of the POST form.
// If key is not present, MultipartFormValue returns the empty string.
func (r *FormRequest) MultipartFormValue(key string) string {
	for k, v := range r.MultipartForm.Value {
		if strings.ToLower(k) == strings.ToLower(key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

// FileName returns the first file key of the POST form.
func (r *FormRequest) FileName() string {
	return r.MultipartFormValue("file")
}

// FormFile returns the File for file key in POST form.
// FormFile calls ParseMultipartForm if necessary.
func (r *FormRequest) FormFile(maxMemory int64) (f multipart.File, size int64, err error) {
	if r.MultipartForm == nil {
		if err = r.ParseMultipartForm(); err != nil {
			return
		}
	}
	defer r.Body.Close()

	// file, store in memory or on disk
	var b bytes.Buffer
	n, err := io.CopyN(&b, r.Body, maxMemory+1)
	if err != nil && err != io.EOF {
		return
	}
	fc := new(fileContent)
	if n > maxMemory {
		// too big, write to disk and flush buffer
		var file *os.File
		file, err = os.CreateTemp("", "multipart-")
		if err != nil {
			return
		}
		size, err = io.Copy(file, io.MultiReader(&b, r.Body))
		if cerr := file.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			os.Remove(file.Name())
			return
		}
		fc.tmpfile = file.Name()
	} else {
		fc.content = b.Bytes()
		size = int64(len(fc.content))
	}

	f, err = fc.open()

	return
}

type formReader struct {
	*multipart.Reader
}

func (fr *formReader) readForm() (file io.ReadCloser, form *multipart.Form, err error) {
	form = &multipart.Form{
		Value: make(map[string][]string),
		File:  make(map[string][]*multipart.FileHeader),
	}

	// reserve 10 MB for non-file parts
	maxValueBytes := int64(10 << 20)
	for {
		p, err := fr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		name := p.FormName()
		if name == "" {
			continue
		}

		var b bytes.Buffer
		if name != "file" {
			// value, store as string in memory
			n, err := io.CopyN(&b, p, maxValueBytes+1)
			if err != nil && err != io.EOF {
				return nil, nil, err
			}
			maxValueBytes -= n
			if maxValueBytes < 0 {
				return nil, nil, multipart.ErrMessageTooLarge
			}
			form.Value[name] = append(form.Value[name], b.String())
			continue
		}
		// "file" is the last form key according aws, all subsequent keys will be discarded
		form.Value["file"] = append(form.Value["file"], p.FileName())
		file = &readCloser{p, fr.close}
		break
	}
	if file == nil {
		return nil, nil, http.ErrMissingFile
	}

	return file, form, nil
}

func (fr *formReader) close() error {
	for {
		p, err := fr.NextPart()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// discard the keys after "file" if exist
		io.Copy(ioutil.Discard, p)
	}
}

type readCloser struct {
	io.Reader
	closer func() error
}

func (r *readCloser) Close() error {
	return r.closer()
}

type fileContent struct {
	content []byte
	tmpfile string
}

func (fc *fileContent) open() (multipart.File, error) {
	if b := fc.content; b != nil {
		r := io.NewSectionReader(bytes.NewReader(b), 0, int64(len(b)))
		return &sectionReadCloser{r}, nil
	}
	f, err := os.Open(fc.tmpfile)
	if err != nil {
		return nil, err
	}

	return &fileCloser{f}, nil
}

type fileCloser struct {
	*os.File
}

func (r *fileCloser) Close() error {
	err := r.File.Close()
	if e := os.Remove(r.File.Name()); e != nil && err == nil {
		err = e
	}
	return err
}

type sectionReadCloser struct {
	*io.SectionReader
}

func (rc sectionReadCloser) Close() error {
	return nil
}
