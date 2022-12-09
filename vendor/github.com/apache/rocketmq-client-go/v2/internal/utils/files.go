/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func FileReadAll(path string) ([]byte, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data := make([]byte, stat.Size())
	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func ensureDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, 0755)
		}
		return err
	}
	if !info.IsDir() {
		return errors.New(path + " is a file")
	}
	return nil
}

func WriteToFile(path string, data []byte) error {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return err
	}
	tmpFile, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	_, err = tmpFile.Write(data)
	if err != nil {
		return err
	}
	CheckError(fmt.Sprintf("close %s", tmpFile.Name()), tmpFile.Close())

	prevContent, err := FileReadAll(path)
	if err == nil {
		bakFile, err := os.Create(path + ".bak")
		if err != nil {
			_, err = bakFile.Write(prevContent)
		}
		if err != nil {
			return err
		}
		CheckError(fmt.Sprintf("close %s", bakFile.Name()), bakFile.Close())
	}

	_, err = os.Stat(path)
	if err == nil {
		CheckError(fmt.Sprintf("remove %s", path), os.Remove(path))
	}
	return os.Rename(path+".tmp", path)
}
