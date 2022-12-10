// Copyright 2022 The CubeFS Authors.
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

package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type reply struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func main() {
	http.HandleFunc("/third/token/check", func(w http.ResponseWriter, r *http.Request) {

		param := &struct {
			Uid   string `json:"uid"`
			Token string `json:"token"`
		}{}

		rp := &reply{}
		defer func() {
			if rp.Code != 0 {
				log.Println("error: token check failed")
			}

			data, err := json.Marshal(rp)
			if err != nil {
				panic(err)
			}

			w.Write(data)
		}()

		err := json.NewDecoder(r.Body).Decode(param)
		if err != nil {
			log.Printf("Decode token check param failed, err %s", err.Error())
			rp.Code = 1
			rp.Msg = "json decode param failed"
			return
		}

		if param.Uid == "" {
			rp.Code = 1
			rp.Msg = "uid is empty"
			return
		}

		if param.Token != "testToken" {
			rp.Code = 1
			rp.Msg = "token check failed"
			return
		}

	})

	log.Println("start auth example server")
	err := http.ListenAndServe(":10121", nil)
	if err != nil {
		log.Fatalf("listen 10121 failed, err %s", err.Error())
	}
}
