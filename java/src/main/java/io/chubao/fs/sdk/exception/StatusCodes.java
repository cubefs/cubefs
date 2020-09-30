// Copyright 2020 The Chubao Authors.
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
package io.chubao.fs.sdk.exception;

import org.apache.commons.lang.NullArgumentException;

public enum StatusCodes {
    CFS_STATUS_OK(0, "Ok"),
    CFS_STATUS_ERROR(-999, "Error"),
    CFS_STATUS_INVALID_ARGUMENT(-100, "Invalid argument."),
    CFS_STATUS_FILE_EXISTS(-17, "File exists"),
    CFS_STATUS_FILIE_NOT_FOUND(-2, "no such file or directory."),
    CFS_STATUS_NULL_ARGUMENT(-3, "Null argument."),
    CFS_STATUS_EOF(-5, "End-of-file reached");

    public static StatusCodes get(int code) {
        switch (code) {
            case 0:
                return CFS_STATUS_OK;
            case -100:
                return CFS_STATUS_INVALID_ARGUMENT;
            case -17:
                return CFS_STATUS_FILE_EXISTS;
            case -2:
                return CFS_STATUS_FILIE_NOT_FOUND;
            case -3:
                return CFS_STATUS_NULL_ARGUMENT;
            default:
                return CFS_STATUS_ERROR;
        }
    }

    private int code;
    private String msg;

    private StatusCodes(int code) {
        this.code = code;
        this.msg = "unkown error";
    }

    private StatusCodes(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int code() {
        return code;
    }

    public String msg() {
        return msg;
    }
}