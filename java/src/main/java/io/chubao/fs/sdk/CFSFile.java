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
package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;

public interface CFSFile {
    void close() throws CFSException;

    void flush() throws CFSException;

    void write(byte[] buff, int buffOffset, int len) throws CFSException;

    void pwrite(byte[] buff, int buffOffset, int len, long fileOffset) throws CFSException;

    void seek(long offset) throws CFSException;

    long read(byte[] buff, int buffOffset, int len) throws CFSException;

    int pread(byte[] buff, int buffOffset, int len, long fileOffset) throws CFSException;

    long getFileSize();

    long getPosition();
}