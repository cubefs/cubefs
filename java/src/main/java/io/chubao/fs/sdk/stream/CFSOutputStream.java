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
package io.chubao.fs.sdk.stream;

import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.CFSFile;

import java.io.IOException;
import java.io.OutputStream;

public class CFSOutputStream extends OutputStream {
    private CFSFile cfile;

    public CFSOutputStream(CFSFile file) {
        this.cfile = file;
    }

    @Override
    public void close() throws IOException {
        try {
            cfile.close();
            super.close();
        } catch (CFSException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            cfile.flush();
        } catch (CFSException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            cfile.write(b, off, len);
        } catch (CFSException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }
}