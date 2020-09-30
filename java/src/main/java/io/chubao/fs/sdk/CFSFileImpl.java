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
import io.chubao.fs.sdk.libsdk.CFSDriverIns;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CFSFileImpl implements CFSFile {
    private static final Log log = LogFactory.getLog(CFSFileImpl.class);
    private CFSDriverIns driver;
    private long position = 0L;
    private long fileSize;
    private boolean isClosed = false;
    private int fd;

    public CFSFileImpl(CFSDriverIns driver, int fd, long fileSize, long position) {
        this.driver = driver;
        this.fd = fd;
        this.fileSize = fileSize;
        this.position = position;
    }

    public boolean isClosed() {
        return this.isClosed;
    }

    public long getFileSize() {
        return this.fileSize;
    }

    public long getPosition() {
        return this.position;
    }

    public void seek(long position) throws CFSException {
        this.position = position;
    }

    public void close() throws CFSException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        driver.flush(fd);
        driver.close(fd);
    }

    @Override
    public void flush() throws CFSException {
        driver.flush(fd);
    }

    private byte[] buffCopy(byte[] buff, int off, int len) {
        byte[] dest = new byte[len];
        System.arraycopy(buff, off, dest, 0, len);
        return dest;
    }

    public synchronized void write(byte[] buff, int off, int len) throws CFSException {
        if (off < 0 || len < 0) {
            throw new CFSException("Invalid arguments.");
        }

        long wsize = 0;
        if (off == 0) {
            wsize = write(position, buff, len);
        } else {
            byte[] newbuff = buffCopy(buff, off, len);
            wsize = write(position, newbuff, len);
        }

        position += wsize;
        if (position > fileSize) {
            fileSize = position;
        }
    }

    private long write(long offset, byte[] data, int len) throws CFSException {
        return driver.write(fd, offset, data, len);
    }

    public synchronized long read(byte[] buff, int off, int len) throws CFSException {
        if (off < 0 || len < 0) {
            throw new CFSException("Invalid arguments.");
        }

        long rsize = 0;
        if (off == 0) {
            rsize = driver.read(fd, position, buff, len);
        } else {
            byte[] newbuff = new byte[len];
            rsize = driver.read(fd, position, newbuff, len);
            System.arraycopy(newbuff, 0, buff, off, len);
        }

        if (rsize > 0) {
            position += rsize;
        }
        return rsize;
    }

    @Override
    public void pwrite(byte[] buff, int buffOffset, int len, long fileOffset) throws CFSException {
        throw new CFSException("Not implement.");
    }

    @Override
    public int pread(byte[] buff, int buffOffset, int len, long fileOffset) throws CFSException {
        throw new CFSException("Not implement.");
    }
}