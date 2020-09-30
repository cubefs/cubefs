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

import io.chubao.fs.CfsLibrary;
import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.libsdk.CFSDriverIns;
import io.chubao.fs.sdk.util.CFSOwnerHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileStorageImpl implements FileStorage {
    private static final Log log = LogFactory.getLog(FileStorageImpl.class);
    private CFSDriverIns driver;
    private CFSOwnerHelper owner;
    private long defaultBlockSize = 128 * 1024 * 1024;
    private int defaultDirPermission = 0644;

    public FileStorageImpl(CFSDriverIns driver) {
        this.driver = driver;
    }

    public void init() throws Exception {
        owner = new CFSOwnerHelper();
        owner.init();
    }

    @Override
    public CFSFile open(String path, int flags, int mode, int uid, int gid) throws CFSException {
        int flagsTmp = flags;
        if ((flags & FileStorage.O_APPEND) != 0) {
            flags = flags & ~(FileStorage.O_APPEND);
        }
        int fd = driver.open(path, flags, mode, uid, gid);
        long size = driver.size(fd);
        long pos = 0L;
        if ((flagsTmp & FileStorage.O_APPEND) != 0) {
            pos = size;
        }
        if (log.isDebugEnabled()) {
            log.debug("Succ to open:" + path + " size:" + size + " pos:" + pos);
        }
        return new CFSFileImpl(driver, fd, size, pos);
    }

    @Override
    public boolean mkdirs(String path, int mode, int uid, int gid) throws CFSException {
        driver.mkdirs(path, mode, uid, gid);
        return true;
    }

    @Override
    public void truncate(String path, long newLength) throws CFSException {
        driver.truncate(path, newLength);
    }

    @Override
    public void close() throws CFSException {
        driver.closeClient();
    }

    @Override
    public void rmdir(String path, boolean recursive) throws CFSException {
        driver.rmdir(path, recursive);
    }

    @Override
    public void unlink(String path) throws CFSException {
        driver.unlink(path);
    }

    @Override
    public void rename(String src, String dst) throws CFSException {
        driver.rename(src, dst);
    }

    @Override
    public CFSStatInfo[] list(String path) throws CFSException {
        int fd = 0;
        ArrayList<CFSStatInfo> stats = new ArrayList<CFSStatInfo>();
        try {
            fd = driver.open(path, O_RDONLY, defaultDirPermission, 0, 0);
            long total = 0;
            long num = 0;
            while (true) {
                num = driver.list(fd, stats);
                if (num <= 0) {
                    break;
                }
                total += num;
            }

        } catch (CFSException ex) {
            throw ex;
        } catch (UnsupportedEncodingException e) {
            throw new CFSException(e);
        } finally {
            if (fd > 0) {
                driver.close(fd);
            }
        }
        CFSStatInfo[] res = new CFSStatInfo[stats.size()];
        stats.toArray(res);
        return res;
    }

    @Override
    public CFSStatInfo stat(String path) throws CFSException {
        CfsLibrary.StatInfo info = driver.getAttr(path);
        if (info == null) {
            return null;
        }
        return new CFSStatInfo(
            info.mode, info.uid, info.gid, info.size,
            info.ctime, info.mtime, info.atime);
    }

    @Override
    public void setXAttr(String path, String name, byte[] value) throws CFSException {
        throw new CFSException("Not implement setXAttr.");
    }

    @Override
    public byte[] getXAttr(String path, String name) throws CFSException {
        throw new CFSException("Not implement getXAttr.");
    }

    @Override
    public List<String> listXAttr(String path) throws CFSException {
        throw new CFSException("Not implement listXAttr.");
    }

    @Override
    public Map<String, byte[]> getXAttrs(String path, List<String> names)
        throws CFSException {
        throw new CFSException("Not implement getXAttrs.");
    }

    @Override
    public void removeXAttr(String path, String name) throws CFSException {
        log.error("Not implement.");
        throw new CFSException("Not implement removeXAttr.");
    }

    @Override
    public void chmod(String path, int mode) throws CFSException {
        driver.chmod(path, mode);
    }

    @Override
    public void chown(String path, int uid, int gid) throws CFSException {
        driver.chown(path, uid, gid);
    }

    @Override
    public void chown(String path, String user, String group) throws CFSException {
        driver.chown(path, owner.getUid(user), owner.getGid(group));
    }

    @Override
    public void setTimes(String path, long mtime, long atime) throws CFSException {
        driver.setTimes(path, mtime, atime);
    }

    @Override
    public int getUid(String username) throws CFSException {
        return owner.getUid(username);
    }

    @Override
    public int getGid(String group) throws CFSException {
        return owner.getGid(group);
    }

    @Override
    public int getGidByUser(String user) throws CFSException {
        return owner.getGidByUser(user);
    }

    @Override
    public String getUser(int uid) throws CFSException {
        return owner.getUser(uid);
    }

    @Override
    public String getGroup(int gid) throws CFSException {
        return owner.getGroup(gid);
    }

    private void setAttr(String path, int mode, int uid, int gid, long mtime, long atime)
        throws CFSException {
        throw new CFSException("Not implement setAttr.");
    }

    @Override
    public long getBlockSize() {
        return defaultBlockSize;
    }


    @Override
    public int getReplicaNumber() {
        return 3;
    }
}
