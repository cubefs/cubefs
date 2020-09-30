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

import java.util.List;
import java.util.Map;

public interface FileStorage {
    int O_RDONLY = 0;
    int O_WRONLY = 1;
    int O_ACCMODE = 3;
    int O_CREAT = 64;
    int O_TRUNC = 512;
    int O_APPEND = 1024;

    int S_IFDIR = 16384;
    int S_IFREG = 32768;
    int S_IFLNK = 40960;

    boolean mkdirs(String path, int mode, int uid, int gid) throws CFSException;

    CFSFile open(String path, int flags, int mode, int uid, int gid) throws CFSException;

    void truncate(String path, long newLength) throws CFSException;

    void close() throws CFSException;

    void rmdir(String path, boolean recursive) throws CFSException;

    void unlink(String path) throws CFSException;

    void rename(String src, String dst) throws CFSException;

    CFSStatInfo[] list(String path) throws CFSException;

    CFSStatInfo stat(String path) throws CFSException;

    void setXAttr(String path, String name, byte[] value) throws CFSException;

    byte[] getXAttr(String path, String name) throws CFSException;

    List<String> listXAttr(String path) throws CFSException;

    Map<String, byte[]> getXAttrs(String path, List<String> names) throws CFSException;

    void removeXAttr(String path, String name) throws CFSException;

    void chown(String path, int uid, int gid) throws CFSException;

    void chown(String path, String user, String group) throws CFSException;

    void chmod(String path, int mode) throws CFSException;

    void setTimes(String path, long mtime, long atime) throws CFSException;

    long getBlockSize();

    int getReplicaNumber();

    int getUid(String username) throws CFSException;

    int getGid(String groupname) throws CFSException;

    int getGidByUser(String user) throws CFSException;

    String getUser(int uid) throws CFSException;

    String getGroup(int gid) throws CFSException;
}
