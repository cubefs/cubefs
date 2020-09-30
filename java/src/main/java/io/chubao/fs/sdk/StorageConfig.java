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

import io.chubao.fs.sdk.exception.CFSNullArgumentException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StorageConfig {
    private static final Log log = LogFactory.getLog(FileStorageImpl.class);
    public final static String CONFIG_KEY_MATSER = "masterAddr";
    public final static String CONFIG_KEY_VOLUME = "volName";
    public final static String CONFIG_KEY_FOLLOWER_READ = "followerRead";
    public final static String CONFIG_KEY_LOG_DIR = "logDir";
    public final static String CONFIG_KEY_LOG_LEVEL = "logLevel";
    private String masters;
    private String volumeName;
    private String owner;
    private String logDir = "/var/log/cfs/";
    private String logLevel = "info";
    private boolean followerRread = false;

    public StorageConfig() {
    }

    public void setMasters(String masters) {
        this.masters = masters;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setLogDir(String path) {
        this.logDir = path;
    }

    public void setLogLevel(String level) {
        this.logLevel = level;
    }

    public String getLogDir() {
        return this.logDir;
    }

    public String getLogLevel() {
        return this.logLevel;
    }

    public String getMasters() throws CFSNullArgumentException {
        if (masters == null) {
            throw new CFSNullArgumentException("The master is null.");
        }
        return this.masters;
    }

    public String getVolumeName() throws CFSNullArgumentException {
        if (volumeName == null) {
            throw new CFSNullArgumentException("The volume name is null.");
        }
        return this.volumeName;
    }

    public boolean getFollowerRead() {
        return this.followerRread;
    }

    public String getOwner() {
        return this.owner;
    }

    public void print() {
        log.info(CONFIG_KEY_MATSER + ":" + masters);
        log.info(CONFIG_KEY_VOLUME + ":" + volumeName);
    }
}
