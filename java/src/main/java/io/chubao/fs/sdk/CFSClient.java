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
import io.chubao.fs.sdk.exception.CFSNullArgumentException;
import io.chubao.fs.sdk.exception.StatusCodes;
import io.chubao.fs.sdk.libsdk.CFSDriverIns;
import com.sun.jna.Native;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class CFSClient {
    private static final Log log = LogFactory.getLog(CFSClient.class);
    //private static AtomicLong clientID = new AtomicLong(0L);
    private CfsLibrary driver;
    private String sdkLibPath;

    public CFSClient(String libpath) {
        this.sdkLibPath = libpath;
    }

    public void init() throws CFSException {
        if (sdkLibPath == null) {
            throw new CFSNullArgumentException("Please specify the libsdk.so path.");
        }
        File file = new File(sdkLibPath);
        if (file.exists() == false) {
            throw new CFSNullArgumentException("Not found the libsdk.so: " + sdkLibPath);
        }
        driver = Native.load(sdkLibPath, CfsLibrary.class);
    }

    public FileStorage openFileStorage(StorageConfig config) throws CFSException {
    /*
    if (clientID.get() > 0 && storage != null) {
      return storage;
    }
     */
        long cid = driver.cfs_new_client();
        if (cid < 0) {
            throw new CFSException("Failed to new a client.");
        }
        //clientID.set(cid);
        driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_MATSER, config.getMasters());
        driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_VOLUME, config.getVolumeName());
        driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_LOG_DIR, config.getLogDir());
        driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_LOG_LEVEL, config.getLogLevel());

        int st = driver.cfs_start_client(cid);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CFSException("Failed to start the client: " + cid + " status code: " + st);
        }

        st = driver.cfs_chdir(cid, "/");
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CFSException("Failed to chdir for client: " + cid + " status code: " + st);
        }

        try {
            CFSDriverIns ins = new CFSDriverIns(driver, cid);
            FileStorageImpl storage = new FileStorageImpl(ins);
            storage.init();
            log.info("Succ to open FileStorage, client id:" + cid);
            return storage;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
