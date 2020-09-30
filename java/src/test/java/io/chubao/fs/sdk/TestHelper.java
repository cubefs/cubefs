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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import java.util.UUID;

public class TestHelper {
    private static final Log log = LogFactory.getLog(TestHelper.class);
    private static String sdkPath = "cfs_libsdk";
    private static String mastersKey = "cfs_masters";
    private static String volumeKey = "cfs_volume";

    public static String getSdkPath() {
        log.info("libsdk:" + System.getenv(sdkPath));
        return System.getenv(sdkPath);
    }

    public static StorageConfig getConfig() {
        StorageConfig config = new StorageConfig();
        String master = System.getenv(mastersKey);
        String vol = System.getenv(volumeKey);

        config.setMasters(master);
        config.setVolumeName(vol);
        return config;
    }

    public static String getRandomUUID() {
        return UUID.randomUUID().toString();
    }

    public static CFSClient creatClient(String libpath) {
        try {
            CFSClient client = new CFSClient(libpath);
            client.init();
            return client;
        } catch (CFSException ex) {
            log.error(ex.getMessage());
            return null;
        }
    }

    public static CFSClient creatClient() {
        try {
            CFSClient client = new CFSClient(getSdkPath());
            client.init();
            return client;
        } catch (CFSException ex) {
            log.error(ex.getMessage(), ex);
            return null;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static FileStorage getStorage() {
        try {
            CFSClient client = creatClient();
            Assert.assertNotNull(client);
            StorageConfig config = getConfig();
            Assert.assertNotNull(config);
            return client.openFileStorage(config);
        } catch (CFSException ex) {
            log.error(ex.getMessage(), ex);
            return null;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static FileStorage getStorage(StorageConfig config) {
        try {
            CFSClient client = creatClient();
            return client.openFileStorage(config);
        } catch (CFSException ex) {
            return null;
        } catch (Exception ex) {
            return null;
        }
    }
}
