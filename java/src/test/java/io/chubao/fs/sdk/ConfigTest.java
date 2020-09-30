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
import org.junit.Assert;
import org.junit.Test;

public class ConfigTest {
    private final static Log log = LogFactory.getLog(ConfigTest.class);

    @Test
    public void testNormal() {
        String master = "localhost:8080";
        String volume = "cfstest";

        StorageConfig config = new StorageConfig();
        config.setMasters(master);
        config.setVolumeName(volume);

        try {
            Assert.assertEquals(config.getMasters(), master);
            Assert.assertEquals(config.getVolumeName(), volume);
        } catch (CFSNullArgumentException ex) {
            Assert.assertFalse(false);
        }
    }

    @Test
    public void testNullArgument() {
        StorageConfig config = new StorageConfig();
        config.setMasters(null);
        config.setVolumeName(null);
        try {
            Assert.assertEquals(config.getMasters(), "");
        } catch (CFSNullArgumentException ex) {
            Assert.assertFalse(false);
        }

        try {
            Assert.assertEquals(config.getVolumeName(), "");
        } catch (CFSNullArgumentException ex) {
            Assert.assertFalse(false);
        }
    }

    @Test
    public void testRequired() {
        StorageConfig config = new StorageConfig();
        try {
            String master = config.getMasters();
            Assert.assertTrue(false);
        } catch (CFSNullArgumentException ex) {
            Assert.assertFalse(false);
        }

        try {
            String vol = config.getVolumeName();
            Assert.assertTrue(false);
        } catch (CFSNullArgumentException ex) {
            Assert.assertFalse(false);
        }
    }
}
