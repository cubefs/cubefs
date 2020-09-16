package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;
import org.junit.Assert;
import org.junit.Test;

public class CFSClientTest {
  @Test
  public void testCreateClient() {
    FileStorage storage = TestHelper.getStorage();
    Assert.assertNotNull(storage);
  }

  @Test
  public void testLackSDK() {
    String libpath = null;
    CFSClient client = TestHelper.creatClient(libpath);
    Assert.assertNull(client);
  }

  @Test
  public void testInvalidSDK() {
    String libpath = "/libcfs.so";
    CFSClient client = TestHelper.creatClient(libpath);
    Assert.assertNull(client);
  }

  @Test
  public void testLackConfig() {
    StorageConfig config = null;
    FileStorage storage = TestHelper.getStorage(config);
    Assert.assertNull(storage);
  }
}
