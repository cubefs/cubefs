package io.chubao.fs.sdk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class RmdirTest extends StorageTest {
  private final static Log log = LogFactory.getLog(RmdirTest.class);
  @Test
  public void testRmdir() {
    boolean res = mkdirs(rmdirTestDir);
    Assert.assertTrue(res);
    Assert.assertTrue(rmdir(rmdirTestDir, false));
  }

  @Test
  public void testRmDirRecurse() {
    String uu1 = TestHelper.getRandomUUID();
    String uu2 = TestHelper.getRandomUUID();
    String path1 = rmdirTestDir + "/" + uu1;
    String path2 = rmdirTestDir + "/" + uu2;

    boolean res = mkdirs(path1);
    Assert.assertTrue(res);
    res = mkdirs(path2);
    Assert.assertTrue(res);
    Assert.assertTrue(rmdir(rmdirTestDir, true));
    Assert.assertNull(stat(rmdirTestDir));

    res = mkdirs(path1);
    Assert.assertTrue(res);
    res = mkdirs(path2);
    Assert.assertTrue(res);
    Assert.assertTrue(rmdir(rmdirTestDir, true));
  }

  @Test
  public void testRmDirRecurse2() {
    /*
    -rmdirtest
      -uu1
        -uu2
      -uu3
        -f1
      -f0
     */
    String uu1 = TestHelper.getRandomUUID();
    String uu2 = TestHelper.getRandomUUID();
    String path1 = rmdirTestDir + "/" + uu1 + "/" + uu2;
    String path2 = rmdirTestDir + "/" + "f0";
    String uu3 = TestHelper.getRandomUUID();
    String path3 = rmdirTestDir + "/" + uu3 + "/" + "f1";

    boolean res = mkdirs(path1);
    Assert.assertTrue(res);

    res = createFile(path2, 0);
    Assert.assertTrue(res);
    Assert.assertFalse(rmdir(path2, true));

    Assert.assertTrue(mkdirs(rmdirTestDir + "/" + uu3));
    res = createFile(path3, 0);
    Assert.assertTrue(res);

    Assert.assertFalse(rmdir(rmdirTestDir, false));
    Assert.assertNotNull(stat(path1));
    Assert.assertNotNull(stat(path2));
    Assert.assertNotNull(stat(path3));

    Assert.assertTrue(rmdir(rmdirTestDir, true));
    Assert.assertNull(stat(path1));
    Assert.assertNull(stat(path2));
    Assert.assertNull(stat(path3));
    Assert.assertNull(stat(rmdirTestDir));
  }

  @Test
  public void testRmdirInvalidPath() {
    String dir1 = "../";
    Assert.assertFalse(rmdir(dir1, true));

    String dir3 = null;
    Assert.assertFalse(rmdir(dir3, true));

    String dir4 = " ";
    Assert.assertFalse(rmdir(dir4, true));

    String dir5 = rmdirTestDir;
    Assert.assertFalse(rmdir(dir5, true));
  }
}