package io.chubao.fs.sdk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.*;

public class MkdirTest extends StorageTest {
  private final static Log log = LogFactory.getLog(MkdirTest.class);
  @Test
  public void testMkdir() {
    Assert.assertTrue(mkdirs(mkdirTestDir));
    CFSStatInfo stat = stat(mkdirTestDir);
    Assert.assertTrue(stat != null);
    checkDirStat(stat);
    //repeat to mkdir
    Assert.assertTrue(mkdirs(mkdirTestDir));
    Assert.assertTrue(rmdir(mkdirTestDir, true));
  }

  @Test
  public void testMkdirTopPath() {
    String uuid = TestHelper.getRandomUUID();
    String path = mkdirTestDir + "/" + uuid + "/../"; // /mkdirtest/uuid/../
    String subdir = mkdirTestDir + "/" + uuid;
    Assert.assertTrue(mkdirs(path));
    CFSStatInfo stat = stat(mkdirTestDir);
    checkDirStat(stat);
    stat = stat(path);
    checkDirStat(stat);
    Assert.assertNull(stat(subdir));
    Assert.assertTrue(rmdir(mkdirTestDir, true));
  }

  @Test
  public void testMkdirDubuleSlas() {
    String uuid = TestHelper.getRandomUUID();
    String path = mkdirTestDir + "//" + uuid;
    Assert.assertTrue(mkdirs(path));
    CFSStatInfo stat = stat(path);
    checkDirStat(stat);
    Assert.assertTrue(rmdir(mkdirTestDir, true));
  }

  @Test
  public void testMkdirInvalidPath() {
    String dir = "/";
    Assert.assertFalse(mkdirs(dir));

    String dir1 = "../";
    Assert.assertFalse(mkdirs(dir1));

    String dir2 = "/../";
    Assert.assertFalse(mkdirs(dir2));

    String dir3 = null;
    Assert.assertFalse(mkdirs(dir3));

    String dir4 = " ";
    Assert.assertFalse(mkdirs(dir4));
  }

  @Test
  public void testMkdirs() {
    String uuid = TestHelper.getRandomUUID();
    String path = mkdirsTestDir + "/" + uuid;
    Assert.assertTrue(mkdirs(path));
    CFSStatInfo stat = stat(path);
    checkDirStat(stat);
    stat = stat(mkdirsTestDir);
    checkDirStat(stat);
    Assert.assertTrue(rmdir(mkdirsTestDir, true));

    String uu2 = TestHelper.getRandomUUID();
    String path2 = path + "/" + uu2;
    Assert.assertTrue(mkdirs(path2));
    stat = stat(path2);
    checkDirStat(stat);

    stat = stat(path);
    checkDirStat(stat);

    stat = stat(mkdirsTestDir);
    checkDirStat(stat);
    Assert.assertTrue(rmdir(mkdirsTestDir, true));
  }

  @Test
  public void testMkdirsTopPath() {
    String uu1 = TestHelper.getRandomUUID();
    String uu2 = TestHelper.getRandomUUID();
    String path = mkdirsTestDir + "/" + uu1 + "/" + uu2 + "/../../";
    String subdir = mkdirsTestDir + "/" + uu1 + "/" + uu2;
    Assert.assertTrue(mkdirs(path));
    CFSStatInfo stat = stat(path);
    checkDirStat(stat);
    stat = stat(mkdirsTestDir);
    checkDirStat(stat);
    stat = stat(subdir);
    Assert.assertNull(stat);
    Assert.assertTrue(rmdir(mkdirsTestDir, true));
  }

  @Test
  public void MkdirsInvalidPath() {
    String uu1 = TestHelper.getRandomUUID();
    String path1 = mkdirsTestDir + "/" + uu1 + "/../../";
    Assert.assertFalse(mkdirs(path1));

    String uu2 = TestHelper.getRandomUUID();
    Assert.assertFalse(mkdirs(uu2));
  }
}