package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.libsdk.CFSDriverIns;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.locks.ReentrantLock;

public class CFSFileImpl implements CFSFile {
  private static final Log log = LogFactory.getLog(CFSFileImpl.class);
  private CFSDriverIns driver;
  private long position = 0L;
  private long fileSize;
  private boolean isClosed;
  private ReentrantLock lock = new ReentrantLock();
  private long fd;

  public CFSFileImpl(CFSDriverIns driver, long fd) {
    this.driver = driver;
    this.fd = fd;
  }

  public boolean isClosed() {
    return this.isClosed;
  }

  public long getFileSize() {
    return  this.fileSize;
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
    if (off < 0 || len <= 0) {
      throw new CFSException("Invalid argument.");
    }

    int wsize = 0;
    if (off == 0) {
      wsize = write(position, buff, len);
    } else {
      byte[] newbuff = buffCopy(buff, off, len);
      wsize = write(position, newbuff, len);
    }

    position += wsize;
    fileSize += wsize;
  }

  private int write(long offset, byte[] data, int len) throws CFSException {
    return driver.write(fd, offset, data, len);
  }

  public synchronized int read(byte[] buff, int off, int len) throws CFSException {
    if (off < 0 || len <= 0) {
      throw new CFSException("Invalid argument.");
    }

    int rsize = 0;
    if (off == 0) {
      rsize = driver.read(fd, position, buff, len);
      /*
      byte[] bf = new byte[256];
      rsize = driver.read(fd, position, bf, 256);
      try {
        String data =  new String(bf, "utf8");
        log.info("2data:" + data);
      } catch (Exception e) {

      }

       */
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

  @Override
  public CFSStatInfo stat() throws CFSException {
    throw new CFSException("Not implement.");
  }
}