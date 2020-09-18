package io.chubao.fs.sdk.stream;

import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.CFSFile;

import java.io.IOException;
import java.io.OutputStream;

public class CFSOutputStream extends OutputStream {
  private CFSFile cfile;

  public CFSOutputStream(CFSFile file) {
    this.cfile = file;
  }

  @Override
  public void close() throws IOException {
    try {
      cfile.close();
      super.close();
    } catch (CFSException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void flush() throws IOException {
    try {
      cfile.flush();
    } catch (CFSException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    try {
      cfile.write(b, off, len);
    } catch (CFSException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(int b) throws IOException {
    byte buf[] = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
  }
}