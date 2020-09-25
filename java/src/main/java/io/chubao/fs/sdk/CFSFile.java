package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;

public interface CFSFile {
  void close() throws CFSException;
  void flush() throws CFSException;
  void write(byte[] buff, int buffOffset, int len) throws CFSException;
  void pwrite(byte[] buff, int buffOffset, int len, long fileOffset) throws CFSException;
  void seek(long offset) throws CFSException;
  long read(byte[] buff, int buffOffset, int len) throws CFSException;
  int pread(byte[] buff, int buffOffset, int len, long fileOffset) throws CFSException;
  long getFileSize();
  long getPosition();
}