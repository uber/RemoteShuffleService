package com.uber.rss.common;

import com.uber.rss.exceptions.RssEndOfStreamException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class FixedLengthInputStreamTest {
  @Test
  public void readEmptyStream() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    FixedLengthInputStream fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    byte[] bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), -1);
  }

  @Test
  public void readOneByteStream() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    FixedLengthInputStream fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);
    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    byte[] bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
  }

  @Test
  public void readTwoByteStream() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9, (byte)10});
    FixedLengthInputStream fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 1);

    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    byte[] bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
    Assert.assertEquals(bytes[0], 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9, (byte)10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 2);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 2);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 2);

    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 1);
    Assert.assertEquals(fixedLengthInputStream.read(), 10);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 2);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9, (byte)10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 2);

    bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 2);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9, (byte)10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
    Assert.assertEquals(bytes[0], 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte)9, (byte)10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 2);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 2);
    Assert.assertEquals(bytes[0], 9);
    Assert.assertEquals(bytes[1], 10);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), -1);
  }

  @Test(expectedExceptions = RssEndOfStreamException.class)
  public void readEmptyStreamWithOneLength_read() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    FixedLengthInputStream fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);
    fixedLengthInputStream.read();
  }

  @Test(expectedExceptions = RssEndOfStreamException.class)
  public void readEmptyStreamWithOneLength_readByteArray1() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    FixedLengthInputStream fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);
    fixedLengthInputStream.read(new byte[1]);
  }

  @Test(expectedExceptions = RssEndOfStreamException.class)
  public void readEmptyStreamWithOneLength_readByteArray2() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {(byte)9});
    FixedLengthInputStream fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 2);
    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    fixedLengthInputStream.read(new byte[1]);
  }
}
