package com.uber.rss.storage;

/***
 * Shuffle output stream interface.
 */
public interface ShuffleOutputStream extends AutoCloseable {
    /***
     * Write data to the stream.
     * @param bytes
     */
    void write(byte[] bytes);

    /***
     * Flush the stream. This might only flush data 
     * into operation system's memory cache without
     * really flushing data to underlying device.
     */
    void flush();

    /***
     * Fsync the stream. Make sure data is written 
     * to underlying device. See this link for fsync:
     * http://man7.org/linux/man-pages/man2/fdatasync.2.html
     */
    void fsync();

    /***
     * Close the stream.
     */
    void close();

    /***
     * Get file location for this stream.
     * @return
     */
    String getLocation();

    long getWrittenBytes();
}
