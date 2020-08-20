package com.uber.rss.storage;

import java.io.InputStream;
import java.util.List;

/***
 * Shuffle storage interface.
 */
public interface ShuffleStorage {

    /***
     * Check whether this is local storage on Remote Shuffle Service, or
     * external storage like HDFS.
     * @return
     */
    boolean isLocalStorage();

    /***
     * Check whether the file exists.
     * @param path
     * @return
     */
    boolean exists(String path);

    /***
     * List all files under a directory.
     * @param dir
     * @return
     */
    List<String> listAllFiles(String dir);

    /***
     * Create directory and its parents.
     * @param dir
     */
    void createDirectories(String dir);

    /***
     * Delete directory and its children.
     * @param dir
     */
    void deleteDirectory(String dir);

    /***
     * Delete file.
     * @param path
     */
    void deleteFile(String path);
    
    /***
     * Get the size of the file.
     * @param path
     * @return
     */
    long size(String path);

    /***
     * Create a stream for a given file path to write shuffle data.
     * @param path
     * @param compressionCodec
     * @return
     */
    ShuffleOutputStream createWriterStream(String path, String compressionCodec);

    /***
     * Create a stream for a given file path to read shuffle data.
     * @param path
     * @return
     */
    InputStream createReaderStream(String path);
}
