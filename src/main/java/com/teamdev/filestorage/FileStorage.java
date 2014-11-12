package com.teamdev.filestorage;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Alex Geta
 */
public interface FileStorage {

    boolean saveFile(String key, InputStream inputStream, long expirationTime) throws IOException;
    boolean saveFile(String key, InputStream inputStream) throws IOException;
    InputStream readFile(String key) throws IOException;
    boolean deleteFile(String key) throws IOException;
}

