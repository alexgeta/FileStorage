package com.teamdev.filestorage;

import com.teamdev.filestorage.exception.OutOfSpaceException;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;

/**
 * @author Alex Geta
 */
public interface FileStorage {

    boolean saveFile(String key, InputStream inputStream, long expirationTime) throws FileAlreadyExistsException,
            OutOfSpaceException;

    boolean saveFile(String key, InputStream inputStream) throws FileAlreadyExistsException,
            OutOfSpaceException;

    InputStream readFile(String key) throws FileNotFoundException;

    boolean deleteFile(String key) throws FileNotFoundException;

    long getFreeSpace();

    void clean(int percent);

}

