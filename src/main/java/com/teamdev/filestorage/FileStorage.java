package com.teamdev.filestorage;

import com.teamdev.filestorage.exception.NotEnoughFreeSpaceException;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;

/**
 * @author Alex Geta
 */
public interface FileStorage {

    boolean saveFile(String key, InputStream inputStream, long expirationTime, CallBack callBack) throws FileAlreadyExistsException,
            NotEnoughFreeSpaceException;

    boolean saveFile(String key, InputStream inputStream, long expirationTime) throws FileAlreadyExistsException,
            NotEnoughFreeSpaceException;

    boolean saveFile(String key, InputStream inputStream, CallBack callBack) throws FileAlreadyExistsException,
            NotEnoughFreeSpaceException;

    boolean saveFile(String key, InputStream inputStream) throws FileAlreadyExistsException,
            NotEnoughFreeSpaceException;

    InputStream readFile(String key) throws FileNotFoundException;

    boolean deleteFile(String key) throws FileNotFoundException;

    long getFreeSpace();

    long clean(long bytes);

}

