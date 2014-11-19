package com.teamdev.filestorage;

import com.teamdev.filestorage.exception.OutOfSpaceException;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;

import static org.junit.Assert.assertTrue;

/**
 * @author Alex Geta
 */
public class TestFileStorageMethods {

    private final String rootPath = "src\\rootFolder";
    private FileStorage fileStorage;

    @Before
    public void setUp() throws IOException {
        final File rootFolder = new File(rootPath);
        boolean isCreated = true;
        if (!rootFolder.exists()) isCreated = rootFolder.mkdirs();
        assertTrue(isCreated);
    }

    @Test
    public void testSaveFile() throws IOException{
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey";
        final byte [] savedBytes = new byte[1024*5];
        final InputStream savedInputStream = new ByteArrayInputStream(savedBytes);
        final File expectedFile =
                new File("src\\rootFolder\\d81\\42c\\bd7\\8c688ae7b47e150472c50c8.dat");
        final boolean isSaved = fileStorage.saveFile(key, savedInputStream);
        savedInputStream.close();
        assertTrue(expectedFile.exists() &&
                (expectedFile.length() == savedBytes.length) && isSaved);
    }

    @Test
    public void testReadFile() throws IOException {
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey";
        final byte [] savedBytes = new byte[1024*5];
        final InputStream expectedInputStream = new ByteArrayInputStream(savedBytes);
        final InputStream actualInputStream = fileStorage.readFile(key);
        final boolean streamsEquals = IOUtils.contentEquals(expectedInputStream, actualInputStream);
        actualInputStream.close();
        expectedInputStream.close();
        assertTrue(streamsEquals);
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void testSaveAlreadyExistsFile() throws IOException {
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey";
        final InputStream savedInputStream = new ByteArrayInputStream(new byte[1024]);
        fileStorage.saveFile(key, savedInputStream);
        savedInputStream.close();
    }

    @Test
    public void testDeleteFile() throws IOException {
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey";
        final File deletedFile = new File("src\\rootFolder\\d81\\42c\\bd7\\8c688ae7b47e150472c50c8.dat");
        final boolean isDeleted = fileStorage.deleteFile(key);
        final boolean isExists = deletedFile.exists();
        assertTrue(!isExists && isDeleted);
    }

    @Test(expected = FileNotFoundException.class)
    public void testDeleteNotExistsFile() throws IOException {
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey";
        fileStorage.deleteFile(key);
    }

    @Test(expected = FileNotFoundException.class)
    public void testReadNotExistsFile() throws IOException {
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey";
        fileStorage.readFile(key);
    }

    @Test(expected = OutOfSpaceException.class)
    public void testOutOfSpace() throws IOException {
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey";
        final byte [] savedBytes = new byte[1024*11];
        final InputStream savedInputStream = new ByteArrayInputStream(savedBytes);
        fileStorage.saveFile(key, savedInputStream);
        savedInputStream.close();
    }


}
