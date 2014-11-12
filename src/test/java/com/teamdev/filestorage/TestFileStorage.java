package com.teamdev.filestorage;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * @author Alex Geta
 */
public class TestFileStorage {
    private final String rootPath = "src\\test\\rootPath";
    private final String fileKey = "someKey";
    private final String savedFilePathName = "src\\test\\rootPath\\d814\\2cbd\\78c688ae7b47e150472c50c8.dat";
    private File savedFile = new File(savedFilePathName);
    private InputStream savedFileInputStream;
    private final FileStorage fileStorage = new FileStorageImpl(rootPath,1024*1024);


    @Before
    public void setUp() throws IOException {
        savedFileInputStream =  new FileInputStream("src\\test\\file.txt");
    }

    @Test
    public void testSaveFile() throws IOException{
        final boolean isSaved = fileStorage.saveFile(fileKey, savedFileInputStream);
        assertTrue(savedFile.exists() && savedFile.isFile() && isSaved);
    }

    @Test
    public void testReadFile() throws IOException {
        final InputStream readsFileInputStream = fileStorage.readFile(fileKey);
        assertEquals(readsFileInputStream.available(), savedFile.length());
        readsFileInputStream.close();
    }

    @Test
    public void testDeleteFile() throws IOException {
        final boolean isDeleted = fileStorage.deleteFile(fileKey);
        final boolean isExists = new File(savedFilePathName).exists();
        assertTrue(!isExists && isDeleted);
    }

}
