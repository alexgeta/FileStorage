package com.teamdev.filestorage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertTrue;

/**
 * @author Alex Geta
 */
public class TestFileStorageServices {

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
    public void testFileExpirationMonitor() throws Exception {
        fileStorage = new FileStorageImpl(rootPath, 1024*10);
        final String key = "someKey1";
        final byte [] savedBytes = new byte[1024*5];
        final InputStream savedInputStream = new ByteArrayInputStream(savedBytes);
        final File savedFile = new File("src\\rootFolder\\5ef\\242\\e11\\85ad0ea23f1569452b4f6de.dat");
        final int expirationTime = 500;
        final boolean isSaved = fileStorage.saveFile(key, savedInputStream, expirationTime);
        assertTrue(isSaved && savedFile.exists());
        Thread.sleep(expirationTime+10);
        assertTrue(!savedFile.exists());
        savedInputStream.close();
    }

    @Test
    public void testCleaner() throws Exception {
        final long capacity = 1024*1024*10;
        fileStorage = new FileStorageImpl(rootPath, capacity);
        final String key = "fileKey";
        final int percentToClean = 30;
        final byte [] savedBytes = new byte[1024*100];
        for(int i = 0; i < 100; i++){
            String currentKey = key + String.valueOf(i);
            fileStorage.saveFile(currentKey, new ByteArrayInputStream(savedBytes));
        }
        fileStorage.clean(percentToClean);
        Thread.sleep(1000);
        final long nearlyExpectedFreeSpace = capacity/100*percentToClean;
        assertTrue(fileStorage.getFreeSpace() >= nearlyExpectedFreeSpace);
        fileStorage.clean(100);
    }

    @After
    public void tearDown() throws Exception {
        if(new File(rootPath).delete()){
            assertTrue(true);
        }
    }
}
