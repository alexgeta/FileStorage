package com.teamdev.filestorage;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of abstract data storage that allows to store millions objects in one folder.
 * Storage represents folder hierarchy which is based on MD5 hash function.
 * This means that storage allow to operate with 16^32 different file keys.
 * Please be aware that keys are case sensitive.
 * @author Alex Geta
 */
public class FileStorageImpl implements FileStorage {

    /**
     * Path to root folder on local disk.
     */
    private final String rootPath;
    /**
     * Allowed for usage space in bytes.
     */
    private final long allocatedSpace;
    /**
     * Currently used space in bytes.
     */
    private long usedSpace = 0;
    /**
     * Files awaiting for deleting.
     */
    private final Map<String,Date> expiringFiles = new ConcurrentHashMap<String, Date>();

    /**
     * Creates a new FileStorage instance with the specified parameters.
     * @param rootPath Root folder pathname string
     * @param bytes Allowed for usage space in bytes
     * @throws IllegalArgumentException if passed arguments is invalid.
     */
    public FileStorageImpl(String rootPath, long bytes) {
        if(new File(rootPath).exists()){
            this.rootPath = rootPath;
        }else throw new IllegalArgumentException("Invalid root path argument "+rootPath);

        if(bytes > 0){
            this.allocatedSpace = bytes;
        }else throw new IllegalArgumentException("Invalid bytes argument "+bytes);
        new FileExpirationMonitoring(this, expiringFiles, Thread.currentThread()).start();
    }

    /**
     * Reads object from InputStream and saves it to the storage.
     * @param key Key string associated with saving object.
     * @param inputStream InputStream with saving object.
     * @param millis Object expiration time in milliseconds.
     * @throws FileAlreadyExistsException if file with same associated key already exists in storage.
     * @throws IllegalStateException if object size is bigger than available free space in storage.
     * @return true if object successfully saved.
     */
    @Override
    public synchronized boolean saveFile(String key, InputStream inputStream, long millis) throws IOException{

        final File file = getFile(key);
        if(file.exists()){
            throw new FileAlreadyExistsException("File with key "+key+" already exists");
        }

        final int fileSize = inputStream.available();
        if((usedSpace + fileSize) > allocatedSpace){
            throw new IllegalStateException("Out of allocated space");
        }

        final File fileDirectory = file.getParentFile();
        if(!fileDirectory.exists()) fileDirectory.mkdirs();
        final boolean isCreated = file.createNewFile();

        final byte[] buffer = new byte[1024 * 32];
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(file));
        int readLength;
        while ((readLength = bufferedInputStream.read(buffer)) > 0){
            fileOutputStream.write(buffer, 0, readLength);
        }
        usedSpace += fileSize;
        fileOutputStream.close();

        if(millis > 0) addExpiringFile(key, millis);
        return isCreated;
    }

    public boolean saveFile(String key, InputStream inputStream) throws IOException{
        return saveFile(key, inputStream, 0);
    }

    public synchronized long getFreeSpace(){
        return allocatedSpace - usedSpace;
    }

    /**
     * Returns InputStream to object in storage which is associated with the specified key.
     * @param key Key string associated with reading object.
     * @return InputStream to object in storage.
     * @throws FileNotFoundException if object with specified key is not exists in storage.
     */
    @Override
    public synchronized InputStream readFile(String key) throws IOException{
        final File file = getFile(key);
        if(!file.exists()){
            throw new FileNotFoundException("File with key "+key+" doesn't exists");
        }
        return new FileInputStream(file);
    }

    /**
     * Delete object with associated key from the storage.
     * @param key Key string associated with deleted object.
     * @return true if the object successfully deleted.
     * @throws FileNotFoundException if object with specified key is not exists in storage.
     */
    @Override
    public synchronized boolean deleteFile(String key) throws IOException{

        final File deletedFile = getFile(key);
        if(!deletedFile.exists()){
            throw new FileNotFoundException("File with key "+key+" doesn't exists");
        }

        final long fileSize = deletedFile.length();
        boolean isDeleted = deletedFile.delete();

        if(isDeleted){
            usedSpace -= fileSize;
            deleteEmptyDirectories(deletedFile);
        }
        return isDeleted;
    }

    private void deleteEmptyDirectories(File deletedFile){
        File parent = deletedFile.getParentFile();
        while (!parent.getPath().equals(rootPath) && parent.list().length == 0){
            parent.delete();
            parent = parent.getParentFile();
        }
    }

    private File getFile(String key){
        if(key.length() < 1) throw new IllegalArgumentException("Invalid key");
        final String filePathName = generateFilePathName(key);
        return new File(filePathName);
    }

    private String generateFilePathName(String key){
        final int FOLDER_TREE_HEIGHT = 2;
        final int CHUNK_SIZE = 4;
        final String FILE_EXT = ".dat";

        final StringBuilder filePathName = new StringBuilder(rootPath).append(File.separator);
        final String keyHash = DigestUtils.md5Hex(key);
        final String[] hashChunks = splitByNumberOfChars(keyHash, CHUNK_SIZE);

        for(int i = 0; i < FOLDER_TREE_HEIGHT; i++){
            filePathName.append(hashChunks[i]);
            filePathName.append(File.separator);
        }

        final String fileName = keyHash.substring(
                CHUNK_SIZE * FOLDER_TREE_HEIGHT, keyHash.length());
        filePathName.append(fileName).append(FILE_EXT);
        return filePathName.toString();
    }

    private String [] splitByNumberOfChars(String hash, int chunkSize){
        final String REG_EX = "(?<=\\G.{"+chunkSize+"})";
        return hash.split(REG_EX);
    }

    private void addExpiringFile(String key, long expire){
        long currentTime = System.currentTimeMillis();
        Date expirationTime = new Date(currentTime + expire);
        expiringFiles.put(key, expirationTime);
    }

}
