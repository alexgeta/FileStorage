package com.teamdev.filestorage;

import com.teamdev.filestorage.exception.OutOfSpaceException;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.Date;
import java.util.Map;
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
    private final File rootFolder;
    /**
     * Allowed for usage space in bytes.
     */
    private final long allocatedSpace;
    /**
     * Currently used space in bytes.
     */
    private volatile long usedSpace = 0;
    /**
     * Files awaiting for deleting.
     */
    private final Map<String,Date> expiringFiles = new ConcurrentHashMap<String, Date>();
    /**
     * Service that monitors expired files.
     */
    private FileExpirationMonitor expirationMonitor;

    /**
     * Creates a new FileStorage instance with the specified parameters.
     * @param rootPath Root folder pathname string
     * @param bytes Allowed for usage space in bytes
     * @throws IllegalArgumentException if passed arguments is invalid.
     */
    public FileStorageImpl(String rootPath, long bytes) {
        File rootFolder = new File(rootPath);
        if(rootFolder.exists() && rootFolder.isDirectory()){
            this.rootFolder = rootFolder;
        }else throw new IllegalArgumentException("Invalid root path argument "+rootPath);

        if(bytes > 0){
            this.allocatedSpace = bytes;
        }else throw new IllegalArgumentException("Bytes argument must be > 0");

    }

    /**
     * Reads object from InputStream and saves it to the storage.
     * @param key Key string associated with saving object.
     * @param inputStream InputStream with saving object.
     * @param millis Object expiration time in milliseconds.
     * @throws FileAlreadyExistsException if file with same associated key already exists in storage.
     * @throws OutOfSpaceException if object size is bigger than available free space in storage.
     * @return true if object successfully saved, false otherwise.
     */
    @Override
    public boolean saveFile(String key, InputStream inputStream, long millis) throws FileAlreadyExistsException,
             OutOfSpaceException{

        final File file = getFile(key);
        if(file.exists()){
            throw new FileAlreadyExistsException("File with key "+key+" already exists");
        }

        BufferedOutputStream bufferedOutputStream = null;
        try {
            if(inputStream.available() == 0){
                throw new IllegalArgumentException("InputStream is empty");
            }
            if(inputStream.available() > getFreeSpace()){
                throw new OutOfSpaceException();
            }
            if(!createFile(file)) return false;

            final byte[] buffer = new byte[1024*32];
            final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
            int readLength;
            while ((readLength = bufferedInputStream.read(buffer)) > 0){
                if(readLength > getFreeSpace()){
                    throw new OutOfSpaceException();
                }
                bufferedOutputStream.write(buffer, 0, readLength);
                usedSpace += readLength;
            }
            bufferedOutputStream.close();

            if(millis > 0) addExpiringFile(key, millis);
            return true;

        }catch (OutOfSpaceException e){
            if(file.exists() && !deleteUnfinishedFile(bufferedOutputStream, file)){
                System.err.println("Unable to delete unsuccessfully saved file!");
            }
            throw new OutOfSpaceException();
        }
        catch (IOException e) {
            e.printStackTrace();
            if(file.exists() && !deleteUnfinishedFile(bufferedOutputStream, file)){
                throw new IllegalStateException("Unable to delete unsuccessfully saved file!");
            }
            return false;
        }
    }

    @Override
    public boolean saveFile(String key, InputStream inputStream) throws FileAlreadyExistsException,
            OutOfSpaceException{
        return saveFile(key, inputStream, 0);
    }

    /**
     * @return byte representation of free space in this storage.
     */
    @Override
    public long getFreeSpace(){
        return allocatedSpace - usedSpace;
    }

    /**
     * Returns InputStream to object in storage which is associated with the specified key.
     * @param key Key string associated with reading object.
     * @return InputStream to object in storage.
     * @throws FileNotFoundException if object with specified key is not exists in storage.
     */
    @Override
    public InputStream readFile(String key) throws FileNotFoundException{
        final File file = getFile(key);
        if(!file.exists()){
            throw new FileNotFoundException("File with key "+key+" doesn't exists");
        }
        return new FileInputStream(file);
    }

    /**
     * Delete object with associated key from the storage.
     * @param key Key string associated with deleted object.
     * @return true if the object successfully deleted, false otherwise.
     * @throws FileNotFoundException if object with specified key is not exists in storage.
     */
    @Override
    public boolean deleteFile(String key) throws FileNotFoundException {
        final File deletedFile = getFile(key);
        if(!deletedFile.exists()){
            throw new FileNotFoundException("File with key "+key+" doesn't exists");
        }
        return deleteFile(deletedFile);
    }

    /**
     * Delete file from storage by File class object representation.
     * Used only in cleaner service.
     * @param file File class object representing deleted file in storage.
     * @return true if the object successfully deleted, false otherwise.
     * @throws FileNotFoundException if object with specified key is not exists in storage.
     */
    protected boolean deleteFile(File file) throws FileNotFoundException {
        final long fileSize = file.length();
        boolean isDeleted = file.delete();
        if(isDeleted){
            usedSpace -= fileSize;
            deleteEmptyDirectories(file);
        }
        return isDeleted;
    }

    /**
     * Cleans storage by specified percent of total allocated space.
     * @param percent desired percent to clean.
     */
    @Override
    public void clean(int percent) {
        if(percent > 100 || percent < 1){
            throw new IllegalArgumentException("percent must be in range from 1 to 100");
        }
        final long cleaningSpace = allocatedSpace / 100 * percent;
        final Thread cleaner = new CleanerService(rootFolder, this, cleaningSpace);
        cleaner.start();
    }

    private boolean createFile(File file){
        boolean isCreated = false;
        final File fileFolder = file.getParentFile();
        if(!fileFolder.exists()) isCreated = fileFolder.mkdirs();
        try {
            if(isCreated) isCreated = file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isCreated;
    }

    private void deleteEmptyDirectories(File deletedFile){
        File parent = deletedFile.getParentFile();
        while (!parent.equals(rootFolder) && parent.list().length == 0){
            if(parent.delete()){
                parent = parent.getParentFile();
            }else return;
        }
    }

    private boolean deleteUnfinishedFile(OutputStream outputStream, File file){
        boolean isDeleted = false;
        try {
            outputStream.close();
            long fileSize = file.length();
            isDeleted = file.delete();
            if(isDeleted){
                usedSpace -= fileSize;
                deleteEmptyDirectories(file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isDeleted;
    }

    private File getFile(String key){
        if(key.isEmpty()) throw new IllegalArgumentException("Invalid key");
        final String filePathName = generateFilePathName(key);
        return new File(filePathName);
    }

    private String generateFilePathName(String key){
        final int FOLDER_TREE_HEIGHT = 3;
        final int CHUNK_SIZE = 3;
        final String REG_EX = "(?<=\\G.{"+CHUNK_SIZE+"})";
        final String FILE_EXT = ".dat";

        final StringBuilder filePathName = new StringBuilder(rootFolder.toString()).append(File.separator);
        final String keyHash = DigestUtils.md5Hex(key);
        final String[] hashChunks = keyHash.split(REG_EX);

        for(int i = 0; i < FOLDER_TREE_HEIGHT; i++){
            filePathName.append(hashChunks[i]);
            filePathName.append(File.separator);
        }

        final String fileName = keyHash.substring(
                CHUNK_SIZE * FOLDER_TREE_HEIGHT, keyHash.length());
        filePathName.append(fileName).append(FILE_EXT);
        return filePathName.toString();
    }

    private void addExpiringFile(String key, long expire){
        long currentTime = System.currentTimeMillis();
        Date expirationTime = new Date(currentTime + expire);
        expiringFiles.put(key, expirationTime);
        if(expirationMonitor == null ||
                          expirationMonitor.getState() == Thread.State.TERMINATED){
            expirationMonitor = new FileExpirationMonitor(this, expiringFiles);
            expirationMonitor.start();
        }
    }
}
