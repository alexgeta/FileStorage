package com.teamdev.filestorage;

import com.teamdev.filestorage.exception.NotEnoughFreeSpaceException;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implementation of abstract data storage that allows to store millions objects in one folder.
 * Storage represents folder hierarchy which is based on MD5 hash function.
 * This means that storage allow to operate with 16^32 different file keys.
 * Please be aware that keys are case sensitive.
 *
 * @author Alex Geta
 */
public class FileStorageImpl implements FileStorage {

    /**
     * Root folder on local disk.
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
    private final Map<String, Date> expiringFiles = new ConcurrentHashMap<String, Date>();
    /**
     * Files sorted by creation time.
     */
    private final Map<FileTime, File> cachedFiles = new ConcurrentSkipListMap<FileTime, File>();
    /**
     * Executor for FileExpirationMonitor.
     */
    private ExecutorService monitoringExecutor = Executors.newSingleThreadExecutor();

    /**
     * Creates a new FileStorage instance with the specified parameters.
     *
     * @param rootPath Root folder pathname string
     * @param bytes    Allowed for usage space in bytes
     * @throws IllegalArgumentException if passed arguments is invalid.
     */
    public FileStorageImpl(String rootPath, long bytes) {
        File rootFolder = new File(rootPath);
        validateRootPathName(rootFolder);
        this.rootFolder = rootFolder;

        if (bytes > 0) {
            this.allocatedSpace = bytes;
        } else throw new IllegalArgumentException("Bytes argument must be > 0");
    }

    private void validateRootPathName(File rootFolder){
        if (!rootFolder.exists()) {
            throw new IllegalArgumentException("Path \""+rootFolder+"\" doesn't found");
        } else if(!rootFolder.isDirectory()){
            throw new IllegalArgumentException("Path \""+rootFolder+"\" is not a folder");
        }
    }

    /**
     * Reads object from InputStream and saves it to the storage.
     *
     * @param key         Key string associated with saving object.
     * @param inputStream InputStream with saving object.
     * @param millis      Object expiration time in milliseconds.
     * @return true if object successfully saved, false otherwise.
     * @throws FileAlreadyExistsException  if file with same associated key already exists in storage.
     * @throws NotEnoughFreeSpaceException if object size is bigger than available free space in storage.
     */
    @Override
    public boolean saveFile(String key, InputStream inputStream, long millis,
                            CallBack callBack) throws FileAlreadyExistsException, NotEnoughFreeSpaceException {

        final File file = getFile(key);
        if (file.exists()) {
            throw new FileAlreadyExistsException("File with key " + key + " already exists");
        }

        BufferedOutputStream bufferedOutputStream = null;
        try {
            if (inputStream.available() == 0) {
                throw new IllegalArgumentException("InputStream is empty");
            }
            checkEnoughFreeSpace(inputStream.available(), callBack);
            if (!createFile(file)) return false;

            final byte[] buffer = new byte[1024 * 1024];
            final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
            int readLength;
            while ((readLength = bufferedInputStream.read(buffer)) > 0) {
                checkEnoughFreeSpace(inputStream.available(), callBack);
                bufferedOutputStream.write(buffer, 0, readLength);
                usedSpace += readLength;
            }
            bufferedOutputStream.close();

            if (millis > 0) addExpiringFile(key, millis);
            return true;

        } catch (NotEnoughFreeSpaceException e) {
            processUnfinishedFile(bufferedOutputStream, file);
            throw new NotEnoughFreeSpaceException();
        } catch (IOException e) {
            e.printStackTrace();
            processUnfinishedFile(bufferedOutputStream, file);
            return false;
        }
    }

    private void checkEnoughFreeSpace(long availableBytes, CallBack callBack) throws NotEnoughFreeSpaceException{
        final long notEnoughBytes = availableBytes - getFreeSpace();
        final boolean isEnough = availableBytes <= getFreeSpace() || callBack != null && callBack.isEnough(notEnoughBytes);
        if (!isEnough) throw new NotEnoughFreeSpaceException();
    }

    @Override
    public boolean saveFile(String key, InputStream inputStream, CallBack callBack) throws FileAlreadyExistsException,
            NotEnoughFreeSpaceException {
        return saveFile(key, inputStream, 0, callBack);
    }

    @Override
    public boolean saveFile(String key, InputStream inputStream, long expirationTime) throws FileAlreadyExistsException,
            NotEnoughFreeSpaceException {
        return saveFile(key, inputStream, expirationTime, null);
    }

    @Override
    public boolean saveFile(String key, InputStream inputStream) throws FileAlreadyExistsException,
            NotEnoughFreeSpaceException {
        return saveFile(key, inputStream, 0, null);
    }


    /**
     * Returns byte representation of free space in this storage.
     * @return bytes amount.
     */
    @Override
    public long getFreeSpace() {
        return allocatedSpace - usedSpace;
    }

    /**
     * Returns InputStream to object in storage which is associated with the specified key.
     *
     * @param key Key string associated with reading object.
     * @return InputStream to object in storage.
     * @throws FileNotFoundException if object with specified key is not exists in storage.
     */
    @Override
    public InputStream readFile(String key) throws FileNotFoundException {
        final File file = getFile(key);
        checkFileExistence(file, key);
        return new FileInputStream(file);
    }

    /**
     * Delete object with associated key from the storage.
     *
     * @param key Key string associated with deleted object.
     * @return true if the object successfully deleted, false otherwise.
     * @throws FileNotFoundException if object with specified key is not exists in storage.
     */
    @Override
    public boolean deleteFile(String key) throws FileNotFoundException {
        final File file = getFile(key);
        checkFileExistence(file, key);
        return deleteFile(file);
    }

    private void checkFileExistence(File file, String key) throws FileNotFoundException{
        if (!file.exists()) {
            throw new FileNotFoundException("File with key \"" + key + "\" doesn't found");
        }
    }

    private boolean deleteFile(File file) throws FileNotFoundException {
        final long fileSize = file.length();
        boolean isDeleted = file.delete();
        if (isDeleted) {
            usedSpace -= fileSize;
            deleteEmptyDirectories(file);
        }
        return isDeleted;
    }

    /**
     * Cleans storage by specified bytes amount.
     *
     * @param bytesToClean amount of bytes desired to clean.
     * @return amount of successfully removed bytes.
     */
    @Override
    public long clean(long bytesToClean) {
        if (isEmptyStorage()) return 0;

        long cleanedBytes = 0;
        for (Map.Entry<FileTime, File> fileEntry : cachedFiles.entrySet()) {
            try {
                final File oldFile = fileEntry.getValue();
                final long fileSize = oldFile.length();
                if(deleteFile(oldFile)){
                    cachedFiles.remove(fileEntry.getKey());
                    cleanedBytes += fileSize;
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            if ((cleanedBytes > bytesToClean) ||  isEmptyStorage()) break;
        }
        return cleanedBytes;
    }

    private boolean isEmptyStorage(){
        return cachedFiles.isEmpty() && !cacheFiles();
    }

    /** Fills cachedFiles.
     *
     * @return true if at least one file added to cachedFiles,
     * otherwise false.
     */
    private boolean cacheFiles(){
        final File[] folders = rootFolder.listFiles();
        final List<File> rootFolderList;
        if (folders != null && folders.length > 0) {
            rootFolderList = Arrays.asList(folders);
        } else return false;

        final Deque<File> foldersStack = new ArrayDeque<File>(rootFolderList);
        while (!foldersStack.isEmpty()) {
            final File childFolder = foldersStack.pop();
            final File[] childsList = childFolder.listFiles();
            if (childsList != null && childsList.length > 0) {
                processChildsList(childsList, foldersStack);
            }
        }
        return cachedFiles.size() > 0;
    }

    private void processChildsList(File[] childsList, Deque<File> foldersStack) {
        for (File child : childsList) {
            if (child.isDirectory()) {
                foldersStack.push(child);
            } else if (child.isFile()) {
                addToCachedFiles(child);
            }
        }
    }

    private void addToCachedFiles(File file) {
        try {
            BasicFileAttributes fileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            FileTime creationTime = fileAttributes.creationTime();
            cachedFiles.put(creationTime, file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean createFile(File file) {
        boolean isCreated = false;
        final File fileFolder = file.getParentFile();
        if (!fileFolder.exists()) isCreated = fileFolder.mkdirs();
        try {
            if (isCreated) isCreated = file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isCreated;
    }

    private void deleteEmptyDirectories(File deletedFile) {
        File parent = deletedFile.getParentFile();
        while (!parent.equals(rootFolder) && parent.list().length == 0) {
            if (parent.delete()) {
                parent = parent.getParentFile();
            } else return;
        }
    }

    private void processUnfinishedFile(OutputStream outputStream, File file) {
        if(!file.exists()) return;
        try {
            outputStream.close();
            long fileSize = file.length();
            usedSpace -= fileSize;
            if (file.delete()) {
                deleteEmptyDirectories(file);
            }else file.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private File getFile(String key) {
        if (key.isEmpty()) throw new IllegalArgumentException("Key is empty");
        final String filePathName = buildFilePathName(key);
        return new File(filePathName);
    }

    private String buildFilePathName(String key) {
        final int FOLDER_TREE_HEIGHT = 3;
        final int CHUNK_SIZE = 3;
        final String REG_EX = "(?<=\\G.{" + CHUNK_SIZE + "})";
        final String FILE_EXT = ".dat";

        final StringBuilder absolutePathName =
                new StringBuilder(rootFolder.toString()).append(File.separator);
        final String keyHash = DigestUtils.md5Hex(key);
        /*split hash string by specified amount of chars*/
        final String[] hashChunks = keyHash.split(REG_EX);

        for (int i = 0; i < FOLDER_TREE_HEIGHT; i++) {
            absolutePathName.append(hashChunks[i]);
            absolutePathName.append(File.separator);
        }

        final String fileName = keyHash.substring(
                CHUNK_SIZE * FOLDER_TREE_HEIGHT, keyHash.length());
        absolutePathName.append(fileName).append(FILE_EXT);
        return absolutePathName.toString();
    }

    private void addExpiringFile(String key, long expire) {
        long currentTime = System.currentTimeMillis();
        final Date expirationTime = new Date(currentTime + expire);
        expiringFiles.put(key, expirationTime);
        final boolean isAlreadyFinished = monitoringExecutor.isShutdown() && monitoringExecutor.isTerminated();
        if(isAlreadyFinished) monitoringExecutor = Executors.newSingleThreadExecutor();
        runService(monitoringExecutor, new FileExpirationMonitor(this, expiringFiles));
    }

    private void runService(ExecutorService executor, Runnable task){
        /*if service has not yet started*/
        if(!executor.isShutdown()){
            executor.execute(task);
            /*manual shutdown after every start to automatically terminate executor's thread*/
            executor.shutdown();
        }/*else service is currently running*/
    }
}
