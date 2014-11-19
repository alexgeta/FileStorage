package com.teamdev.filestorage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;

/**
 * Cleaner service is responsible for cleaning free space.
 * @author Alex Geta
 */
public class CleanerService extends Thread {

    private final File rootFolder;
    private final FileStorage fileStorage;
    private final long cleaningSpace;
    private final Map<FileTime, File> timeFileMap = new TreeMap<FileTime, File>();

    public CleanerService(File rootFolder, FileStorage fileStorage, long cleaningSpace) {
        this.rootFolder = rootFolder;
        this.fileStorage = fileStorage;
        this.cleaningSpace = cleaningSpace;
    }

    private void clean() {

        final File[] folders = rootFolder.listFiles();
        final List<File> rootFolderList;
        if(folders != null && folders.length > 0){
            rootFolderList = Arrays.asList(folders);
        }else return;

        final Deque<File> foldersStack = new ArrayDeque<File>(rootFolderList);
        while (!foldersStack.isEmpty()){
            final File childFolder = foldersStack.pop();
            final File[] childsList = childFolder.listFiles();
            if(childsList != null && childsList.length > 0 ){
                processChildsList(childsList, foldersStack);
            }
        }

        for(Map.Entry<FileTime, File> fileEntry : timeFileMap.entrySet()){
            if(fileStorage.getFreeSpace() > cleaningSpace) break;
            try {
                ((FileStorageImpl)fileStorage).deleteFile(fileEntry.getValue());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void processChildsList(File[] childsList, Deque<File> foldersStack ) {
        for(File child : childsList){
            if(child.isDirectory()){
                foldersStack.push(child);
            }else if(child.isFile()){
                putToTimeFileMap(child, timeFileMap);
            }
        }
    }

    private void putToTimeFileMap(File file, Map<FileTime, File> timeFileMap) {
        try {
            BasicFileAttributes fileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            FileTime creationTime = fileAttributes.creationTime();
            timeFileMap.put(creationTime, file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        clean();
    }
}
