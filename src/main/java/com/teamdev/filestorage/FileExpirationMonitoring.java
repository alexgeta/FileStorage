package com.teamdev.filestorage;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Monitoring service is responsible for deleting expired files.
 * @author Alex Geta
 */
public class FileExpirationMonitoring extends Thread{

    private final FileStorage fileStorage;
    private final Map <String, Date> expiringFiles;
    private final Thread parentThread;

    public FileExpirationMonitoring(FileStorage fileStorage, Map <String, Date> expiringFiles, Thread thread) {
        this.fileStorage = fileStorage;
        this.expiringFiles = expiringFiles;
        this.parentThread = thread;
    }

    private void checkExpiredFiles(){
        for(Map.Entry<String, Date> entry : expiringFiles.entrySet()){
            long currentTime = System.currentTimeMillis();
            long fileExpirationTime = entry.getValue().getTime();

            if(currentTime > fileExpirationTime){
                try {
                    fileStorage.deleteFile(entry.getKey());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                expiringFiles.remove(entry.getKey());
            }
        }
    }

    @Override
    public void run() {
        while (parentThread.isAlive()){
            checkExpiredFiles();
        }
    }
}
