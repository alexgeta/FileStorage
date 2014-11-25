package com.teamdev.filestorage;

import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Map;

/**
 * Monitoring service is responsible for deleting expired files.
 *
 * @author Alex Geta
 */
public class FileExpirationMonitor extends Thread {

    private final FileStorage fileStorage;
    private final Map<String, Date> expiringFiles;

    public FileExpirationMonitor(FileStorage fileStorage, Map<String, Date> expiringFiles) {
        this.fileStorage = fileStorage;
        this.expiringFiles = expiringFiles;
    }

    private void checkExpiredFiles() {
        for (Map.Entry<String, Date> entry : expiringFiles.entrySet()) {
            long currentTime = System.currentTimeMillis();
            long fileExpirationTime = entry.getValue().getTime();

            if (currentTime > fileExpirationTime) {
                try {
                    fileStorage.deleteFile(entry.getKey());
                } catch (FileNotFoundException e) {
                    System.out.println("File already deleted");
                }
                expiringFiles.remove(entry.getKey());
            }
        }
    }

    @Override
    public void run() {
        while (!expiringFiles.isEmpty()) {
            checkExpiredFiles();
        }
    }
}
