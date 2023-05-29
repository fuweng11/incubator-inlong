/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.mysql.connector.dbsync.local;

import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BinLogFileQueue {

    private String baseName = "mysql-bin.";
    private List<File> binlogs = new ArrayList<File>();
    private File directory;
    private ReentrantLock lock = new ReentrantLock();
    private Condition nextCondition = lock.newCondition();
    private Timer timer = new Timer(true);
    private long reloadInterval = 10 * 1000L; // 10s

    public BinLogFileQueue(String directory) {
        this(new File(directory));
    }

    public BinLogFileQueue(File directory) {
        this.directory = directory;

        if (!directory.canRead()) {
            throw new CanalParseException("Binlog index missing or unreadable;  " + directory.getAbsolutePath());
        }

        List<File> files = listBinlogFiles();
        for (File file : files) {
            offer(file);
        }

        timer.scheduleAtFixedRate(new TimerTask() {

            public void run() {
                List<File> files = listBinlogFiles();
                for (File file : files) {
                    offer(file);
                }
            }
        }, reloadInterval, reloadInterval);
    }

    public File getNextFile(File pre) {
        try {
            lock.lockInterruptibly();
            if (binlogs.size() == 0) {
                return null;
            } else {
                if (pre == null) {
                    return binlogs.get(0);
                } else {
                    int index = seek(pre);
                    if (index < binlogs.size() - 1) {
                        return binlogs.get(index + 1);
                    } else {
                        return null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    public File getBefore(File file) {
        try {
            lock.lockInterruptibly();
            if (binlogs.size() == 0) {
                return null;
            } else {
                if (file == null) {
                    return binlogs.get(binlogs.size() - 1);
                } else {
                    int index = seek(file);
                    if (index > 0) {
                        return binlogs.get(index - 1);
                    } else {
                        return null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    public File waitForNextFile(File pre) throws InterruptedException {
        try {
            lock.lockInterruptibly();
            if (binlogs.size() == 0) {
                nextCondition.await();// wait for new file
            }

            if (pre == null) {
                return binlogs.get(0);
            } else {
                int index = seek(pre);
                if (index < binlogs.size() - 1) {
                    return binlogs.get(index + 1);
                } else {
                    nextCondition.await();
                    return waitForNextFile(pre);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public List<File> currentBinlogs() {
        return new ArrayList<File>(binlogs);
    }

    public void destory() {
        try {
            lock.lockInterruptibly();
            timer.cancel();
            binlogs.clear();

            nextCondition.signalAll();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private boolean offer(File file) {
        try {
            lock.lockInterruptibly();
            if (!binlogs.contains(file)) {
                binlogs.add(file);
                nextCondition.signalAll();
                return true;
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            lock.unlock();
        }
    }

    private List<File> listBinlogFiles() {
        List<File> files = new ArrayList<File>();
        files.addAll(FileUtils.listFiles(directory, new IOFileFilter() {

            public boolean accept(File file) {
                return file.getName().startsWith(baseName);
            }

            public boolean accept(File dir, String name) {
                return true;
            }
        }, null));
        Collections.sort(files, Comparator.comparing(File::getName));
        return files;
    }

    private int seek(File file) {
        for (int i = 0; i < binlogs.size(); i++) {
            File binlog = binlogs.get(i);
            if (binlog.getName().equals(file.getName())) {
                return i;
            }
        }

        return -1;
    }

    public void setBaseName(String baseName) {
        this.baseName = baseName;
    }
}
