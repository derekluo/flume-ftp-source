/*
 * KEEDIO
 */
package org.keedio.flume.source.ftp.source;

import java.util.*;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.ChannelException;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

import org.keedio.flume.source.ftp.source.utils.FTPSourceEventListener;

import org.keedio.flume.source.ftp.metrics.SourceCounter;
import java.util.List;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.keedio.flume.source.ftp.client.factory.SourceFactory;
import org.keedio.flume.source.ftp.client.KeedioSource;
import java.nio.charset.Charset;
import org.apache.flume.source.AbstractSource;

/**
 *
 * @author luislazaro lalazaro@keedio.com - KEEDIO
 *
 */
public class Source extends AbstractSource implements Configurable, PollableSource {

    private SourceFactory sourceFactory = new SourceFactory();
    private KeedioSource keedioSource;

    private static final Logger LOGGER = LoggerFactory.getLogger(Source.class);
    private static final short ATTEMPTS_MAX = 3; //  max limit attempts reconnection
    private static final long EXTRA_DELAY = 10000;
    private int counterConnect = 1;
    private FTPSourceEventListener listener = new FTPSourceEventListener();
    private SourceCounter sourceCounter;

    /**
     * Request keedioSource to the factory
     *
     * @param context
     * @return KeedioSource
     */
    public KeedioSource orderKeedioSource(Context context) {
        keedioSource = sourceFactory.createKeedioSource(context);
        return keedioSource;
    }

    /**
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        keedioSource = orderKeedioSource(context);
        if (keedioSource.existFolder()) {
            keedioSource.makeLocationFile();
        } else {
            LOGGER.error("Folder " + keedioSource.getPathTohasmap().toString() + " not exists");
        }
        keedioSource.connect();
        sourceCounter = new SourceCounter("SOURCE." + getName());
        keedioSource.checkPreviousMap();
    }

    /**
     * @return Status , process source configured from context
     * @throws org.apache.flume.EventDeliveryException
     */
    @Override
    public PollableSource.Status process() throws EventDeliveryException {

        try {
            LOGGER.info("Processing: (dir: " + keedioSource.getDirectoryserver() + "; files: "
                        + keedioSource.getFileList().size() + ", " + keedioSource.getExistFileList().size());

            keedioSource.getExistFileList().clear();
            discoverElements(keedioSource, keedioSource.getDirectoryserver(), "", 0);
            keedioSource.cleanList(); //clean list according existing actual files

        } catch (IOException e) {
            LOGGER.error("Exception thrown in process, try to reconnect " + counterConnect, e);

            if (!keedioSource.connect()) {
                counterConnect++;
            } else {
                keedioSource.checkPreviousMap();
            }

            if (counterConnect <= ATTEMPTS_MAX) {
                process();
            } else {
                LOGGER.error("Server connection closed without indication, reached limit reconnections " + counterConnect);
                try {
                    Thread.sleep(keedioSource.getRunDiscoverDelay() + EXTRA_DELAY);
                    counterConnect = 0;
                } catch (InterruptedException ce) {
                    LOGGER.error("InterruptedException", ce);
                }
            }
        }

        try {
            Thread.sleep(keedioSource.getRunDiscoverDelay());
            return PollableSource.Status.READY;     //source was successfully able to generate events
        } catch (InterruptedException inte) {
            LOGGER.error("Exception thrown in process while putting to sleep", inte);
            return PollableSource.Status.BACKOFF;   //inform the runner thread to back off for a bit
        }
    }

    /**
     * @return void
     */
    @Override
    public synchronized void start() {
        LOGGER.info("Starting Keedio source ...", this.getName());
        LOGGER.info("Source {} starting. Metrics: {}", getName(), sourceCounter);
        super.start();
        sourceCounter.start();
    }

    /**
     * @return void
     */
    @Override
    public synchronized void stop() {
        if (keedioSource.isConnected()) {
            keedioSource.disconnect();
        }
        sourceCounter.stop();
        super.stop();
        LOGGER.info("Keedio source stopped: ", this.getName());
    }

    /**
     * discoverElements: find files to process them
     *
     * @param <T>
     * @param keedioSource
     * @param parentDir, will be the directory retrieved by the server when
     * connected
     * @param currentDir, actual dir in the recursive method
     * @param level, deep to search
     * @throws IOException
     */
    // @SuppressWarnings("UnnecessaryContinue")
    public <T> void discoverElements(KeedioSource keedioSource, String parentDir, String currentDir, int level) throws IOException {
        long position = 0L;

        String dirToList = parentDir;
        if (!("").equals(currentDir)){
            dirToList += "/" + currentDir;
        }

        List<T> list = keedioSource.listElements(dirToList);

        LOGGER.info("discoverElements: " + dirToList + ", " + level + ", " + list.size());

        if (!(list.isEmpty())) {

            for (T element : list) {
                String elementName = keedioSource.getObjectName(element);
                if (elementName.equals(".") || elementName.equals("..")) {
                    continue;
                }

                if (keedioSource.isDirectory(element)) {
                    keedioSource.changeToDirectory(parentDir);
                    discoverElements(keedioSource, dirToList, elementName, level + 1);

                } else if (keedioSource.isFile(element)) { //element is a regular file
                    keedioSource.changeToDirectory(dirToList);
                    keedioSource.getExistFileList().add(dirToList + "/" + elementName);  //control of deleted files in server

                    //test if file is new in collection
                    if (!(keedioSource.getFileList().containsKey(dirToList + "/" + elementName))) { //new file
                        sourceCounter.incrementFilesCount(); //include all files, even not yet processed
                        position = 0L;
                        LOGGER.info("Discovered: " + elementName + ", size: " + keedioSource.getObjectSize(element));
                    } else { //known file
                        long prevSize = (long) keedioSource.getFileList().get(dirToList + "/" + elementName);
                        position = prevSize;
                        long diff = keedioSource.getObjectSize(element) - (long) keedioSource.getFileList().get(dirToList + "/" + elementName);

                        if (0 == diff) {
                            LOGGER.info("Same: " + elementName);
                            continue;
                        } else if (diff < 0) {
                            LOGGER.info("Shrinked: " + elementName + ", diff: " + diff);

                            keedioSource.getFileList().remove(dirToList + "/" + elementName); //will be rediscovered as new file
                            keedioSource.saveMap();
                            LOGGER.info("FileMap file removed: " + dirToList + "/" + elementName);

                            continue;
                        } else { // diff > 0
                            LOGGER.info("Modified: " + elementName + ", diff: " + diff);
                        }
                    } //end if known file

                    //common for all regular files
                    InputStream inputStream = null;
                    try {
                        inputStream = keedioSource.getInputStream(element);
                        listener.fileStreamRetrieved();

                        if (!readStream(inputStream, position, elementName)) {
                            inputStream = null;
                        }

                        boolean success = inputStream != null && keedioSource.particularCommand(); //mandatory if FTPClient
                        if (success) {
                            keedioSource.getFileList().put(dirToList + "/" + elementName, keedioSource.getObjectSize(element));
                            keedioSource.saveMap();
                            LOGGER.info("KeedioSourceFileList file added: " + dirToList + "/" + elementName);

                            if (position != 0) {
                                sourceCounter.incrementCountModProc();
                            } else {
                                sourceCounter.incrementFilesProcCount();
                            }

                            LOGGER.info("Processed:  " + elementName + " ,total files: " + this.keedioSource.getFileList().size() + "\n");

                        } else {
                            handleProcessError(elementName);
                        }
                    } catch (IOException e) {
                        handleProcessError(elementName);
                        LOGGER.error("Failed retrieving inputStream on discoverElements ", e);
                        continue;
                    }

                    keedioSource.changeToDirectory(dirToList);

                } else if (keedioSource.isLink(element)) {
                    LOGGER.info(elementName + " is a link of " + this.keedioSource.getLink(element) + " could not retrieve size");
                    keedioSource.changeToDirectory(parentDir);
                    continue;
                } else {
                    LOGGER.info(elementName + " unknown type of file");
                    keedioSource.changeToDirectory(parentDir);
                    continue;
                }
                keedioSource.changeToDirectory(parentDir);

            }
        }
    }

    /**
     * Read retrieved stream from ftpclient into byte[] and process. If
     * flushlines is true the retrieved inputstream will be readed by lines. And
     * the type of file is set to ASCII from KeedioSource.
     *
     * @return boolean
     * @param inputStream
     * @param position
     * @param fileName
     */
    public boolean readStream(InputStream inputStream, long position, String fileName) {
        if (inputStream == null) {
            return false;
        }

        boolean successRead = true;

        if (keedioSource.isFlushLines()) {
            try {
                inputStream.skip(position);
                try (BufferedReader in = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()))) {
                    String line = null;

                    while ((line = in.readLine()) != null) {
                        processMessage(fileName, line.getBytes());
                    }

                }
                inputStream.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
                successRead = false;
            }
        } else {

            try {
                inputStream.skip(position);
                int chunkSize = keedioSource.getChunkSize();
                byte[] bytesArray = new byte[chunkSize];
                int bytesRead = -1;
                while ((bytesRead = inputStream.read(bytesArray)) != -1) {
                    try (ByteArrayOutputStream baostream = new ByteArrayOutputStream(chunkSize)) {
                        baostream.write(bytesArray, 0, bytesRead);
                        byte[] data = baostream.toByteArray();
                        processMessage(fileName, data);
                    }
                }

                inputStream.close();
            } catch (IOException e) {
                LOGGER.error("on readStream", e);
                successRead = false;

            }
        }
        return successRead;
    }

    /**
     * @void process last appended data to files
     * @param lastInfo byte[]
     * @param fileName String
     */
    public void processMessage(String fileName, byte[] lastInfo) {
        Event event = new SimpleEvent();

        Map<String, String> headers = new HashMap<>();
        headers.put("fileName", fileName);
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        event.setHeaders(headers);

        byte[] message = lastInfo;
        event.setBody(message);

        try {
            getChannelProcessor().processEvent(event);
        } catch (ChannelException e) {
            LOGGER.error("ChannelException", e);
        }
        sourceCounter.incrementCountSizeProc(message.length);
        sourceCounter.incrementEventCount();
    }

    /**
     * @param listener
     */
    public void setListener(FTPSourceEventListener listener) {
        this.listener = listener;
    }

    /**
     * @param fileName
     */
    public void handleProcessError(String fileName) {
        LOGGER.info("failed retrieving stream from file, will try in next poll :" + fileName);
        sourceCounter.incrementFilesProcCountError();
    }

    /**
     *
     * @param ftpSourceCounter
     */
    public void setFtpSourceCounter(SourceCounter ftpSourceCounter) {
        this.sourceCounter = ftpSourceCounter;
    }

    /**
     * @return KeedioSource
     */
    public KeedioSource getKeedioSource() {
        return keedioSource;
    }

} //endclass
