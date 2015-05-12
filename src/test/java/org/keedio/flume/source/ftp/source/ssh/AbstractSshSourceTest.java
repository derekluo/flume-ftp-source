//package org.apache.flume.source.ssh;
//
//import com.keedio.flume.client.sources.SFTPSource;
//import java.io.IOException;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.List;
//
//import org.apache.flume.Event;
//import org.apache.flume.Context;
//import org.apache.flume.channel.ChannelProcessor;
//import static org.mockito.Mockito.*;
//
//import com.keedio.flume.source.Source;
//import com.keedio.flume.metrics.SourceCounter;
//import TestFileUtils;
//import org.apache.flume.source.sshd.server.EmbeddedSSHDServer;
//import org.apache.log4j.Logger;
//import org.mockito.MockitoAnnotations;
//import org.mockito.Mock;
//import org.slf4j.LoggerFactory;
//import org.testng.annotations.AfterMethod;
//import org.testng.annotations.BeforeMethod;
//
//public abstract class AbstractSshSourceTest extends EmbeddedSSHDServer {
//
//    private Logger logger = Logger.getLogger(getClass());
//
//    @Mock
//    Context mockContext = new Context();
//
//    Source sftpSource;
//    SourceCounter sourceCounter;
//
//    String getSource = "sftp";
//    Integer getPort = 2222;
//    String getUser = "flumetest";
//    String getPassword = "flumetest";
//    String getHost = "localhost";
//    String getWorkingDirectory = "/var/tmp/";
//    String getFileName = "hasmap.ser";
//    String getFolder = System.getProperty("java.io.tmpdir");
//    String getAbsoutePath = System.getProperty("java.io.tmpdir") + "hasmap.ser";
//    String getKnownHosts = "/var/tmp/known_hosts";
//    Integer getBuffer = 1024;
//    Integer getDiscover = 1000;
//    Integer getChunkSize = 1024;
//        
//
//    @BeforeMethod
//    public void beforeMethod() {
//        MockitoAnnotations.initMocks(this);
//
//        when(mockContext.getString("client.source")).thenReturn(getSource);
//        when(mockContext.getInteger("port")).thenReturn(getPort);
//        when(mockContext.getString("user")).thenReturn(getUser);
//        when(mockContext.getString("password")).thenReturn(getPassword);
//        when(mockContext.getString("name.server")).thenReturn(getHost);
//        when(mockContext.getString("working.directory")).thenReturn(getWorkingDirectory);
//        when(mockContext.getString("file.name")).thenReturn(getFileName);
//        when(mockContext.getString("folder", System.getProperty("java.io.tmpdir"))).thenReturn(System.getProperty("java.io.tmpdir"));
//        when(mockContext.getString("knownHosts")).thenReturn(getKnownHosts);
//        when(mockContext.getInteger("buffer.size")).thenReturn(getBuffer);
//        when(mockContext.getInteger("run.discover.delay")).thenReturn(getDiscover);
//        when(mockContext.getInteger("chunk.size", 1024)).thenReturn(getChunkSize);
//
//        logger.info("Creating SFTP source");
//
//        sftpSource = new Source();
//        sftpSource.configure(mockContext);
//        sourceCounter = new SourceCounter("SOURCE.");
//        sftpSource.setFtpSourceCounter(sourceCounter);
//
//        class DummyChannelProcessor extends ChannelProcessor {
//
//            public DummyChannelProcessor() {
//                super(null);
//            }
//
//            @Override
//            public void processEvent(Event event) {
//            }
//        }
//
//        sftpSource.setChannelProcessor(new DummyChannelProcessor());
//    }
//
//    @AfterMethod
//    public void afterMethod() {
//        try {
//            logger.info("Stopping SFTP source");
//            sftpSource.stop();
//
//            Paths.get("hasmap.ser").toFile().delete();
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void cleanup(Path file) {
//        try {
//            TestFileUtils.forceDelete(file);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void cleanup(List<Path> files) {
//        for (Path f : files) {
//            try {
//                TestFileUtils.forceDelete(f);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}