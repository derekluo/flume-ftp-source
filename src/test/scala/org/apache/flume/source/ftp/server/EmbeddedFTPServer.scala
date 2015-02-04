package org.apache.flume.source.ftp.server

import java.nio.file.Path

import org.apache.flume.source.TestFileUtils
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.filesystem.nativefs.impl.NativeFileSystemView
import org.apache.ftpserver.ftplet.Authority
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.{WritePermission, BaseUser}
import org.testng.annotations.BeforeClass

/**
 * Created by luca on 30/1/15.
 */
class EmbeddedFTPServer

object EmbeddedFTPServer extends TestFileUtils{
  val serverFactory = new FtpServerFactory
  val listenerFactory = new ListenerFactory

  val homeDirectory: Path = createTmpDir

  val userManagerFactory=new PropertiesUserManagerFactory()

  listenerFactory.setPort(2121)
  serverFactory.addListener("default", listenerFactory.createListener());

  val userManager = userManagerFactory.createUserManager()
  val user = new BaseUser()
  user.setName("flumetest")
  user.setPassword("flumetest")
  user.setHomeDirectory(homeDirectory.toFile.getAbsolutePath)
  userManager.save(user)
  serverFactory.setUserManager(userManager);
  val ftpServer = serverFactory.createServer

  @BeforeClass
  def initServer: Unit = {
    ftpServer.start()
  }

}

