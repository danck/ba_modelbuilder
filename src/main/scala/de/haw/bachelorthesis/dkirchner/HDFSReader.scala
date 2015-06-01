package de.haw.bachelorthesis.dkirchner

/**
 * Created by Daniel on 25.05.2015.
 */
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object HDFSFileService {
  private val hadoopConf = new Configuration()
  private val hdfsCoreSitePath = new Path("/opt/hadoop/conf/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/opt/hadoop/conf/hdfs-site.xml")

  hadoopConf.addResource(hdfsCoreSitePath)
  hadoopConf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(hadoopConf)

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}