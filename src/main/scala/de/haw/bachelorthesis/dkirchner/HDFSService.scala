package de.haw.bachelorthesis.dkirchner

import java.io.{IOException, File}
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/*
 * This file is part of my bachelor thesis.
 *
 * Copyright 2015 Daniel Kirchner <daniel.kirchner1@haw-hamburg.de>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */

/**
 * Created by Daniel on 25.05.2015.
 */
object HDFSService {
  private val hadoopConf = new Configuration()
  private val hdfsCoreSitePath = new Path("/opt/hadoop/etc/hadoop-2.7.0/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/opt/hadoop/etc/hadoop-2.7.0/hdfs-site.xml")

  hadoopConf.addResource(hdfsCoreSitePath)
  hadoopConf.addResource(hdfsHDFSSitePath)

  private val hdfs = FileSystem.get(hadoopConf)

  /**
   * Workaround to append a string to a text file since
   * append is not supported in Hadoop 1.x
   * @param pathToFile
   * @param content
   */
  def appendToTextFile(pathToFile: String, content: String): Unit = {
    val file = new File(pathToFile)
    val path = new Path(file.getName)

    try {
      if (!hdfs.exists(path)){
        if (!hdfs.createNewFile(path)) {
          throw new IOException("Datei konnte nicht angelegt werden.")
        }
      }
      println("Lege Datei an: " + path.getName)
      val outStream = hdfs.append(path)
      outStream.writeChars(content)
      outStream.flush()
      outStream.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)
    }
  }
}
