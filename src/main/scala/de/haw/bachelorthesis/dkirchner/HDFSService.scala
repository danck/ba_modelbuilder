package de.haw.bachelorthesis.dkirchner

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
  private val hdfsCoreSitePath = new Path("/opt/hadoop/conf/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/opt/hadoop/conf/hdfs-site.xml")

  hadoopConf.addResource(hdfsCoreSitePath)
  hadoopConf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(hadoopConf)

  def appendToTextFile(pathToFile: String, content: String): Unit = {
    try {
      val outStream = fileSystem.append(new Path(pathToFile))
      outStream.writeChars(content)
      outStream.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
