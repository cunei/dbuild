package distributed
package repo
package core

import java.io.File
import collection.JavaConversions._
import sbt.IO
import java.io.InputStream
import java.io.FileOutputStream
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream
import sbt.Path._
import org.apache.commons.io.IOUtils

/** A cached remote repository. */
class LocalRepository(cacheDir: File) extends Repository {
  protected def file(sel:Selector) = cacheDir / sel.section / sel.index
  def fetch(sel: Selector) = {
    val cacheFile = file(sel)
    Option(cacheFile.exists) map { _ =>
       new BufferedInputStream(new FileInputStream(cacheFile))
    }
  }
  def date(sel: Selector) = None // todo: when Storing, also write a timestamp
  def size(sel: Selector) = file(sel).length
  def hasData(sel: Selector) = file(sel).exists
  def scan(section: String): Seq[Selector] = {
    val dir = cacheDir / section
    dir.listFiles().toSeq map { file =>
      Selector(section,file.getName)
    }
  }
  // TODO: implement a proper file-based locking
  protected def lock: Unit = ()
  protected def unlock: Unit = ()

  def store(data: InputStream, sel: Selector) = {
    val outputStream = new FileOutputStream(file(sel))
    IOUtils.copy(data, outputStream)
    outputStream.close()
    data.close() // unnecessary, put() should do it for us
  }
  def delete(sel: Selector) = file(sel).delete()
}