package distributed
package repo
package core

import java.io.File
import java.util.concurrent.Semaphore
import collection.JavaConversions._
import sbt.IO
import java.io.InputStream
import java.io.FileOutputStream
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.util.Date
import sbt.Path._
import org.apache.commons.io.IOUtils

/** A local repository. */
class LocalRepository(cacheDir: File) extends Repository {
  private def file(sel: Selector) = cacheDir / sel.section / sel.index
  private def dataOrNone[Data](sel: Selector)(f:File => Data):Option[Data] = {
    val cacheFile = file(sel)
    Option(cacheFile.exists) map { _ =>
      f(cacheFile)
    }
  }
  def fetch(sel: Selector) = dataOrNone(sel) { f =>  new BufferedInputStream(new FileInputStream(f)) }
  def date(sel: Selector) = dataOrNone(sel) { f => new Date(f.lastModified) }
  def size(sel: Selector) = dataOrNone(sel) { _.length }
  def hasData(sel: Selector) = file(sel).exists
  def scan(section: String): Seq[Selector] = {
    val dir = cacheDir / section
    dir.listFiles().toSeq map { file =>
      Selector(section, file.getName)
    }
  }
  // TODO: implement a proper file-based locking.
  // We might eventually use FileLock from nio, or something similar.
  // sbt also has locking routines that we might steal.
  private val s = new Semaphore(1)
  protected[core] def lock = s.acquire
  protected[core] def unlock = s.release

  def store(data: InputStream, sel: Selector) = {
    val outputStream = new FileOutputStream(file(sel))
    IOUtils.copy(data, outputStream)
    outputStream.close()
    data.close() // unnecessary, put() should do it for us
  }
  def delete(sel: Selector) = file(sel).delete()
  def uncache(sel: Selector) = ()
}