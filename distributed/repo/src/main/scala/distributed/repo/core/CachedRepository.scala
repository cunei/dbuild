package distributed
package repo
package core

import dispatch.classic._
import java.io.File
import java.util.concurrent.Semaphore
import collection.JavaConversions._
import sbt.IO
import java.io.InputStream
import java.io.FileOutputStream
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import sbt.Path._
import org.apache.commons.io.IOUtils


/**
 * The most general caching: use one local repository as cache for any
 * number of remote repositories (or none at all).
 * Note that this will also work upside down: to upload a build cached in the
 * local dir repo, just set up a cache that caches some Artifactory repo
 * using as remote the local dir, then re-run the same repeatable build,
 * which will cause the artifacts to travel upstream.
 * 
 * I will probably add this as a utility in drepo, so that it becomes
 * possible to build offline, and then use drepo to upload the build with
 * all its generated metadata and artifact files.
 * 
 * At least in theory, if no remotes are defined, the Cache should act exactly
 * as the "local" repository, without changing semantics
 */
class Cache(local: Repository, readWriteRemotes: Seq[Repository], readOnlyRemotes: Seq[Repository] = Seq.empty) extends Repository {

  /**
   * The most typical case: one local cache repo, and one remote repo
   */
  def this(local: Repository, remote: Repository) = this(local,Seq(remote))

  val remotes = readWriteRemotes ++ readOnlyRemotes

  // TODO: add a lock/unlock of ALL repositories, remote and local,
  // around each of the calls below.
  // TODO: locking is still a bit iffy. Do we lock on the local machine only,
  // or on the network for shared resources as well? Some repos may not allow you to
  // do that (artifactory, for instance). So? Do we consider fetch/store atomic? (they should be, anyway).

  
  //
  // A simple rule is: try and check if the request can be complete using only
  // local. If so, safe {...} on the local only. If not, while keeping local lock
  // also lock all the remotes, and execute.
  private def safeRemotes[A,B](f: => B): B = {
    remotes foreach {_.lock}
    try {
      f
    } finally { remotes.reverse foreach {_.unlock }}
  }
  
  // Disconcertingly, Scala's collectFirst() may evaluate its function twice
  // in certain cases. So we create our own lazy version here.
  private def getFirstGeneral[A,B](al:Seq[A])(f:A=>Option[B]) = al.toStream.map(f).find(_.nonEmpty).map(_.get)
  // Check the local repo first, then the remotes
  private def getFirst[B](f:Repository=>Option[B]) = getFirstGeneral[Repository, B](local +: remotes)(f)
  // Check the remotes only
  private def getFirstOnlyRemotes[B](f:Repository=>Option[B]) = getFirstGeneral[Repository, B](remotes)(f)

  def fetch(sel: Selector) = {
    local.safe {
      local.fetch(sel) match {
        case Some(data) => Some(data)
        case None =>
        // lockRemotes? Probably no.
        val data = getFirstOnlyRemotes(_.fetch(sel))
        data match {
          case None => None
          case Some(stream) =>
            // we cannot split the stream easily, so we
            // write it, then we fetch it again
            local.store(stream, sel)
            stream.close
            local.fetch(sel)
        }
      }
    }
  }

  def hasData(sel: Selector) = {
    (local +: remotes) exists {_.hasData(sel)}
  }
  
  // do not cache, if just for the date.
  def date(sel: Selector) = getFirst{_.date(sel)}
  // do not cache, if just for the size
  def size(sel: Selector) = getFirst{_.size(sel)}

  // this is fun. We need to combine local and remotes together
  def scan(section: String): Seq[Selector] = {
    ((local +: remotes) flatMap (_.scan(section))).distinct
  }
  // even though we do not have local state in this class, we need
  // to lock/unlock as multiple low-level calls in this cache class
  // must be kept in a single logical block at the general high-level
  // calls of class Repository.
  private val s = new Semaphore(1)
  protected[core] def lock = s.acquire // do we need to lock local, in order to reflect the semantics if no remotes???
  protected[core] def unlock = s.release

  def delete(sel: Selector) = {
    (local +: readWriteRemotes) map {_.delete(sel)} contains (true)
  }
  def uncache(sel: Selector) = {
    // try to delete the local copy.
    // BUT do keep the local copy if no remote has it
    // If it is only local, try to uncache it in the local repo
    // in order to save some space, at least.
    local.safe {
      safeRemotes {
        if (remotes exists {_.hasData(sel)})
          local.delete(sel)
        else 
          local.uncache(sel)
      }
    }
  }
  def store(data: InputStream, sel: Selector) = {
    // Where do we push to? We push to all the (writeable) remotes,
    // as well as to the local repo.
    // We cannot reuse an InputStream multiple times, and Piped streams
    // have many restrictions, so we use a plain ByteArray instead.
    val byteArray = IOUtils.toByteArray(data)
    (local +: readWriteRemotes) foreach {
      _.store(new java.io.ByteArrayInputStream(byteArray), sel)
    }
  }
}

/*

class DiskCache(cacheDir: File, remote: Repository) extends Repository {
  private def file(sel: Selector) = cacheDir / sel.section / sel.index
    def recache(sel:Selector) = {
    val cacheFile = file(sel)
    if (!cacheFile.exists) {
      remote.lock
      store(remote.fetch(sel), sel)
    } finally { remote.unlock }
  }

  def fetch(sel: Selector) = {
    val cacheFile = file(sel)
    if (!cacheFile.exists) {
      remote.lock
      store(remote.fetch(sel), sel)
    } finally { remote.unlock }
    new BufferedInputStream(new FileInputStream(cacheFile))
  }
  def date(sel: Selector) = None // todo: when Storing, also write a timestamp
  def size(sel: Selector) = {
    fetch(sel)
    file(sel).length
  }
  def hasData(sel: Selector) = {
    fetch(sel)
    file(sel).exists
  }
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
  protected def lock = s.acquire
  protected def unlock = s.release

  def store(data: InputStream, sel: Selector) = {
    val outputStream = new FileOutputStream(file(sel))
    IOUtils.copy(data, outputStream)
    outputStream.close()
    data.close() // unnecessary, put() should do it for us
  }
  def delete(sel: Selector) = file(sel).delete()
  def uncache(sel: Selector) = ()
}



/** A cached remote repository. */
class CachedRemoteReadableRepository(cacheDir: File, uri: String) extends ReadableRepository {
  if(!cacheDir.exists) cacheDir.mkdirs()
  
  protected def makeUrl(args: String*) = args mkString "/"
  
  def get(key: String): InputStream  = {
    val cacheFile = new File(cacheDir, key)
    // TODO - Are we guaranteed uniqueness?  Should we embed knowledge of
    // `raw` vs `meta` keys here?
    // For now, let's assume immutable repos.
    if(!cacheFile.exists)
      try Remote pull (makeUrl(uri, key), cacheFile)
      catch {
        case e: Exception =>
          throw new ResolveException(key, e.getMessage)
      }
    val stream=new BufferedInputStream(new FileInputStream(cacheFile))
      stream
  }
}

/** A cached remote repository where we can update files. */
final class CachedRemoteRepository(cacheDir: File, uri: String, credentials: Credentials) 
    extends CachedRemoteReadableRepository(cacheDir, uri) 
    with Repository {
  def put(key: String, file: File): Unit = {
    val cacheFile = new File(cacheDir, key)
    // IF the cache file exists, we assume this file has already been pushed...
    // If we get corrupted files, we should check these and evict them.  For now, let's just
    // avoid 0-length files, which represents some kind of error downloaded that we've cleaned up,
    // but may reappair on file system exceptions.
    if(!cacheFile.exists || cacheFile.length == 0) {
      // TODO - immutability restrictions?
      try {
        Remote push (makeUrl(uri, key), file, credentials)
        IO.copyFile(file, cacheFile)
      } catch {
        case t: Exception =>
          throw new StoreException(key, t.getMessage())
      }      
    }
  }
  
  
  
  override def toString = "CachedRemoteRepository(uri="+uri+", cache="+cacheDir+")"
}

/** Helpers for free-form HTTP repositories */
object Remote {
  def push(uri: String, file: File, cred: Credentials): Unit = {
   import dispatch._
   // TODO - Discover mime type from file extension if necessary, or just send
   // as binary always.
   val sender = 
    url(uri).PUT.as(cred.user,cred.pw) <<< (file, "application/octet-stream")
    // TODO - output to logger.
    Http(sender >>> System.out)
  }
  def pull(uri: String, local: File): Unit = {
    // Ensure directory exists.
    local.getParentFile.mkdirs()
    
    // Pull to temporary file, then move.
    // uri must be sanitized first: can't contain slashes etc.
    val saneUri=java.net.URLEncoder.encode(uri)
    val suffix=saneUri.substring(Math.max(0,saneUri.length-45))
    IO.withTemporaryFile("dbuild-cache", suffix) { tmp =>
      import dispatch._
      val fous = new java.io.FileOutputStream(tmp)
      // IF there's an error, we must delete the file...
      try Http(url(uri) >>> fous)
      finally fous.close()
      // IF we made it here with no exceptions thrown, it's safe to move the temp file to the
      // appropriate location.  This should be a far more atomic operation, and "safer".
      IO.move(tmp, local)
    }
    
  }
}

*/