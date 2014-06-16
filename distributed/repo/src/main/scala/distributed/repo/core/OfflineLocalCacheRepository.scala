package distributed
package repo
package core

import java.io.File
import sbt.IO
import java.io.InputStream
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream

/** A cached remote repository. */
class OfflineLocalCacheRepository(cacheDir: File) extends ReadableRepository {
  def get(key: String): InputStream  = {
    val cacheFile = new File(cacheDir, key)
    // TODO - Are we guaranteed uniqueness?  Should we embed knowledge of
    // `raw` vs `meta` keys here?
    // For now, let's assume immutable repos.
    if(!cacheFile.exists)
      throw new ResolveException(key, "Key ["+key+"] does not exist!")
    val stream=new BufferedInputStream(new FileInputStream(cacheFile))
    if (key.startsWith("meta/"))
      new GZIPInputStream(stream)
    else
      stream
  }
}

class LocalRepository(repo: File) 
    extends OfflineLocalCacheRepository(repo)
    with Repository {
  
  
  def put(key: String, file: File): Unit = {
    val cacheFile = new File(repo, key)
    IO.copyFile(file, cacheFile, false)
  }
}