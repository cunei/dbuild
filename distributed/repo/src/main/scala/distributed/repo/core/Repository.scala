package distributed
package repo
package core

import java.io.File
import sbt.Path._
import java.io.InputStream
import java.io.OutputStream
import project.model._

/**
 * Defines the kind of repository keys that can be used to access the various
 * data types that can be stored into a repository.
 * KeySource is the "uuid" supplier for put operations, which is not always
 * a DataType
 */
sealed protected class KeyUser {
  def uuid: String
  def key = path :+ uuid
}
sealed abstract class PutKey[DataType] extends KeyUser {
  type Source <: { def uuid: String }
  def source: Source
  def uuid = source.uuid
  def dataToStream(d: DataType): OutputStream
}
sealed abstract class GetKey[DataType] extends KeyUser {
  def uuid: String
  def streamToData(a: InputStream)(implicit m: Manifest[DataType]): DataType
}
sealed abstract class Key[DataType, Source <: { def uuid: String }, Get <: GetKey[DataType]] {
  def keyName: String
  def path: Seq[String]
}
case class PutRaw(source: ArtifactSha) extends PutKey {
  type Source = ArtifactSha
  def dataToStream(d: Array[Byte]) = new java.io.ByteArrayOutputStream()
}
case class GetRaw(uuid: String) extends GetKey[Array[Byte]] {
  def streamToData(a: InputStream) = Array[Byte]()
}
package object keys {
  implicit object RawKey extends Key[Array[Byte], ArtifactSha, GetRaw] {
    def keyName = "raw"
    def path = Seq(keyName)
  }
}
import keys._


sealed abstract class KeyMeta[DataType] extends Key[DataType] {
  abstract case class PutMeta extends PutKey {
    def dataToStream(d: DataType) = /* gzip */ new java.io.ByteArrayOutputStream()
  }
  abstract class GetMeta extends GetKey {
    def streamToData(a: InputStream) = /* gunzip */ null
  }
  def path = Seq("meta", keyName)
}

object ProjectKey extends KeyMeta[RepeatableProjectBuild] {
  case class PutProject(source: RepeatableProjectBuild) extends PutMeta {
    type Source = RepeatableProjectBuild
  }
  case class GetProject(uuid: String) extends GetMeta
  def keyName = "project"
}

/** Interface for a readable repository of data, indexed by key.
 *  All calls MUST be thread-safe, and safe for use by multiple processes.
 */
abstract class ReadableRepository {
  /**
   * Retrieves the contents stored at a given key, if present.
   */
  def get[DataType, Get](get: Get)(implicit key:Key[DataType,_,Get]): Option[DataType]
  /**
   * Retrieves the space concretely taken in the repository to store
   * the data indexed by this key. Returns zero if key not present.
   */
  def getSize[DataType, Get](get: Get)(implicit key:Key[DataType,_,Get]): Int
  /**
   * Get the list of keys for items of this DataType currently in the repo
   */
  def enumerate[DataType, Get](implicit key:Key[DataType,_,Get]): Seq[Get]
}
/** Abstract class representing the interface by which we can put/get data.
 *  All calls MUST be thread-safe, and safe for use by multiple processes.
 */
abstract class WriteableRepository extends ReadableRepository {
  /** Puts some contents into the given key. */
  // wrong. KeySource is not parametrized, so it can be anything! Crap.
  def put[DataType, Get](data: DataType)(implicit key:Key[DataType,DataType,Get]): Get = put(data,data)
  def put[DataType,KeySource, Get](data: DataType, keySource:KeySource)(implicit key:Key[DataType,KeySource,Get]): Get = {
//    keySource.putKey
    //
    null
  }
}

object Test {
  def z(r:WriteableRepository, key1:RepeatableProjectBuild,key2:ArtifactSha, data: Array[Byte]) = {
    val k2=r.put(data,key2)
    val m=r.get(k2)
    val k1=r.put(key1)(ProjectKey)
    val n=r.get(k1)
  }
}



// Data under the "Meta" key is compressed during serialization/deserialization,  directly while in transit.
abstract sealed class GetMeta[T] extends GetKey[T] {
  def dataToStream(d: T) = new java.io.ByteArrayOutputStream()
  def streamToData(a: InputStream)(implicit m: Manifest[T]) = distributed.project.model.Utils.readValue[T]( /* add GUnZip to stream */ a)
  protected def keyName: String
  def key = "meta/" + keyName + "/" + uuid
}
case class KeyBuild(uuid: String) extends GetMeta[SavedConfiguration,SavedConfiguration] { def keyName = "build" }
case class KeyArtifacts(uuid: String) extends GetMeta[BuildArtifactsOut,RepeatableProjectBuild] { def keyName = "artifacts" }
case class KeyExtract(uuid: String) extends GetMeta[ExtractionConfig,ExtractionConfig] { def keyName = "extract" }
//case class KeyProject(uuid: String) extends KeyMeta[RepeatableProjectBuild,RepeatableProjectBuild] { def keyName = "project" }
//case class KeyBuild(uuid: String) extends KeyMeta[SavedConfiguration,SavedConfiguration] { def keyName = "build" }
//case class KeyArtifacts(uuid: String) extends KeyMeta[BuildArtifactsOut,RepeatableProjectBuild] { def keyName = "artifacts" }
//case class KeyExtract(uuid: String) extends KeyMeta[ExtractionConfig,ExtractionConfig] { def keyName = "extract" }

// must add some sort of enumeration facility (we might need to abstract the generic path for each key type?)
// Rather than exporting stuff for use by drepo, it makes instead sense to bring the enumeration/traversal
// facilities needed by drepo INSIDE the Repository, which is where it should have lived all along.
// Also, it would be nice to link once and for all each KeyMeta to the original source of UUIDs, so that we know
// for instance that the uuid for a put for a BuildArtifactsOut *always* comes from a RepeatableProjectBuild.
// So: a "put" could accept a further keySource of a related type.



object Repository {

  def default: Repository = {
    // Look for repository/credentials file
    def cacheDir = sysPropsCacheDir getOrElse defaultUserHomeCacheDir
    def repoCredFile = GlobalDirs.repoCredFile

    def readCredFile(f: File): Option[(String, Credentials)] =
      if (f.exists) {
        val props = new java.util.Properties
        sbt.IO.load(props, f)
        def getProp(name: String): Option[String] =
          Option(props getProperty name)
        for {
          url <- getProp("remote.url")
          user <- getProp("remote.user")
          pw <- getProp("remote.password")
        } yield url -> Credentials(user, pw)
      } else None

    def remoteRepo =
      for {
        (url, credentials) <- readCredFile(repoCredFile)
      } yield remote(url, credentials, cacheDir)
    def localRepo = local(cacheDir)
    remoteRepo getOrElse localRepo
  }
  
  /** Construct a repository that reads from a given URI and stores in a local cache. */
  def readingFrom(uri: String, cacheDir: File = defaultCacheDir): ReadableRepository = 
    new CachedRemoteReadableRepository(cacheDir, uri)
  /** Construct a repository that reads/writes to a given URI and stores in a local cache. */
  def remote(uri: String, cred: Credentials, cacheDir: File = defaultCacheDir): Repository =
    new CachedRemoteRepository(cacheDir, uri, cred)
  
  def localCache(cacheDir: File = defaultCacheDir): ReadableRepository =
    new OfflineLocalCacheRepository(cacheDir)
  
  def local(cacheDir: File = defaultCacheDir): Repository = 
    new LocalRepository(cacheDir)
  
  def defaultCacheDir = sysPropsCacheDir getOrElse defaultUserHomeCacheDir
    
  def sysPropsCacheDir =
    sys.props get "dbuild.cache.dir" map (new File(_))
  
  def defaultUserHomeCacheDir = GlobalDirs.userCache
       
}
