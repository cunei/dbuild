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
sealed abstract class GetKey[DataType] extends {
  def uuid: String
  def streamToData(a: InputStream)(implicit m: Manifest[DataType]): DataType
}
sealed abstract class Key[DataType, Source <: { def uuid: String }, Get <: GetKey[DataType]] {
  def keyName: String
  def path: Seq[String]
  def newGet(uuid:String): Get
  def dataToStream(d: DataType): OutputStream
/*
 * A "Key" also knows about how to build a selector from a key and a uuid,
 * and how to extract a uuid again from a selector.
 * A repository does not know anything about a selector, other than it is
 * a sequence of strings that represents a full key path under which
 * to store an unstructured stream of data.
 */
  def selector(get:Get) = path :+ get.uuid
  def uuid(selector:Seq[String]) = selector.tail
}
case class GetRaw(uuid: String) extends GetKey[Array[Byte]] {
  def streamToData(a: InputStream) = Array[Byte]()
}
abstract class GetMeta[DataType] extends GetKey[DataType] {
  def streamToData(a: InputStream)(implicit m: Manifest[DataType]): DataType = /* gunzip */ null
}
case class GetProject(uuid: String) extends GetMeta
package object keys {
  implicit object RawKey extends Key[Array[Byte], ArtifactSha, GetRaw] {
    def keyName = "raw"
    def path = Seq(keyName)
    def newGet(uuid: String) = GetRaw(uuid)
  }
  // Data under any "Meta" key is compressed during serialization/deserialization,  directly while in transit.
  private[keys] sealed abstract class KeyMeta[DataType, Source <: { def uuid: String }, Get <: GetKey[DataType]]
    extends Key[DataType, Source, Get] {
    def path = Seq("meta", keyName)
  }
  implicit object ProjectKey extends KeyMeta[RepeatableProjectBuild, RepeatableProjectBuild, GetProject] {
    def keyName = "project"
  }
}
import keys._


/**
 * Interface for a readable repository of data, indexed by key.
 *  All calls MUST be thread-safe, and safe for use by multiple processes.
 */
abstract class ReadableRepository {
  /**
   * Retrieves the contents stored at a given key, if present.
   */
  def get[DataType, Get <: GetKey[DataType]](g: Get)(implicit key: Key[DataType, _, Get], m: Manifest[DataType]): Option[DataType] = {
    lock
    val data = fetch(key.selector(g)) map { g.streamToData }
    unlock
    data
  }
  /**
   * Retrieves the space concretely taken in the repository to store
   * the data indexed by this key. Returns zero if key not present.
   */
  def getSize[DataType, Get <: GetKey[DataType]](g: Get)(implicit key: Key[DataType, _, Get], m: Manifest[Get]): Int = {
    lock
    val len = size(key.selector(g))
    unlock
    len
  }
  /**
   * Get the list of keys for items of this DataType currently in the repo
   */
  def enumerate[DataType, Get](implicit key: Key[DataType, _, Get]): Seq[Get] = {
    lock
    val seq = scan(key.path) map { key.newGet(key.uuid(_)) }
    unlock
    seq
  }

  // the internal, non type safe versions, defined by the concrete implementations
  protected def fetch(selector:Seq[String]): Option[InputStream]
  protected def size(selector:Seq[String]): Int
  // scan checks under the path "selector", and returns all the immediate subselectors containing data
  protected def scan(selector:Seq[String]): Seq[String]
  protected def lock:Unit
  protected def unlock:Unit
}
/** Abstract class representing the interface by which we can put/get data.
 *  All calls MUST be thread-safe, and safe for use by multiple processes.
 */
abstract class WriteableRepository extends ReadableRepository {
  /** Puts some contents into the given key. */
  // wrong. KeySource is not parametrized, so it can be anything! Crap.
  def put[DataType <: { def uuid: String }, Get <: GetKey[DataType]](data: DataType)(implicit key: Key[DataType, DataType, Get], m: Manifest[Get]): Get = put[DataType, DataType, Get](data, data)
  def put[DataType, KeySource <: { def uuid: String }, Get <: GetKey[DataType]](data: DataType, keySource: KeySource)(implicit key: Key[DataType, KeySource, Get], m: Manifest[Get]): Get = {
    lock
    val uuid = keySource.uuid
    val out = key.dataToStream(data)
    val get = key.newGet(uuid)
    store(out, key.selector(get))
    unlock
    get
  }
  // the internal, non type safe versions, defined by the concrete implementations
  protected def store(out: OutputStream, selector: Seq[String]): Unit
}

object Test {
  def z(r:WriteableRepository, key1:RepeatableProjectBuild,key2:ArtifactSha, data: Array[Byte]) = {
    val k2=r.put(data,key2)
    val m=r.get(k2)
    val k1=r.put(key1)
    val n=r.get(k1)
  }
}



case class KeyBuild(uuid: String) extends GetMeta[SavedConfiguration,SavedConfiguration] { def keyName = "build" }
case class KeyArtifacts(uuid: String) extends GetMeta[BuildArtifactsOut,RepeatableProjectBuild] { def keyName = "artifacts" }
case class KeyExtract(uuid: String) extends GetMeta[ExtractionConfig,ExtractionConfig] { def keyName = "extract" }

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
