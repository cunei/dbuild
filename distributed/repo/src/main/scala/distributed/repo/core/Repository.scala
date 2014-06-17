package distributed
package repo
package core

import java.io.File
import sbt.Path._
import java.io.InputStream
import java.io.OutputStream
import project.model._

/*
 * This implementation of repositories operates on two levels. Internally, all concrete
 * implementations of repositories need only implement unstructured data access:
 * they accept an InputStream and store its content under an access key (a path),
 * and they retrieve some content under a key and return it as an OutputStream.
 * 
 * Externally, at the user level, access is made using a put() and a get() methods,
 * which strictly check the arguments and automatically generate correct keys depending
 * on the kind of data supplied. Further, since some kinds of data can only be stored
 * and retrieved using a uuid obtained from a specific different kind of data, the
 * type-safe put() will only compile when the supplied combination of types is correct.
 * The result of a put() is a type-specific abstract access key (a GetKey), which can
 * be then used in further get() calls, which again will return only data of the correct kind.
 * 
 * The higher-level, type-safe level of a Repository, works using:
 * put() -> returns a GetKey -> get(getKey)
 * 
 * The internal (hidden) lower-level, type-unsafe level of a Repository works by converting
 * the GetKey into a Selector using the information contained into a Key (see below)and then
 * using the unstructured calls: store(data,selector) / fetch(selector)
 */

/**
 * A GetKey is a generic, but type-safe, way to access some data stored in a repository.
 * It is only ever be created by the put() call of Repository, and used by its get(),
 * but never created explicitly by other user code.
 * TODO: add protection against external creation, by making constructors private to Repository,
 * or to this package. All methods should be private to Repository.
 *
 * Internally, a GetKey knows how to deserialize the stream returned by the repository into
 * its specific corresponding DataType.
 */
// Note: do *not* make GetKey a subclass of Repository, as its instances are serialized via Jacks.
sealed abstract class GetKey[DataType] extends {
  def uuid: String
  def streamToData(a: InputStream)(implicit m: Manifest[DataType]): DataType
}
/**
 * An access GetKey used to retrieve a raw Array[Byte], which we use to store raw artifacts.
 */
case class GetRaw(uuid: String) extends GetKey[Array[Byte]] {
  def streamToData(a: InputStream)(implicit m: Manifest[Array[Byte]]) = /* TODO: implement */ Array[Byte]()
}
/**
 * A GetMeta is a kind of GetKey used to access JSON-serializable metadata.
 * We define streamToData() here for all such metadata classes; since the serialized
 * form is simple textual JSON, we can automatically filter it through a gzip compressor,
 * in order to save space.
 * Concrete subclasses of GetMeta are used for project descriptions, extraction metadata, etc.
 */
abstract class GetMeta[DataType] extends GetKey[DataType] {
  def streamToData(a: InputStream)(implicit m: Manifest[DataType]): DataType = /* TODO: gunzip */ sys.error("bah")
}
case class GetProject(uuid: String) extends GetMeta[RepeatableProjectBuild]
case class GetBuild(uuid: String) extends GetMeta[SavedConfiguration]
case class GetArtifacts(uuid: String) extends GetMeta[BuildArtifactsOut]
case class GetExtract(uuid: String) extends GetMeta[ExtractionConfig]

/**
 * A Key data structure contains the logic needed to interface the higher, type-safe
 * level of repositories with its lower, type-unsafe implementation.
 *
 * To access data, a combination of the kind of a GetKey, plus its uuid, is turned into an
 * internal "Selector" by the Key. Such a Selector is then used as the lower level of a
 * Repository implementation in order to represent an untyped, general data access path.
 * The lower level of a Repository only work on Selectors and raw data, and have no concept
 * of the specific kind of data that is being written or retrieved, nor about what a
 * selector is all about, other than it describes a path to data (as a Seq[String]).
 *
 * Note that not all of the kinds of data are placed in the repository under their own
 * uuid: in some cases (raw files for instance, or BuildArtifactsOuts), the uuid depends
 * on a related piece of data of a different kind. That is represented by KeySource, which
 * must be able to provide the needed uuid.
 * 
 * TODO: all Key methods should be private to Repository. Keys themselves should be public,
 * as they are implicitly used by put() and get().
 */
sealed abstract class Key[DataType, KeySource <: { def uuid: String }, Get <: GetKey[DataType]] {
  /** the concrete keys use "keyName" as a convenience, to generate the path */
  protected def keyName: String
  protected def path: Seq[String]
  def newGet(uuid: String): Get
  def dataToStream(d: DataType): OutputStream
  /*
 * Selector-related utils
 */
  /**
   * From higher level (typed) to lower level (untyped)
   */
  def selector(get: GetKey[DataType]) = Selector(path :+ get.uuid)
  /**
   * Given a low-level Selector to an element, recreates the
   * corresponding high-level GetKey. This call is used in scan(),
   * in order to generate the list of GetKeys, given the list of
   * low-level Selectors.
   */
  def getFromSelector(selector: Selector) = newGet(selector.path.last)
  /**
   * In order to implement enumerate() (the low-level equivalent to scan()),
   * we need a Selector to the parent of all Selectors that contain data
   * of the kind supported by this Key.
   * "scanSelector" returns such a Selector.
   */
  def scanSelector = Selector(path)
}
/**
 * Use "import keys._" in client code, to bring the implicit repository Keys into scope.
 */
package object keys {
  implicit object RawKey extends Key[Array[Byte], ArtifactSha, GetRaw] {
    private def keyName = "raw"
    def path = Seq(keyName)
    def newGet(uuid: String) = GetRaw(uuid)
    def dataToStream(d: Array[Byte]): OutputStream = new java.io.ByteArrayOutputStream()
  }
  /** Data under any "Meta" key is compressed during serialization/deserialization,  directly while in transit. */
  private[keys] sealed abstract class KeyMeta[DataType, Source <: { def uuid: String }, Get <: GetKey[DataType]]
    extends Key[DataType, Source, Get] {
    def path = Seq("meta", keyName)
    def dataToStream(d: DataType): OutputStream = new java.io.ByteArrayOutputStream()
  }
  implicit object ProjectKey extends KeyMeta[RepeatableProjectBuild, RepeatableProjectBuild, GetProject] {
    def keyName = "project"
    def newGet(uuid: String) = GetProject(uuid)
  }
  implicit object BuildKey extends KeyMeta[SavedConfiguration, SavedConfiguration, GetBuild] {
    def keyName = "build"
    def newGet(uuid: String) = GetBuild(uuid)
  }
  implicit object ArtifactsKey extends KeyMeta[BuildArtifactsOut, RepeatableProjectBuild, GetArtifacts] {
    def keyName = "artifacts"
    def newGet(uuid: String) = GetArtifacts(uuid)
  }
  implicit object ExtractKey extends KeyMeta[ExtractionConfig, ExtractionConfig, GetExtract] {
    def keyName = "extract"
    def newGet(uuid: String) = GetExtract(uuid)
  }
}

/**
 * A Selector is a full path to an actual piece of data saved to a repository (at the lower,
 * type-unsafe conceptual level). It is implemented as a Seq[String], where each String is
 * a component of a path.
 */
case class Selector(path: Seq[String])

/**
 * Abstract class for a readable repository of data, indexed by GetKeys.
 * The high-level, type-safe interface (get/size/scan) is also safe to use
 * from multiple processes simultaneously.
 *
 * To implement a concrete subclass of a ReadableRepository, implement
 * the low-level: fetch, size, scan, lock, and unlock.
 */
abstract class ReadableRepository {
  /**
   * Retrieves the contents stored at a given key, if present.
   */
  def get[DataType](g: GetKey[DataType])(implicit key: Key[DataType, _, _], m: Manifest[DataType]): Option[DataType] = {
    lock
    val data = fetch(key.selector(g)) map g.streamToData
    unlock
    data
  }
  /**
   * Retrieves the space concretely taken in the repository to store
   * the data indexed by this key. Returns zero if key not present.
   */
  def getSize[DataType](g: GetKey[DataType])(implicit key: Key[DataType, _, _], m: Manifest[DataType]): Int = {
    lock
    val len = size(key.selector(g))
    unlock
    len
  }
  /**
   * Get the list of keys for items of this DataType currently in the repo
   */
  def enumerate[DataType](implicit key: Key[DataType, _, GetKey[DataType]]): Seq[GetKey[DataType]] = {
    lock
    val seq = scan(key.scanSelector) map key.getFromSelector
    unlock
    seq
  }

  /*
   * To create a concrete implementation of a Repository, just implement the low-level primitives
   * listed below. fetch(), size() and scan() need not be thread-safe: lock and unlock are
   * called to lock the repository when needed.
   */
 
  /**
   * Read from the repository the data stored at Selector. If no data is present, return None.
   */
  protected def fetch(selector: Selector): Option[InputStream]
  /**
   * Return, if known, the actual space occupied in the Repository by the data stored
   * under Selector. If no data, return zero.
   */
  protected def size(selector: Selector): Int
  /**
   * scan() checks under the given Selector path, and returns all the immediate
   * subselectors for which data exist in the repository. If no subselectors, return
   * an empty Seq.
   */
  protected def scan(selector: Selector): Seq[Selector]
  /**
   * lock and unlock are used to protect against concurrent accesses to the repository,
   * and must protect the repository content even if multiple independent processes
   * are trying to access this repository at the same time.
   */
  protected def lock: Unit
  protected def unlock: Unit
}
/**
 * Abstract class for a readable/writeable repository of data, indexed by GetKeys.
 * The high-level, type-safe interface (get/put) is also safe to use from
 * multiple processes simultaneously.
 *
 * To implement a concrete subclass of a Repository, implement
 * the low-level: fetch, store, size, scan, lock, and unlock.
 */
abstract class Repository extends ReadableRepository {
  /**
   * Puts some contents into the given key.
   * The data of type DataType will be placed into the repository
   * under a key decided by the uuid of keySource, which may be
   * different than the main data. For instance, a BuildArtifactsOut
   * is always saved under the key corresponding to the matching
   * RepeatableProjectBuild.
   */
  def put[DataType, KeySource <: { def uuid: String }, Get <: GetKey[DataType]](data: DataType, keySource: KeySource)(implicit key: Key[DataType, KeySource, Get], m: Manifest[Get]): Get = {
    lock
    val uuid = keySource.uuid
    val out = key.dataToStream(data)
    val get = key.newGet(uuid)
    store(out, key.selector(get))
    unlock
    get
  }
  /**
   * In case the uuid source and the saved data coincide, a plain single-argument "put(data)" can be used instead.
   */
  def put[DataType <: { def uuid: String }, Get <: GetKey[DataType]](data: DataType)(implicit key: Key[DataType, DataType, Get], m: Manifest[Get]): Get = put[DataType, DataType, Get](data, data)

  // the internal, low-level, non-type-safe equivalent to put()
  /**
   * store() does not need to be thread-safe: lock and unlock are
   * used to lock the repository when needed.
   */
  protected def store(out: OutputStream, selector: Selector): Unit
}

/*
object Test {
  def z(r: Repository, key1: RepeatableProjectBuild, key2: ArtifactSha, key3: BuildArtifactsOut, data: Array[Byte]) = {
    // bring all the implicit Keys into scope
    import keys._

    // usage examples:
    val ak2 = r.put(data, key2)
    val am = r.get(ak2)
    val ak1 = r.put(key1, key1)
    val an = r.get(ak1)
    val ak3 = r.put(key1)
    val ao = r.get(ak3)
    val ak4 = r.put(key3, key1)
    val ap = r.get(ak4)

    // same examples, but let's also check that all the types are correct:
    val k2: GetRaw = r.put(data, key2)
    val m: Option[Array[Byte]] = r.get(k2)
    val k1: GetProject = r.put(key1, key1)
    val n: Option[RepeatableProjectBuild] = r.get(k1)
    val k3: GetProject = r.put(key1)
    val o: Option[RepeatableProjectBuild] = r.get(k3)
    val k4: GetArtifacts = r.put(key3, key1)
    val p: Option[BuildArtifactsOut] = r.get(k4)

    // and now, let's make some mistakes.
    //    val k5:GetArtifacts=r.put(key1, key3)
    // will tell you: could not find implicit value for parameter key: Key[RepeatableProjectBuild,BuildArtifactsOut,Get]
    // which is just perfect, as we accidentally swapped key and data
  }
}
*/

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
