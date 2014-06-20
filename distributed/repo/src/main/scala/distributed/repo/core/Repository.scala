package distributed
package repo
package core

import java.io.File
import distributed.project.model.Utils.{ testSectionName, testIndexName }
import java.util.Date
import org.apache.commons.io.IOUtils
import sbt.Path._
import java.io.InputStream
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.BufferedInputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import project.model._
import Utils.{ readValue, writeValue }

/*
 * This implementation of repositories operates on two levels. Internally, all concrete
 * implementations of repositories need only implement unstructured data access:
 * they accept an InputStream and store its content under an "access key" (GetKey). Also,
 * they can retrieve existing content stored under a certain key, and return it again
 * as an InputStream.
 * 
 * Externally, at the user level, access is made using a put() and a get() methods,
 * which strictly check the arguments and automatically generate correct keys depending
 * on the kind of data supplied. Since some kinds of data can only be stored and retrieved
 * using a uuid that originates from a different kind of data, the
 * type-safe put() will only compile when the supplied combination of types is correct.
 * The result of a put() is a type-specific abstract access key (a GetKey), which can
 * be then used in further get() calls, which again will return only data of the correct kind.
 * 
 * In summary, the higher-level and type-safe public interface of a Repository works as:
 * put() -> returns a GetKey -> get(getKey)
 * 
 * The internal (hidden) lower-level, type-unsafe level of a Repository works by converting
 * the GetKey into a Selector using the information contained into a Key (see below) and
 * using the unstructured calls: store(data,selector) / fetch(selector)
 * 
 * A low-level selector is a combination of a "section" and an "index". Each section designates
 * an independent set of data, each item of which can be accessed using an index.
 * The same index used in different sections refers to two different pieces of data.
 */

////////////////////
/*
 * A GetKey is a generic, but type-safe, way to access some data stored in a repository.
 * It is only ever be created by the put() call of Repository, and used by its get(),
 * but never created explicitly by any other user code.
 * Its content should be treated as opaque: just store it somewhere, and use it later to retrieve data.
 * 
 * Since GetKeys are in turn serialized/deserialized as part of metadata, their definition is in the
 * metadata project, although logically they belong next to the definition of Key and Repository.
 */

// sealed abstract class GetKey[DataType] extends { def uuid: String }

/**
 * A Section structure contains the logic needed to interface the higher, type-safe
 * level of repositories with its lower, type-unsafe implementation.
 *
 * To access data, a combination of the kind of a GetKey, plus its uuid, is turned into an
 * internal "Selector" by the Section instance. Such a Selector is then used at the lower level of a
 * Repository implementation in order to represent an untyped, general data access path.
 *
 * The lower level of a Repository only work on Selectors and raw data, and have no concept
 * of the specific kind of data that is being written or retrieved.
 *
 * Note that not all of the kinds of data are placed in the repository under their own
 * uuid: in some cases (raw files for instance, or BuildArtifactsOuts), the uuid depends
 * on a related piece of data of a different kind. That is represented by KeySource, which
 * must be able to provide the needed uuid.
 */
/* In theory, all Section methods should be private to Repository, or at least to the enclosing package.
 * The Sections themselves need to be public, as they are used as implicit parameters by put() and get().
 */
sealed abstract class Section[DataType, KeySource <: { def uuid: String }, Get <: GetKey[DataType]] {
  def newGet(uuid: String): Get
  def dataToStream(d: DataType)(implicit m: Manifest[DataType]): InputStream
  def streamToData(is: InputStream)(implicit m: Manifest[DataType]): DataType
  /*
 * Selector-related utils
 */
  /**
   * From higher level (typed) to lower level (untyped)
   */
  def selector(get: GetKey[DataType]) = Selector(name, get.uuid)
  /**
   * Given a low-level Selector to an element, recreates the
   * corresponding high-level GetKey. This call is used in scan(),
   * in order to generate the list of GetKeys, given the list of
   * low-level Selectors.
   */
  def getFromSelector(selector: Selector) = newGet(selector.index)
  /**
   * The name of the section. It is also used when calling scan() (the low-level
   * equivalent to enumerate() in the implementation of a Repository.
   */
  def name: String
}

/**
 * In order to get a sha for a raw file/inputstream that contains an artifact,
 * wrap it into a RawUUID. This class is not serialized.
 */
case class RawUUID(f: File) {
  def uuid = hashing.files sha1 f
}

/**
 * Use "import sections._" in client code, to bring the implicit repository Keys into scope.
 */
package object sections {
  implicit object RawSection extends Section[InputStream, RawUUID, GetRaw] {
    val name = "artifactFiles"
    def newGet(uuid: String) = GetRaw(uuid)
    def dataToStream(d: InputStream)(implicit m: Manifest[InputStream]): InputStream = d
    def streamToData(is: InputStream)(implicit m: Manifest[InputStream]) = is
  }
  /**
   * MetaSection groups the sections used to access JSON-serializable metadata.
   * We define streamToData() and dataToStream here for all such metadata classes; since the serialized
   * form is simple textual JSON, we can automatically filter it through a gzip compressor,
   * in order to save space.
   */
  private[sections] sealed abstract class MetaSection[DataType, KeySource <: { def uuid: String }, Get <: GetKey[DataType]]
    extends Section[DataType, KeySource, Get] {
    def streamToData(is: InputStream)(implicit m: Manifest[DataType]): DataType =
      readValue[DataType](new GZIPInputStream(new BufferedInputStream(is))) // GZIPInputStream will decompress
    def dataToStream(d: DataType)(implicit m: Manifest[DataType]): InputStream = {
      val asString = writeValue(d)
      val asBytes = asString.getBytes()
      val pipeOut = new java.io.PipedOutputStream()
      val zip = new GZIPOutputStream(pipeOut)
      zip.write(asBytes)
      zip.close()
      new java.io.PipedInputStream(pipeOut)
      /*
      // Alternative, without pipes but using a further byte array.
      // Might be faster, since most metadata chunks are small.
      val bos = new java.io.ByteArrayOutputStream()
      val zip = new GZIPOutputStream(bos)
      zip.write(asBytes)
      zip.close()
      new java.io.ByteArrayInputStream(bos.toByteArray())
*/
    }
  }
  implicit object ProjectSection extends MetaSection[RepeatableProjectBuild, RepeatableProjectBuild, GetProject] {
    val name = "projects"
    def newGet(uuid: String) = GetProject(uuid)
  }
  implicit object BuildSection extends MetaSection[SavedConfiguration, SavedConfiguration, GetBuild] {
    val name = "fullBuilds"
    def newGet(uuid: String) = GetBuild(uuid)
  }
  implicit object ArtifactsSection extends MetaSection[BuildArtifactsOut, RepeatableProjectBuild, GetArtifacts] {
    val name = "artifactsData"
    def newGet(uuid: String) = GetArtifacts(uuid)
  }
  implicit object ExtractSection extends MetaSection[ExtractedBuildMeta, ExtractionConfig, GetExtract] {
    val name = "extractions"
    def newGet(uuid: String) = GetExtract(uuid)
  }
}

/**
 * A Selector is a unique reference to an actual piece of data saved to a repository (at the lower,
 * type-unsafe conceptual level). Selectors (and Sections) are never serialized/deserialized.
 */
private[core] case class Selector(section: String, index: String) {
  testSectionName(section)
  testIndexName(index)
}
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
  /**
   * Use get(getKey) to retrieve data from the repository.
   * If you do not have a GetKey, but you have an instance of a KeySource, you can use that to retrieve your
   * data as well. In that case, you may have to supply the DataType type parameter explicitly if the
   * KeySource type you are using is used to access different DataTypes.
   *
   * If DataType you received is an InputStream, you should close it once you are done with it.
   */
  /*
   * Implementation trick: getKey presents itself as a one-argument function that is parametric on a single
   * type parameter. In reality, get returns an instance of a class whose apply() will apply the single
   * parameter, returning the needed data. The advantage is that the type parameters KeySource and
   * Get are always inferred automatically, and it is only necessary to specify (when needed) the single type
   * parameter for DataType.
   */
  final def get[DataType] = new {
    def apply[KeySource <: { def uuid: String }, Get <: GetKey[DataType]](source: KeySource)(implicit section: Section[DataType, KeySource, Get], ms: Manifest[KeySource], m: Manifest[DataType]): Option[DataType] =
      apply(Repository.getKey(source)(section, ms))(section, m)
    def apply[KeySource <: { def uuid: String }, Get <: GetKey[DataType]](g: GetKey[DataType])(implicit section: Section[DataType, _, _], m: Manifest[DataType]): Option[DataType] = {
      lock
      try {
        fetch(section.selector(g)) map { section.streamToData(_)(m) }
      } finally { unlock }
    }
  }
  /** see getKey in the companion object. */
  final /*private[core]*/ def getKey[DataType] = Repository.getKey[DataType]
  /**
   * Retrieves the space concretely taken in the repository to store
   * the data indexed by this key. Returns zero if key not present.
   */
  final def getSize[DataType](g: GetKey[DataType])(implicit section: Section[DataType, _, _], m: Manifest[DataType]): Long = {
    lock
    try {
      size(section.selector(g))
    } finally { unlock }
  }
  /**
   * Get the list of keys for items of this DataType currently in the repo. You will need to provide
   * the DataType type parameter explicitly, as there are no regular arguments to infer it from.
   * The form for the invocation is: "enumerate[Type]()". It needs an empty argument list at the end
   * for the type inference magic to work.
   */
  final def enumerate[DataType] = new {
    def apply[KeySource <: { def uuid: String }, Get <: GetKey[DataType]]()(implicit section: Section[DataType, KeySource, Get]): Seq[Get] = {
      lock
      try {
        scan(section.name) map section.getFromSelector
      } finally { unlock }
    }
  }

  /*
   * To create a concrete implementation of a Repository, just implement the low-level primitives
   * listed below. fetch(), size() and scan(), etc. need not be thread-safe: lock and unlock are
   * called to lock the repository when needed.
   */

  /**
   * Read from the repository the data stored at Selector. If no data is present, return None.
   */
  protected def fetch(selector: Selector): Option[InputStream]
  /**
   * Read from the repository the timestamp when the data stored at Selector was last written.
   * If no data is present, return None.
   * Note that there is no corresponding high-level call: date() and delete() will only be
   * used inside a lock/unlock section from a GC operation (whose details we will elaborate later).
   *
   * Currently unused.
   */
  // Note: should we split atime and ctime? For a proxy, atime might be useful.
  protected def date(selector: Selector): Option[Date]
  /**
   * Return, if known, the actual space occupied in the Repository by the data stored
   * under Selector. If no data, return zero.
   */
  protected def size(selector: Selector): Long
  /**
   * Read from the repository the data stored at Selector. If no data is present, return None.
   */
  protected def hasData(selector: Selector): Boolean
  /**
   * scan() checks the given Section, and returns all of its Selectors
   * for which data exist in the repository. If none exist, returns
   * an empty Seq.
   */
  protected def scan(section: String): Seq[Selector]
  /**
   * lock and unlock are used to protect against concurrent accesses to the repository,
   * and must protect the repository content even if multiple independent processes
   * are trying to access this repository at the same time.
   *
   * An attempt to lock an already locked repository should result in a (possibly indefinitely
   * long) wait for the repo to be unlocked. Optionally, the lock can fail after a repository-
   * defined timeout, throwing a RepoLockException. If you opt to do that, select a rather long
   * timeout (for instance, thirty minutes).
   * An attempt to unlock an unlocked repository should result in a RepoLockException exception.
   *
   * The lock must /NOT/ be reentrant: an attempt to double-lock by the same thread must result
   * in a RepoLockException.
   */
  protected def lock: Unit
  protected def unlock: Unit
}
class RepoLockException(msg: String) extends Exception(msg)

/**
 * Abstract class for a readable/writeable repository of data, indexed by GetKeys.
 * The high-level, type-safe interface (get/put) is also safe to use from
 * multiple processes simultaneously.
 *
 * To implement a concrete subclass of a Repository, implement
 * the low-level primitives of ReadableRepository, plus store() and
 * possibly delete().
 */
abstract class Repository extends ReadableRepository {
  /**
   * Puts some contents into the given key.
   * The data of type DataType will be placed into the repository
   * under a key decided by the uuid of keySource, which may be
   * different than the main data. For instance, a BuildArtifactsOut
   * is always saved under the key corresponding to the matching
   * RepeatableProjectBuild.
   * If "overwrite" is false, then an attempt to write data that should
   * already be in will result in the data being verified against what
   * is already there. If "overwrite" is true, then overwrite regardless.
   */
  final def put[DataType, KeySource <: { def uuid: String }, Get <: GetKey[DataType]](data: DataType,
    keySource: KeySource, overwrite: Boolean = false)(implicit section: Section[DataType, KeySource, Get],
      m: Manifest[DataType] /*, log:distributed.logging.Logger = distributed.logging.NullLogger */ ): Get = {
    lock
    try {
      val uuid = keySource.uuid
      val get = section.newGet(uuid)
      // short circuit evaluation, hence hasData is called only if not overwrite
      if (!overwrite && hasData(section.selector(get))) {
        val previous = fetch(section.selector(get)).get
        // compare the two InputStreams
        val same = IOUtils.contentEquals(section.dataToStream(data), previous)
        if (!same) sys.error("Internal Repository Error: data already in the repository does not match. Please report")
      }
      val out = section.dataToStream(data)
      store(out, section.selector(get))
      out.close() // if store has already closed, this does nothing.
      get
    } finally { unlock }
  }

  // TODO: Add logging, following this scheme (more or less):
  /*
  {
    IO.withTemporaryFile("meta-data", data.uuid) { file =>
      val key = makeKey(data.uuid)
      // is the file already there? We might try to publish twice, as a previous run
      // failed. If so, let's check that what is there matches what we have (as a sanity
      // check) and print a message.
      val f: InputStream = try {
        remote get key
        // if we are here the file exists, so we either match, or it's an error.
        // We might also fail to deserialize, though. We continue after the try.
      } catch {
        case e =>
          // the meta doesn't exist in the repo, or other I/O error (wrong privs, for instance).
          // we try to write, hoping we succeed.
          log.debug("While reading from repo: " + e.getMessage)
          val s = writeValue(data)
          remote put (key, s.getBytes)
          log.info("Written " + data.getClass.getSimpleName + " metadata: " + key)
          // all ok
          return
      }
      val existingBuild: T = try {
        readValue[T](f)
        // deserialized ok. We continue after the try
      } catch {
        case e =>
          // failed to deserialize. Should be impossible.
          log.error("The data already present in the dbuild repository for this data (uuid = " + data.uuid + ")")
          log.error("does not seem to be a valid " + data.getClass.getSimpleName + ". This shouldn't happen! Please report.")
          log.error("Key: " + key)
          throw new Exception("Repository consistency check failed", e)
      }
      if (existingBuild == data) {
        log.info("The " + data.getClass.getSimpleName + " metadata (uuid " + data.uuid + ") is already in the repository.")
      } else {
        log.error("The data already present in the dbuild repository for this data (uuid = " + data.uuid + ")")
        log.error("does not match the current metadata. This shouldn't happen! Please report.")
        log.error("Key: " + key)
        throw new Exception("Repository consistency check failed")
      }
    }
*/

  /**
   * In case the uuid source and the saved data coincide, a plain single-argument "put(data)" can be used instead.
   */
  final def put[DataType <: { def uuid: String }, Get <: GetKey[DataType]](data: DataType)(implicit section: Section[DataType, DataType, Get], m: Manifest[DataType]): Get =
    put[DataType, DataType, Get](data, data)

  // the internal, low-level, non-type-safe equivalent to put()
  /**
   * store() does not need to be thread-safe: lock and unlock are
   * used to lock the repository when needed.
   * Grab the data from the InputStream and place it in the repository.
   * If some data already exists at that selector, overwrite it.
   * When the data is written, also create/update an associated
   * timestamp: it will be accessible as an instance of java.util.Date
   * using date(selector).
   */
  protected def store(out: InputStream, selector: Selector): Unit
  /**
   * delete() removes the data at this selector, if any.
   * If no data is present, do nothing; if you need to check whether
   * any data exists, use hasData() beforehand. If this repository
   * is a cache or proxy for some other repository, do not delete anything
   * in the original.
   * This low-level method should be implemented if possible, or it should
   * do nothing if unsupported: it will be used to implement garbage collection
   * for repositories at some point in the future. Considering the potential for
   * race conditions, there is no corresponding high-level "delete()": this
   * primitive will typically only be called as part of a more complicated transaction,
   * in order to preserve the repository integrity.
   *
   * Returns true if the file was deleted, false otherwise.
   */
  // TODO: do we need this? Or is it just unnecessary added complexity, at this time?
  protected def delete(selector: Selector): Boolean
}

// in client code use:
// import distributed.repo.core
// import distributed.repo.core.keys._

// This is technically dead code, but it is useful to leave it in an compile it with
// the rest, in order to make sure that all its test calls compile successfully.
// Please leave this code where it is.
private object RepositoryCompilationTest {
  def z(r: Repository, key1: RepeatableProjectBuild, key2: ArtifactSha, key3: BuildArtifactsOut, data: InputStream, f: File) = {
    // bring all the implicit Sections into scope
    import sections._
    val rawuuid = RawUUID(f)

    // usage examples:

    val ak2 = r.put(data, rawuuid)
    val am = r.get(ak2)
    val amk2 = r.get(rawuuid)
    val amk = r.get(r.getKey(rawuuid))
    val ak1 = r.put(key1, key1)
    val an = r.get(ak1)
    val ank2 = r.get[RepeatableProjectBuild](key1)
    val ank = r.get(r.getKey[RepeatableProjectBuild](key1))
    val ak3 = r.put(key1)
    val ao = r.get(ak3)
    val ak4 = r.put(key3, key1)
    val ap = r.get(ak4)
    val apk2 = r.get[BuildArtifactsOut](key1)
    val apk = r.get(r.getKey[BuildArtifactsOut](key1))
    val al = r.enumerate[BuildArtifactsOut]()

    // same examples, but let's also check that all the types are correct:

    val k2: GetRaw = r.put(data, rawuuid)
    val m: Option[InputStream] = r.get(k2)
    val mk2: Option[InputStream] = r.get(rawuuid)
    val mk: Option[InputStream] = r.get(r.getKey(rawuuid))
    val k1: GetProject = r.put(key1, key1)
    val n: Option[RepeatableProjectBuild] = r.get(k1)
    val nk2: Option[RepeatableProjectBuild] = r.get[RepeatableProjectBuild](key1)
    val nk: Option[RepeatableProjectBuild] = r.get(r.getKey[RepeatableProjectBuild](key1))
    val k3: GetProject = r.put(key1)
    val o: Option[RepeatableProjectBuild] = r.get(k3)
    val k4: GetArtifacts = r.put(key3, key1)
    val p: Option[BuildArtifactsOut] = r.get(k4)
    val pk2: Option[BuildArtifactsOut] = r.get[BuildArtifactsOut](key1)
    val pk: Option[BuildArtifactsOut] = r.get(r.getKey[BuildArtifactsOut](key1))
    val l: Seq[GetArtifacts] = r.enumerate[BuildArtifactsOut]()

    // and now, let's make some mistakes.
    //    val k5:GetArtifacts=r.put(key1, key3)
    // This should tell you: could not find implicit value for parameter section: Section[RepeatableProjectBuild,BuildArtifactsOut,Get]
    // which is just perfect, as we accidentally swapped key and data
  }
}

object Repository {

  /**
   * If needed, you can speculatively obtain a GetKey directly from a given KeySource, without storing any
   * data. That is not recommended, as storing somewhere a GetKey that has no associated data in the
   * repository is the equivalent of creating a dangling reference. Use the GetKeys returned by a put(), instead.
   * If the same KeySource type is used for multiple DataTypes, you may have to supply the DataType type parameter explicitly.
   * Note: this should ideally be a private[core] def, once all the code that uses it has been properly refactored.
   */
  final /*private[core]*/ def getKey[DataType] = new {
    def apply[KeySource <: { def uuid: String }, Get <: GetKey[DataType]](source: KeySource)(implicit section: Section[DataType, KeySource, Get], m: Manifest[KeySource]): Get =
      section.newGet(source.uuid)
  }

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
