package distributed
package repo
package core

import java.io.File
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
      // As an alternative, one could conceivably use Piped streams, but
      // they are rather quirky and best avoided. All our artifacts should easily
      // fit in array buffers anyway.
      val bos = new java.io.ByteArrayOutputStream()
      val zip = new GZIPOutputStream(bos)
      zip.write(asBytes)
      zip.close()
      new java.io.ByteArrayInputStream(bos.toByteArray())
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

// in client code use:
// import distributed.repo.core.Repository
// import distributed.repo.core.sections._

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
