package distributed.project.model
import java.io.InputStream

/**
 * A GetKey is a generic, but type-safe, way to access some data stored in a repository.
 * It is only ever be created by the put() call of Repository, and used by its get(),
 * but never created explicitly by any other user code.
 * Its content should be treated as opaque: just store it somewhere, and use it later to retrieve data.
 * 
 * Since GetKeys are in turn serialized/deserialized as part of metadata, their definition is in the
 * project d-metadata, although logically they belong next to the definition of Key and Repository.
 */
sealed abstract class GetKey[DataType] extends { def uuid: String }
/**
 * An access GetKey used to retrieve a raw artifact.
 */
case class GetRaw(uuid: String) extends GetKey[InputStream]
/**
 * A GetMeta is a kind of GetKey used to access JSON-serializable metadata.
 * We define streamToData() here for all such metadata classes; since the serialized
 * form is simple textual JSON, we can automatically filter it through a gzip compressor,
 * in order to save space.
 * Concrete subclasses of GetMeta are used for project descriptions, extraction metadata, etc.
 */
abstract class GetMeta[DataType] extends GetKey[DataType]
case class GetProject(uuid: String) extends GetMeta[RepeatableProjectBuild]
case class GetBuild(uuid: String) extends GetMeta[SavedConfiguration]
case class GetArtifacts(uuid: String) extends GetMeta[BuildArtifactsOut]
case class GetExtract(uuid: String) extends GetMeta[ExtractionConfig]

