package distributed.project.model
import java.io.InputStream
import java.io.File

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
case class GetExtract(uuid: String) extends GetMeta[ExtractedBuildMeta]

// Note: this may have to become:
//case class GetArtifacts(proj: GetProject) extends GetMeta[BuildArtifactsOut] {
//  def uuid=proj.uuid
//}
// and the ArtifactKey may have to be adapted accordingly.
// More in general, it's going to be:
// - extract, publish all extractions data, collect their GetExtract
// - publish all projects, collect their GetProject
// - assemble the SavedConfiguration, save it storing the GetProjects
// and the GetExtract
// after building of each project:
// - publish the raw files, collect the GetRaw
// - publish the BuildArtifactsOut, including the GetRaws (in ArtifactShas)
//   using the key from GetProject to publish, obtaining a GetArtifacts (or not)
// The GetArtifactss get discarded; we can start again from the GetProject in order
// to generate a new GetArtifacts as needed.
// That seems appropriate. I might come up with a different abstraction though, as
// this publishing of GetArtifacts is not entirely satisfactory.
// Not sure I am missing anything else, though.
case class GetArtifacts(uuid: String) extends GetMeta[BuildArtifactsOut]
