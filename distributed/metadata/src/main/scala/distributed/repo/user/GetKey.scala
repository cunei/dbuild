package distributed.repo.user
import java.io.InputStream
import distributed.repo.core.GetKey
import distributed.project.model.BuildArtifactsOut
import distributed.project.model.ExtractedBuildMeta
import distributed.project.model.RepeatableProjectBuild
import distributed.project.model.SavedConfiguration

// The GetKeys should never be created directly, as their content (in theory) may change.
// Please use instead Repository.getKey(data), which will internally invoke the appropriate
// constructor, via the factories defined in RepoSections.scala.

/**
 * An access GetKey used to retrieve a raw artifact.
 */
case class GetRaw(private[repo] uuid: String) extends GetKey[InputStream]
/**
 * A GetMeta is a kind of GetKey used to access JSON-serializable metadata.
 * We define streamToData() here for all such metadata classes; since the serialized
 * form is simple textual JSON, we can automatically filter it through a gzip compressor,
 * in order to save space.
 * Concrete subclasses of GetMeta are used for project descriptions, extraction metadata, etc.
 */
abstract class GetMeta[DataType] extends GetKey[DataType]
case class GetProject(private[repo] uuid: String) extends GetMeta[RepeatableProjectBuild]
case class GetBuild(private[repo] uuid: String) extends GetMeta[SavedConfiguration]
case class GetExtract(private[repo] uuid: String) extends GetMeta[ExtractedBuildMeta]

// Note: this may also be:
//case class GetArtifacts(proj: GetProject) extends GetMeta[BuildArtifactsOut] {
//  def uuid=proj.uuid
//}
// More in general, it's going to be:
// - extract, publish all extractions data, collect their GetExtract
// - publish all projects, collect their GetProject
// - assemble the SavedConfiguration, save it storing the GetProjects
// and the GetExtract
// after building of each project:
// - publish the raw files, collect the GetRaw
// - publish the BuildArtifactsOut, including the GetRaws (in ArtifactShas)
//   using the key from GetProject to publish, obtaining a GetArtifacts (or not)
// The GetArtifacts get discarded; we can start again from the GetProject in order
// to generate new GetArtifacts as needed.
case class GetArtifacts(private[repo] uuid: String) extends GetMeta[BuildArtifactsOut]
