package distributed
package repo
package core

import project.model._
import java.io.File
import sbt.{ RichFile, IO, Path }
import Path._
import distributed.project.model.Utils.{ writeValue, readValue }
import distributed.project.model.{ ArtifactLocation, BuildArtifactsIn }
import logging.Logger
import java.io.InputStream
import sections._

object LocalRepoHelper {

  // We write SavedConfiguration right after extraction: GetBuild
  //
  // the RepeatableProjectBuild gets written
  // once all the project dependencies are ready and
  // we are ready to build for the first time: GetProject
  //
  // the Seq[BuildSubArtifactsOut] produced by a
  // build is written only after the build
  // completes successfully, wrapped into a
  // BuildArtifactsOut. The sha that is used
  // to find the data is NOT that of the BuildArtifactsOut,
  // but rather the one of the corresponding RepeatableProjectBuild: GetArtifacts

  /** Publishes the given repeatable build configuration to the repository. */
  def publishBuildMeta(saved: SavedConfiguration, remote: Repository, log: Logger): GetBuild =
    // TODO: use the returned GetKey!
    remote.put(saved)

  def readBuildMeta(g: GetBuild, remote: ReadableRepository): Option[SavedConfiguration] = remote.get(g)

  def makeArtifactRelative(file: File, localRepo: File) = {
    val name = IO.relativize(localRepo, file) getOrElse sys.error("Internal error while relativizing")
    ArtifactRelative(name)
  }

  // In case we already have an existing build in the cache, we might be interested in getting diagnostic
  // information on what is in there. That is done here.
  def debugArtifactsInfo(extracted: BuildArtifactsOut, log: Logger) = {
    log.debug("Published files:")
    extracted.results foreach {
      case BuildSubArtifactsOut(subproj, _, shas, _) =>
        if (subproj != "") log.debug("in subproject: " + subproj)
        shas foreach {
          case ArtifactSha(sha, location) =>
            log.debug(location)
        }
    }
  }

  /**
   * Publishes all files in the localRepo directory, according to the SHAs calculated
   * by the build system.
   */
  protected def publishRawArtifacts(localRepo: File, subproj: String, files: Seq[ArtifactSha], remote: Repository, log: Logger) = {
    if (subproj != "") log.debug("Checking files for subproject: " + subproj)
    files foreach {
      case artSha@ArtifactSha(sha, location) =>
        log.debug("Checking file: " + location)
        val artifactFile = localRepo / location
        // we need exactly an InputStream, rather than a FileInputStream, because of the implicit "key" parameter
        val stream:InputStream = new java.io.FileInputStream(artifactFile)
        val getKey = remote.put(stream, RawUUID(artifactFile))
    }
  }

  protected def publishArtifactsMetadata(meta: ProjectArtifactInfo, remote: Repository, log: Logger): Unit = {
    val getKey = remote.put(meta.versions, meta.project)
    log.debug("Published artifacts meta info for project " + meta.project.config.name + ", uuid" + getKey.uuid)
  }

  /**
   * Publishes the metadata for a project build.
   *
   * @param project  The repeatable project build, used to generate UUIDs and find dependencies.
   * @param remote  The repository to publish into.
   */
  def publishProjectInfo(project: RepeatableProjectBuild,
    remote: Repository, log: Logger): GetProject =
    // TODO: use the returned GetKey!
    remote.put(project)

  /**
   * Publishes the resulting artifacts metadata for a project build.
   *
   * @param project  The repeatable project build, used to generate UUIDs and find dependencies.
   * @param extracted The extracted artifacts that this project generates.
   * @param remote  The repository to publish into.
   */
  def publishArtifactsInfo(project: RepeatableProjectBuild, extracted: BuildArtifactsOut,
    localRepo: File, remote: Repository, log: Logger): ProjectArtifactInfo = {
    extracted.results foreach { case BuildSubArtifactsOut(subproj, _, shas, _) =>
      publishRawArtifacts(localRepo, subproj, shas, remote, log) }
    val info = ProjectArtifactInfo(project, extracted)
    publishArtifactsMetadata(info, remote, log)
    info
  }

  def materializeProjectMetadata(gp: GetProject, ga: GetArtifacts, remote: ReadableRepository): ProjectArtifactInfo = {
    val projectMeta = remote.get(gp) getOrElse sys.error("Unable to read the project info from repository")
    val artifactsMeta = remote.get(ga) getOrElse sys.error("Unable to read the artifacts info: this project may not have completed compilation or testing.")
    ProjectArtifactInfo(projectMeta, artifactsMeta)
  }

  /**
   * This utility case class is used to return information that was rematerialized via resolveArtifacts or
   * resolvePartialArtifacts, or getProjectInfo. It is never serialized.
   */
  case class ResolutionResult[T](projectInfo: ProjectArtifactInfo, results: Seq[T],
      filteredArts: Seq[ArtifactLocation], filteredModuleInfos: Seq[com.typesafe.reactiveplatform.manifest.ModuleInfo])
  /**
   * This method takes in a project UUID, a repository and a function that operates on every
   * Artifact that the project has in the repository.  It returns the project metadata and a sequence of
   * results of the operation run against each artifact in the repository.
   */
  protected def resolveArtifacts[T](uuid: String,
    remote: ReadableRepository): ((File, ArtifactSha) => T) => ResolutionResult[T] =
    resolvePartialArtifacts(uuid, Seq.empty, remote)

  /**
   * As for resolveArtifacts, but only for a list of subprojects. If the list is empty, grab all the files.
   * Also return the list of artifacts corresponding to the selected subprojects.
   * Note that, upon return, "results" and "artifacts" contain only the items selected according to the
   * "subprojs" list of subprojects; however, "metadata" contain the *full* project description, which
   * includes the full list of modules, and the full list of ArtifactLocations.
   */
  protected def resolvePartialArtifacts[T](gp: GetProject, ga: GetArtifacts, subprojs: Seq[String], remote: ReadableRepository)(f: (File, ArtifactSha) => T): ResolutionResult[T] = {
    val metadata =
      materializeProjectMetadata(gp, ga, remote)
    val fetch = if (subprojs.isEmpty) metadata.versions.results.map { _.subName } else {
      val unknown = subprojs.diff(metadata.versions.results.map { _.subName })
      if (unknown.nonEmpty) {
        sys.error(unknown.mkString("The following subprojects are unknown: ", ", ", ""))
      }
      subprojs
    }
    val artifactFiles = metadata.versions.results.filter { v => fetch.contains(v.subName) }.flatMap { _.shas }
    val results = for {
      artifactFile <- artifactFiles
      resolved = remote.get(artifactFile.sha)
    } yield f(resolved, artifactFile)

    // TODO: artifacts should be associated with (be contained into) each ModuleInfo. Right now
    // the list of ModuleInfos is only used while generating the index in DeployBuild, while
    // artifacts are used everywhere else, hence the two need not be aligned in any manner.
    val artifacts = metadata.versions.results.filter { v => fetch.contains(v.subName) }.flatMap { _.artifacts }
    val moduleInfos = metadata.versions.results.filter { v => fetch.contains(v.subName) } map {_.moduleInfo}
    ResolutionResult[T](metadata, results, artifacts, moduleInfos)
  }

  /**
   * Materialize the artifacts for a given project UUID.
   * Does *not* pull down transitive dependencies.
   *
   *   @param uuid  The id of the project to materialize
   *   @param remote  The repository to pull artifacts from.
   *   @param localRepo  The location to store artifacts read from the repository.
   *   @return The list of *versioned* artifacts that are now in the local repo,
   *   plus a log message as a sequence of strings.
   */
  def materializeProjectRepository(uuid: String, remote: ReadableRepository, localRepo: File, debug: Boolean): (Seq[ArtifactLocation], Seq[com.typesafe.reactiveplatform.manifest.ModuleInfo], Seq[String]) =
    materializePartialProjectRepository(uuid, Seq.empty, remote, localRepo, debug)

  /* Materialize only parts of a given projects, and specifically
   * those specified by the given subproject list. If the list is empty, grab everything.
   */
  def materializePartialProjectRepository(uuid: String, subprojs: Seq[String], remote: ReadableRepository,
    localRepo: File, debug: Boolean): (Seq[ArtifactLocation], Seq[com.typesafe.reactiveplatform.manifest.ModuleInfo], Seq[String]) = {
    val ResolutionResult(meta, _, arts, modInfos) = resolvePartialArtifacts(uuid, subprojs, remote) { (resolved, artifact) =>
      val file = new File(localRepo, artifact.location)
      IO.copyFile(resolved, file, false)
    }
    val space = meta.project.config.space getOrElse
      sys.error("Internal error: space is None in " + meta.project.config.name + " while rematerializing artifacts.")
    val spaceInfo = if (debug) space.to.length match {
      case 0 => sys.error("Internal error: rematerializing artifacts from project published in no spaces: " + meta.project.config.name)
      case 1 => ", published to space: " + space.to.head
      case _ => space.to.mkString(", published to spaces: ", ",", "")
    } else ""
    val fragment = " (commit: " + (meta.project.getCommit getOrElse "none") + spaceInfo + ")"
    val info1 = "Retrieved from project " +
      meta.project.config.name + fragment
    val info2 = ": " + arts.length + " artifacts"
    val msg = if (subprojs.isEmpty) Seq(info1 + info2) else
      Seq(subprojs.mkString(info1 + ", subprojects ", ", ", info2))
    // TODO: eventually, artifactLocations will be embedded as part of ModuleInfo
    (arts, modInfos, msg)
  }

  // rematerialize artifacts. "uuid" is a sequence: each element represents group of artifacts that
  // needs to be rematerialized into a separate directory, each for a separate level of the build
  // Returns for each group the list of rematerialized artifacts
  def getArtifactsFromUUIDs(diagnostic: ( => String) => Unit, repo: Repository, localRepos: Seq /*Levels*/ [File],
    uuidGroups: Seq /*Levels*/ [Seq[String]], fromSpaces: Seq /*Levels*/ [String], debug: Boolean): BuildArtifactsInMulti =
    BuildArtifactsInMulti(
      ((uuidGroups, fromSpaces, localRepos).zipped.toSeq.zipWithIndex) map {
        case ((uuids, fromSpace, localRepo), index) =>
          if (uuidGroups.length > 1 && uuids.length > 0) diagnostic("Resolving artifacts, level " + index + ", space: " + fromSpace)
          val artifacts = for {
            uuid <- uuids
            (arts, modInfos, msg) = LocalRepoHelper.materializeProjectRepository(uuid, repo, localRepo, debug)
            _ = msg foreach { diagnostic(_) }
            art <- arts
          } yield art
          BuildArtifactsIn(artifacts, fromSpace, localRepo)
      })

  def getProjectInfo(uuid: String, remote: ReadableRepository) =
    resolveArtifacts(uuid, remote)((x, y) => x -> y)

  /** Checks whether or not a given project (by UUID) is published. */
  def getPublishedDeps(uuid: String, remote: ReadableRepository, log: Logger): BuildArtifactsOut = {
    // We run this to ensure all artifacts are resolved correctly.
    val ResolutionResult(meta, results, _, _) = resolveArtifacts(uuid, remote) { (file, artifact) => () }
    log.info("Found cached project build, uuid " + uuid)
    meta.versions
  }

}
