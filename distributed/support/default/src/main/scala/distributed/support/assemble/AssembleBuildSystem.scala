package distributed
package support
package assemble

import project.model._
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.FileUtils
import _root_.java.io.File
import _root_.sbt.Path._
import _root_.sbt.IO
import _root_.sbt.IO.relativize
import logging.Logger
import sys.process._
import distributed.repo.core.LocalRepoHelper
import distributed.project.model.Utils.{ writeValue, readValue }
import distributed.project.dependencies.Extractor
import distributed.project.build.LocalBuildRunner
import collection.JavaConverters._
import org.apache.maven.model.{ Model, Dependency }
import org.apache.maven.model.io.xpp3.{ MavenXpp3Reader, MavenXpp3Writer }
import org.apache.maven.model.Dependency
import org.apache.ivy.util.ChecksumHelper
import distributed.support.NameFixer.fixName
import _root_.sbt.NameFilter

/**
 * The group build system accepts a list of nested projects, with the same format
 * as the "build" section of a normal dbuild configuration file.
 * All of those nested projects will be built *independently*, meaning that they
 * will not use one the artifacts of the others. At the end, when all of the
 * projects are built, the "group" build system will collect all of the artifacts
 * generated by the nested projects, and patch their "pom" files by modifying their
 * dependencies, so that they refer to one another. The net result is that they
 * will all appear to have been originating from a single dbuild project.
 * This mechanism works only for Maven-based artifacts; Ivy artifacts will not
 * be adapted.
 */
object AssembleBuildSystem extends BuildSystemCore {
  val name: String = "assemble"

  private def assembleExpandConfig(config: ProjectBuildConfig) = config.extra match {
    case None => AssembleExtraConfig(None) // pick default values
    case Some(ec: AssembleExtraConfig) => ec
    case _ => throw new Exception("Internal error: Assemble build config options are the wrong type in project \"" + config.name + "\". Please report")
  }

  private def projectsDir(base: File, config: ProjectBuildConfig) = {
    // don't use the entire nested project config, as it changes after resolution (for submodules)
    // also, avoid using the name as-is as the last path component (it might confuse the dbuild's heuristic
    // used to determine sbt's default project names, see dbuild's issue #66)
    val uuid = hashing sha1 config.name
    base / "projects" / uuid
  }

  // overriding resolve, as we need to resolve its nested projects as well
  override def resolve(config: ProjectBuildConfig, dir: File, extractor: Extractor, log: Logger): ProjectBuildConfig = {
    if (!config.uri.startsWith("nil:"))
      sys.error("Fatal: the uri in Assemble " + config.name + " must start with the string \"nil:\"")
    // resolve the main URI (which will do nothing, but we may have
   // some debugging diagnostic, so let's call it anyway)
    val rootResolved = super.resolve(config, dir, extractor, log)
   // and then the nested projects (if any)
    val newExtra = rootResolved.extra match {
      case None => None
      case Some(extra: AssembleExtraConfig) =>
        val newParts = extra.parts map { buildConfig =>
          val nestedResolvedProjects =
            buildConfig.projects.foldLeft(Seq[ProjectBuildConfig]()) { (s, p) =>
              log.info("----------")
              log.info("Resolving module: " + p.name)
              val nestedExtractionConfig = ExtractionConfig(p, buildConfig.options getOrElse BuildOptions())
              val moduleConfig = extractor.dependencyExtractor.resolve(nestedExtractionConfig.buildConfig, projectsDir(dir, p), extractor, log)
              s :+ moduleConfig
            }
          DistributedBuildConfig(nestedResolvedProjects, buildConfig.options)
        }
        Some(extra.copy(parts = newParts))
      case _ => throw new Exception("Internal error: Assemble build config options are the wrong type in project \"" + config.name + "\". Please report")
    }
    rootResolved.copy(extra = newExtra)
  }

  def extractDependencies(config: ExtractionConfig, dir: File, extractor: Extractor, log: Logger): ExtractedBuildMeta = {
    val ec = assembleExpandConfig(config.buildConfig)

    // we have no root sources in the "assemble" project per se; therefore,
    // we need to have an explicit "set-version" in order to know what the
    // resulting version number should be.
    val assembleVersion = config.buildConfig.setVersion getOrElse
      sys.error("A \"set-version\" clause is required for the Assemble build system.")

    // we consider the names of parts in the same way as subprojects, allowing for a
    // partial deploy, etc.
    val subProjects = ec.parts.toSeq.flatMap(_.projects).map(_.name)
    if (subProjects != subProjects.distinct) {
      sys.error(subProjects.diff(subProjects.distinct).distinct.mkString("These subproject names appear twice: ", ", ", ""))
    }
    val partOutcomes = ec.parts.toSeq flatMap { buildConfig =>
      buildConfig.projects map { p =>
        log.info("----------")
        val nestedExtractionConfig = ExtractionConfig(p, buildConfig.options getOrElse BuildOptions())
        extractor.extractedResolvedWithCache(nestedExtractionConfig, projectsDir(dir, p), log)
      }
    }
    if (partOutcomes.exists(_.isInstanceOf[ExtractionFailed])) {
      sys.error(partOutcomes.filter { _.isInstanceOf[ExtractionFailed] }.map { _.project }.mkString("failed: ", ", ", ""))
    }
    val partsOK = partOutcomes.collect({ case e: ExtractionOK => e })
    val allConfigAndExtracted = (partsOK flatMap { _.pces })

    // time to do some more checking:
    // - do we have a duplication in provided artifacts?
    // let's start building a sequence of all modules, with the name of the subproject they come from
    val artiSeq = allConfigAndExtracted.flatMap { pce => pce.extracted.projects.map(art => ((art.organization + "#" + art.name), pce.config.name)) }
    log.info(artiSeq.toString)
    // group by module ID, and check for duplications
    val artiMap = artiSeq.groupBy(_._1)
    log.info(artiMap.toString)
    val duplicates = artiMap.filter(_._2.size > 1)
    if (duplicates.nonEmpty) {
      duplicates.foreach { z =>
        log.error(z._2.map(_._2).mkString(z._1 + " is provided by: ", ", ", ""))
      }
      sys.error("Duplicate artifacts found in project")
    }

    // ok, now we just have to merge everything together.
    val newMeta = ExtractedBuildMeta(assembleVersion, allConfigAndExtracted.flatMap(_.extracted.projects),
      partOutcomes.map { _.project })
    log.info(newMeta.subproj.mkString("These subprojects will be built: ", ", ", ""))
    newMeta
  }

  // runBuild() is called with the (empty) root source resolved, but the parts have not been checked out yet.
  // Therefore, we will call localBuildRunner.checkCacheThenBuild() on each part,
  // which will in turn resolve it and then build it (if not already in cache).
  def runBuild(project: RepeatableProjectBuild, dir: File, input: BuildInput, localBuildRunner: LocalBuildRunner, log: logging.Logger): BuildArtifactsOut = {
    val ec = assembleExpandConfig(project.config)
    val version = input.version

    log.info(ec.parts.toSeq.flatMap(_.projects).map(_.name).mkString("These subprojects will be built: ", ", ", ""))

    val localRepo = input.outRepo
    // We do a bunch of in-place file operations in the localRepo, before returning.
    // To avoid problems due to stale files, delete all contents before proceeding.
    IO.delete(localRepo.*("*").get)

    // initial part of the artifacts dir, including only the organization
    def orgDir(repoDir: File, organization: String) =
      organization.split('.').foldLeft(repoDir)(_ / _)
    def artifactDir(repoDir: File, ref: ProjectRef, crossSuffix: String) =
      orgDir(repoDir, ref.organization) / (ref.name + crossSuffix)

    // Since this is a real local maven repo, it also contains
    // the "maven-metadata-local.xml" files, which should /not/ end up in the repository.
    //
    // Since we know the repository format, and the list of "subprojects", we grab
    // the files corresponding to each one of them right from the relevant subdirectory.
    // We then calculate the sha, and package each subproj's results as a BuildSubArtifactsOut.

    def scanFiles[Out](artifacts: Seq[ProjectRef], crossSuffix: String)(f: File => Out) = {
      // use the list of artifacts as a hint as to which directories should be looked up,
      // but actually scan the dirs rather than using the list of artifacts (there may be
      // additional files like checksums, for instance).
      artifacts.map(artifactDir(localRepo, _, crossSuffix)).distinct.flatMap { _.***.get }.
        filterNot(file => file.isDirectory || file.getName == "maven-metadata-local.xml").map(f)
    }

    def projSHAs(artifacts: Seq[ProjectRef], crossSuffix: String): Seq[ArtifactSha] = scanFiles(artifacts, crossSuffix) {
      LocalRepoHelper.makeArtifactSha(_, localRepo)
    }

    // OK, now build the parts, each separately from the others
    val (preCrossArtifactsMap, repeatableProjectBuilds) = (ec.parts.toSeq flatMap { build =>
      build.projects map { p =>
        // the modules are build ENTIRELY independently from one another. Their list
        // of dependencies is cleared before building, so that they do not rely on one another

        // first, we need to build a RepeatableProjectBuild. In order to do that, we need
        // again the ExtractedBuildMeta, but we no longer have it (it was dissolved into the
        // main one). So, we extract again (it is cached at this point, anyway).

        log.info("----------")
        log.info("Building part: " + p.name)
        val nestedExtractionConfig = ExtractionConfig(p, build.options getOrElse BuildOptions())
        val partConfigAndExtracted = localBuildRunner.extractor.cachedExtractOr(nestedExtractionConfig, log) {
          // if it's not cached, something wrong happened.
          sys.error("Internal error: extraction metadata not found for part " + p.name)
        } match {
          case outcome: ExtractionOK => outcome.pces.headOption getOrElse
            sys.error("Internal error: PCES empty after cachedExtractOr(); please report")
          case _ => sys.error("Internal error: cachedExtractOr() returned incorrect outcome; please report.")
        }
        val repeatableProjectBuild = RepeatableProjectBuild(partConfigAndExtracted.config, partConfigAndExtracted.extracted.version,
          Seq.empty, // remove all dependencies, and pretend this project stands alone
          partConfigAndExtracted.extracted.subproj, build.options getOrElse BuildOptions())
        val outcome = localBuildRunner.checkCacheThenBuild(projectsDir(dir, p), repeatableProjectBuild, Seq.empty, Seq.empty, log)
        val artifactsOut = outcome match {
          case o: BuildGood => o.artsOut
          case o: BuildBad => sys.error("Part " + p.name + ": " + o.status)
        }
        val q=(p.name, artifactsOut)
        log.debug("---> "+q)
        (q, repeatableProjectBuild)
      }
    }).unzip

    // Excellent, we now have in preCrossArtifactsMap a sequence of BuildArtifactsOut from the parts.
    // Out of them, we need to find out which ones originate from the scala core, and separate them
    // from the rest.
    // We cannot rely on the cross suffix, as the non-scala nested projects might also be published
    // with cross versioning disabled (it's the default in dbuild). Our only option is going after
    // the organization id "org.scala-lang".

    def isScalaCore(name: String, org: String) =
      org == "org.scala-lang" && name.startsWith("scala")    
    def isScalaCoreArt(l: ArtifactLocation) =
      isScalaCore(l.info.organization, l.info.name)
    
    // we also need the new scala version, which we take from the scala-library artifact, among
    // our subprojects. If we cannot find it, then we have none.
      
    val scalaVersion = {
      val allArts = preCrossArtifactsMap.map(_._2).flatMap(_.results).flatMap(_.artifacts)
      allArts.find(l => l.info.organization == "org.scala-lang" && l.info.name == "scala-library").map(_.version)
    }
    def getScalaVersion(crossLevel: String) = scalaVersion getOrElse
      sys.error("In Assemble, the requested cross-version level is " + crossLevel + ", but no scala-library was found among the artifacts.")
    
    // ------
    //
    // now, let's retrieve the parts' artifacts again (they have already been published)
    val uuids = repeatableProjectBuilds map { _.uuid }
    log.info("Retrieving module artifacts")
    log.debug("into " + localRepo)
    val artifactLocations = LocalRepoHelper.getArtifactsFromUUIDs(log.info, localBuildRunner.repository, localRepo, uuids)

    // ------
    // ok. At this point, we have:
    // preCrossArtifactsMap: map name -> artifacts, from the nested parts. Each mapping corresponds to one nested project,
    //   and the list of artifacts may contain multiple subprojects, each with their own BuildSubArtifactsOut
    //
    // ------
    //
    // Before rearranging the poms, we may need to adapt the cross-version strings in the part
    // names. That depends on the value of cross-version in our main build.options.cross-version.
    // If it is "disabled" (default), the parts already have a version without a cross-version
    // string, so we are good. If it is anything else, we need to adjust the cross-version suffix
    // of all artifacts (except those of the scala core) according to the value of the new scala
    // version, according to the "scala-library" artifact we have in our "Assemble". If we have
    // no scala-library, however, we can't change the suffixes at all, so we stop.
    // If cross-version is "full", the parts will have a cross suffix like
    // "_2.11.0-M5"; we should replace that with the new full Scala version.
    // For "standard" it may be either "_2.11.0-M5" or "_2.11", depending on what each part
    // decides. For binaryFull, it will be "_2.11" even for milestones.
    // The cross suffix for the parts depends on their own build.options.
    // 
    // We change that in conformance to project.crossVersion, so that:
    // - disabled => no suffix
    // - full => full version string
    // - binaryFull => binaryScalaVersion
    // - standard => binary if stable, full otherwise
    // For "standard" we rely on the simple 0.12 algorithm (contains "-"), as opposed to the
    // algorithms detailed in sbt's pull request #600.
    //
    // We have to patch both the list of BuildSubArtifactsOut, as well as the actual filenames
    // (including checksums, if any)

    val Part = """(\d+\.\d+)(?:\..+)?""".r
    def binary(s: String) = s match {
      case Part(z) => z
      case _ => sys.error("Fatal: cannot extract Scala binary version from string \"" + s + "\"")
    }
    val crossSuff = project.buildOptions.crossVersion match {
      case "disabled" => ""
      case l@"full" => "_" + getScalaVersion(l)
      case l@"binary" => "_" + binary(getScalaVersion(l))
      case l@"standard" =>
        val version = getScalaVersion(l)
        "_" + (if (version.contains('-')) version else binary(version))
      case cv => sys.error("Fatal: unrecognized cross-version option \"" + cv + "\"")
    }
    def patchName(s: String) = fixName(s) + crossSuff

    // this is the renaming section: the artifacts are renamed according
    // to the crossSuffix selection
    val artifactsMap = preCrossArtifactsMap map {
      case (projName, BuildArtifactsOut(subs)) => (projName, BuildArtifactsOut(
        subs map {
          case BuildSubArtifactsOut(name, artifacts, shas) =>
            val renamedArtifacts = artifacts map { l =>
              log.debug("we had : "+l)
              val newl = if (isScalaCoreArt(l)) l else
                l.copy(crossSuffix = crossSuff)
              log.debug("we have: "+newl)
              newl
            }
            val newSHAs = shas map { sha =>
              val OrgNameVerFilenamesuffix = """(.*)/([^/]*)/([^/]*)/\2(-[^/]*)""".r
              val oldLocation = sha.location
              try {
                val OrgNameVerFilenamesuffix(org, oldName, ver, suffix) = oldLocation
                log.debug("We had : "+org+" "+oldName)
                if (isScalaCore(name, org)) { log.debug("isScalaCore"); sha } else {
                  val newName = patchName(oldName)
                  if (newName == oldName) { log.debug("unchanged"); sha } else {
                    val newLocation = org + "/" + newName + "/" + ver + "/" + (newName + suffix)
                    def fileDir(name: String) = org.split('/').foldLeft(localRepo)(_ / _) / name / ver
                    def fileLoc(name: String) = fileDir(name) / (name + suffix)
                    val oldFile = fileLoc(oldName)
                    val newFile = fileLoc(newName)
                    fileDir(newName).mkdirs() // ignore if already present
                    log.debug("renaming "+oldFile)
                    log.debug("to       "+newFile)
                    if (!oldFile.renameTo(newFile))
                      sys.error("cannot rename " + oldLocation + " to " + newLocation + ".")
                    sha.copy(location = newLocation)
                  }
                }
              } catch {
                case e: _root_.scala.MatchError =>
                  log.error("Path cannot be parsed: " + oldLocation + ". Continuing...")
                  sha
              }
            }
            BuildSubArtifactsOut(name, renamedArtifacts, newSHAs)
        }))
    }

    //
    // we have all our artifacts ready. Time to rewrite the POMs!
    // Note that we will also have to recalculate the shas
    //
    // Let's collect the list of available artifacts:
    //
    val allArtifactsOut = artifactsMap.map { _._2 }
    val available = allArtifactsOut.flatMap { _.results }.flatMap { _.artifacts }

    (localRepo.***.get).filter(_.getName.endsWith(".pom")).map {
      pom =>
        val reader = new MavenXpp3Reader()
        val model = reader.read(new _root_.java.io.FileReader(pom))
        // transform dependencies
        val deps: Seq[Dependency] = model.getDependencies.asScala
        val newDeps: _root_.java.util.List[Dependency] = (deps map { m =>
          available.find { artifact =>
            artifact.info.organization == m.getGroupId &&
              artifact.info.name == fixName(m.getArtifactId)
          } map { art =>
            val m2 = m.clone
            m2.setArtifactId(fixName(m.getArtifactId) + art.crossSuffix)
            m2.setVersion(art.version)
            m2
          } getOrElse m
        }).asJava
        val newModel = model.clone
        // has the artifactId (aka the name) changed? If so, patch that as well.
        val NameExtractor = """.*/([^/]*)/([^/]*)/\1-[^/]*.pom""".r
        val NameExtractor(newArtifactId, _) = pom.getCanonicalPath()
        newModel.setArtifactId(newArtifactId)
        newModel.setDependencies(newDeps)
        // we overwrite in place, there should be no adverse effect at this point
        val writer = new MavenXpp3Writer
        writer.write(new _root_.java.io.FileWriter(pom), newModel)
        // It's not over, yet. we also have to change the .sha1 and .md5 files
        // corresponding to this pom, if they exist, otherwise artifactory and ivy
        // will refuse to use the pom in question.
        Seq("md5", "sha1") foreach { algorithm =>
          val checksumFile = new File(pom.getCanonicalPath + "." + algorithm)
          if (checksumFile.exists) {
            FileUtils.writeStringToFile(checksumFile, ChecksumHelper.computeAsString(pom, algorithm))
          }
        }
    }

    // dbuild SHAs must be re-computed (since the POMs changed), and the ArtifactsOuts must be merged
    //
    val out = BuildArtifactsOut(artifactsMap.map {
      case (project, arts) =>
        val modArtLocs = arts.results.flatMap { _.artifacts }
        BuildSubArtifactsOut(project, modArtLocs, projSHAs(modArtLocs.map { _.info }, crossSuff))
    })
    log.debug("out: " + writeValue(out))
    out
  }

}
