package distributed
package repo
package core

import project.model._
import LocalRepoHelper.ResolutionResult
import java.io.File
import sbt.{ RichFile, IO, Path }
import Path._
import distributed.project.model.Utils.writeValue

/** Expose for SBT launcher support. */
class SbtRepoMain extends xsbti.AppMain {
  def run(configuration: xsbti.AppConfiguration) =
    try {
      val args = configuration.arguments
      ProjectRepoMain.main(args.toArray)
      Exit(0)
    } catch {
      case e: Exception =>
        e.printStackTrace
        Exit(1)
    }
  case class Exit(val code: Int) extends xsbti.Exit
}

/** Direct main for use in SBT. */
object ProjectRepoMain {
  import sections._
  // Removed the evil file-specific knowledge, which
  // will also allow to use this code to inspect remote repositories
  val cache = Repository.default

  // TODO: integrate drepo as a subcommand "dbuild repo" in the main command line;
  // add an option to select which should be the cache to inspect;
  // optionally, add a repo subsubcommand to upload a build from one cache to another
  def main(args: Array[String]): Unit = {
    args.toSeq match {
      case Seq("project", uuid) =>
        printProjectInfo(uuid)
      case Seq("project-files", uuid) =>
        printProjectInfo(uuid, printFiles = true, printArts = false)
      case Seq("project-artifacts", uuid) =>
        printProjectInfo(uuid, printFiles = false, printArts = true)
      case Seq("build", uuid) => printBuildInfo(uuid)
      case Seq("build-projects", uuid) => printAllProjectInfo(uuid)
      case Seq("list-builds") => printAvailableBuilds()
      case Seq("list-projects") => printAvailableProjects()
      case _ =>
        println("""|Usage: <repo-reader> <cmd>
                   |  where cmd in:
                   |  -  project <uuid>
                   |      prints the information about a project.
                   |  -  project-files <uuid>
                   |      prints the files stored for a project.
                   |  -  project-artifacts <uuid>
                   |      prints the artifacts published by a project.
                   |  -  build <uuid>
                   |      prints the information about a build.
                   |  -  build-projects <uuid>
                   |      prints the information about projects within a build.
                   |  -  list-projects
                   |      lists all projects available in the selected cache.
                   |  -  list-builds
                   |      lists all builds available in the selected cache.
                   |""".stripMargin)
    }
  }

  def printAvailableBuilds(): Unit = {
    println("--- Available Builds")
    val keys = cache.enumerate[SavedConfiguration]()
    keys foreach {
      case key @ GetBuild(uuid) =>
        val size = cache.getSize(key)
        val date = "" // TODO: cache.date() is not reliable, as it changes across cache copies. We must add the timestamp to SavedConfiguration 
        cache.get(key) match {
          case None => println("  * " + uuid + " <deleted>")
          case Some(saved) =>
            println("  * " + uuid + " @ " + date)
            val SavedConfiguration(expandedDBuildConfig, build) = saved
            val projects = build.repeatableBuilds map { project => (project.configAndExtracted.config.name, project.uuid) }
            val names = padStrings(projects map (_._1))
            val uuids = projects map (_._2)
            for ((name, id) <- names zip uuids) {
              println("      + " + name + " " + id)
            }
        }
    }
  }

  def printAvailableProjects(): Unit = {
    println("--- Available Projects")
    val keys = cache.enumerate[RepeatableProjectBuild]()
    val projectsWithMeta = (keys zip (keys map { cache.get(_) })).
      // we might get None, if projects are being deleted
      filterNot(_._2.nonEmpty)
    val names = padStrings(projectsWithMeta map (_._2.get.config.name))
    (names zip projectsWithMeta) foreach {
      case (paddedName, (GetProject(uuid), Some(p))) =>
        println("  * " + uuid + " " + paddedName + " @ " + p.config.uri)
    }
  }

  private def printProjectHeader(uuid: String): Unit =
    println("--- Project Build: " + uuid)

  def getProject(uuid: String): Option[RepeatableProjectBuild] = {
    printProjectHeader(uuid)
    val key = GetProject(uuid)
    cache.get(key)
  }
  def printProjectInfo(uuid: String, printArts: Boolean = true, printFiles: Boolean = true, name: Option[String] = None): Unit = {
    val project = getProject(uuid)
    project match {
      case None => name match {
        case None => println("<project not available>")
        case Some(s) => "     " + s + " is not built."
      }
      case Some(p) =>
        printProjectDependencies(p)
        val buildArtifactsOut = cache.get[BuildArtifactsOut](p)
        buildArtifactsOut match {
          case None => println("<no artifacts published>")
          case Some(bao) =>
            printProjectArtifacts(bao)
            printProjectFiles(bao)
        }
    }
  }

  def printProjectDependencies(p: RepeatableProjectBuild): Unit = {
    println(" -- Dependencies --")
    for (GetProject(uuid) <- (p.depInfo flatMap (_.dependencyUUIDs)).distinct)
      println("    * " + uuid)
  }

  // TODO: now that module information is available, we might print artifacts grouped by modules
  def printProjectArtifacts(bao: BuildArtifactsOut): Unit = {
    val arts = bao.results flatMap { _.artifacts }
    println(" -- Artifacts -- ")
    val groups = padStrings(arts map (_.info.organization))
    val names = padStrings(arts map (_.info.name))
    val classifiers = padStrings(arts map (_.info.classifier getOrElse ""))
    val extensions = padStrings(arts map (_.info.extension))
    val versions = padStrings(arts map (_.version))
    for {
      ((((group, name), classifier), extension), version) <- groups zip names zip classifiers zip extensions zip versions
    } println("  - " + group + " : " + name + " : " + classifier + " : " + extension + " : " + version)
  }

  def padStrings(strings: Seq[String]): Seq[String] = {
    val max = ((strings map (_.length)) :+ 0).max
    val pad = Seq.fill(max)(' ') mkString ""
    for {
      string <- strings
      myPad = pad take (max - string.length)
    } yield myPad + string
  }

  def printProjectFiles(bao: BuildArtifactsOut): Unit = {
    println(" -- Files -- ")
    val artShas = bao.results flatMap { _.shas }
    val groups = artShas groupBy { x => x.location.location.take(x.location.location.lastIndexOf('/')) }
    for ((dir, arts) <- groups.toSeq.sortBy(_._1)) {
      println("  " + dir)
      printArtifactSeq(arts, true, "    ")
    }
  }

  def printArtifactSeq(arts: Seq[ArtifactSha], shrinkLocation: Boolean = false, pad: String = ""): Unit = {
    arts foreach {
      case ArtifactSha(rawKey @ GetRaw(sha), ArtifactRelative(location)) =>
        val loc = if (shrinkLocation) location.drop(location.lastIndexOf('/') + 1)
        else location
        val size = cache.getSize(rawKey) getOrElse "<size unknown>"
        println(pad + sha + "  " + size + "  " + loc)
    }
  }

  def prettyFileLength(length: Long) = length match {
    case x if x > (1024 * 1024 * 1023) => "%3.1fG" format (x.toDouble / (1024.0 * 1024 * 1024))
    case x if x > (1024 * 1024) => "%3.1fM" format (x.toDouble / (1024.0 * 1024))
    case x if x > 1024 => "%3.1fk" format (x.toDouble / 1024.0)
    case x => "%4d" format (x)
  }

  def printBuildInfo(uuid: String): Unit = {
    println("--- Repeatable Build: " + uuid)
    val key = GetBuild(uuid)
    cache.get(key) match {
      case Some(SavedConfiguration(expandedDBuildConfig, build)) =>
        println(" = Projects = ")
        build.repeatableBuilds foreach { project =>
          println("  - " + project.uuid + " " + project.config.name)
        }
        println(" = Repeatable dbuild configuration =")
        println(writeValue(expandedDBuildConfig))
      case None => println("This build UUID was not found in the cache.")
    }
  }

  def printAllProjectInfo(buildUUID: String): Unit = {
    println("--- Repeatable Build: " + buildUUID)
    val savedOpt = cache.get(GetBuild(buildUUID))
    savedOpt match {
      case None => println("<build not found>")
      case Some(SavedConfiguration(expandedDBuildConfig, build)) =>
        build.repeatableBuilds foreach { p =>
          // drepo bypasses the standard keys interface, but that is necessary as
          // it deals directly with uuids. No other part of the code should
          // access uuids inside keys directly
          val key = Repository.getKey[RepeatableProjectBuild](p)
          printProjectInfo(key.uuid, name = Some(p.config.name))
        }
    }
  }
}

