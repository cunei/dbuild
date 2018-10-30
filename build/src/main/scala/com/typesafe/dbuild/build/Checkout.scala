package com.typesafe.dbuild.build

import java.io.File
import com.typesafe.dbuild.repo.core.{ Repository, LocalRepoHelper }
import com.typesafe.dbuild.model._
import com.typesafe.dbuild.project.resolve.AggregateProjectResolver
import com.typesafe.dbuild.logging.ConsoleLogger
import com.typesafe.dbuild.project.build.LocalBuildRunner
import com.typesafe.dbuild.utils.TrackedProcessBuilder
import sbt.Path._
import sys.process._
import com.typesafe.dbuild.support.sbt.SbtBuildSystem
import scala.collection.immutable.SortedMap
import com.typesafe.dbuild.support.sbt.{ SbtRunner, SbtBuildConfig }
import com.typesafe.dbuild.support.sbt.SbtRunner.SbtFileNames._
import com.typesafe.dbuild.logging.Logger
import java.io.PrintWriter

/**
 * Implementation of the "dbuild checkout" command. It will resolve and reconfigure an sbt project
 * from a previous build, and drop the user into a debugging sbt session.
 * This is an sbt-specific functionality, which replaces the previous "dbuild-setup" plugin command.
 */
object Checkout {
  def dbuildCheckout(uuid: String, projectName: String, path: String, debug: Boolean,
      useLocalResolvers: Boolean, localRepos: List[xsbti.Repository]) = {
    val dir = (new File(path)).getCanonicalFile
    if (dir.exists)
      sys.error("The path \"" + path + "\" already exists. Please move it aside, or use a different name.")
    if (dir.getName == projectName)
      sys.error("Using a directory name that is identical to the project name may confuse dbuild, due to an sbt restriction. Please use a different name.")

    // ok. First of all, we need to retrieve from the cache the build description, and the project
    val cache = Repository.default
    val log = ConsoleLogger(debug)
    println("Please wait...") // reloading the metadata may take several seconds
    val buildMeta = LocalRepoHelper.readBuildMeta(uuid, cache) getOrElse
      sys.error("The requested UUID \"" + uuid + "\" was not found in the cache.")
    log.debug("Build UUID " + uuid + " found in cache.")
    buildMeta match {
      case SavedConfiguration(expandedDBuildConfig, build) =>
        val allProjects = build.repeatableBuilds.map(_.config.name)
        if (allProjects.contains(dir.getName))
          sys.error("Using a directory name that is identical to the name of an sbt subproject of this project may confuse dbuild, due to an sbt restriction. Please use a different name.")
        val project = build.repeatableBuilds.find(_.config.name == projectName) getOrElse
          sys.error("Project \"" + projectName + "\" was not found in build " + uuid + ".")
        if (project.config.system != "sbt")
          sys.error("Project \"" + projectName + "\" is not sbt-based. Cannot continue.")
        log.debug("Found project \"" + projectName + "\", project UUID: " + project.uuid)
        if (!dir.mkdirs())
          sys.error("The requested directory \"" + dir.getCanonicalPath + "\" could not be created.")

        // We should be good now: we can resolve the project.
        val resolver = new AggregateProjectResolver(Seq(
          new com.typesafe.dbuild.support.git.GitProjectResolver(skipGitUpdates = false),
          new com.typesafe.dbuild.support.svn.SvnProjectResolver))
        if (!resolver.canResolve(project.config.uri))
          sys.error("\"dbuild checkout\" cannot be used with this project; only git and svn URIs are supported. Found: " + project.config.uri)
        val config2 = resolver.resolve(project.config, dir, log)
        resolver.prepare(config2, dir, log)

        // it is now time to set up the project, exactly as we would before the build stage in dbuild
        val (dependencies, version, writeRepo) = LocalBuildRunner.prepareDepsArtifacts(cache, project,
          project.configAndExtracted.extracted.projects, dir, log, debug)

        // Let's retrieve the list of repositories from the original repeatable description;
        // we will have to do some post-processing in order to get again a List[xsbti.Repository]
        val initialResolvers = expandedDBuildConfig.options.resolvers
        val sortedInitialResolvers = (SortedMap[String, String]() ++ initialResolvers).values
        val resolversMap = sortedInitialResolvers map {
          _.split(":", 2) match {
            case Array(x) => (x, None)
            case Array(x, y) => (x, Some(y))
            case z => sys.error("Internal error, unexpected split result: " + z)
          }
        }
        // getRepositories contains a ListMap.toList, where sbt's definition
        // of toList is "backing.reverse". So we have to reverse again,
        // and we finally get the needed List[xsbti.Repository]
        val listMap = xsbt.boot.ListMap(resolversMap.toSeq.reverse: _*)
        val savedRepos = (new xsbt.boot.ConfigurationParser).getRepositories(listMap)

        val repos = if (useLocalResolvers || savedRepos.isEmpty)
          localRepos
        else {
          savedRepos
        }
        if (debug) {
          println("Resolvers:")
          repos foreach println
        }

        log.debug("Resolvers:")
        repos foreach { r => log.debug(r.toString) }

        // we might need to build the project within a subdir of the checked out project. Therefore:
        val ec = project.extra[SbtExtraConfig]
        val projDir = SbtBuildSystem.projectDir(dir, ec)

        // Prepare a new sbt base, and runner. These lines roughly mirror what appears
        // in SbtBuildSystem.runBuild()
        val globalBase = dir / dbuildSbtDirName / "sbt-base"
        val sbtRunner = new SbtRunner(repos, globalBase, debug)
        val subprojs = project.configAndExtracted.extracted.projInfo.map { _.subproj }

        // Ready to go!
        val info = BuildInput(dependencies, version, subprojs, writeRepo, projectName)
        val config = SbtBuildConfig(ec, project.config.crossVersion getOrElse sys.error("Internal error: crossVersion not expanded in runBuild."),
            project.config.checkMissing getOrElse sys.error("Internal error: checkMissing not expanded in runBuild."),
            project.config.rewriteOverrides getOrElse sys.error("Internal error: rewriteOverrides not expanded in runBuild."), info)
        val tracker = new TrackedProcessBuilder
        val artsOut = com.typesafe.dbuild.support.sbt.SbtBuilder.buildSbtProject(repos, sbtRunner)(projDir, config, tracker, log, debug, customProcess = Some({
          (_, _, _, _, cmd: Seq[String]) =>
            val commandFile = dir / dbuildSbtDirName / "start"
            val writer = new PrintWriter(commandFile)
            writer.println("#!/usr/bin/env bash")
            writer.println("cd '" + (projDir.getCanonicalPath.replaceAll("'", "'\"'\"'")) + "'")
            writer.println("echo 'Preparing sbt. This may take a while...'")
            writer.println(cmd.map { _.replaceAll("'", "'\"'\"'") }.mkString("'", "' '", "'"))
            writer.close()
            Seq("chmod", "u+x", commandFile.getCanonicalPath) !;
            println()
            println("Ready! You can start the debugging environment by running: " + commandFile.getCanonicalPath)
            println()
            println("You can also rebuild the project just like dbuild would do, by issuing \"dbuild-build\" at the sbt prompt.")
        }), targetCommands = Seq("shell"))

      case _ => sys.error("This build UUID was found, but its data seems corrupted. Please report.")
    }
    // return declaring success
    new xsbti.Exit { def code = 0 }
  }
}
