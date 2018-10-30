package com.typesafe.dbuild.support.git

import _root_.sbt.Path._
import com.typesafe.dbuild.model._
import com.typesafe.dbuild.project.resolve.ProjectResolver
import com.typesafe.dbuild.logging.Logger
import com.typesafe.dbuild.logging.Logger.prepareLogMsg
import com.typesafe.dbuild.support.UriUtil
import com.typesafe.dbuild.hashing

/**
 * This class knows how to resolve Git projects and
 * update the build configuration for repeatable checkouts.
 */
class GitProjectResolver(skipGitUpdates: Boolean) extends ProjectResolver {
  def canResolve(configUri: String): Boolean = {
    val uri = new _root_.java.net.URI(configUri)
    (uri.getPath != null) && ((uri.getScheme == "git") || (uri.getScheme == "jgit") ||
      (uri.getPath endsWith ".git") || ((uri.getScheme == "file") &&
        (new _root_.java.io.File(uri.getPath()) / ".git").exists))
  }

  /** 
   *  Use the scheme "jgit" if you prefer jgit (will not use hardlinks, hence more disk space will be used).
   *  The regular scheme "git" will use the command line tool by default.
   */
  def resolve(config: ProjectBuildConfig, dir: _root_.java.io.File, log: Logger): ProjectBuildConfig = {
    val git: GitImplementation = if (config.useJGit.getOrElse(sys.error("Internal error: usejgit is None. Please report.")))
      GitJGit
    else
      GitGit

    val uri = new _root_.java.net.URI(config.uri)
    val uriString = UriUtil.dropFragment(uri).toASCIIString
    val baseName = ({s:String => if (s.endsWith(".git")) s.dropRight(4) else s})(uri.getRawPath().split("/").last)
    val cloneDir = com.typesafe.dbuild.repo.core.GlobalDirs.clonesDir / ((hashing sha1 uriString) + "-" + baseName)
    val ref = Option(uri.getFragment()) getOrElse "master"

    // We cache a single git clone for this repository URI (sans fragment),
    // then we re-clone just the local clone. Note that there are never
    // working files checked out in the cache clone: the directories
    // contain only a ".git" subdirectory.
    // TODO: locking
    val clone = if (!cloneDir.exists) {
      cloneDir.mkdirs()
      git.clone(uriString, cloneDir, log)
    } else {
      git.getRepo(cloneDir)
    }
    git.fetch(clone, true /* ignore failures */ , log)

    // Now: clone that cache into the local directory
    if (!dir.exists) dir.mkdirs()
    // NB: clone does not check out anything in particular.
    // An explicit checkout follows later
    val localRepo = if (!(dir / ".git").exists) git.clone(cloneDir.getCanonicalPath, dir, log) else git.getRepo(dir)

    git.fetch(localRepo, false /* stop on failures */ , log)
    // scrub the dir from all extraneous stuff before returning
    git.clean(localRepo, log)

    val sha = git.checkoutRef(localRepo, ref, log)
    val newUri = UriUtil.dropFragment(uri).toASCIIString + "#" + sha
    config.copy(uri = newUri)
  }
}