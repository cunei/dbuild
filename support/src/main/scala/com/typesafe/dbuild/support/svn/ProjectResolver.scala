package com.typesafe.dbuild.support.svn

import com.typesafe.dbuild.adapter.Adapter
import Adapter.Path._
import Adapter.{IO,toFF}
import Adapter.syntaxio._
import com.typesafe.dbuild.model._
import com.typesafe.dbuild.logging.Logger
import com.typesafe.dbuild.project.resolve.ProjectResolver
import com.typesafe.dbuild.support.UriUtil

/** This class knows how to resolve Git projects and
 * update the build configuration for repeatable checkouts.
 */
class SvnProjectResolver extends ProjectResolver {
  def canResolve(configUri: String): Boolean = {
    val uri = new _root_.java.net.URI(configUri)
    ((uri.getScheme == "svn") || 
     (uri.getScheme == "http") ||
     (uri.getScheme == "https")
    ) && Svn.isSvnRepo(uri)
  }
    
  def resolve(config: ProjectBuildConfig, dir: _root_.java.io.File, log: Logger): ProjectBuildConfig = {
    val uri = new _root_.java.net.URI(config.uri)

    // clean the directory content, just in case there are leftovers
    IO.delete(dir.*(toFF("*")).get)
    if(!(dir / ".svn" ).exists) Svn.checkout(uri, dir, log)
    else Svn.revert(dir, log)

    // TODO - Fetch non-standard references?
    // Then checkout desired branch/commit/etc.
    Option(uri.getFragment()) match {
      case Some(revision) => Svn.update(revision, dir, log)
      case _ => Svn.update("", dir, log)
    }
    val rev = Svn.revision(dir, log)
    val newUri = UriUtil.dropFragment(uri).toASCIIString + "#" + rev
    config.copy(uri = newUri)
  }
}
