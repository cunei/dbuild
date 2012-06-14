package com.typesafe.dsbt

import sbt._
import distributed.project.model
import _root_.pretty.PrettyPrint
import StateHelpers._

object DependencyAnalysis {
  // TODO - make a task that generates this metadata and just call it!
  
  /** Pulls the name/organization/version for each project in the build. */
  private def getProjectInfos(extracted: Extracted, state: State, refs: Iterable[ProjectRef]): Seq[model.Project] =
    (Vector[model.Project]() /: refs) { (dependencies, ref) =>
      val (_, pdeps) = extracted.runTask(Keys.projectDependencies in ref, state)
      val ldeps = extracted.get(Keys.libraryDependencies in ref)
      
      val deps = for {
        d <- (pdeps ++ ldeps)
      } yield model.ProjectDep(fixName(d.name), d.organization)
      
      dependencies :+ model.Project(
        fixName(extracted.get(Keys.name in ref)),
        extracted.get(Keys.organization in ref),
        deps)
    }
  /** Actually prints the dependencies to the given file. */
  def printDependencies(state: State, uri: String, file: String): Unit = {
    val extracted = Project.extract(state)
    import extracted._
    val refs = getProjectRefs(session.mergeSettings)
    val deps = getProjectInfos(extracted, state, refs)    
    val meta = model.ExtractedBuildMeta(uri, deps)
    val output = new java.io.PrintStream(new java.io.FileOutputStream(file))
    try output println PrettyPrint(meta)
    finally output.close()
  }
  
  /** The implementation of the print-deps command. */
  def printCmd(state: State): State = {
    val uri = System.getProperty("remote.project.uri")
    (Option(System.getProperty("project.dependency.metadata.file")) 
        foreach (f => printDependencies(state, uri, f)))
    state
  }

  private def print = Command.command("print-deps")(printCmd)

  /** Settings you can add your build to print dependencies. */
  def printSettings: Seq[Setting[_]] = Seq(
    Keys.commands += print
  )
  
  // Remove scala version from names so we can do cross-compile magikz.
  val ScalaVersioned = new util.matching.Regex("(.+)_((\\d+)\\.(\\d+)(.+))")
  def fixName(name: String): String = name match {
    case ScalaVersioned(name, _*) => name
    case name => name
  }
}