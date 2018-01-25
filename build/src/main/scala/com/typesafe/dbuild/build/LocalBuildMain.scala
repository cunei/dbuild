package com.typesafe.dbuild.build

import java.io.File
import akka.actor.{ ActorSystem, Props }
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import com.typesafe.dbuild.model._
import com.typesafe.dbuild.model.Utils.{ readValue, writeValue }
import com.typesafe.dbuild.repo.core._
import com.typesafe.dbuild.model.ClassLoaderMadness
import com.typesafe.dbuild.project.dependencies.Extractor
import com.typesafe.dbuild.support.BuildSystemCore
import akka.pattern.ask
import com.typesafe.dbuild.repo.core.GlobalDirs.checkForObsoleteDirs
import com.typesafe.dbuild.support
import com.typesafe.dbuild.logging

class LocalBuildMain(repos: List[xsbti.Repository], options: BuildRunOptions) {

  val targetDir = GlobalDirs.targetDir
  val resolvers = Seq(
    new support.git.GitProjectResolver,
    new support.svn.SvnProjectResolver,
    new support.ivy.IvyProjectResolver(repos),
    new support.test.TestProjectResolver,
    new support.nil.NilProjectResolver,
    new support.aether.AetherProjectResolver(repos))
  val buildSystems: Seq[BuildSystemCore] =
    Seq(new support.sbt.SbtBuildSystem(repos, targetDir, options.debug),
      support.scala.ScalaBuildSystem,
      new support.ivy.IvyBuildSystem(repos, targetDir),
      support.assemble.AssembleBuildSystem,
      support.test.TestBuildSystem,
      support.nil.NilBuildSystem,
      new support.aether.AetherBuildSystem(repos, targetDir))

  // Gymnastics for classloader madness

  val system = ClassLoaderMadness.withContextLoader(getClass.getClassLoader)(ActorSystem.create)
  val logMgr = {
    val mgr = system.actorOf(Props(new logging.ChainedLoggerSupervisorActor))
    mgr ! Props(new logging.LogDirManagerActor(new File(targetDir, "logs")))
    mgr ! Props(new logging.SystemOutLoggerActor(options.debug))
    mgr
  }
  val repository = Repository.default
  val logger = new logging.ActorLogger(logMgr)
  checkForObsoleteDirs(logger.warn _)

  val builder = system.actorOf(Props(new LocalBuilderActor(resolvers, buildSystems, repository, targetDir, logger, options)))
  // TODO - Look up target elsewhere...

  def build(conf: DBuildConfiguration, confName: String, buildTarget: Option[String]): BuildOutcome = {
    implicit val timeout: Timeout = Timeouts.dbuildTimeout
    val result = builder ? RunLocalBuild(conf, confName, buildTarget)
    try {
      Await.result(result.mapTo[BuildOutcome], Duration.Inf)
    } catch {
      case e: Exception => if (options.debug) {
        println("Exception thrown while building!")
        println("Actors after unexpected exception:\n" + dumpAllActors())
        println("Exception caught:")
        e.printStackTrace()
        println("Rethrowing...")
      }
      throw e
    }
  }
  def dispose(): Unit = {
    implicit val timeout: Timeout = 5.minutes
    if (options.debug) {
      println("Actors prior to logMgr exit:\n" + dumpAllActors())
    }
    Await.result((logMgr ? "exit").mapTo[String], Duration.Inf)
    if (options.debug) {
      println("Actors after logMgr exit, prior to shutdown:\n" + dumpAllActors())
    }
    system.shutdown() // pro forma, as all loggers should already be stopped at this point
    try {
      system.awaitTermination(4.minute)
    } catch {
      case e:Exception =>
        println("Warning: system did not shut down within the allotted time")
        if (options.debug) {
          println(e.getMessage)
          println("Actors still alive after awaitTermination() timeout:\n" + dumpAllActors())
        }
    }
  }

  def dumpAllActors():String = {
    class PrivateMethodCaller(x: AnyRef, methodName: String) {
      def apply(_args: Any*): Any = {
        val args = _args.map(_.asInstanceOf[AnyRef])
        def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
        val parents = _parents.takeWhile(_ != null).toList
        val methods = parents.flatMap(_.getDeclaredMethods)
        val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
            method.setAccessible(true)
        method.invoke(x, args: _*)
      }
    }

    class PrivateMethodExposer(x: AnyRef) {
      def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)
    }

    val res = new PrivateMethodExposer(system)('printTree)()
    res.toString
  }
}
