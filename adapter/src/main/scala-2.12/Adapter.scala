import org.apache.ivy.core.module.id.ModuleRevisionId
import org.apache.ivy.core.module.descriptor.ModuleDescriptor

package sbt.dbuild.hack {
object DbuildHack {
  val Load = sbt.internal.Load
  val applyCross: (String, Option[String => String]) => String =
   sbt.librarymanagement.CrossVersion.applyCross
  val defaultID: (java.io.File,String) => String =
   sbt.internal.BuildDef.defaultID
  val ExceptionCategory = sbt.ExceptionCategory
}
}
package com.typesafe.dbuild.adapter {
import java.io.File

object LoggingInterface {
  val Level = sbt.util.Level
  type Logger = sbt.util.Logger
  type LogEvent = sbt.util.LogEvent
  val ControlEvent = sbt.util.ControlEvent
  val StackTrace = sbt.internal.util.StackTrace
  type BasicLogger = sbt.internal.util.BasicLogger
}

trait StreamLoggerAdapter

abstract class ReapplyInterface {
  def reapplySettings(newSettings: Seq[sbt.Def.Setting[_]],
    structure: sbt.internal.BuildStructure,
    log: sbt.util.Logger)(implicit display: sbt.util.Show[sbt.Def.ScopedKey[_]]): sbt.internal.BuildStructure
}

class Reapply2 extends ReapplyInterface {
  def reapplySettings(newSettings: Seq[sbt.Def.Setting[_]],
    structure: sbt.internal.BuildStructure,
    log: sbt.util.Logger)(implicit display: sbt.util.Show[sbt.Def.ScopedKey[_]]): sbt.internal.BuildStructure = {
    sbt.dbuild.hack.DbuildHack.Load.reapply(newSettings, structure)(display)
  }
}

object Adapter {
  val IO = sbt.io.IO
  val Path = sbt.io.Path
  type RichFile = sbt.io.RichFile
  def newIvyPaths(baseDirectory: java.io.File, ivyHome: Option[java.io.File]) =
    sbt.librarymanagement.ivy.IvyPaths(baseDirectory, ivyHome)
  type FileFilter = sbt.io.FileFilter
  def toFF = sbt.io.FileFilter.globFilter _
  val DirectoryFilter = sbt.io.DirectoryFilter
  type ExactFilter = sbt.io.ExactFilter
  type NameFilter = sbt.io.NameFilter
  type FileRepository = sbt.librarymanagement.FileRepository
  type Logger = sbt.util.Logger
  def allPaths(f:File) = sbt.io.PathFinder(f).allPaths
  val syntaxio = sbt.io.syntax
  type ModuleID = sbt.librarymanagement.ModuleID
  type Artifact = sbt.librarymanagement.Artifact
  type CrossVersion = sbt.librarymanagement.CrossVersion
  type IvyScala = sbt.librarymanagement.ScalaModuleInfo
  def interProjectResolver(k:Map[ModuleRevisionId, ModuleDescriptor]) =
    new sbt.librarymanagement.RawRepository(new sbt.internal.librarymanagement.ProjectResolver("inter-project", k), "inter-project")
  val keyIvyScala = sbt.Keys.scalaModuleInfo
  def moduleWithName(m:ModuleID, name:String) = m.withName(name)
  def moduleWithRevision(m:ModuleID, revision:String) = m.withRevision(revision)
  def moduleWithCrossVersion(m:ModuleID, cross:CrossVersion) = m.withCrossVersion(cross)
  def moduleWithExplicitArtifacts(m:ModuleID, ea:Seq[Artifact]) = m.withExplicitArtifacts(ea.toVector)
  def moduleWithExtraAttributes(m:ModuleID, ea:Map[String,String]) = m.withExtraAttributes(ea)
  def ivyScalaWithCheckExplicit(i:IvyScala, ce:Boolean) = i.withCheckExplicit(ce)
  def artifactWithClassifier(a:Artifact, cl:Option[String]) = a.withClassifier(cl)
  val crossDisabled = sbt.librarymanagement.Disabled()
  type crossDisabled = sbt.librarymanagement.Disabled
  val crossBinary = sbt.librarymanagement.Binary()
  type crossBinary = sbt.librarymanagement.Binary
  val crossFull = sbt.librarymanagement.Full()
  type crossFull = sbt.librarymanagement.Full
  type ProjectResolver = sbt.internal.librarymanagement.ProjectResolver
  type ScalaInstance = sbt.internal.inc.ScalaInstance
  val ScalaInstance = sbt.internal.inc.ScalaInstance
  val Load = sbt.dbuild.hack.DbuildHack.Load
  val applyCross = sbt.dbuild.hack.DbuildHack.applyCross
  def defaultID(base: File, prefix: String = "default") =
   sbt.dbuild.hack.DbuildHack.defaultID(base, prefix)

  val Reapply:ReapplyInterface = {
    val ru = scala.reflect.runtime.universe
    val rm = ru.runtimeMirror(Load.getClass.getClassLoader)
    val im = rm.reflect(Load)
    val reapplySymbol = ru.typeOf[Load.type].decl(ru.TermName("reapply")).asMethod
    val reapply = im.reflectMethod(reapplySymbol)
    if (reapplySymbol.paramLists(0).size == 2) {
      new Reapply2
    } else {
      /*
      package com.typesafe.dbuild.adapter
      class Reapply3 extends ReapplyInterface {
        def reapplySettings(newSettings: Seq[sbt.Def.Setting[_]],
          structure: sbt.internal.BuildStructure,
          log: sbt.util.Logger)(implicit display: sbt.util.Show[sbt.Def.ScopedKey[_]]): sbt.internal.BuildStructure = {
          sbt.dbuild.hack.DbuildHack.Load.reapply(newSettings, structure, log)(display)
        }
      }
      */
      val reapply3Bytecode = """
      yv66vgAAADQAOwEAJGNvbS90eXBlc2FmZS9kYnVpbGQvYWRhcHRlci9SZWFwcGx5MwcAAQEALGNv
      bS90eXBlc2FmZS9kYnVpbGQvYWRhcHRlci9SZWFwcGx5SW50ZXJmYWNlBwADAQANQWRhcHRlci5z
      Y2FsYQEAHkxzY2FsYS9yZWZsZWN0L1NjYWxhU2lnbmF0dXJlOwEABWJ5dGVzAQJsBgF5M0EhAQIB
      FwlBIStaMXFhMkw4RwMCBAkFORENWjFxaRYUKEJBAwcDGSEnLV41bUkqEXEBQwEJaWYEWG1dMWdL
      KgkRIkECZF82HAFhBQIBGUERUUJEBwIFJRFxQgECESUWDAcPHTd6EzokWE0dNGJHFkRRIQUBBQJJ
      CWEBUDVvU1J0RCNBChEFNQEBIkILAQkDMRJhBDpmQ0IESC5fKmZpUkxnblo6FQldKXUrFwsDMQEC
      IiEHEA4DaVEhYQcPAhEldEcvGjpvQzJUESFIAQRnCiQYQkEQGwU5EVUvGzdlJ1IUWG8ZO3ZlFkRR
      IQkLQQQJCnEBWjV0YTIMFxBFAiRNIWoRAQoGA0txCUEhHjtqWSYRcQUKAgUnInx3Dw0CKnNBGSFG
      TRwPBS0CZEIBFzAbBWkjQgEYCwMZYSRvXDh1fSUJUSQDAjI5BRlBKRo0CgVNIiQhQypkX0IsR21T
      M3oTCSlkRwEDSl0mJChCQRMbIQlBFAgEAQUTaQITESFBAQYDWSRhQTAlaUURQUgRCQN7AWsRQRAG
      An8FKTFvWTFtQyYREUkQAggdPiQILhs4aCEJaTQpAwJFfQkZEUlcPQkLGSMCGUEkAhc5LHdvVTN1
      aSZ0d20dCQQRNgNmQkElTB0JYSMqQwFAEwlhZShBBHFDDlwXbVozCgU5eyVhQSpmYyoRQUoQGQMj
      VgMyQQsqVRMJGUZHQQRUS1IkGE5cNBEFYSpGIQMsRgMDBQlRIQE8BQ15RmUNBQYxUgENAUcBCmdS
      FFhvGTt2ZRZEUUEXC0ECbQsxAVw4aCEJGUMsAwJeSQkxQWpcNGhLSgQBACBzYnQvaW50ZXJuYWwv
      dXRpbC9Jbml0JFNjb3BlZEtleQcACQEAFnNidC9pbnRlcm5hbC91dGlsL0luaXQHAAsBAAlTY29w
      ZWRLZXkBAB5zYnQvaW50ZXJuYWwvdXRpbC9Jbml0JFNldHRpbmcHAA4BAAdTZXR0aW5nAQAPcmVh
      cHBseVNldHRpbmdzAQByKExzY2FsYS9jb2xsZWN0aW9uL1NlcTtMc2J0L2ludGVybmFsL0J1aWxk
      U3RydWN0dXJlO0xzYnQvdXRpbC9Mb2dnZXI7THNidC91dGlsL1Nob3c7KUxzYnQvaW50ZXJuYWwv
      QnVpbGRTdHJ1Y3R1cmU7AQALbmV3U2V0dGluZ3MBAAlzdHJ1Y3R1cmUBAANsb2cBAAdkaXNwbGF5
      AQAbc2J0L2RidWlsZC9oYWNrL0RidWlsZEhhY2skBwAXAQAHTU9EVUxFJAEAHUxzYnQvZGJ1aWxk
      L2hhY2svRGJ1aWxkSGFjayQ7DAAZABoJABgAGwEABExvYWQBABYoKUxzYnQvaW50ZXJuYWwvTG9h
      ZCQ7DAAdAB4KABgAHwEAEnNidC9pbnRlcm5hbC9Mb2FkJAcAIQEAB3JlYXBwbHkMACMAEgoAIgAk
      AQAEdGhpcwEAJkxjb20vdHlwZXNhZmUvZGJ1aWxkL2FkYXB0ZXIvUmVhcHBseTM7AQAWTHNjYWxh
      L2NvbGxlY3Rpb24vU2VxOwEAHUxzYnQvaW50ZXJuYWwvQnVpbGRTdHJ1Y3R1cmU7AQARTHNidC91
      dGlsL0xvZ2dlcjsBAA9Mc2J0L3V0aWwvU2hvdzsBAAY8aW5pdD4BAAMoKVYMACwALQoABAAuAQAE
      Q29kZQEAEkxvY2FsVmFyaWFibGVUYWJsZQEAD0xpbmVOdW1iZXJUYWJsZQEACVNpZ25hdHVyZQEA
      2ChMc2NhbGEvY29sbGVjdGlvbi9TZXE8THNidC9pbnRlcm5hbC91dGlsL0luaXQ8THNidC9TY29w
      ZTs+LlNldHRpbmc8Kj47PjtMc2J0L2ludGVybmFsL0J1aWxkU3RydWN0dXJlO0xzYnQvdXRpbC9M
      b2dnZXI7THNidC91dGlsL1Nob3c8THNidC9pbnRlcm5hbC91dGlsL0luaXQ8THNidC9TY29wZTs+
      LlNjb3BlZEtleTwqPjs+OylMc2J0L2ludGVybmFsL0J1aWxkU3RydWN0dXJlOwEAEE1ldGhvZFBh
      cmFtZXRlcnMBAApTb3VyY2VGaWxlAQAMSW5uZXJDbGFzc2VzAQAZUnVudGltZVZpc2libGVBbm5v
      dGF0aW9ucwEAD1NjYWxhSW5saW5lSW5mbwEACFNjYWxhU2lnACEAAgAEAAAAAAACAAEAEQASAAMA
      MAAAAGEABQAFAAAAD7IAHLYAICssLRkEtgAlsAAAAAIAMQAAADQABQAAAA8AJgAnAAAAAAAPABMA
      KAABAAAADwAUACkAAgAAAA8AFQAqAAMAAAAPABYAKwAEADIAAAAGAAEAAAAmADMAAAACADQANQAA
      ABEEABMAEAAUABAAFQAQABYAEAABACwALQABADAAAAAvAAEAAQAAAAUqtwAvsQAAAAIAMQAAAAwA
      AQAAAAUAJgAnAAAAMgAAAAYAAQAAACIABQA2AAAAAgAFADcAAAASAAIACgAMAA0AAQAPAAwAEAAB
      ADgAAAALAAEABgABAAdzAAgAOQAAAA4BAAACACwALQAAEQASAAA6AAAAAwUAAA==
      """
      val reapply3Bytes = java.util.Base64.getMimeDecoder.decode(reapply3Bytecode)
      class Reapply3ClassLoader(s:ClassLoader) extends ClassLoader(s)  {
        def defineClass(name:String):Class[_] = {
            defineClass(name, reapply3Bytes, 0, reapply3Bytes.length)
        }
      }
      val cl = new Reapply3ClassLoader(getClass.getClassLoader)
      val reapply3Class = cl.defineClass("com.typesafe.dbuild.adapter.Reapply3")
      reapply3Class.getConstructor().newInstance().asInstanceOf[ReapplyInterface]
    }
  }

  def reapplySettings(newSettings: Seq[sbt.Def.Setting[_]],
    structure: sbt.internal.BuildStructure,
    log: sbt.util.Logger)(implicit display: sbt.util.Show[sbt.Def.ScopedKey[_]]): sbt.internal.BuildStructure = {
      Reapply.reapplySettings(newSettings, structure, log)(display)
    }

// These bits are inappropriately copied from various versions of zinc; some have been
// removed and some made private, but we need them.
// See: internal/zinc-classpath/src/main/scala/sbt/internal/inc/ScalaInstance.scala

  import java.net.{ URL, URLClassLoader }

  /** Runtime exception representing a failure when finding a `ScalaInstance`. */
  class InvalidScalaInstance(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

  /** The prefix being used for Scala artifacts name creation. */
  val VersionPrefix = "version "

  private def slowActualVersion(scalaLoader: ClassLoader)(label: String) = {
    val scalaVersion = {
      try {
        // Get scala version from the `Properties` file in Scalac
        Class
          .forName("scala.tools.nsc.Properties", true, scalaLoader) 
          .getMethod("versionString")
          .invoke(null)
          .toString
      } catch {
        case cause: Exception =>
          val msg = s"Scala instance doesn't exist or is invalid: $label"
          throw new InvalidScalaInstance(msg, cause)
      }
    }

    if (scalaVersion.startsWith(VersionPrefix))
      scalaVersion.substring(VersionPrefix.length)
    else scalaVersion
  }  

  private def fastActualVersion(scalaLoader: ClassLoader): String = {
    val stream = scalaLoader.getResourceAsStream("compiler.properties")
    try {
      val props = new java.util.Properties  
      props.load(stream)
      props.getProperty("version.number")  
    } finally stream.close()
  }

  /** Gets the version of Scala in the compiler.properties file from the loader.*/
  private def actualVersion(scalaLoader: ClassLoader)(label: String) = {
    try fastActualVersion(scalaLoader)
    catch { case e: Exception => slowActualVersion(scalaLoader)(label) }
  }

  private def scalaLoader(launcher: xsbti.Launcher): Seq[File] => ClassLoader = { jars =>
    import java.net.{ URL, URLClassLoader }
    new URLClassLoader(
      jars.map(_.toURI.toURL).toArray[URL],
      launcher.topLoader
    )
  }

  private def scalaInstanceHelper(libraryJar: File, compilerJar: File, extraJars: File*)(classLoader: List[File] => ClassLoader): ScalaInstance =
    {
      val loader = classLoader(libraryJar :: compilerJar :: extraJars.toList)
      val version = actualVersion(loader)(" (library jar  " + libraryJar.getAbsolutePath + ")")
      new ScalaInstance(version, loader, libraryJar, compilerJar, extraJars.toArray, None)
    }

  def scalaInstance(libraryJar: File, compilerJar: File, launcher: xsbti.Launcher, extraJars: File*): ScalaInstance =
    scalaInstanceHelper(libraryJar, compilerJar, extraJars: _*)(scalaLoader(launcher))

}
}
