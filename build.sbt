import _root_.sbtassembly.Plugin.PathList
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

assemblySettings

name := "mapDBLearning"

version := "1.0"

scalaVersion := "2.11.6"

test in assembly :={}

scalacOptions ++= Seq("-unchecked", "-Xlint", "-deprecation", "-Yno-adapted-args", "-feature", "-Xfatal-warnings")

fork := true

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

