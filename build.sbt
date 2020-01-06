import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",   // source files are in UTF-8
  "-deprecation",         // warn about use of deprecated APIs
  "-unchecked",           // warn about unchecked type parameters
  "-feature",             // warn about misused language features
  "-language:higherKinds",// allow higher kinded types without `import scala.language.higherKinds`
  "-Ypartial-unification" // allow the compiler to unify type constructors of different arities
)

lazy val root = (project in file("."))
  .settings(name := "crdt_zio", libraryDependencies ++= Seq(scalaTest % Test))

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.typelevel"   %% "kind-projector" % "0.10.3")
addCompilerPlugin("org.scalamacros" %% "paradise"       % "2.1.1" cross CrossVersion.full)

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
