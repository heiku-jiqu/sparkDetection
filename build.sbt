// give the user a nice default project!

val sparkVersion = settingKey[String]("Spark version")

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "io.github.heikujiqu",
      scalaVersion := "2.12.19"
    )),
    name := "sparkDetection",
    version := "0.0.1",

    sparkVersion := "3.5.1",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    Test / parallelExecution := false,
    fork := true,

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",

      "org.scalatest" %% "scalatest" % "3.2.2" % "test",
      "org.scalacheck" %% "scalacheck" % "1.15.2" % "test",
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    Compile / run := Defaults.runTask(Compile / fullClasspath , Compile / run / mainClass, Compile / run / runner).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
    ),

    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )
