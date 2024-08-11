enablePlugins(PackPlugin)

val scala3Version = "3.4.2"

lazy val root = project
  .in(file("."))
  .settings(
    name         := "1brc-scala",
    fork         := true,
    version      := "1.0.0-SNAPSHOT",
    scalaVersion := scala3Version,
    compileOrder := CompileOrder.JavaThenScala,
    javacOptions ++= Seq(
      "-Xlint:preview",
      "--release",
      "21",
      "--enable-preview",
    ),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect"      % "3.5.4",
      "co.fs2"        %% "fs2-core"         % "3.10.2",
      "co.fs2"        %% "fs2-io"           % "3.10.2",
      "dev.zio"       %% "zio"              % "2.1.6",
      "dev.zio"       %% "zio-streams"      % "2.1.6",
      "dev.zio"       %% "zio-interop-cats" % "23.1.0.2",
      "org.scalameta" %% "munit"            % "1.0.0" % Test
    )
  )
