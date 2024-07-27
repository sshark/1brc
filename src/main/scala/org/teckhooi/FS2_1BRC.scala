package org.teckhooi

import cats.effect.*
import cats.syntax.all.*
import fs2.io.file.{Files, Path}

object FS2_1BRC extends IOApp:
  def readLine[F[_]: Files: Concurrent](path: Path): F[Map[String, City]] =
    def _readLine(
        path: Path,
        acc: Ref[F, Map[String, City]]
    ): F[Map[String, City]] =
      Files[F]
        .readUtf8Lines(path)
        .evalMap(s =>
          if s.isEmpty then Concurrent[F].unit
          else
            val ndx  = LocalUtils.indexOf(s, ';')
            val name = s.substring(0, ndx)
            val curr = s.substring(ndx + 1).toDouble
            acc.update(m =>
              m + (name -> m
                .get(name)
                .map(c =>
                  City(
                    name,
                    math.max(c.max, curr),
                    math.min(c.min, curr),
                    c.total + curr,
                    c.count + 1
                  )
                )
                .getOrElse(City(name, curr, curr, curr, 1)))
            )
        )
        .compile
        .drain *> acc.get

    for
      acc    <- Ref[F].of(Map.empty[String, City])
      result <- _readLine(path, acc)
    yield result

  override def run(args: List[String]): IO[ExitCode] =
    if (args.isEmpty) IO.println("Please provide a CSV filename").as(ExitCode.Error)
    else
      readLine[IO](Path(args.head))
        .flatMap(m => IO.println(s"${m.values.toVector.sortBy(_.name).mkString("{", ", ", "}")}"))
        .as(ExitCode.Success)
