package org.teckhooi

import cats.effect.kernel.Resource
import cats.effect.*
import cats.implicits.*

import java.io.{BufferedReader, FileReader}

object Cats_1BRC extends IOApp:
  def openFile[F[_]: Sync](filename: String): F[BufferedReader] =
    Sync[F].blocking(BufferedReader(FileReader(filename)))

  def readLine[F[_]: Sync](reader: BufferedReader): F[Map[String, City]] =
    def _readLine(
        reader: BufferedReader,
        acc: Ref[F, Map[String, City]]
    ): F[Map[String, City]] =
      for
        line <- Sync[F].blocking(reader.readLine())
        result <- Option(line).fold(acc.get)(s =>
          val ndx  = s.indexOf(';')
          val name = s.substring(0, ndx)
          val curr = line.substring(ndx + 1).toDouble
          acc.update(m =>
            m + (name -> m
              .get(name)
              .fold(City(name, curr, curr, curr, 1))(c =>
                City(
                  name,
                  math.max(c.max, curr),
                  math.min(c.min, curr),
                  c.total + curr,
                  c.count + 1
                )
              ))
          ) *> _readLine(reader, acc)
        )
      yield result

    for
      acc    <- Ref[F].of(Map.empty[String, City])
      result <- _readLine(reader, acc)
    yield result

  override def run(args: List[String]): IO[ExitCode] =
    if args.isEmpty then IO.raiseError(Exception("Please provide the measurement filename"))
    else
      ((for {
        in <- Resource.make[IO, BufferedReader](openFile(args.head))(reader =>
          IO.blocking(reader.close())
        )
      } yield in).use(readLine) >>= (m => IO.println(s"${m.values.toVector.sortBy(_.name).mkString("{", ", ", "}")}")))
        .as(ExitCode.Success)
