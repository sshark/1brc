package org.teckhooi

import zio.*
import java.io.{BufferedReader, FileReader}

object ZIO_1BRC extends ZIOAppDefault:

  def readLine(
      reader: BufferedReader,
      acc: Map[String, City] = Map.empty
  ): Task[Map[String, City]] =
    for
      line <- ZIO.attemptBlockingIO(reader.readLine())
      result <- Option(line).fold(ZIO.succeed(acc))(s =>
        val ndx  = s.indexOf(';')
        val name = s.substring(0, ndx)
        val curr = line.substring(ndx + 1).toDouble
        readLine(
          reader,
          acc + (name -> acc
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
        )
      )
    yield result

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] = for
    args <- getArgs
    in <-
      if args.isEmpty then ZIO.fail(Exception("Please provide the measurement filename"))
      else
        ZIO.acquireRelease(ZIO.attempt(BufferedReader(FileReader(args.head))))(file =>
          ZIO.succeed(file.close())
        )
    result <- readLine(in)
    _ <- Console.printLine(s"${result.values.toVector.sortBy(_.name).mkString("{", ", ", "}")}")
  yield ()
