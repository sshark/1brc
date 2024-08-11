package org.teckhooi

import java.io.{BufferedReader, FileReader}
import scala.collection.mutable
import scala.util.Using

object Plain_1BRC {
  val stationMap: mutable.Map[String, City] = mutable.Map.empty

  def main(args: Array[String]): Unit =
    if args.isEmpty then throw Exception("Please provide the measurement filename")
    else
      Using.resource(new BufferedReader(new FileReader(args.head))) {
        reader => Iterator.continually(reader.readLine()).takeWhile(_ != null).foreach(line =>
          val ndx  = line.indexOf(';')
          val name = line.substring(0, ndx)
          val curr = line.substring(ndx + 1).toDouble

          stationMap += (name -> stationMap
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
      }

      println(s"${stationMap.values.toVector.sortBy(_.name).mkString("{", ", ", "}")}")
}