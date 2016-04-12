import java.io.File

import scala.util.parsing.json.JSON

val lines = scala.io.Source.fromFile("D:/wspace/data/isis_tiny.data").getLines()

val jtweets = lines.map(JSON.parseFull(_))

println(jtweets.toList(0).get)

