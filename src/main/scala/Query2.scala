import org.apache.spark.{SparkConf, SparkContext}

object Query2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Query 2")
    val sc = new SparkContext(conf)

    val people = sc.textFile("src/main/data/people/PEOPLE-large.csv").map(line => line.split(","))
    val infected = sc.textFile("src/main/data/people/INFECTED-small.csv").map(line => line.split(","))

    val infectedBroadcast = sc.broadcast(infected.collect())

    // Find close contacts for each person
    val closeContactRDD = people.flatMap { person =>
      val personId = person(0)
      val personCoordinates = (person(1).toInt, person(2).toInt)

      val infectedPeople = infectedBroadcast.value
        .filter{ infected =>
          val infectedCoordinates = (infected(1).toInt, infected(2).toInt)
          infectedCoordinates != personCoordinates &&
          calculateDistance(personCoordinates, (infected(1).toInt, infected(2).toInt)) <= 6
        }

      infectedPeople.headOption.map(_ => personId)
    }.distinct()

    closeContactRDD.collect().foreach(println)

    sc.stop();

  }

  def calculateDistance(p1: (Int, Int), p2: (Int, Int)): Double = {
    math.sqrt(math.pow(p1._1 - p2._1, 2) + math.pow(p1._2 - p2._2, 2))
  }

}
