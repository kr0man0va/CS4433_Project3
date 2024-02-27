import org.apache.spark.{SparkConf, SparkContext}

object Query3_2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Query 3")
    val sc = new SparkContext(conf)

    val people_some = sc.textFile("src/main/data/people/PEOPLE-SOME-INFECTED-large.csv").map(line => line.split(","))

    // Original data
    val initialRDD = people_some.map { person =>
      val personId = person(0)
      val personCoordinates = (person(1).toInt, person(2).toInt)
      val personStatus = person(3)
      (personId, personCoordinates, personStatus)
    }

    // Only those that are infected
    val infectedRDD = initialRDD.filter { case (_, _, personStatus) =>
      personStatus == "yes"
    }

    val infectedBroadcast = sc.broadcast(infectedRDD.collect())

    // Find close contacts for each infected
    val closeContactRDD = initialRDD.flatMap { person =>
      val personCoordinates = (person._2)

      val closeContacts = infectedBroadcast.value
        .filter(infected => (calculateDistance(personCoordinates, infected._2) <= 6) && (personCoordinates != infected._2))

      closeContacts.map { infectedPerson =>
        (infectedPerson._1, 1)
      }
    }

    // Add up close contacts for each infected
    val closeContactCountRDD = closeContactRDD.reduceByKey(_ + _)

    closeContactCountRDD.foreach(println)
  }

  def calculateDistance(p1: (Int, Int), p2: (Int, Int)): Double = {
    math.sqrt(math.pow(p1._1 - p2._1, 2) + math.pow(p1._2 - p2._2, 2))
  }

}