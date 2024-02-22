import org.apache.spark.{SparkConf, SparkContext}

object Query3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Query 3")
    val sc = new SparkContext(conf)

    val allPeople = sc.textFile("src/main/data/people/PEOPLE-SOME-INFECTED-large.csv").map(line => line.split(","))

    val infectedPeople = allPeople.filter(person => person(3) == "yes")

    val allPeopleBr = sc.broadcast(allPeople.collect())

    // Calculate close contacts for each infected person
    val closeContactsCount = infectedPeople.map { infectedPerson =>
      val infectedPersonId = infectedPerson(0)
      val infectedPersonCoordinates = (infectedPerson(1).toInt, infectedPerson(2).toInt)

      val countCloseContacts = allPeopleBr.value.filter { person =>
        val personId = person(0)
        val personCoordinates = (person(1).toInt, person(2).toInt)

        personId != infectedPersonId && calculateDistance(personCoordinates, infectedPersonCoordinates) <= 6
      }.length

      (infectedPersonId, countCloseContacts)
    }

    // Print the result
    closeContactsCount.collect().foreach(println)

    sc.stop()
  }

  def calculateDistance(p1: (Int, Int), p2: (Int, Int)): Double = {
    math.sqrt(math.pow(p1._1 - p2._1, 2) + math.pow(p1._2 - p2._2, 2))
  }
}