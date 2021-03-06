package com.edu.neu.csye7200.finalproject.configure

/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-16
  * Time: 18:16
  * Description: This object store the file path. This can make is easier
  *    to modify the file path just by changing the path in this object
  */

object FileConfig {
  val dataDir = "input/"
  val ratingFile = dataDir + "ratings_small.csv"
  val movieFile = dataDir + "movies_metadata.csv"
  val linkFile = dataDir + "links_small.csv"
  val keywordsFile = dataDir + "keywords.csv"
  val creditFIle = dataDir + "credits.csv"
  val bookRating = dataDir + "book_rating.csv"
  val userRating = dataDir + "user_rating.csv"
}
