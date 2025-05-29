// Databricks notebook source
// MAGIC %md
// MAGIC #### Create DataFrames from the Soccer data files

// COMMAND ----------

val worldcupHistoryDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/Worldcup_History.csv")

worldcupHistoryDF.createOrReplaceTempView("Worldcup_History")

val countryDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/Country.csv")

countryDF.createOrReplaceTempView("Country")

val match_resultsDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/Match_results.csv")

match_resultsDF.createOrReplaceTempView("Match_results")

val playersDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/Players.csv")

playersDF.createOrReplaceTempView("Players")

val player_Assists_GoalsDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/Player_Assists_Goals.csv")

player_Assists_GoalsDF.createOrReplaceTempView("Player_Assists_Goals")

val player_CardsDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/Player_Cards.csv")

player_CardsDF.createOrReplaceTempView("Player_Cards")

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.	Retrieve the list of country names that have won a world cup.

// COMMAND ----------

spark.sql("""
 SELECT  DISTINCT Winner 
  FROM  worldcup_history ;
""").show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.	Retrieve the list of country names that have won a world cup and the number of world cup each has won in descending order.

// COMMAND ----------

spark.sql("""
SELECT  Winner,
        COUNT(*) AS World_Cup_Wins
  FROM  worldcup_History
 GROUP  BY winner
 ORDER  BY world_cup_wins DESC;
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.	List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.

// COMMAND ----------

spark.sql("""
SELECT  Capital 
  FROM  country
 WHERE  population > 100
 ORDER  BY population ASC;
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.	List the Name of the stadium which has hosted a match where the number of goals scored by a single team was greater than 4.

// COMMAND ----------

spark.sql("""
 SELECT  DISTINCT Stadium 
   FROM  match_results
  WHERE  score1 > 4 OR score2 > 4;
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.	List the names of all the cities which have the name of the Stadium starting with “Estadio”.

// COMMAND ----------

spark.sql("""
 SELECT  DISTINCT City 
    FROM  match_results
   WHERE  stadium LIKE 'Estadio%';
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.	List all stadiums and the number of matches hosted by each stadium.

// COMMAND ----------

spark.sql("""
 SELECT  Stadium, 
           COUNT(*) AS matches_hosted
     FROM  match_results
    GROUP  BY stadium
    ORDER  BY matches_hosted DESC;
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 7.	List the First Name, Last Name and Date of Birth of Players whose heights are greater than 198 cms.

// COMMAND ----------

spark.sql("""
 SELECT  fname AS First_Name, 
            lname AS Last_Name, 
			BirthDate
      FROM  players
     WHERE  height > 198;
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 8.	List the Fname, Lname, Position and No of Goals scored by the Captain of a team who has more than 2 Yellow cards or 1 Red card.

// COMMAND ----------

spark.sql("""
 SELECT  P.fname       AS Player_First_Name, 
			  P.lname       AS Player_Last_Name, 
			  P.Position    AS Player_Position,
              G.goals       AS Goals_cored
        FROM  players P
	    JOIN  player_assists_goals G ON P.PID = G.PID
	    JOIN  player_cards C ON P.PID = C.PID
       WHERE  P.iscaptain = 'TRUE'
	          AND (C.no_of_yellow_cards > 2 OR C.no_of_red_cards = 1);
""").show()