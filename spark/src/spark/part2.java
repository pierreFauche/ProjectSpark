
//  il faut impérativement lancer spark shell avec le paramètre packages com.databricks...
// si l'on souhaite de servire par la suite de ce paquets pour exporter nos resultats en csv 
//./bin/spark-shell --packages com.databricks:spark-csv_2.11:1.2.0 



//notre requete (nous avons juste regroupé les crimes par description car la question prétais a confusion)
castsDF.groupBy('crimedescr).count().orderBy(desc("count")).show

//pour exporter resultat en csv
castsDF.groupBy('crimedescr).count().orderBy(desc("count")).repartition(1).write.format("com.databricks.spark.csv").save("CSVresultat2.csv")

   
   

   