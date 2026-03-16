// ============================================================
// IT 462 – Big Data Systems
// Phase 5: Machine Learning with Apache Spark MLlib
// Chicago District-Level Crime Count Forecasting
// Semester 2, 1447H (January–May 2026)
// Group 2: Raghad Almutairi, Sarah Alruwayte,
//           Norah Alfaheed, Atheer Budie
// ============================================================

// ============================================
// STEP 1: Load Preprocessed Dataset
// ============================================

import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

val preprocessedDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/Users/raghadfares/Desktop/BigData/preprocessed_Data.csv")

println(s"Preprocessed dataset rows: ${preprocessedDF.count()}")
println(s"Preprocessed dataset columns: ${preprocessedDF.columns.length}")
preprocessedDF.printSchema()

// ============================================
// STEP 2: Verify Data Quality Before ML
// ============================================

println("Null counts in key columns:")
preprocessedDF.select(
  preprocessedDF.columns.filter(c =>
    Seq("total_crime_count","prev_week_total",
        "month","week_no","district_indexed").contains(c)
  ).map(c => sum(col(c).isNull.cast("int")).alias(c)): _*
).show()

preprocessedDF.select("total_crime_count")
  .summary("min", "max", "mean", "stddev")
  .show()

// ============================================
// STEP 3: Define Feature Columns
// ============================================

val crimeTypeCols = Array(
  "ARSON", "ASSAULT", "BATTERY", "BURGLARY",
  "CONCEALED CARRY LICENSE VIOLATION", "CRIMINAL DAMAGE",
  "CRIMINAL SEXUAL ASSAULT", "CRIMINAL TRESPASS",
  "DECEPTIVE PRACTICE", "GAMBLING", "HOMICIDE",
  "HUMAN TRAFFICKING", "INTERFERENCE WITH PUBLIC OFFICER",
  "INTIMIDATION", "KIDNAPPING", "LIQUOR LAW VIOLATION",
  "MOTOR VEHICLE THEFT", "NARCOTICS", "NON-CRIMINAL",
  "OBSCENITY", "OFFENSE INVOLVING CHILDREN",
  "OTHER NARCOTIC VIOLATION", "OTHER OFFENSE",
  "PROSTITUTION", "PUBLIC INDECENCY",
  "PUBLIC PEACE VIOLATION", "ROBBERY", "SEX OFFENSE",
  "STALKING", "THEFT", "WEAPONS VIOLATION"
)

// Full feature set: 4 temporal/spatial + 31 crime-type columns
val featureCols = Array(
  "month", "week_no", "district_indexed", "prev_week_total"
) ++ crimeTypeCols

// Reduced feature set: temporal + spatial + lag only (honest forecasting)
val reducedFeatureCols = Array(
  "month", "week_no", "district_indexed", "prev_week_total"
)

println(s"Full feature columns: ${featureCols.length}")
println(s"Reduced feature columns: ${reducedFeatureCols.length}")

// ============================================
// STEP 4: Assemble Full Features
// Scaling is deferred until AFTER the split
// to prevent data leakage as planned in Phase 2
// ============================================

val cleanDF = preprocessedDF.na.fill(0)

val assembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features_raw")

val assembledDF = assembler.transform(cleanDF)

println("Feature assembly complete.")
println("Note: Scaling will be applied AFTER train/test split")
println("      to prevent data leakage from test set into scaler.")
assembledDF.select("district", "week_start", "total_crime_count")
  .show(3, truncate = false)

// ============================================
// STEP 5: Train/Test Split (70/30, seed = 42)
// Split happens BEFORE scaling — this is critical
// to avoid data leakage
// ============================================

val Array(trainRaw, testRaw) = assembledDF.randomSplit(Array(0.7, 0.3), seed = 42)

println(s"Training set size: ${trainRaw.count()} rows")
println(s"Test set size:     ${testRaw.count()} rows")
println(s"Total:             ${trainRaw.count() + testRaw.count()} rows")

// ============================================
// STEP 6: Apply StandardScaler
// Fitted on TRAINING DATA ONLY
// Then applied to both train and test sets
// This fulfills the Phase 2 promise of
// methodological integrity and no data leakage
// ============================================

val scaler = new StandardScaler()
  .setInputCol("features_raw")
  .setOutputCol("features")
  .setWithMean(true)
  .setWithStd(true)

// FIT only on training data
val scalerModel = scaler.fit(trainRaw)

// Transform both sets using training statistics
val trainDF = scalerModel.transform(trainRaw)
val testDF  = scalerModel.transform(testRaw)

println("StandardScaler fitted on training data only.")
println("Scaler applied to both training and test sets.")
println(s"Training rows: ${trainDF.count()}")
println(s"Test rows:     ${testDF.count()}")

// ============================================
// STEP 7: Baseline Model (Mean Prediction)
// Predicts the training mean for every week
// Serves as the minimum performance threshold
// ============================================

val trainMean = trainDF
  .select(mean("total_crime_count"))
  .first()
  .getDouble(0)

println(s"Training set mean crime count: $trainMean")

val baselineTestDF = testDF
  .withColumn("prediction", lit(trainMean))

val evaluatorRMSE = new RegressionEvaluator()
  .setLabelCol("total_crime_count")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val evaluatorMAE = new RegressionEvaluator()
  .setLabelCol("total_crime_count")
  .setPredictionCol("prediction")
  .setMetricName("mae")

val evaluatorR2 = new RegressionEvaluator()
  .setLabelCol("total_crime_count")
  .setPredictionCol("prediction")
  .setMetricName("r2")

val baselineRMSE = evaluatorRMSE.evaluate(baselineTestDF)
val baselineMAE  = evaluatorMAE.evaluate(baselineTestDF)
val baselineR2   = evaluatorR2.evaluate(baselineTestDF)

println(s"Baseline RMSE: $baselineRMSE")
println(s"Baseline MAE:  $baselineMAE")
println(s"Baseline R2:   $baselineR2")

// ============================================
// STEP 8: Linear Regression — Full Features
// ============================================

val lr = new LinearRegression()
  .setLabelCol("total_crime_count")
  .setFeaturesCol("features")
  .setMaxIter(100)
  .setRegParam(0.1)
  .setElasticNetParam(0.0)

val lrModel = lr.fit(trainDF)
val lrPredictions = lrModel.transform(testDF)

val lrRMSE = evaluatorRMSE.evaluate(lrPredictions)
val lrMAE  = evaluatorMAE.evaluate(lrPredictions)
val lrR2   = evaluatorR2.evaluate(lrPredictions)

println(s"Linear Regression Test  RMSE: $lrRMSE")
println(s"Linear Regression Test  MAE:  $lrMAE")
println(s"Linear Regression Test  R2:   $lrR2")
println(s"Linear Regression Train RMSE: ${lrModel.summary.rootMeanSquaredError}")
println(s"Linear Regression Train R2:   ${lrModel.summary.r2}")

lrPredictions.select(
  "district", "week_start", "total_crime_count", "prediction"
).show(10, truncate = false)

// ============================================
// STEP 9: Decision Tree — Full Features
// ============================================

val dt = new DecisionTreeRegressor()
  .setLabelCol("total_crime_count")
  .setFeaturesCol("features")
  .setMaxDepth(5)
  .setSeed(42)

val dtModel = dt.fit(trainDF)
val dtPredictions = dtModel.transform(testDF)

val dtRMSE = evaluatorRMSE.evaluate(dtPredictions)
val dtMAE  = evaluatorMAE.evaluate(dtPredictions)
val dtR2   = evaluatorR2.evaluate(dtPredictions)

println(s"Decision Tree RMSE: $dtRMSE")
println(s"Decision Tree MAE:  $dtMAE")
println(s"Decision Tree R2:   $dtR2")

dtPredictions.select(
  "district", "week_start", "total_crime_count", "prediction"
).show(10, truncate = false)

// ============================================
// STEP 10: Random Forest — Full Features
// ============================================

val rf = new RandomForestRegressor()
  .setLabelCol("total_crime_count")
  .setFeaturesCol("features")
  .setNumTrees(50)
  .setMaxDepth(5)
  .setSeed(42)

val rfModel = rf.fit(trainDF)
val rfPredictions = rfModel.transform(testDF)

val rfRMSE = evaluatorRMSE.evaluate(rfPredictions)
val rfMAE  = evaluatorMAE.evaluate(rfPredictions)
val rfR2   = evaluatorR2.evaluate(rfPredictions)

println(s"Random Forest RMSE: $rfRMSE")
println(s"Random Forest MAE:  $rfMAE")
println(s"Random Forest R2:   $rfR2")

rfPredictions.select(
  "district", "week_start", "total_crime_count", "prediction"
).show(10, truncate = false)

// ============================================
// STEP 11: Model Comparison — Full Features
// ============================================

println("=" * 65)
println(f"${"Model"}%-25s ${"RMSE"}%10s ${"MAE"}%10s ${"R2"}%10s")
println("=" * 65)
println(f"${"Baseline (Mean)"}%-25s ${baselineRMSE}%10.4f ${baselineMAE}%10.4f ${baselineR2}%10.4f")
println(f"${"Linear Regression"}%-25s ${lrRMSE}%10.4f ${lrMAE}%10.4f ${lrR2}%10.4f")
println(f"${"Decision Tree"}%-25s ${dtRMSE}%10.4f ${dtMAE}%10.4f ${dtR2}%10.4f")
println(f"${"Random Forest"}%-25s ${rfRMSE}%10.4f ${rfMAE}%10.4f ${rfR2}%10.4f")
println("=" * 65)

// ============================================
// STEP 12: Feature Importance — Full RF
// ============================================

val featureImportances = rfModel.featureImportances
val importanceWithNames = featureCols
  .zip(featureImportances.toArray)
  .sortBy(-_._2)

println("Top 15 Most Important Features (Random Forest — Full):")
println(f"${"Feature"}%-40s ${"Importance"}%10s")
println("-" * 55)
importanceWithNames.take(15).foreach { case (name, importance) =>
  println(f"${name}%-40s ${importance}%10.6f")
}

// ============================================
// STEP 13: Save Full RF Predictions to Disk
// ============================================

rfPredictions
  .select(
    "district", "week_start", "month", "week_no",
    "total_crime_count", "prediction"
  )
  .withColumn("prediction", round(col("prediction"), 2))
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv("/Users/raghadfares/Desktop/BigData_Phase5/rf_predictions_full")

println("Full RF predictions saved.")

// ============================================
// STEP 14: Reduced Feature Set
// Only temporal + spatial + lag features
// Excludes crime-type columns to avoid
// the mathematical identity problem
// ============================================

val assemblerReduced = new VectorAssembler()
  .setInputCols(reducedFeatureCols)
  .setOutputCol("features_raw_reduced")

val assembledReducedDF = assemblerReduced.transform(cleanDF)

// Split FIRST
val Array(trainReducedRaw, testReducedRaw) =
  assembledReducedDF.randomSplit(Array(0.7, 0.3), seed = 42)

// Fit scaler on training data only
val scalerReduced = new StandardScaler()
  .setInputCol("features_raw_reduced")
  .setOutputCol("features_reduced")
  .setWithMean(true)
  .setWithStd(true)

val scalerReducedModel = scalerReduced.fit(trainReducedRaw)
val trainReducedDF = scalerReducedModel.transform(trainReducedRaw)
val testReducedDF  = scalerReducedModel.transform(testReducedRaw)

println(s"Reduced training rows: ${trainReducedDF.count()}")
println(s"Reduced test rows:     ${testReducedDF.count()}")

// ============================================
// STEP 15: Train All 3 Models — Reduced Features
// This is the honest forecasting evaluation
// ============================================

// Linear Regression - Reduced
val lrReduced = new LinearRegression()
  .setLabelCol("total_crime_count")
  .setFeaturesCol("features_reduced")
  .setMaxIter(100)
  .setRegParam(0.1)
  .setElasticNetParam(0.0)

val lrReducedModel = lrReduced.fit(trainReducedDF)
val lrReducedPred  = lrReducedModel.transform(testReducedDF)

val lrReducedRMSE = evaluatorRMSE.evaluate(lrReducedPred)
val lrReducedMAE  = evaluatorMAE.evaluate(lrReducedPred)
val lrReducedR2   = evaluatorR2.evaluate(lrReducedPred)

println(s"LR Reduced RMSE: $lrReducedRMSE")
println(s"LR Reduced MAE:  $lrReducedMAE")
println(s"LR Reduced R2:   $lrReducedR2")

// Decision Tree - Reduced
val dtReduced = new DecisionTreeRegressor()
  .setLabelCol("total_crime_count")
  .setFeaturesCol("features_reduced")
  .setMaxDepth(5)
  .setSeed(42)

val dtReducedModel = dtReduced.fit(trainReducedDF)
val dtReducedPred  = dtReducedModel.transform(testReducedDF)

val dtReducedRMSE = evaluatorRMSE.evaluate(dtReducedPred)
val dtReducedMAE  = evaluatorMAE.evaluate(dtReducedPred)
val dtReducedR2   = evaluatorR2.evaluate(dtReducedPred)

println(s"DT Reduced RMSE: $dtReducedRMSE")
println(s"DT Reduced MAE:  $dtReducedMAE")
println(s"DT Reduced R2:   $dtReducedR2")

// Random Forest - Reduced
val rfReduced = new RandomForestRegressor()
  .setLabelCol("total_crime_count")
  .setFeaturesCol("features_reduced")
  .setNumTrees(50)
  .setMaxDepth(5)
  .setSeed(42)

val rfReducedModel = rfReduced.fit(trainReducedDF)
val rfReducedPred  = rfReducedModel.transform(testReducedDF)

val rfReducedRMSE = evaluatorRMSE.evaluate(rfReducedPred)
val rfReducedMAE  = evaluatorMAE.evaluate(rfReducedPred)
val rfReducedR2   = evaluatorR2.evaluate(rfReducedPred)

println(s"RF Reduced RMSE: $rfReducedRMSE")
println(s"RF Reduced MAE:  $rfReducedMAE")
println(s"RF Reduced R2:   $rfReducedR2")

// ============================================
// STEP 16: Full Comparison Table
// ============================================

println("\n" + "=" * 75)
println(f"${"Model"}%-35s ${"RMSE"}%10s ${"MAE"}%10s ${"R2"}%10s")
println("=" * 75)
println(f"${"Baseline (Mean)"}%-35s ${baselineRMSE}%10.4f ${baselineMAE}%10.4f ${baselineR2}%10.4f")
println("--- Full Feature Set (35 features) ---")
println(f"${"LR - Full Features"}%-35s ${lrRMSE}%10.4f ${lrMAE}%10.4f ${lrR2}%10.4f")
println(f"${"DT - Full Features"}%-35s ${dtRMSE}%10.4f ${dtMAE}%10.4f ${dtR2}%10.4f")
println(f"${"RF - Full Features"}%-35s ${rfRMSE}%10.4f ${rfMAE}%10.4f ${rfR2}%10.4f")
println("--- Reduced Feature Set (4 features) ---")
println(f"${"LR - Reduced Features"}%-35s ${lrReducedRMSE}%10.4f ${lrReducedMAE}%10.4f ${lrReducedR2}%10.4f")
println(f"${"DT - Reduced Features"}%-35s ${dtReducedRMSE}%10.4f ${dtReducedMAE}%10.4f ${dtReducedR2}%10.4f")
println(f"${"RF - Reduced Features"}%-35s ${rfReducedRMSE}%10.4f ${rfReducedMAE}%10.4f ${rfReducedR2}%10.4f")
println("=" * 75)

// ============================================
// STEP 17: Feature Importance — Reduced RF
// ============================================

val rfReducedImportances = rfReducedModel.featureImportances
val reducedImportanceWithNames = reducedFeatureCols
  .zip(rfReducedImportances.toArray)
  .sortBy(-_._2)

println("Feature Importance (Reduced Random Forest):")
println(f"${"Feature"}%-25s ${"Importance"}%10s")
println("-" * 40)
reducedImportanceWithNames.foreach { case (name, importance) =>
  println(f"${name}%-25s ${importance}%10.6f")
}

// ============================================
// STEP 18: Save Reduced RF Predictions to Disk
// ============================================

rfReducedPred
  .select(
    "district", "week_start", "month", "week_no",
    "total_crime_count", "prediction"
  )
  .withColumn("prediction", round(col("prediction"), 2))
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv("/Users/raghadfares/Desktop/BigData_Phase5/rf_predictions_reduced")

println("Reduced RF predictions saved successfully.")
println("Path: /Users/raghadfares/Desktop/BigData_Phase5/rf_predictions_reduced")
