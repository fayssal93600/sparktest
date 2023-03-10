package com.carmignac.icare.elysee.project.render

import com.carmignac.icare.elysee.project.connector.PostgresConnector.{getBusinessConnection, getRenderConnection}
import com.carmignac.icare.elysee.utils.AdlsUtils.mountContainer
import com.carmignac.icare.elysee.utils.{AdlsUtils, SparkObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.carmignac.icare.elysee.utils._
import com.carmignac.icare.elysee.utils.ProductUtils._
import com.carmignac.icare.elysee.utils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.carmignac.icare.elysee.utils._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object BiStagingToRender extends SparkObject {
  import spark.implicits._
  //spark.conf.set("spark.sql.shuffle.partitions", "32") // AQE enabled ?


  val dataSource = "morningstar"
  val datasetName = "wendy"
  //val stagingDeltaPath = "/mnt/staging/"+ dataSource + "/" + datasetName + "/deltatable"
  val transcodificationPath = "/configuration/BIDigitalisation/transcoIASector.csv.txt"
  val biTableName = s"""marketingbi."MorningstarWendyMetrics""""


  def mountContainers: Unit = {
    mountContainer("raw")
    mountContainer("staging")
    mountContainer("configuration")
    mountContainer("wip")
  }

  def readLastStagingData(path: String): DataFrame = {

    val stagingDf = getStagingDataBi(dataSource, datasetName)
      stagingDf.select($"data.*", $"sourceFileName")
  }

  def pivotStagingDf(stagingDf: DataFrame): DataFrame = {

    val firstPartDf = stagingDf.withColumn("morningstarRating", regexp_replace(col("morningstarRatingOverall"), "Ù", "*"))
      .withColumn("currency", regexp_replace(col("currency"), "Euro", "EUR"))
      .withColumn("currency", regexp_replace(col("currency"), "Pound Sterling", "GBP"))
      .withColumn("calculationDate", col("CalculatedOn"))
      .withColumn("exportDate", col("exportedOn"))
      .withColumn("reportName", col("scopeName"))
      .withColumn("reportCurrency", col("currency"))
      .withColumn("isin", col("isin"))
      .withColumn("groupInvestment", col("groupInvestment"))
      .withColumn("peersValue", when(col("peerGroup").startsWith("Peers "), regexp_replace(col("peerGroup"), "Peers ", ""))
        .when(col("peerGroup").startsWith("Peer "), regexp_replace(col("peerGroup"), "Peer ", ""))
        .otherwise(col("peerGroup")))
     

    val secondPartDf = firstPartDf
      //pain point 6 --> transco
      .withColumn("peersValue", when(col("reportName").like("2%"),
        when( col("peersValue") === "CI", "UKCI")//.otherwise(col("peersValue")))
          .when(col("peersValue") === "CC", "UKCC")
          .when(col("peersValue") === "CLSEE", "UKCLSEE")
          .when(col("peersValue") === "CS", "UKCS")
          .when(col("peersValue") === "CGC", "UKCGC")
          .when(col("peersValue") === "CCNE", "UKCCNE")
          .when(col("peersValue") === "CE", "UKCE")
          .when(col("peersValue") === "CEL", "UKCEL")
          .when(col("peersValue") === "CP", "UKCP")
          .when(col("peersValue") === "CGEC", "UKCGEC")
          .when(col("peersValue") === "CGB", "UKCGB")
          .when(col("peersValue") === "CED", "UKCED")
          .when(col("peersValue") === "CEMP", "UKCEMP")
          .otherwise(col("peersValue")))
        .otherwise(col("peersValue")))

      //Pas Sur qu'il faille le fair epour les period type aussi
      .withColumn("metricPeriodType", when(col("reportName").like("2%"),
        when(col("metricPeriodType") === "CE", "UKCE")
          .when(col("metricPeriodType") === "CI", "UKCI")
          .when(col("metricPeriodType") === "CC", "UKCC")
          .when(col("metricPeriodType") === "CLSEE", "UKCLSEE")
          .when(col("metricPeriodType") === "CEL", "UKCEL")
          .when(col("metricPeriodType") === "CS", "UKCS")
          .when(col("metricPeriodType") === "CGC", "UKCGC")
          .when(col("metricPeriodType") === "CCNE", "UKCCNE")
          .when(col("metricPeriodType") === "CP", "UKCP")
          .when(col("metricPeriodType") === "CGEC", "UKCGEC")
          .when(col("metricPeriodType") === "CGB", "UKCGB")
          .when(col("metricPeriodType") === "CED", "UKCED")
          .when(col("metricPeriodType") === "CEMP", "UKCEMP")
          .when(col("metricPeriodType") === "UK Funds W-1", "Since UK Funds W-1")
          .otherwise(col("metricPeriodType")))
        .when(col("reportName") === "6%" && col("metricPeriodType") === "CSEM EUR", "Ret since launch")
        .when(col("reportName") === "6%" && col("metricPeriodType") === "CSEM EUR W-1", "Ret since launch W-1")
        .otherwise(col("metricPeriodType")))
      //.cache()


    /*val allPeers = secondPartDf.select("peersValue")
      .distinct()
      .as[String]
      .collect.toList*/

    val peersValueDf: DataFrame = secondPartDf.select("peersValue").distinct()//.cache()

    val specialValue = secondPartDf.where("(peersValue LIKE CONCAT('CI') and metricPeriodType LIKE CONCAT('CGC & CFG') and groupInvestment like 'EAA Fund%') or (peersValue LIKE CONCAT('CCR') and metricPeriodType LIKE CONCAT('CFB') and groupInvestment like 'EAA Fund%' and reportName like '3%')")
      .withColumn("peersValue",
        expr(
          "case when peersValue LIKE CONCAT('CI') and metricPeriodType LIKE CONCAT('CGC & CFG')  and groupInvestment like 'EAA Fund%' then CONCAT('CGC')" +
            "when peersValue LIKE CONCAT('CCR') and metricPeriodType LIKE CONCAT('CFB') and groupInvestment like 'EAA Fund%' then CONCAT('CFB') end")
          .otherwise(col("peersValue")))


    val intermediaireDf = secondPartDf.unionByName(specialValue)


    val thirdPartDf = intermediaireDf
      //redundant code .withColumn("fundSize", col("fundSize"))
      //redundant code .withColumn("returnDateDaily", col("inceptionDate"))
      //redundant code .withColumn("netFlowYTD", col("flowYTD"))
      //redundant code .withColumn("periodType", col("metricPeriodType"))
      //redundant code .withColumn("periodStartDate", col("startDate"))
      //redundant code .withColumn("periodEndDate", col("endDate"))
      .withColumn("peersValue",
        //expr("case when peersValue LIKE CONCAT('CFB') and metricPeriodType LIKE CONCAT('CCR & CEMD') and groupInvestment like 'EAA Fund%' then CONCAT('CCR')" +
        //  "when peersValue LIKE CONCAT('CI') and metricPeriodType LIKE CONCAT('CGC & CFG') and groupInvestment like 'EAA Fund%' then CONCAT('CFG')" +
        //  "when peersValue LIKE CONCAT('CI') and metricPeriodType LIKE CONCAT('CHX') and groupInvestment like 'EAA Fund%' then CONCAT('CHX') end").otherwise(col("peersValue")))
        when(col("peersValue") === "CFB" && col("metricPeriodType") === "CCR & CEMD" && col("groupInvestment").like("EAA Fund%"), "CCR")
          .when(col("peersValue") === "CI" && col("metricPeriodType") === "CGC & CFG" && col("groupInvestment").like("EAA Fund%"), "CFG")
          .when(col("peersValue") === "CI" && col("metricPeriodType") === "CHX" && col("groupInvestment").like("EAA Fund%"), "CHX")
          .otherwise(col("peersValue"))
      )
      .withColumn("metricPeriodType",
        expr("case when metricPeriodType LIKE CONCAT('% ',peersValue) then 'Since PM*'" +
          "when metricPeriodType LIKE CONCAT(peersValue) then 'Since PM*'" +
          "when metricPeriodType LIKE 'CS' and peersValue LIKE CONCAT('CS%') then 'Since PM*'" +
          "when metricPeriodType LIKE CONCAT(peersValue,' %') then 'Since PM*'" +
          "when metricPeriodType LIKE CONCAT('% ',peersValue,' %') then 'Since PM*'" +
          "else metricPeriodType end"))
        // TODO could be nicer : when(col("metricPeriodType").like("% " + col("peersValue")), "Since PM*")
        // TODO could be nicer :   .when(col("metricPeriodType") === col("peersValue"), "Since PM*")
        // TODO could be nicer :   .when(col("metricPeriodType") === "CS" && col("peersValue").like("CS%"), "Since PM*")
        // TODO could be nicer :   .when(col("metricPeriodType").like("" + col("peersValue") + " %"), "Since PM*")
        // TODO could be nicer :   .when(col("metricPeriodType").like("% " + col("peersValue") + " %"), "Since PM*")
        // TODO could be nicer :   .otherwise(col("metricPeriodType")) )
      // Remove rows when value is not associated to the right peerGroup
      // changed with filter .withColumn("filter",
      // changed with filter   /*when(col("metricPeriodType") === "Since PM*", "keep")
      // changed with filter   .when(col("metricPeriodType").startsWith("Since UK"), "keep")
      // changed with filter   .when(col("peersValue") === "CSEM EUR" && col("metricPeriodType").startsWith("Ret since launch"), "keep")
      // changed with filter   .when(col("peersValue") === "CSEM USD" && col("metricPeriodType").startsWith("Ret since launch"), "keep")
      // changed with filter   .*/ when(col("metricPeriodType").isin(allPeers: _*), "remove")
      // changed with filter     .when(col("metricPeriodType").like("%CEL UKCE UKCED UKCP UKCGB%"), "remove")
      // changed with filter     .otherwise("keep"))
      // changed with filter .where("filter == 'keep'")
      // replace by left anti .filter(!col("metricPeriodType").isin(allPeers: _*))
      .join(peersValueDf, col("metricPeriodType") === peersValueDf("peersValue"), "left_anti" )
      .filter(!col("metricPeriodType").like("%CEL UKCE UKCED UKCP UKCGB%"))
      // no op .withColumn("fundSize", col("fundSize"))
      .withColumn("returnDateDaily", col("inceptionDate"))/// I believe it should be renaming old col not used after TODO
      .withColumn("netFlowYTD", col("flowYTD"))/// I believe it should be renaming old col not used after TODO
      .withColumn("periodType", col("metricPeriodType"))/// I believe it should be renaming old col not used after TODO
      .withColumn("periodStartDate", col("startDate"))/// I believe it should be renaming old col not used after TODO
      .withColumn("periodEndDate", col("endDate"))/// I believe it should be renaming old col not used after TODO
      //.cache()

    val importantColumns = Seq("sourceFileName", "EuSfdrFundType", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Principle_Adverse_Impact_Consideration", "ISR_EMT", "Towards_Sustainability_EMT", "GlobalCategory", "SustainableInvestmentOverall", "PAI_Scope_1_GHG_EmissionsTonnes", "PAI_Scope_1_2_3_GHG_EmissionsTonnes", "PAI_Carbon_Footprint_Scope_1_2_Tonnes_Per_EURm", "PAI_Carbon_Footprint_Scope_1_2_3_Tonnes_Per_EURm", "PAI_GHG_Intensity_Scope_1_2_Average_Value", "PAI_GHG_Intensity_Scope_1_2_3_Average_Value", "PAI_Fossil_Fuel_%_of_Covered_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Eligible_Portfolio_Involved", "PAI_Non_Renewable_%_Energy_Consumption_Average_Value", "PAI_Non_Renewable_of_Energy_Production_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_A_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_B_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_C_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_D_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_E_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_F_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_G_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_H_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_L_Average_Value", "PAI_Negative_Affect_On_Biodiversity_%_Portfolio_Involved", "PAI_Emissions_Water_Tonnes_Per_EURm", "PAI_Hazardous_Waste_Tonnes_Per_EURm", "PAI_Ungc_Lack_Compliance_Mechanisms_%_Portfolio_Involved", "PAI_Controversial_Weapons_%_Eligible_Portfolio_Involved", "PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Gender_Pay_Gap_Average_Value", "PAI_Percentage_Female_Board_Members_Average_Value", "calculationDate", "exportDate", "reportName", "reportCurrency", "isin", "groupInvestment", "peersValue", "morningstarRating", "currency", "morningstarCategory", "fundSize", "returnDateDaily", "netFlowYTD", "periodType", "periodStartDate", "periodEndDate")
    secondPartDf.unpersist()

    thirdPartDf.groupBy(importantColumns.map(col): _*)
      .pivot("metricType").sum("metricValue")
      .drop("metricType")

  }

  def renameMetricColumns(pivotDf: DataFrame): DataFrame = {

    val colsSharpe = pivotDf.columns.filter(_.startsWith("Sharpe Ratio")).map(col(_))
    val colsSortino = pivotDf.columns.filter(_.startsWith("Sortino Ratio")).map(col(_))
    val colsReturn = pivotDf.columns.filter(_.startsWith("Return")).map(col(_))
    val colsStd = pivotDf.columns.filter(_.startsWith("Std Dev")).map(col(_))
    val colsMaxDrawdown = pivotDf.columns.filter(_.startsWith("Max Drawdown")).map(col(_))

    pivotDf.withColumn("sharpeRatio", coalesce(colsSharpe: _*))
      .withColumn("sortinoRatio", coalesce(colsSortino: _*))
      .withColumn("return", coalesce(colsReturn: _*))
      .withColumn("stdDev", coalesce(colsStd: _*))
      .withColumn("maxDrawdown", coalesce(colsMaxDrawdown: _*))
  }


  def readVendomeStagingData(): DataFrame = {

    val vendomeDf =  getVendomeStagingData("vendome", "wendy_report").select("data")
   // spark.read.table("vendome_staging.wendy_report").select("data")
    val vendomeDf1 = vendomeDf.select($"data.*")
    val vendomeDf2 = vendomeDf1.withColumn("IsPartPrinc", lit(null))
    val vendomeShareDf = getVendomeStagingData("vendome", "wendy_report_share_perf").select("data").where("data.Lib like 'Carmignac Portfolio Global Market Neutral%'").select($"data.*")
     // spark.read.table("vendome_staging.wendy_report_share_perf").select("data").where("data.Lib like 'Carmignac Portfolio Global Market Neutral%'").select($"data.*")
    val unionVendome = vendomeDf2.unionByName(vendomeShareDf)
    unionVendome//.cache()
  }

  def mergeVendomeBenchData(renamedColumnsDf: DataFrame, vendomeExplodedData: DataFrame): DataFrame = {

    // called 2 times -> passed to main : val vendomeExplodedData = readVendomeStagingData()
    val isinDf: DataFrame = vendomeExplodedData.select("isin").where("isin is not null").distinct().withColumn("IsCarmignacSC", lit(1)) // nicer :: .where(col("isin").isNotNull()) than .where("isin is not null")

    val iaSectorDf: DataFrame = renamedColumnsDf.join(isinDf, Seq("isin"), "left")
      .na.fill(0, Seq("isCarmignacSC"))//.cache()

    /*val peersPeriodTypeToGet = iaSectorDf.select("PeersValue")
      .distinct().as[String]
      .collect.toList /*:+ "Since UK Funds"*/ :+ "Since UK Funds W-1"
*/
    //Merge with VENDOME
    // mettre ça dans un array de struct et après explode le tout

    // todo :: n'est pas plus efficace de faire un union de 7 df ??
    /*val benchDataColumns = vendomeExplodedData.withColumn("bench", array(
      struct(
        lit("1 week").alias("periodType"),
        col("Dat").alias("periodStartDate"),
        col("bench1W").alias("return")),
      struct(
        lit("3 months").alias("periodType"),
        col("Dat").alias("periodStartDate"),
        col("bench3M").alias("return")),
      struct(
        lit("YTD").alias("periodType"),
        col("Dat").alias("periodStartDate"),
        col("benchYtd").alias("return")),
      struct(
        lit("1 year").alias("periodType"),
        col("Dat").alias("periodStartDate"),
        col("bench1Y").alias("return")),
      struct(
        lit("3 years").alias("periodType"),
        col("Dat").alias("periodStartDate"),
        col("bench3Y").alias("return")),
      struct(
        lit("5 years").alias("periodType"),
        col("Dat").alias("periodStartDate"),
        col("bench5Y").alias("return")),
      struct(
        lit("Since PM*").alias("periodType"),
        col("Dat").alias("periodStartDate"),
        col("benchPm").alias("return")),
    ))
      .withColumn("explodedBench", explode($"bench"))
      .withColumn("periodType", col("explodedBench.periodType"))
      .withColumn("periodStartDate", col("explodedBench.periodStartDate"))
      .withColumn("date", col("periodStartDate"))
      .withColumn("return", col("explodedBench.return"))*/
    val benchDataColumns = vendomeExplodedData.withColumn("periodType", lit("1 week"))
      .withColumn("return", col("bench1W"))
      .union(
        vendomeExplodedData.withColumn("periodType", lit("3 months"))
          .withColumn("return", col("bench3M")))
      .union(
        vendomeExplodedData.withColumn("periodType", lit("YTD"))
          .withColumn("return", col("benchYtd")))
      .union(
        vendomeExplodedData.withColumn("periodType", lit("1 year"))
          .withColumn("return", col("bench1Y")))
      .union(
        vendomeExplodedData.withColumn("periodType", lit("3 years"))
          .withColumn("return", col("bench3Y")))
      .union(
        vendomeExplodedData.withColumn("periodType", lit("5 years"))
          .withColumn("return", col("bench5Y")))
      .union(
        vendomeExplodedData.withColumn("periodType", lit("Since PM*"))
          .withColumn("return", col("benchPm")))
      .withColumn("periodStartDate", col("Dat"))
      .withColumn("date", col("periodStartDate"))

    val specialLab = benchDataColumns.where("Lib like 'Carmignac Portfolio Global Market Neutral%'")
      .withColumn("groupInvestment", col("Lib"))
      .withColumn("reportCurrency", col("DevCot"))
      .withColumn("peersValue", col("Cod"))
      .withColumn("reportName", lit("7 - Wendy Lab Digital"))
      .select("isin", "periodType", "periodStartDate", "return", "date", "groupInvestment", "reportCurrency", "peersValue", "reportName")


    // gérer les index
    val groupedByPeers = iaSectorDf.where("IsCarmignacSC == 1")
      //.groupBy("peersValue", "calculationDate", "exportDate", "reportCurrency", "reportName", "date")
      //.agg(first("isin").as("isin"))
      .select("peersValue", "calculationDate", "exportDate", "reportCurrency", "reportName", "date", "isin")
      .dropDuplicates("peersValue", "calculationDate", "exportDate", "reportCurrency", "reportName", "date")


    val indexRowsToAppend = groupedByPeers.join(benchDataColumns
      .select("isin", "periodType", "periodStartDate", "return", "date"), Seq("isin", "date"), "left")
      .withColumn("groupInvestment", concat(col("peersValue"), lit(" Index")))
      .withColumn("isin", when(col("groupInvestment").endsWith(" Index"), lit(null)).otherwise(col("isin")))
      .where("return is not null")
      .distinct

    //display(indexRowsToAppend)
    // To be able to join with vendome data
   /* replaced by allowMissingColumns ::
    val sameColumnsDf = iaSectorDf.columns.foldLeft(indexRowsToAppend) {
      (df, column) =>
        if (!df.columns.exists(x => {
          x contains column
        })) {
          df.withColumn(column, lit(null))
        }
        else df
    }
    val sameColumnsDfSpecialLab = iaSectorDf.columns.foldLeft(specialLab) {
      (df, column) =>
        if (!df.columns.exists(x => {
          x contains column
        })) {
          df.withColumn(column, lit(null))
        }
        else df
    }*/

    iaSectorDf.unionByName(indexRowsToAppend, true).unionByName(specialLab, true)
  }

  def handleEAADf(ciIndexDf: DataFrame): DataFrame = {

    // Pain point 3 règle 2

    //val ltranscodificationPath: String = AdlsUtils.buildPath(transcodificationPath)
    /*val transcoDf =  spark.read.option("inferSchema", false).option("header", true).option("sep", ";").csv(ltranscodificationPath)

    val peersWithIASector = ciIndexDf.where("Isin is null and GroupInvestment LIKE 'IA%'").select("PeersValue")
      .distinct().as[String]
      .collect.toList

    val allPeers = ciIndexDf.select("PeersValue")
      .distinct()
      .as[String]
      .collect.toList

    val withoutIASector = allPeers.diff(peersWithIASector)*/

    // Refaire pain point 3



    val handledEaDf = ciIndexDf.withColumn("averagecat",
      when(col("reportName") =!= "5 - Wendy DCP Digital",
        when(col("groupInvestment").startsWith("EAA"),
          when(col("groupInvestment").endsWith("Hedged"),
            concat(col("peersValue"), lit(" Category Average "), substring(col("groupInvestment"), -10, 10)))
            .otherwise(concat(col("peersValue"), lit(" Category Average")))
        )
        .when(col("groupInvestment").startsWith("Japan Fund"), concat(col("peersValue"), lit(" Category Average")))
        .otherwise(col("groupInvestment"))
      ).otherwise(col("groupInvestment"))
    ).cache()


    // val handledPeersLibDf = handledEaDf.join(vendomeExplodedData.select("fundMnemonic", "lib").withColumnRenamed("lib", "peersLib"), handledEaDf("peersValue") === vendomeExplodedData("fundMnemonic"), "left").distinct()

    //val categoryAverageDf = handledEaDf.where($"averagecat" like "%Category Average%")
    //val peersValuesWithAverageCategory = categoryAverageDf.select("peersValue").distinct.as[String].collect.toList
    val peersValuesWithAverageCategoryDf = handledEaDf.where($"averagecat" like "%Category Average%")//.select("peersValue")//.distinct//.as[String].collect.toList

    // Garder que les peers qui ont pas de EAA, et qui sont like Carmignac
    val rule2FilteredDf = handledEaDf.join(peersValuesWithAverageCategoryDf, List("peersValue"), "left_anti")//.filter(!col("peersValue").isin(peersValuesWithAverageCategory:_*))
      .where($"groupInvestment" like "Carmignac%")
      .select("reportName", "peersValue", "morningstarCategory").cache()
      //.distinct

    val subdataset = handledEaDf.withColumn("equivalentpeer", col("peersValue")).drop("peersValue", "reportName", "morningstarCategory")//.distinct

    val groupInvestmentRule2RowsToAppend = rule2FilteredDf.join(subdataset, rule2FilteredDf("morningstarCategory") === subdataset("groupInvestment"), "left")
      .withColumn("averagecat", concat(col("peersValue"), lit(" Category Average")))//.distinct

    val reorderedColumnsDf = groupInvestmentRule2RowsToAppend.select(handledEaDf.columns.map(col): _*) // or can do a unionbyname ?

    handledEaDf.union(reorderedColumnsDf)
      .withColumn("groupInvestment", col("averagecat"))
      .drop("averagecat")
      //.drop("groupInvestment").withColumnRenamed("averagecat", "groupInvestment")?
      .distinct
  }




  // Handle peers lib

  def handleIASectorDf(finalHandledEAADf: DataFrame, withoutIASector: Seq[Any], vendomeExplodedData: DataFrame): DataFrame = {
    //val vendomeExplodedData = readVendomeStagingData()
    val ltranscodificationPath: String = AdlsUtils.buildPath(transcodificationPath)
    //println(s"withoutIASector :: $withoutIASector")
    //println(s"withoutIASector :: ${withoutIASector.size}")
    val transcoDf = spark.read.option("inferSchema", false).option("header", true).option("sep", ";").csv(ltranscodificationPath)

    val handledPeersLibDf = finalHandledEAADf.join(vendomeExplodedData.select("Cod", "lib").withColumnRenamed("lib", "peersLib"), finalHandledEAADf("peersValue") === vendomeExplodedData("Cod"), "left").distinct()
    val transcodedDf = handledPeersLibDf.join(transcoDf, handledPeersLibDf("groupInvestment") === transcoDf("name"), "left")
    //val groupByDf = transcodedDf.where($"IA Sector".isNotNull).groupBy("peersValue").agg(first("IA Sector").as("Transco"))
    /*val peersTransco = withoutIASector.map(x => (x, groupByDf.where($"peersValue" === x).select("Transco").head(1)))
    val peersMap = peersTransco.toMap*/
    val peersTranscoDf: DataFrame = transcodedDf.where($"IA Sector".isNotNull).select("peersValue","IA Sector").dropDuplicates("peersValue")
      .where(col("peersValue").isin(withoutIASector: _*))//.dropDuplicates("peersValue")
      .withColumnRenamed("peersValue", "peersValuePeersTranscoDf")
      .withColumnRenamed("IA Sector", "TranscoPeersTranscoDf")
    //println( s"peersTranscoDf.count() :: ${peersTranscoDf.count()}")

    val handledPeersLibDfFiltered: DataFrame = handledPeersLibDf.where("groupInvestment is not null")
      .where("groupInvestment LIKE 'IA%'")
    handledPeersLibDfFiltered//.cache()

    /*
        val iaSectorRowsToAppend = withoutIASector.par.map(peerGroupWithoutIASector => {
          val peersMapLocal = peersMap
          if (peersMapLocal.getOrElse(peerGroupWithoutIASector, null).headOption != None) {
            handledPeersLibDfFiltered
              .where("groupInvestment LIKE 'IA%" + peersMapLocal.getOrElse(peerGroupWithoutIASector, null)(0)(0) + "%'")
              .withColumn("peersValue", explode(when($"groupInvestment".like("%" + peersMapLocal.getOrElse(peerGroupWithoutIASector, null)(0)(0) + "%"), array(col("peersValue"), lit(peerGroupWithoutIASector))).otherwise(array($"peersValue"))))
              .withColumn("groupInvestment", explode(when($"groupInvestment".like("%" + peersMapLocal.getOrElse(peerGroupWithoutIASector, null)(0)(0) + "%"), array(col("peersValue"), lit("IA SECTOR"))).otherwise(array($"groupInvestment"))))
              .where("groupInvestment == 'IA SECTOR'")
          }
          else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], handledPeersLibDf.schema)
        }
          )
          .reduce(_.union(_)).distinct()*/

    val iaSectorRowsToAppend = handledPeersLibDfFiltered.crossJoin(peersTranscoDf)
      .where("groupInvestment LIKE 'IA%" + peersTranscoDf.col("TranscoPeersTranscoDf") + "%'")
      .withColumn("peersValue", explode(when($"groupInvestment".like("%" + peersTranscoDf.col("TranscoPeersTranscoDf") + "%"), array(col("peersValue"), lit($"peersValuePeersTranscoDf"))).otherwise(array($"peersValue"))))
      .withColumn("groupInvestment", explode(when($"groupInvestment".like("%" + peersTranscoDf.col("TranscoPeersTranscoDf") + "%"), array(col("peersValue"), lit("IA SECTOR"))).otherwise(array($"groupInvestment"))))
      .where($"groupInvestment" === "IA SECTOR")
      .drop("TranscoPeersTranscoDf")
      .drop("peersValuePeersTranscoDf")
      .distinct()
    //println(s"handledPeersLibDf.count() :: ${handledPeersLibDf.count()}")
    //println(s"iaSectorRowsToAppend.count() :: ${iaSectorRowsToAppend.count()}")
    handledPeersLibDf.union(iaSectorRowsToAppend)
      .withColumn("groupInvestment",
        when($"groupInvestment".like("%IA%") && $"isin".isNull, lit("IA SECTOR"))
          .otherwise($"groupInvestment")
      )

  }

  def handleIASectorDf(finalHandledEAADf: DataFrame, withoutIASector: DataFrame, vendomeExplodedData: DataFrame): DataFrame = {
    //val vendomeExplodedData = readVendomeStagingData()
    val ltranscodificationPath: String = AdlsUtils.buildPath(transcodificationPath)
    //println(s"withoutIASector :: $withoutIASector")
    //println(s"withoutIASector :: ${withoutIASector.size}")
    val transcoDf = spark.read.option("inferSchema", false).option("header", true).option("sep", ";").csv(ltranscodificationPath)

    val handledPeersLibDf = finalHandledEAADf.join(vendomeExplodedData.select("Cod", "lib").withColumnRenamed("lib", "peersLib"), finalHandledEAADf("peersValue") === vendomeExplodedData("Cod"), "left").distinct()
    val transcodedDf = handledPeersLibDf.join(transcoDf, handledPeersLibDf("groupInvestment") === transcoDf("name"), "left")

    val peersTranscoDf: DataFrame = transcodedDf.where($"IA Sector".isNotNull).select("peersValue","IA Sector").dropDuplicates("peersValue")
      .join(withoutIASector.select("peersValue"), Seq("peersValue"), "inner").dropDuplicates("peersValue") // ? useless drop ?
      .withColumnRenamed("peersValue", "peersValuePeersTranscoDf")
      .withColumnRenamed("IA Sector", "TranscoPeersTranscoDf")
    //println( s"peersTranscoDf.count() :: ${peersTranscoDf.count()}")

    val handledPeersLibDfFiltered: DataFrame = handledPeersLibDf.where("groupInvestment is not null")
      .where("groupInvestment LIKE 'IA%'")

    val iaSectorRowsToAppend = handledPeersLibDfFiltered.crossJoin(peersTranscoDf)
      .where("groupInvestment LIKE 'IA%" + peersTranscoDf.col("TranscoPeersTranscoDf") + "%'")
      .withColumn("peersValue", explode(when($"groupInvestment".like("%" + peersTranscoDf.col("TranscoPeersTranscoDf") + "%"), array(col("peersValue"), lit($"peersValuePeersTranscoDf"))).otherwise(array($"peersValue"))))
      .withColumn("groupInvestment", explode(when($"groupInvestment".like("%" + peersTranscoDf.col("TranscoPeersTranscoDf") + "%"), array(col("peersValue"), lit("IA SECTOR"))).otherwise(array($"groupInvestment"))))
      .where("groupInvestment == 'IA SECTOR'")
      .drop("TranscoPeersTranscoDf")
      .drop("peersValuePeersTranscoDf")
      .distinct()
    //println(s"handledPeersLibDf.count() :: ${handledPeersLibDf.count()}")
    //println(s"iaSectorRowsToAppend.count() :: ${iaSectorRowsToAppend.count()}")
    handledPeersLibDf.union(iaSectorRowsToAppend)
      .withColumn("groupInvestment",
        when($"groupInvestment".like("%IA%") && $"isin".isNull, lit("IA SECTOR"))
          .otherwise($"groupInvestment")
      )

  }

  def handleDateDf(handledIaSectorDf: DataFrame): DataFrame = {

    val datesDf = handledIaSectorDf.where("periodType == '1 week'" )
      .select("reportName","periodEndDate","sourceFileName").where($"periodEndDate".isNotNull)
      .distinct
      .withColumnRenamed("periodEndDate", "date")

    // replaced by inner Join :: val handledIaSectorDfJoin = handledIaSectorDf.join(datesDf,Seq("sourceFileName","reportName"), "left")
    // replaced by inner Join :: val removeNullDate = handledIaSectorDfJoin.where($"date".isNotNull)
    val handledIaSectorDfJoin = handledIaSectorDf.join(datesDf,Seq("sourceFileName","reportName"), "inner")
    //val removeNullDate = handledIaSectorDfJoin
    val windowSpec  = Window.partitionBy("date", "reportName", "EuSfdrFundType", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Principle_Adverse_Impact_Consideration", "ISR_EMT", "Towards_Sustainability_EMT", "GlobalCategory", "SustainableInvestmentOverall", "PAI_Scope_1_GHG_EmissionsTonnes", "PAI_Scope_1_2_3_GHG_EmissionsTonnes", "PAI_Carbon_Footprint_Scope_1_2_Tonnes_Per_EURm", "PAI_Carbon_Footprint_Scope_1_2_3_Tonnes_Per_EURm", "PAI_GHG_Intensity_Scope_1_2_Average_Value", "PAI_GHG_Intensity_Scope_1_2_3_Average_Value", "PAI_Fossil_Fuel_%_of_Covered_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Eligible_Portfolio_Involved","PAI_Energy_Consumption_Intensity_NACE_L_Average_Value","PAI_Negative_Affect_On_Biodiversity_%_Portfolio_Involved", "PAI_Emissions_Water_Tonnes_Per_EURm", "PAI_Hazardous_Waste_Tonnes_Per_EURm", "PAI_Ungc_Lack_Compliance_Mechanisms_%_Portfolio_Involved", "PAI_Controversial_Weapons_%_Eligible_Portfolio_Involved","PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Gender_Pay_Gap_Average_Value", "PAI_Percentage_Female_Board_Members_Average_Value", "reportCurrency", "isin", "groupInvestment", "peersValue", "morningstarRating", "currency", "morningstarCategory", "fundSize", "returnDateDaily", "netFlowYTD", "periodType", "periodStartDate", "periodEndDate", "sharpeRatio", "sortinoRatio", "stdDev", "maxDrawdown").orderBy($"calculationDate".desc)
    handledIaSectorDfJoin.withColumn("row_number",row_number.over(windowSpec)).where("row_number = 1").drop("row_number")
  }

  def handleperioddate(ciIndexDf: DataFrame): DataFrame = {
    val df1 = ciIndexDf.where(!(($"groupInvestment".like("% Index")) || ($"groupInvestment".like("% Category Average")) || ($"groupInvestment" ==="IA SECTOR")))
    //and periodType like 'Since PM*'")
    // TODO :: 3 distinct à la suite ...
    val df = df1.select($"date", $"reportName", $"reportCurrency", $"periodType", $"peersValue", $"periodStartDate".as("newperiodStartDate"), $"periodEndDate".as("newperiodEndDate")).distinct()
    val newciIndexDf = ciIndexDf.join(df, Seq("date", "peersValue", "reportName", "reportCurrency", "periodType"), "left")//.distinct()
    //display(newciIndexDf.where("groupInvestment like '% Index' and periodType like 'Since PM*'"))
    val lastdf = newciIndexDf.withColumn("periodStartDate", when(($"groupInvestment".like("% Index")) || ($"groupInvestment".like("% Category Average")) || ($"groupInvestment" ==="IA SECTOR"), $"newperiodStartDate")
      .otherwise($"periodStartDate"))
      .withColumn("periodEndDate", when(($"groupInvestment".like("% Index")) || ($"groupInvestment".like("% Category Average")) || ($"groupInvestment".like("IA SECTOR")), $"newperiodEndDate")
        .otherwise($"periodEndDate")).distinct()
    val cleandf = lastdf.drop("newperiodEndDate", "newperiodEndDate")
    cleandf
  }

  def main(args: Array[String]): Unit = {
    mountContainers
    import java.util.Calendar

    //println(s""" init :: ${Calendar.getInstance().getTime()}""")

    val StagingDeltaPath: String = AdlsUtils.buildPath(s"/staging/$dataSource/$datasetName/deltatable")

    val stagingDf = readLastStagingData(StagingDeltaPath).distinct//.cache()
    //println(s""" pivot :: ${Calendar.getInstance().getTime()}""")

    val pivotDf = pivotStagingDf(stagingDf)//.cache()
    //println(s""" post pivot :: ${Calendar.getInstance().getTime()}""")

    val renamedColumnsDf = renameMetricColumns(pivotDf)//.cache()
    val dateDf = handleDateDf(renamedColumnsDf)//.cache()

    val vendomeExplodedData = readVendomeStagingData()//.cache()
    val ciIndexDf = mergeVendomeBenchData(dateDf, vendomeExplodedData)//.cache()

    /*   val peersWithIASector = ciIndexDf.where("Isin is null and GroupInvestment LIKE 'IA%'").select("PeersValue")
          .distinct()
          .as[String]
          .collect.toList

        val allPeers = ciIndexDf.select("PeersValue")
          .distinct()
          .as[String]
          .collect.toList

        val withoutIASector = allPeers.diff(peersWithIASector)*/

    val withoutIASectorDf: DataFrame = ciIndexDf.where("Isin is not null OR GroupInvestment NOT LIKE 'IA%'").select("PeersValue")
    val finalHandledEAADf = handleEAADf(ciIndexDf)//.cache()
    //finalHandledEAADf//.cache()
    //println(s""" finalHandledEAADf.count() :: ${finalHandledEAADf.count()}""")
    //println(s""" handledIaSectorDf :: ${Calendar.getInstance().getTime()}""")

    //val handledIaSectorDf = handleIASectorDf(finalHandledEAADf, withoutIASector, vendomeExplodedData)
    val handledIaSectorDf = handleIASectorDf(finalHandledEAADf, withoutIASectorDf, vendomeExplodedData)

    //println(s""" handledIaSectorDf.count :: ${handledIaSectorDf.count()}""")
    //println(s""" post handledIaSectorDf :: ${Calendar.getInstance().getTime()}""")

        val handledIaSectorDf_average = handleperioddate(handledIaSectorDf)//.cache()

        val writtableDf = handledIaSectorDf_average.select("date", "reportName", "reportCurrency", "isin", "groupInvestment", "peersValue", "fundSize", "returnDateDaily", "netFlowYTD", "periodType", "periodStartDate", "periodEndDate", "return", "Morningstar category percentile", "sharpeRatio", "sortinoRatio", "maxDrawdown", "stdDev", "morningstarRating", "peersLib", "isCarmignacSC", "EuSfdrFundType", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Principle_Adverse_Impact_Consideration", "ISR_EMT", "Towards_Sustainability_EMT", "GlobalCategory", "SustainableInvestmentOverall", "PAI_Scope_1_GHG_EmissionsTonnes", "PAI_Scope_1_2_3_GHG_EmissionsTonnes", "PAI_Carbon_Footprint_Scope_1_2_Tonnes_Per_EURm", "PAI_Carbon_Footprint_Scope_1_2_3_Tonnes_Per_EURm", "PAI_GHG_Intensity_Scope_1_2_Average_Value", "PAI_GHG_Intensity_Scope_1_2_3_Average_Value", "PAI_Fossil_Fuel_%_of_Covered_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Eligible_Portfolio_Involved", "PAI_Non_Renewable_%_Energy_Consumption_Average_Value", "PAI_Non_Renewable_of_Energy_Production_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_A_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_B_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_C_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_D_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_E_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_F_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_G_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_H_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_L_Average_Value", "PAI_Negative_Affect_On_Biodiversity_%_Portfolio_Involved", "PAI_Emissions_Water_Tonnes_Per_EURm", "PAI_Hazardous_Waste_Tonnes_Per_EURm", "PAI_Ungc_Lack_Compliance_Mechanisms_%_Portfolio_Involved", "PAI_Controversial_Weapons_%_Eligible_Portfolio_Involved", "PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Gender_Pay_Gap_Average_Value", "PAI_Percentage_Female_Board_Members_Average_Value").distinct//.cache()
    handledIaSectorDf_average.unpersist()
    finalHandledEAADf.unpersist()
    vendomeExplodedData.unpersist()
    dateDf.unpersist()
    stagingDf.unpersist()
    //println(s""" writtableDf.count :: ${writtableDf.count()}""")
    //println(s""" lastwrittableDf :: ${Calendar.getInstance().getTime()}""")

        val lastwrittableDf = writtableDf.withColumn("return",
          when($"groupInvestment".like("% Index"), col("return") * lit(100))
            .when($"groupInvestment" === "Carmignac Portfolio Global Market Neutral", col("return") * lit(100))
            .otherwise($"return"))
          .withColumn("reportName", initcap(col("reportName")))
          .withColumnRenamed("EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned")
          .withColumnRenamed("Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_So")
          .withColumnRenamed("PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Invo").distinct()//.cache()
    //println(s""" post lastwrittableDf :: ${Calendar.getInstance().getTime()}""")

       val data_to_delete = lastwrittableDf.where("date is not null and groupInvestment not like 'Carmignac Portfolio Global Market Neutral%'").select("date").first().getDate(0)
        val selectRightDate = s"""date = """ + s"'${data_to_delete}'"
        val lastwrittableDf_filtered = lastwrittableDf.where(selectRightDate)
        val tmpWritePath = AdlsUtils.buildPath("/wip/render/BIDigit/lastwrittableDf.parquet")
        // todo lastwrittableDf_filtered.write.mode("overwrite").parquet(tmpWritePath)
        // todo spark.catalog.clearCache()
        val lastDf = lastwrittableDf_filtered //spark.read.parquet(tmpWritePath)

    //println(s""" lastDf.count :: ${lastDf.count()}""")
    //println(s""" lastDf :: ${Calendar.getInstance().getTime()}""")

     /*   val particulacaseDf = lastDf
          .where("(groupInvestment like 'CI Category Average' and (reportName like '1%' or reportName like '3%') and periodType not like 'Since PM*') or (groupInvestment like 'CCR Category Average' and reportName like '3%' and periodType not like 'Since PM*') or (groupInvestment like 'CFB Category Average' and reportName like '1%' and periodType not like 'Since PM*')").cache()

        val CFGdf = particulacaseDf.where("groupInvestment like 'CI Category Average' and reportName like '1%'").withColumn("peersValue", lit("CFG"))
          .withColumn("groupInvestment", lit("CFG Category Average"))
        val CFB_LatamDf = particulacaseDf.where("groupInvestment like 'CCR Category Average' and reportName like '3%'").withColumn("peersValue", lit("CFB"))
          .withColumn("groupInvestment", lit("CFB Category Average"))
          .withColumn("reportName", lit("3 - Wendy Latam Digital"))
        val CGCdf = particulacaseDf.where("groupInvestment like 'CI Category Average' and reportName like '1%'").withColumn("peersValue", lit("CGC"))
          .withColumn("groupInvestment", lit("CGC Category Average"))
        val CHXdf = particulacaseDf.where("groupInvestment like 'CI Category Average' and reportName like '1%'").withColumn("peersValue", lit("CHX"))
          .withColumn("groupInvestment", lit("CHX Category Average"))
        val CCRdf = particulacaseDf.where("groupInvestment like 'CFB Category Average' and reportName like '1%'").withColumn("peersValue", lit("CCR"))
          .withColumn("groupInvestment", lit("CCR Category Average"))
        val dfToWrite = lastDf.unionByName(CHXdf).unionByName(CGCdf).unionByName(CFGdf).unionByName(CCRdf).unionByName(CFB_LatamDf).cache()
    */
    //lastDf.cache()
    val lastCI: DataFrame = lastDf.where("groupInvestment = 'CI Category Average' and reportName like '1%'")
    val CFGdf = lastCI.withColumn("peersValue", lit("CFG"))
      .withColumn("groupInvestment", lit("CFG Category Average"))
    val CGCdf = lastCI.withColumn("peersValue", lit("CGC"))
      .withColumn("groupInvestment", lit("CGC Category Average"))
    val CHXdf = lastCI.withColumn("peersValue", lit("CHX"))
      .withColumn("groupInvestment", lit("CHX Category Average"))
    val CFB_LatamDf = lastDf.where("groupInvestment = 'CCR Category Average' and reportName like '3%'").withColumn("peersValue", lit("CFB"))
      .withColumn("groupInvestment", lit("CFB Category Average"))
      .withColumn("reportName", lit("3 - Wendy Latam Digital"))
    val CCRdf = lastDf.where("groupInvestment = 'CFB Category Average' and reportName like '1%'").withColumn("peersValue", lit("CCR"))
      .withColumn("groupInvestment", lit("CCR Category Average"))
    val dfToWrite = lastDf.unionByName(CHXdf).unionByName(CGCdf).unionByName(CFGdf).unionByName(CCRdf).unionByName(CFB_LatamDf)//.cache()

        val lastwrittableDf_filtered2 = dfToWrite.where(selectRightDate) //TODO redundant with lastwrittableDf_filtered
        val dropCurrentTableWendy = s"""delete from marketingbi."MorningstarWendyMetrics" where date = """ + s"'${data_to_delete}'"
    //lastwrittableDf_filtered2.cache()
    println(s""" lastwrittableDf_filtered2.count :: ${lastwrittableDf_filtered2.count()}""")
    println(s""" lastwrittableDf_filtered2 :: ${Calendar.getInstance().getTime()}""")
        val lPgRender: PostgresUtils = getRenderConnection
        val renderDf = lPgRender.executePgStatement(dropCurrentTableWendy)

       // val tmplastWritePath = AdlsUtils.buildPath("/wip/render/BIDigit/lastDf.parquet")
       // lastwrittableDf_filtered2.write.mode("overwrite").parquet(tmplastWritePath)
   // spark.read.parquet(tmplastWritePath)
        lPgRender.writeDfIntoTable(lastwrittableDf_filtered2, biTableName,"append")
    println(s""" the end :: ${Calendar.getInstance().getTime()}""")

        AdlsUtils.removeFile(tmpWritePath,true)
        //AdlsUtils.removeFile(tmplastWritePath,true)
    println(s""" the end :: ${Calendar.getInstance().getTime()}""")
  }
}