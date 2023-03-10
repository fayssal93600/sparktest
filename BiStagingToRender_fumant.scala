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
  spark.conf.set("spark.sql.shuffle.partitions", "16")


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
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CI", "UKCI").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CC", "UKCC").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CLSEE", "UKCLSEE").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CS", "UKCS").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CGC", "UKCGC").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CCNE", "UKCCNE").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CE", "UKCE").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CEL", "UKCEL").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CP", "UKCP").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CGEC", "UKCGEC").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CGB", "UKCGB").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CED", "UKCED").otherwise(col("peersValue")))
      .withColumn("peersValue", when(col("reportName").like("2%") && col("peersValue") === "CEMP", "UKCEMP").otherwise(col("peersValue")))


      //Pas Sur qu'il faille le fair epour les period type aussi
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CE", "UKCE").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CI", "UKCI").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CC", "UKCC").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CLSEE", "UKCLSEE").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CEL", "UKCEL").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CS", "UKCS").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CGC", "UKCGC").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CCNE", "UKCCNE").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CP", "UKCP").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CGEC", "UKCGEC").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CGB", "UKCGB").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CED", "UKCED").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "CEMP", "UKCEMP").otherwise(col("metricPeriodType")))


      .withColumn("metricPeriodType", when(col("reportName").like("2%") && col("metricPeriodType") === "UK Funds W-1", "Since UK Funds W-1").otherwise(col("metricPeriodType")))

      .withColumn("metricPeriodType", when(col("reportName") === "6%" && col("metricPeriodType") === "CSEM EUR", "Ret since launch").otherwise(col("metricPeriodType")))
      .withColumn("metricPeriodType", when(col("reportName") === "6%" && col("metricPeriodType") === "CSEM EUR W-1", "Ret since launch W-1").otherwise(col("metricPeriodType")))

      .cache()


    val allPeers = secondPartDf.select("peersValue")
      .distinct()
      .as[String]
      .collect.toList

    val specialValue = secondPartDf.where("(peersValue LIKE CONCAT('CI') and metricPeriodType LIKE CONCAT('CGC & CFG') and groupInvestment like 'EAA Fund%') or (peersValue LIKE CONCAT('CCR') and metricPeriodType LIKE CONCAT('CFB') and groupInvestment like 'EAA Fund%' and reportName like '3%')")
      .withColumn("peersValue",
        expr(
          "case when peersValue LIKE CONCAT('CI') and metricPeriodType LIKE CONCAT('CGC & CFG')  and groupInvestment like 'EAA Fund%' then CONCAT('CGC')" +
            "when peersValue LIKE CONCAT('CCR') and metricPeriodType LIKE CONCAT('CFB') and groupInvestment like 'EAA Fund%' then CONCAT('CFB') end")
          .otherwise(col("peersValue")))


    val intermediaireDf = secondPartDf.unionByName(specialValue)


    val thirdPartDf = intermediaireDf
      .withColumn("fundSize", col("fundSize"))
      .withColumn("returnDateDaily", col("inceptionDate"))
      .withColumn("netFlowYTD", col("flowYTD"))
      .withColumn("periodType", col("metricPeriodType"))
      .withColumn("periodStartDate", col("startDate"))
      .withColumn("periodEndDate", col("endDate"))
      .withColumn("peersValue",
        expr("case when peersValue LIKE CONCAT('CFB') and periodType LIKE CONCAT('CCR & CEMD') and groupInvestment like 'EAA Fund%' then CONCAT('CCR')" +
          "when peersValue LIKE CONCAT('CI') and periodType LIKE CONCAT('CGC & CFG') and groupInvestment like 'EAA Fund%' then CONCAT('CFG')" +
          "when peersValue LIKE CONCAT('CI') and periodType LIKE CONCAT('CHX') and groupInvestment like 'EAA Fund%' then CONCAT('CHX') end").otherwise(col("peersValue")))
      .withColumn("metricPeriodType",
        expr("case when periodType LIKE CONCAT('% ',peersValue) then 'Since PM*'" +
          "when periodType LIKE CONCAT(peersValue) then 'Since PM*'" +
          "when periodType LIKE 'CS' and peersValue LIKE CONCAT('CS%') then 'Since PM*'" +
          "when periodType LIKE CONCAT(peersValue,' %') then 'Since PM*'" +
          "when periodType LIKE CONCAT('% ',peersValue,' %') then 'Since PM*'" +
          "else periodType end"))

      // Remove rows when value is not associated to the right peerGroup
      .withColumn("filter",
        /*when(col("metricPeriodType") === "Since PM*", "keep")
        .when(col("metricPeriodType").startsWith("Since UK"), "keep")
        .when(col("peersValue") === "CSEM EUR" && col("metricPeriodType").startsWith("Ret since launch"), "keep")
        .when(col("peersValue") === "CSEM USD" && col("metricPeriodType").startsWith("Ret since launch"), "keep")
        .*/ when(col("metricPeriodType").isin(allPeers: _*), "remove")
          .when(col("metricPeriodType").like("%CEL UKCE UKCED UKCP UKCGB%"), "remove")
          .otherwise("keep"))

      .where("filter == 'keep'")
      .withColumn("fundSize", col("fundSize"))
      .withColumn("returnDateDaily", col("inceptionDate"))
      .withColumn("netFlowYTD", col("flowYTD"))
      .withColumn("periodType", col("metricPeriodType"))
      .withColumn("periodStartDate", col("startDate"))
      .withColumn("periodEndDate", col("endDate"))
      .cache()

    val importantColumns = Seq("sourceFileName", "EuSfdrFundType", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Principle_Adverse_Impact_Consideration", "ISR_EMT", "Towards_Sustainability_EMT", "GlobalCategory", "SustainableInvestmentOverall", "PAI_Scope_1_GHG_EmissionsTonnes", "PAI_Scope_1_2_3_GHG_EmissionsTonnes", "PAI_Carbon_Footprint_Scope_1_2_Tonnes_Per_EURm", "PAI_Carbon_Footprint_Scope_1_2_3_Tonnes_Per_EURm", "PAI_GHG_Intensity_Scope_1_2_Average_Value", "PAI_GHG_Intensity_Scope_1_2_3_Average_Value", "PAI_Fossil_Fuel_%_of_Covered_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Eligible_Portfolio_Involved", "PAI_Non_Renewable_%_Energy_Consumption_Average_Value", "PAI_Non_Renewable_of_Energy_Production_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_A_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_B_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_C_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_D_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_E_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_F_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_G_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_H_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_L_Average_Value", "PAI_Negative_Affect_On_Biodiversity_%_Portfolio_Involved", "PAI_Emissions_Water_Tonnes_Per_EURm", "PAI_Hazardous_Waste_Tonnes_Per_EURm", "PAI_Ungc_Lack_Compliance_Mechanisms_%_Portfolio_Involved", "PAI_Controversial_Weapons_%_Eligible_Portfolio_Involved", "PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Gender_Pay_Gap_Average_Value", "PAI_Percentage_Female_Board_Members_Average_Value", "calculationDate", "exportDate", "reportName", "reportCurrency", "isin", "groupInvestment", "peersValue", "morningstarRating", "currency", "morningstarCategory", "fundSize", "returnDateDaily", "netFlowYTD", "periodType", "periodStartDate", "periodEndDate")


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
    unionVendome.cache()
  }

  def mergeVendomeBenchData(renamedColumnsDf: DataFrame): DataFrame = {

    val vendomeExplodedData = readVendomeStagingData()
    val isinDf = vendomeExplodedData.select("isin").distinct().where("isin is not null").withColumn("IsCarmignacSC", lit(1))

    val iaSectorDf = renamedColumnsDf.join(isinDf, Seq("isin"), "left")
      .na.fill(0, Seq("isCarmignacSC")).cache()

    /*val peersPeriodTypeToGet = iaSectorDf.select("PeersValue")
      .distinct().as[String]
      .collect.toList /*:+ "Since UK Funds"*/ :+ "Since UK Funds W-1"
*/
    //Merge with VENDOME
    // mettre ça dans un array de struct et après explode le tout
    val benchDataColumns = vendomeExplodedData.withColumn("bench", array(
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
      .withColumn("return", col("explodedBench.return"))

    val specialLab = benchDataColumns.where("Lib like 'Carmignac Portfolio Global Market Neutral%'")
      .withColumn("groupInvestment", col("Lib"))
      .withColumn("reportCurrency", col("DevCot"))
      .withColumn("peersValue", col("Cod"))
      .withColumn("reportName", lit("7 - Wendy Lab Digital"))
      .select("isin", "periodType", "periodStartDate", "return", "date", "groupInvestment", "reportCurrency", "peersValue", "reportName")


    // gérer les index
    val groupedByPeers = iaSectorDf.where("IsCarmignacSC == 1")
      .groupBy("peersValue", "calculationDate", "exportDate", "reportCurrency", "reportName", "date")
      .agg(first("isin").as("isin"))


    val indexRowsToAppend = groupedByPeers.join(benchDataColumns
      .select("isin", "periodType", "periodStartDate", "return", "date"), Seq("isin", "date"), "left")
      .withColumn("groupInvestment", concat(col("peersValue"), lit(" Index")))
      .withColumn("isin", when(col("groupInvestment").endsWith(" Index"), lit(null)).otherwise(col("isin")))
      .where("return is not null")
      .distinct

    //display(indexRowsToAppend)
    // To be able to join with vendome data

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
    }

    iaSectorDf.unionByName(sameColumnsDf).unionByName(sameColumnsDfSpecialLab)
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
      when(col("reportName") =!= "5 - Wendy DCP Digital" && col("groupInvestment").startsWith("EAA") && col("groupInvestment").endsWith("Hedged"), concat(col("peersValue"), lit(" Category Average "), substring(col("groupInvestment"), -10, 10)))
        .when(col("reportName") =!= "5 - Wendy DCP Digital" && col("groupInvestment").startsWith("EAA") && !col("groupInvestment").endsWith("Hedged"), concat(col("peersValue"), lit(" Category Average")))
        .when(col("reportName") =!= "5 - Wendy DCP Digital" && col("groupInvestment").startsWith("Japan Fund"), concat(col("peersValue"), lit(" Category Average")))
        .otherwise(col("groupInvestment"))
    ).cache()


    // val handledPeersLibDf = handledEaDf.join(vendomeExplodedData.select("fundMnemonic", "lib").withColumnRenamed("lib", "peersLib"), handledEaDf("peersValue") === vendomeExplodedData("fundMnemonic"), "left").distinct()

    val categoryAverageDf = handledEaDf.where("averagecat like '%Category Average%'")
    val peersValuesWithAverageCategory = categoryAverageDf.select("peersValue").distinct.as[String].collect.toList

    // Garder que les peers qui ont pas de EAA, et qui sont like Carmignac
    val rule2FilteredDf = handledEaDf.filter(!col("peersValue").isin(peersValuesWithAverageCategory:_*))
      .where("groupInvestment like 'Carmignac%'")
      .select("reportName", "peersValue", "morningstarCategory")
      .distinct

    val subdataset = handledEaDf.withColumn("equivalentpeer", col("peersValue")).drop("peersValue", "reportName", "morningstarCategory").distinct

    val groupInvestmentRule2RowsToAppend = rule2FilteredDf.join(subdataset, rule2FilteredDf("morningstarCategory") === subdataset("groupInvestment"), "left")
      .withColumn("averagecat", concat(col("peersValue"), lit(" Category Average"))).distinct

    val reorderedColumnsDf = groupInvestmentRule2RowsToAppend.select(handledEaDf.columns.map(col): _*)

    handledEaDf.union(reorderedColumnsDf)
      .withColumn("groupInvestment", col("averagecat"))
      .drop("averagecat")
      .distinct
  }




  // Handle peers lib

  def handleIASectorDf(finalHandledEAADf: DataFrame, withoutIASector: Seq[Any]): DataFrame = {
    val vendomeExplodedData = readVendomeStagingData()
    val ltranscodificationPath: String = AdlsUtils.buildPath(transcodificationPath)

    val transcoDf = spark.read.option("inferSchema", false).option("header", true).option("sep", ";").csv(ltranscodificationPath)

    val handledPeersLibDf = finalHandledEAADf.join(vendomeExplodedData.select("Cod", "lib").withColumnRenamed("lib", "peersLib"), finalHandledEAADf("peersValue") === vendomeExplodedData("Cod"), "left").distinct()
    val transcodedDf = handledPeersLibDf.join(transcoDf, handledPeersLibDf("groupInvestment") === transcoDf("name"), "left")
    val groupByDf = transcodedDf.where($"IA Sector".isNotNull).groupBy("peersValue").agg(first("IA Sector").as("Transco"))
    val peersTransco = withoutIASector.map(x => (x, groupByDf.where($"peersValue" === x).select("Transco").head(1)))
    val peersMap = peersTransco.toMap


    val iaSectorRowsToAppend = withoutIASector.map(peerGroupWithoutIASector =>
      if (peersMap.getOrElse(peerGroupWithoutIASector, null).headOption != None) {

        handledPeersLibDf.where("groupInvestment is not null")
          .where("groupInvestment LIKE 'IA%" + peersMap.getOrElse(peerGroupWithoutIASector, null)(0)(0) + "%'")
          .withColumn("peersValue", explode(when($"groupInvestment".like("%" + peersMap.getOrElse(peerGroupWithoutIASector, null)(0)(0) + "%"), array(col("peersValue"), lit(peerGroupWithoutIASector))).otherwise(array($"peersValue"))))
          .withColumn("groupInvestment", explode(when($"groupInvestment".like("%" + peersMap.getOrElse(peerGroupWithoutIASector, null)(0)(0) + "%"), array(col("peersValue"), lit("IA SECTOR"))).otherwise(array($"groupInvestment"))))
          .where("groupInvestment == 'IA SECTOR'")
      }
      else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], handledPeersLibDf.schema)

    )
      .reduce(_.union(_)).distinct()

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

    val handledIaSectorDfJoin = handledIaSectorDf.join(datesDf,Seq("sourceFileName","ReportName"), "left")
    val removeNullDate = handledIaSectorDfJoin.where($"date".isNotNull)
    val windowSpec  = Window.partitionBy("date", "reportName", "EuSfdrFundType", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Principle_Adverse_Impact_Consideration", "ISR_EMT", "Towards_Sustainability_EMT", "GlobalCategory", "SustainableInvestmentOverall", "PAI_Scope_1_GHG_EmissionsTonnes", "PAI_Scope_1_2_3_GHG_EmissionsTonnes", "PAI_Carbon_Footprint_Scope_1_2_Tonnes_Per_EURm", "PAI_Carbon_Footprint_Scope_1_2_3_Tonnes_Per_EURm", "PAI_GHG_Intensity_Scope_1_2_Average_Value", "PAI_GHG_Intensity_Scope_1_2_3_Average_Value", "PAI_Fossil_Fuel_%_of_Covered_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Eligible_Portfolio_Involved","PAI_Energy_Consumption_Intensity_NACE_L_Average_Value","PAI_Negative_Affect_On_Biodiversity_%_Portfolio_Involved", "PAI_Emissions_Water_Tonnes_Per_EURm", "PAI_Hazardous_Waste_Tonnes_Per_EURm", "PAI_Ungc_Lack_Compliance_Mechanisms_%_Portfolio_Involved", "PAI_Controversial_Weapons_%_Eligible_Portfolio_Involved","PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Gender_Pay_Gap_Average_Value", "PAI_Percentage_Female_Board_Members_Average_Value", "reportCurrency", "isin", "groupInvestment", "peersValue", "morningstarRating", "currency", "morningstarCategory", "fundSize", "returnDateDaily", "netFlowYTD", "periodType", "periodStartDate", "periodEndDate", "sharpeRatio", "sortinoRatio", "stdDev", "maxDrawdown").orderBy($"calculationDate".desc)
    removeNullDate.withColumn("row_number",row_number.over(windowSpec)).where("row_number = 1").drop("row_number")
  }

  def handleperioddate(ciIndexDf: DataFrame): DataFrame = {
    val df1 = ciIndexDf.where("groupInvestment not like '% Index' and groupInvestment not like '% Category Average' and groupInvestment not like 'IA SECTOR'")
    //and periodType like 'Since PM*'")
    val df = df1.select($"date", $"reportName", $"reportCurrency", $"periodType", $"peersValue", $"periodStartDate".as("newperiodStartDate"), $"periodEndDate".as("newperiodEndDate")).distinct()
    val newciIndexDf = ciIndexDf.join(df, Seq("date", "peersValue", "reportName", "reportCurrency", "periodType"), "left").distinct()
    //display(newciIndexDf.where("groupInvestment like '% Index' and periodType like 'Since PM*'"))
    val lastdf = newciIndexDf.withColumn("periodStartDate", when(($"groupInvestment".like("% Index")) || ($"groupInvestment".like("% Category Average")) || ($"groupInvestment".like("IA SECTOR")), $"newperiodStartDate")
      .otherwise($"periodStartDate"))
      .withColumn("periodEndDate", when(($"groupInvestment".like("% Index")) || ($"groupInvestment".like("% Category Average")) || ($"groupInvestment".like("IA SECTOR")), $"newperiodEndDate")
        .otherwise($"periodEndDate")).distinct()
    val cleandf = lastdf.drop("newperiodEndDate", "newperiodEndDate")
    cleandf
  }

  def main(args: Array[String]): Unit = {
    mountContainers

    val StagingDeltaPath: String = AdlsUtils.buildPath(s"/staging/$dataSource/$datasetName/deltatable")

    val stagingDf = readLastStagingData(StagingDeltaPath).distinct.cache()

    val pivotDf = pivotStagingDf(stagingDf)
    val renamedColumnsDf = renameMetricColumns(pivotDf).cache()
    val dateDf = handleDateDf(renamedColumnsDf).cache()

    val ciIndexDf = mergeVendomeBenchData(dateDf).cache()

       val peersWithIASector = ciIndexDf.where("Isin is null and GroupInvestment LIKE 'IA%'").select("PeersValue")
          .distinct()
          .as[String]
          .collect.toList

        val allPeers = ciIndexDf.select("PeersValue")
          .distinct()
          .as[String]
          .collect.toList

        val withoutIASector = allPeers.diff(peersWithIASector)

        val finalHandledEAADf = handleEAADf(ciIndexDf).cache()
        val handledIaSectorDf = handleIASectorDf(finalHandledEAADf, withoutIASector).cache()

        val handledIaSectorDf_average = handleperioddate(handledIaSectorDf)

        val writtableDf = handledIaSectorDf_average.select("date", "reportName", "reportCurrency", "isin", "groupInvestment", "peersValue", "fundSize", "returnDateDaily", "netFlowYTD", "periodType", "periodStartDate", "periodEndDate", "return", "Morningstar category percentile", "sharpeRatio", "sortinoRatio", "maxDrawdown", "stdDev", "morningstarRating", "peersLib", "isCarmignacSC", "EuSfdrFundType", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Principle_Adverse_Impact_Consideration", "ISR_EMT", "Towards_Sustainability_EMT", "GlobalCategory", "SustainableInvestmentOverall", "PAI_Scope_1_GHG_EmissionsTonnes", "PAI_Scope_1_2_3_GHG_EmissionsTonnes", "PAI_Carbon_Footprint_Scope_1_2_Tonnes_Per_EURm", "PAI_Carbon_Footprint_Scope_1_2_3_Tonnes_Per_EURm", "PAI_GHG_Intensity_Scope_1_2_Average_Value", "PAI_GHG_Intensity_Scope_1_2_3_Average_Value", "PAI_Fossil_Fuel_%_of_Covered_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Portfolio_Involved", "PAI_Fossil_Fuel_%_of_Eligible_Portfolio_Involved", "PAI_Non_Renewable_%_Energy_Consumption_Average_Value", "PAI_Non_Renewable_of_Energy_Production_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_A_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_B_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_C_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_D_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_E_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_F_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_G_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_H_Average_Value", "PAI_Energy_Consumption_Intensity_NACE_L_Average_Value", "PAI_Negative_Affect_On_Biodiversity_%_Portfolio_Involved", "PAI_Emissions_Water_Tonnes_Per_EURm", "PAI_Hazardous_Waste_Tonnes_Per_EURm", "PAI_Ungc_Lack_Compliance_Mechanisms_%_Portfolio_Involved", "PAI_Controversial_Weapons_%_Eligible_Portfolio_Involved", "PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Gender_Pay_Gap_Average_Value", "PAI_Percentage_Female_Board_Members_Average_Value").distinct.cache()

        val lastwrittableDf = writtableDf.withColumn("return",
          when($"groupInvestment".like("% Index"), col("return") * lit(100))
            .when($"groupInvestment" === "Carmignac Portfolio Global Market Neutral", col("return") * lit(100))
            .otherwise($"return"))
          .withColumn("reportName", initcap(col("reportName")))
          .withColumnRenamed("EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned_Investments_Sustainable_Investments_Taxonomy_Aligned", "EU_Sustainable_Finance_Disclosure_Regulation_Minimum_Or_Planned")
          .withColumnRenamed("Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_Sovereign_Bonds", "Minimum_Percentage_Investments_Aligned_EU_Taxonomy_Including_So")
          .withColumnRenamed("PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Involved", "PAI_Ungc_Principles_OECD_Guidelines_Violations_%_Portfolio_Invo").distinct()

       val data_to_delete = lastwrittableDf.where("date is not null and groupInvestment not like 'Carmignac Portfolio Global Market Neutral%'").select("date").first().getDate(0)
        val selectRightDate = s"""date = """ + s"'${data_to_delete}'"
        val lastwrittableDf_filtered = lastwrittableDf.where(selectRightDate)
        val tmpWritePath = AdlsUtils.buildPath("/wip/render/BIDigit/lastwrittableDf.parquet")
        lastwrittableDf_filtered.write.mode("overwrite").parquet(tmpWritePath)
        spark.catalog.clearCache()
        val lastDf = spark.read.parquet(tmpWritePath)

        val particulacaseDf = lastDf
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
        val lastwrittableDf_filtered2 = dfToWrite.where(selectRightDate)
        val dropCurrentTableWendy = s"""delete from marketingbi."MorningstarWendyMetrics" where date = """ + s"'${data_to_delete}'"

        val lPgRender: PostgresUtils = getRenderConnection
        val renderDf = lPgRender.executePgStatement(dropCurrentTableWendy)

       // val tmplastWritePath = AdlsUtils.buildPath("/wip/render/BIDigit/lastDf.parquet")
       // lastwrittableDf_filtered2.write.mode("overwrite").parquet(tmplastWritePath)
   // spark.read.parquet(tmplastWritePath)
        lPgRender.writeDfIntoTable(lastwrittableDf_filtered2, biTableName,"append")

        AdlsUtils.removeFile(tmpWritePath,true)
        //AdlsUtils.removeFile(tmplastWritePath,true)
  }
}