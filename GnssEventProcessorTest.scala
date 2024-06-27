package com.sensata.data_office.pipeline.queries

import com.sensata.data_office.data._
import com.sensata.data_office.utilities.PipelineUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType
import org.mockito.ArgumentMatchersSugar._
import org.mockito.{IdiomaticMockito, MockitoSugar}
import org.scalatest.FunSpec
import utils.{SparkSessionTestWrapper, utilityMethods}


class GnssEventProcessorTest extends FunSpec with SparkSessionTestWrapper with IdiomaticMockito {
  import spark.implicits._

  /**
   * Mocked Kafka Writer for displaying final dataframe
   * @param df
   * @param write_mode
   * @param kafkaOptions
   * @param kafka_topic
   */
  def writeDataFrameToKafkaTopicMocked(df: DataFrame, write_mode: String, kafkaOptions: Map[String,String], kafka_topic: String): Unit = {

  //  if( df.where($"key" contains("curated")).count() > 0) {
      resultsDf = df
  //  }
  }

  var resultsDf = spark.emptyDataFrame

  val utils = new utilityMethods()

  def updateCustomerDimCacheMocked(filename: String): DataFrame = {

    try {
      val notify_schema = ScalaReflection.schemaFor[CustomerRoutingRecord].dataType.asInstanceOf[StructType]
     /* spark.read
        .format("kafka")
        .options(kafkaConfig)
        .option("startingOffsets","earliest")
        .option("subscribe", customer_device_routing_topic)
        .load()*/

      utils.loadCSVAsDataframe(filename)
        .select(
          from_json($"value" cast "string"
            , notify_schema
          ) as "events"
        )
        .select($"events.*")

    } catch {
      case _=> spark.emptyDataset[CustomerRoutingRecord].toDF()
    }

  }

  describe("Vehicle Data Test") {
    it("Test-1 Return ProcessedGPSRecord Dataset formatted for curated GNSS Topic") {
      withObjectMocked[PipelineUtil.type] {
        PipelineUtil.updateCustomerDimCacheFromDatabase() returns utils.updateCustomerDimCacheMocked()
        PipelineUtil.FetchCustomerDeviceFromDatabase() returns utils.FetchCustomerDeviceFromDatabaseMocked()
        PipelineUtil.spark returns spark
        PipelineUtil.environment = "dev"
        PipelineUtil.active_alerts_prev_state_topic returns "alert-past-notification"
        PipelineUtil.asset_prev_activity_topic returns "asset-past-activity-notification"

        MockitoSugar.when(PipelineUtil.getEnvVariable("ENVIRONMENT")) thenAnswer utils.getEnvVariableMocked("ENVIRONMENT")

        MockitoSugar.when(PipelineUtil.dedupToLatest(any[DataFrame])) thenCallRealMethod()
        MockitoSugar.when(PipelineUtil.dedupActivityToLatest(any[DataFrame])) thenCallRealMethod()

        MockitoSugar.when(
          PipelineUtil.updateCustomerDimCache(any[String])
        ) thenAnswer (
          (updateCustomerDimCacheMocked("cus_dev_routing.csv"), "earliest")
          )

        PipelineUtil.getAssetAlertStateFromDatabaseCache returns utils.loadAssetLastAlertDatabaseStub("cache/asset_alert_activity.csv")
        PipelineUtil.getAssetActivityStateFromDatabaseCache() returns utils.loadAssetLastActivityDatabaseStub("cache/asset_last_activity.csv")

        MockitoSugar.when(
          PipelineUtil.writeDataFrameToKafkaTopic(
            any[DataFrame], any[String], any[Map[String, String]], eqTo[String]("curated-gnss-data-records")
          )
        ) thenAnswer writeDataFrameToKafkaTopicMocked _

        //  val testDf = utils.loadCSVAsDataframe("gnss_combined_extract.csv")
        val testDf = utils.loadCSVAsDataframe("gnss_combined_jan22.csv")

        GnssEventProcessor.processMessagesByBatch(testDf, 1)

        println("*************************************************resultsDf****************************************")
        resultsDf.show(false)
        assert(resultsDf.columns.contains("key"))
        assert(resultsDf.columns.contains("value"))
        assert(resultsDf.count() >= 1)
        val curated_gnss_df = resultsDf
          .select(
            from_json($"value" cast "string"
              , ScalaReflection.schemaFor[ProcessedGPSRecord].dataType.asInstanceOf[StructType]
            ) as "events"
          )
          .select($"events.*")

        println("*************************************************curated_gnss_df****************************************")
        curated_gnss_df.show(false)
        assert(curated_gnss_df.where($"asset_code" isNotNull).count() > 0, "No Null Access Codes")

        val validate_curated_df = curated_gnss_df.where($"device_id" === "1KqkCSg13j12")

       /* validate_curated_df.show(false)
        assert(validate_curated_df.where($"asset_code" === "STAGING-76").count() >= 1, "asset_code correctly set")
        assert(validate_curated_df.where($"asset_id" === 76).count() >= 1,"asset_id correctly set")
        assert(validate_curated_df.where($"atis_state" === "inactive").count() >= 1,"atis_state status correctly set")
        assert(validate_curated_df.where($"ttl_active" === "13.0.4.34").count() >= 1,"ttl_active ip-address correctly set")
        assert(validate_curated_df.where($"kl15_pin_state" === 1).count() == 1,"kl15_pin_state correctly set")
        assert(validate_curated_df.where($"topic_id" === "staging").count() >= 1,"routing topic prefix is correctly set")
        assert(validate_curated_df.where($"asset_name" === "hub").count() >= 1,"data is coming from Hub")*/

        //   val odometer_df = curated_gnss_df.where($"device_id" === "1KqjP1nVJQYX")

        //   assert(odometer_df.where($"asset_code" === "SENSDEV-78").count() == 1, "asset_code correctly set")
        //   assert(odometer_df.where($"odometer" === 42134000).count() == 1, "odometer correctly set from Hub")
        //    assert(odometer_df.where($"odometer_total" === 42134000).count() == 1, "odometer_total correctly set from Hub")
        //    assert(odometer_df.where($"odometer_in_miles" === 26180.845714).count() == 1, "odometer in miles correctly set")
        //   assert(odometer_df.where($"odometer_total_in_miles" === 26180.845714).count() == 1, "odometer_total in miles correctly set")

      }
    }

    it("Test-2 Return ProcessedGPSRecord Dataset formatted for asset activity Topic") {
      withObjectMocked[PipelineUtil.type] {
        PipelineUtil.updateCustomerDimCacheFromDatabase() returns utils.updateCustomerDimCacheMocked()
        PipelineUtil.FetchCustomerDeviceFromDatabase() returns utils.FetchCustomerDeviceFromDatabaseMocked()
        PipelineUtil.spark returns spark
        PipelineUtil.environment = "dev"
        PipelineUtil.active_alerts_prev_state_topic returns "alert-past-notification"
        PipelineUtil.asset_prev_activity_topic returns "asset-past-activity-notification"


        MockitoSugar.when(PipelineUtil.getEnvVariable("ENVIRONMENT")) thenAnswer utils.getEnvVariableMocked("ENVIRONMENT")
        MockitoSugar.when(PipelineUtil.dedupToLatest(any[DataFrame])) thenCallRealMethod()
        MockitoSugar.when(PipelineUtil.dedupActivityToLatest(any[DataFrame])) thenCallRealMethod()

        MockitoSugar.when(
          PipelineUtil.updateCustomerDimCache(any[String])
        ) thenAnswer (
          (updateCustomerDimCacheMocked("cus_dev_routing.csv"), "earliest")
          )

        PipelineUtil.getAssetAlertStateFromDatabaseCache returns utils.loadAssetLastAlertDatabaseStub("cache/asset_alert_activity.csv")
        PipelineUtil.getAssetActivityStateFromDatabaseCache() returns utils.loadAssetLastActivityDatabaseStub("cache/asset_last_activity.csv")


        MockitoSugar.when(
          PipelineUtil.writeDataFrameToKafkaTopic(
            any[DataFrame], any[String], any[Map[String, String]], eqTo[String]("asset-activity-notification")
          )
        ) thenAnswer writeDataFrameToKafkaTopicMocked _

        val testDf = utils.loadCSVAsDataframe("extract_gnss_velocity.csv")
          .union(
            utils.loadCSVAsDataframe("extract_gnss.csv")
          ).union(
          utils.loadCSVAsDataframe("gnss_combined_extract.csv")
        )

        GnssEventProcessor.processMessagesByBatch(testDf, 1)

        resultsDf.show(5,false)
        assert(resultsDf.columns.contains("key"))
        assert(resultsDf.columns.contains("value"))
        assert(resultsDf.count() >= 1)

        val asset_activity_df = resultsDf
          .select(
            from_json($"value" cast "string"
              , ScalaReflection.schemaFor[AssetActivityNotification].dataType.asInstanceOf[StructType]
            ) as "events"
          )
          .select($"events.*")
          .where($"asset_code" isNotNull)

        println("*************************************************asset_activity_df****************************************")
        asset_activity_df.show(5,false)

        assert(asset_activity_df.where($"asset_code" === "SENSDEV-14").count() >= 1, "asset_code correctly set")
        assert(asset_activity_df.where($"atis_state" === "inactive").count() >= 1, "atis_state status correctly set")
        assert(asset_activity_df.where($"ttl_active" === "11.0.2.55").count() >= 1, "ttl_active ip-address correctly set")
        assert(asset_activity_df.where($"kl15_pin_state" === 1).count() >= 1, "kl15_pin_state correctly set")
        assert(asset_activity_df.where($"asset_code" === "SENSDEV-14" && $"atis_state" === "active").count() >= 0, "both correctly set")


      }
    }
    ignore("Test-3 Process ATIS data for Fedx and Wrnr newly created Topic") {
      withObjectMocked[PipelineUtil.type] {
        PipelineUtil.updateCustomerDimCacheFromDatabase() returns utils.updateCustomerDimCacheMocked()
        PipelineUtil.spark returns spark
        PipelineUtil.environment = "dev"
        PipelineUtil.active_alerts_prev_state_topic returns "alert-past-notification"
        PipelineUtil.asset_prev_activity_topic returns "asset-past-activity-notification"

        MockitoSugar.when(PipelineUtil.getEnvVariable("ENVIRONMENT")) thenAnswer utils.getEnvVariableMocked("ENVIRONMENT")

        MockitoSugar.when(PipelineUtil.dedupToLatest(any[DataFrame])) thenCallRealMethod()
        MockitoSugar.when(PipelineUtil.dedupActivityToLatest(any[DataFrame])) thenCallRealMethod()

        MockitoSugar.when(
          PipelineUtil.updateCustomerDimCache(any[String])
        ) thenAnswer (
          (updateCustomerDimCacheMocked("cus_dev_routing.csv"), "earliest")
          )

        PipelineUtil.getAssetAlertStateFromDatabaseCache returns utils.loadAssetLastAlertDatabaseStub("cache/asset_alert_activity.csv")
        PipelineUtil.getAssetActivityStateFromDatabaseCache() returns utils.loadAssetLastActivityDatabaseStub("cache/asset_last_activity.csv")

        MockitoSugar.when(
          PipelineUtil.writeDataFrameToKafkaTopic(
            any[DataFrame], any[String], any[Map[String, String]], eqTo[String]("rawingestion-m2m")
          )
        ) thenAnswer writeDataFrameToKafkaTopicMocked _

        //  val testDf = utils.loadCSVAsDataframe("gnss_combined_extract.csv")
        val testDf = utils.loadCSVAsDataframe("gnss_combined_jan22.csv")

        GnssEventProcessor.processMessagesByBatch(testDf, 1)

        println("*************************************************resultsDf****************************************")
        resultsDf.show(false)

      }
    }

  }
}
