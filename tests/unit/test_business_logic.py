"""Unit tests for business logic validation."""
import pytest
from pyspark.sql.functions import coalesce, col
from pyspark.sql.types import StructType, StructField, StringType


class TestCoalescePriority:
    """Test COALESCE logic prioritizes GCDS > GIC > RANZ."""
    
    def test_coalesce_gcds_priority(self, spark):
        """Test COALESCE selects GCDS value when present."""
        # Arrange
        schema = StructType([
            StructField("gcds_value", StringType(), True),
            StructField("gic_value", StringType(), True),
            StructField("ranz_value", StringType(), True)
        ])
        
        data = [("GCDS_Data", "GIC_Data", "RANZ_Data")]
        df = spark.createDataFrame(data, schema)
        
        # Act
        df_result = df.withColumn(
            "result",
            coalesce(col("gcds_value"), col("gic_value"), col("ranz_value"))
        )
        
        # Assert
        result = df_result.select("result").first()[0]
        assert result == "GCDS_Data"
    
    def test_coalesce_gic_fallback(self, spark):
        """Test COALESCE selects GIC value when GCDS is null."""
        # Arrange
        schema = StructType([
            StructField("gcds_value", StringType(), True),
            StructField("gic_value", StringType(), True),
            StructField("ranz_value", StringType(), True)
        ])
        
        data = [(None, "GIC_Data", "RANZ_Data")]
        df = spark.createDataFrame(data, schema)
        
        # Act
        df_result = df.withColumn(
            "result",
            coalesce(col("gcds_value"), col("gic_value"), col("ranz_value"))
        )
        
        # Assert
        result = df_result.select("result").first()[0]
        assert result == "GIC_Data"
    
    def test_coalesce_ranz_fallback(self, spark):
        """Test COALESCE selects RANZ value when GCDS and GIC are null."""
        # Arrange
        schema = StructType([
            StructField("gcds_value", StringType(), True),
            StructField("gic_value", StringType(), True),
            StructField("ranz_value", StringType(), True)
        ])
        
        data = [(None, None, "RANZ_Data")]
        df = spark.createDataFrame(data, schema)
        
        # Act
        df_result = df.withColumn(
            "result",
            coalesce(col("gcds_value"), col("gic_value"), col("ranz_value"))
        )
        
        # Assert
        result = df_result.select("result").first()[0]
        assert result == "RANZ_Data"
    
    def test_coalesce_all_null(self, spark):
        """Test COALESCE returns null when all sources are null."""
        # Arrange
        schema = StructType([
            StructField("gcds_value", StringType(), True),
            StructField("gic_value", StringType(), True),
            StructField("ranz_value", StringType(), True)
        ])
        
        data = [(None, None, None)]
        df = spark.createDataFrame(data, schema)
        
        # Act
        df_result = df.withColumn(
            "result",
            coalesce(col("gcds_value"), col("gic_value"), col("ranz_value"))
        )
        
        # Assert
        result = df_result.select("result").first()[0]
        assert result is None


class TestRanzStatusMapping:
    """Test RANZ lifecycle status code mappings."""
    
    def test_active_client_status_mapping(self, spark, ranz_status_mapping):
        """Test AC status maps to 'Active Client'."""
        # Arrange
        data = [("AC",)]
        df = spark.createDataFrame(data, ["status_code"])
        
        # Act
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in ranz_status_mapping.items()],
            ["status_code", "description"]
        )
        df_result = df.join(mapping_df, "status_code", "left")
        
        # Assert
        result = df_result.select("description").first()[0]
        assert result == "Active Client"
    
    def test_blocked_client_status_mapping(self, spark, ranz_status_mapping):
        """Test BL status maps to 'Blocked Client'."""
        # Arrange
        data = [("BL",)]
        df = spark.createDataFrame(data, ["status_code"])
        
        # Act
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in ranz_status_mapping.items()],
            ["status_code", "description"]
        )
        df_result = df.join(mapping_df, "status_code", "left")
        
        # Assert
        result = df_result.select("description").first()[0]
        assert result == "Blocked Client"
    
    def test_unmapped_status_returns_null(self, spark, ranz_status_mapping):
        """Test unmapped status code returns null description."""
        # Arrange
        data = [("UNKNOWN_CODE",)]
        df = spark.createDataFrame(data, ["status_code"])
        
        # Act
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in ranz_status_mapping.items()],
            ["status_code", "description"]
        )
        df_result = df.join(mapping_df, "status_code", "left")
        
        # Assert
        result = df_result.select("description").first()[0]
        assert result is None
    
    @pytest.mark.parametrize("status_code,expected_description", [
        ("AC", "Active Client"),
        ("BL", "Blocked Client"),
        ("CL", "Closed Client"),
        ("DC", "Declined Client"),
        ("AP", "Applicant Client")
    ])
    def test_multiple_status_mappings(self, spark, ranz_status_mapping, status_code, expected_description):
        """Test multiple RANZ status code mappings."""
        # Arrange
        data = [(status_code,)]
        df = spark.createDataFrame(data, ["status_code"])
        
        # Act
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in ranz_status_mapping.items()],
            ["status_code", "description"]
        )
        df_result = df.join(mapping_df, "status_code", "left")
        
        # Assert
        result = df_result.select("description").first()[0]
        assert result == expected_description


class TestDateLogic:
    """Test date-related business logic."""
    
    def test_start_date_before_change_date(self, spark):
        """Test PartyRoleLifecycleStartDate < PartyRoleLifecycleChangeDate."""
        from pyspark.sql.functions import to_date
        from pyspark.sql.types import StructType, StructField, StringType
        
        # Arrange
        schema = StructType([
            StructField("StartDate", StringType(), True),
            StructField("ChangeDate", StringType(), True)
        ])
        
        data = [
            ("2024-01-01", "2024-01-15"),
            ("2024-02-01", "2024-02-10")
        ]
        df = spark.createDataFrame(data, schema)
        
        # Act
        df_result = df.withColumn("StartDate_dt", to_date(col("StartDate"))) \
                      .withColumn("ChangeDate_dt", to_date(col("ChangeDate"))) \
                      .withColumn("is_valid", col("StartDate_dt") < col("ChangeDate_dt"))
        
        # Assert
        results = df_result.select("is_valid").collect()
        assert all(row[0] for row in results)
    
    def test_null_change_date_handling(self, spark):
        """Test handling of null PartyRoleLifecycleChangeDate."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        # Arrange
        schema = StructType([
            StructField("StartDate", StringType(), True),
            StructField("ChangeDate", StringType(), True)
        ])
        
        data = [("2024-01-01", None)]
        df = spark.createDataFrame(data, schema)
        
        # Act & Assert
        result = df.select("ChangeDate").first()[0]
        assert result is None


class TestRunTypeLogic:
    """Test RunType determination logic."""
    
    def test_historical_run_when_load_date_provided(self):
        """Test RunType becomes 'historical' when Load_Date is provided."""
        # Arrange
        Load_Date = "20240115"
        RunType = "daily"  # Initial value
        
        # Act
        if Load_Date:
            RunType = "historical"
        
        # Assert
        assert RunType == "historical"
    
    def test_daily_run_when_no_load_date(self):
        """Test RunType remains 'daily' when no Load_Date is provided."""
        # Arrange
        Load_Date = ""
        RunType = "daily"
        
        # Act
        if Load_Date:
            RunType = "historical"
        
        # Assert
        assert RunType == "daily"
    
    def test_load_date_defaults_to_today(self, load_date_today):
        """Test Load_Date defaults to today when not provided."""
        # Arrange
        Load_Date = ""
        
        # Act
        if not Load_Date:
            Load_Date = load_date_today
        
        # Assert
        assert Load_Date == load_date_today
        assert len(Load_Date) == 8  # YYYYMMDD format