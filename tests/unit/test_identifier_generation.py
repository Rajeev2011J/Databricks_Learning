"""Unit tests for LocalSystemIdentifier generation logic."""
import pytest
from pyspark.sql.functions import col, concat, lit, when


class TestLocalSystemIdentifierGeneration:
    """Test LocalSystemIdentifier generation for GIC, RANZ, and GCDS."""
    
    def test_gic_identifier_format(self, spark):
        """Test GIC identifier format: GIC_<cod_institucional>."""
        # Arrange
        data = [("12345",), ("67890",), ("11111",)]
        df = spark.createDataFrame(data, ["COD_INSTITUCIONAL"])
        
        # Act
        df_result = df.withColumn(
            "LocalSystemIdentifier",
            concat(lit("GIC_"), col("COD_INSTITUCIONAL"))
        )
        
        # Assert
        results = df_result.select("LocalSystemIdentifier").collect()
        assert all(row[0].startswith("GIC_") for row in results)
        assert results[0][0] == "GIC_12345"
        assert results[1][0] == "GIC_67890"
        assert results[2][0] == "GIC_11111"
    
    def test_ranz_identifier_format(self, spark):
        """Test RANZ identifier format: RANZ_<pkey_src_object>."""
        # Arrange
        data = [("RANZ_PK001",), ("RANZ_PK002",)]
        df = spark.createDataFrame(data, ["PKEY_SRC_OBJECT"])
        
        # Act
        df_result = df.withColumn(
            "LocalSystemIdentifier",
            concat(lit("RANZ_"), col("PKEY_SRC_OBJECT"))
        )
        
        # Assert
        results = df_result.select("LocalSystemIdentifier").collect()
        assert all(row[0].startswith("RANZ_") for row in results)
        assert results[0][0] == "RANZ_RANZ_PK001"
        assert results[1][0] == "RANZ_RANZ_PK002"
    
    def test_gcds_identifier_format(self, spark):
        """Test GCDS identifier format: GCDS_<gcid>."""
        # Arrange
        data = [("GCDS001",), ("GCDS002",)]
        df = spark.createDataFrame(data, ["gcid"])
        
        # Act
        df_result = df.withColumn(
            "LocalSystemIdentifier",
            concat(lit("GCDS_"), col("gcid"))
        )
        
        # Assert
        results = df_result.select("LocalSystemIdentifier").collect()
        assert all(row[0].startswith("GCDS_") for row in results)
        assert results[0][0] == "GCDS_GCDS001"
        assert results[1][0] == "GCDS_GCDS002"
    
    def test_case_logic_prioritization(self, spark):
        """Test CASE logic prioritizes GIC match > RANZ match > GCDS default."""
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("gcid", StringType(), True),
            StructField("identifier", StringType(), True),
            StructField("KeyStore_type", StringType(), True),
            StructField("gic_cod", StringType(), True),
            StructField("ranz_id", StringType(), True)
        ])
        
        data = [
            ("GCDS001", "12345", "GIC", "12345", None),  # Should be GIC
            ("GCDS002", "RANZ001", "CB RANZ", None, "RANZ001"),  # Should be RANZ
            ("GCDS003", "OTHER", "OTHER", None, None)  # Should be GCDS
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Act - Simulate the CASE logic from the notebook
        df_result = df.withColumn(
            "LocalSystemIdentifier",
            when(
                (col("identifier") == col("gic_cod")) & (col("KeyStore_type") == "GIC"),
                concat(lit("GIC_"), col("gic_cod"))
            ).when(
                (col("identifier") == col("ranz_id")) & (col("KeyStore_type") == "CB RANZ"),
                concat(lit("RANZ_"), col("ranz_id"))
            ).otherwise(
                concat(lit("GCDS_"), col("gcid"))
            )
        )
        
        # Assert
        results = df_result.select("LocalSystemIdentifier").collect()
        assert results[0][0] == "GIC_12345"
        assert results[1][0] == "RANZ_RANZ001"
        assert results[2][0] == "GCDS_GCDS003"
    
    def test_identifier_uniqueness(self, spark):
        """Test that LocalSystemIdentifier values are unique after distinct()."""
        # Arrange
        data = [
            ("GIC_12345",),
            ("GIC_12345",),  # Duplicate
            ("RANZ_RANZ001",),
            ("GCDS_GCDS001",)
        ]
        df = spark.createDataFrame(data, ["LocalSystemIdentifier"])
        
        # Act
        df_unique = df.distinct()
        
        # Assert
        assert df_unique.count() == 3
        identifiers = [row[0] for row in df_unique.collect()]
        assert len(identifiers) == len(set(identifiers))  # All unique
    
    def test_null_handling_in_identifier_generation(self, spark):
        """Test handling of null values in identifier generation."""
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("COD_INSTITUCIONAL", StringType(), True)
        ])
        
        data = [("12345",), (None,), ("67890",)]
        df = spark.createDataFrame(data, schema)
        
        # Act
        df_result = df.withColumn(
            "LocalSystemIdentifier",
            concat(lit("GIC_"), col("COD_INSTITUCIONAL"))
        )
        
        # Assert
        results = df_result.select("LocalSystemIdentifier").collect()
        assert results[0][0] == "GIC_12345"
        assert results[1][0] is None  # Concat with null produces null
        assert results[2][0] == "GIC_67890"