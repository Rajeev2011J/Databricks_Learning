"""Integration tests for multi-system scenarios (GCDS + GIC + RANZ)."""
import pytest
from pyspark.sql.functions import col, coalesce, concat, lit, when


class TestGCDSGICIntegration:
    """Test integration between GCDS and GIC systems."""
    
    def test_gcds_gic_join_on_keystore(self, spark, sample_gcds_keystore_data, 
                                        sample_gic_pessoa_data, sample_gcds_client_data):
        """Test GCDS-GIC join on KeyStore_type = 'GIC'."""
        # Arrange
        df_gcds_keystore = sample_gcds_keystore_data
        df_gic = sample_gic_pessoa_data
        df_gcds_client = sample_gcds_client_data
        
        # Join GCDS Client with KeyStore
        df_gcds = df_gcds_client.join(df_gcds_keystore, "gcid", "inner")
        
        # Act - Join with GIC
        df_result = df_gcds.join(
            df_gic,
            (df_gcds["KeyStore_value"] == df_gic["COD_INSTITUCIONAL"]) & 
            (df_gcds["KeyStore_type"] == "GIC"),
            "left"
        )
        
        # Assert
        gic_matches = df_result.filter(col("COD_INSTITUCIONAL").isNotNull()).count()
        assert gic_matches == 2  # Should match GCDS001 and GCDS002
        
        # Verify specific match
        gcds001_match = df_result.filter(
            col("gcid") == "GCDS001"
        ).select("COD_INSTITUCIONAL").first()[0]
        assert gcds001_match == "12345"
    
    def test_party_in_gcds_and_gic_only(self, spark, sample_gcds_keystore_data,
                                         sample_gic_pessoa_data, sample_gic_tipo_cadastro_data):
        """Test scenario: Party exists in GCDS + GIC only (no RANZ)."""
        # Arrange
        df_gcds_keystore = sample_gcds_keystore_data.filter(
            col("KeyStore_type") == "GIC"
        )
        df_gic_pessoa = sample_gic_pessoa_data
        df_gic_tipo = sample_gic_tipo_cadastro_data
        
        # Simulate GIC view
        df_gic = df_gic_pessoa.join(df_gic_tipo, "COD_INSTITUCIONAL", "left")
        df_gic = df_gic.withColumn(
            "LocalSystemIdentifier",
            concat(lit("GIC_"), col("COD_INSTITUCIONAL"))
        )
        
        # Act - Simulate the main query join
        df_result = df_gcds_keystore.join(
            df_gic,
            df_gcds_keystore["KeyStore_value"] == df_gic["COD_INSTITUCIONAL"],
            "left"
        ).withColumn(
            "FinalIdentifier",
            when(
                col("KeyStore_type") == "GIC",
                col("LocalSystemIdentifier")
            ).otherwise(concat(lit("GCDS_"), col("gcid")))
        )
        
        # Assert
        result = df_result.filter(col("gcid") == "GCDS001").select("FinalIdentifier").first()[0]
        assert result == "GIC_12345"


class TestGCDSRANZIntegration:
    """Test integration between GCDS and RANZ systems."""
    
    def test_gcds_ranz_join_on_keystore(self, spark, sample_gcds_keystore_data,
                                         sample_ranz_party_data, sample_gcds_client_data):
        """Test GCDS-RANZ join on KeyStore_type = 'CB RANZ'."""
        # Arrange
        df_gcds_keystore = sample_gcds_keystore_data
        df_ranz = sample_ranz_party_data
        df_gcds_client = sample_gcds_client_data
        
        # Join GCDS Client with KeyStore
        df_gcds = df_gcds_client.join(df_gcds_keystore, "gcid", "inner")
        
        # Act - Join with RANZ
        df_result = df_gcds.join(
            df_ranz,
            (df_gcds["KeyStore_value"] == df_ranz["SRC_PARTY_ID"]) & 
            (df_gcds["KeyStore_type"] == "CB RANZ"),
            "left"
        )
        
        # Assert
        ranz_matches = df_result.filter(col("SRC_PARTY_ID").isNotNull()).count()
        assert ranz_matches == 2  # Should match GCDS003 and GCDS004
        
        # Verify specific match
        gcds003_match = df_result.filter(
            col("gcid") == "GCDS003"
        ).select("SRC_PARTY_ID").first()[0]
        assert gcds003_match == "RANZ001"
    
    def test_party_in_gcds_and_ranz_only(self, spark, sample_gcds_keystore_data,
                                          sample_ranz_party_data):
        """Test scenario: Party exists in GCDS + RANZ only (no GIC)."""
        # Arrange
        df_gcds_keystore = sample_gcds_keystore_data.filter(
            col("KeyStore_type") == "CB RANZ"
        )
        df_ranz = sample_ranz_party_data.withColumn(
            "LocalSystemIdentifier",
            concat(lit("RANZ_"), col("PKEY_SRC_OBJECT"))
        )
        
        # Act - Simulate the main query join
        df_result = df_gcds_keystore.join(
            df_ranz,
            df_gcds_keystore["KeyStore_value"] == df_ranz["SRC_PARTY_ID"],
            "left"
        ).withColumn(
            "FinalIdentifier",
            when(
                col("KeyStore_type") == "CB RANZ",
                col("LocalSystemIdentifier")
            ).otherwise(concat(lit("GCDS_"), col("gcid")))
        )
        
        # Assert
        result = df_result.filter(col("gcid") == "GCDS003").select("FinalIdentifier").first()[0]
        assert result == "RANZ_RANZ_PK001"


class TestAllSystemsIntegration:
    """Test scenarios involving all three systems."""
    
    def test_party_in_all_three_systems(self, spark, sample_gcds_keystore_data,
                                        sample_gic_tipo_cadastro_data,
                                        sample_gcds_partyrole_data):
        """Test scenario: Party exists in GCDS + GIC + RANZ (COALESCE priority)."""
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType
        
        # Create mock data for all three systems
        schema = StructType([
            StructField("PartyRoleType_GCDS", StringType(), True),
            StructField("PartyRoleType_GIC", StringType(), True),
            StructField("PartyRoleType_RANZ", StringType(), True)
        ])
        
        data = [("GCDS_Client", "GIC_Cliente", "RANZ_BO")]
        df = spark.createDataFrame(data, schema)
        
        # Act - Apply COALESCE
        df_result = df.withColumn(
            "PartyRoleType",
            coalesce(
                col("PartyRoleType_GCDS"),
                col("PartyRoleType_GIC"),
                col("PartyRoleType_RANZ")
            )
        )
        
        # Assert - Should pick GCDS value
        result = df_result.select("PartyRoleType").first()[0]
        assert result == "GCDS_Client"
    
    def test_left_anti_join_excludes_matched_parties(self, spark):
        """Test left anti join excludes parties already matched in GCDS."""
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([StructField("LocalSystemIdentifier", StringType(), True)])
        
        # GIC parties
        gic_data = [("GIC_12345",), ("GIC_67890",), ("GIC_11111",)]
        df_gic = spark.createDataFrame(gic_data, schema)
        
        # GCDS-matched parties
        gcds_data = [("GIC_12345",), ("GIC_67890",)]
        df_gcds = spark.createDataFrame(gcds_data, schema)
        
        # Act - Left anti join
        df_result = df_gic.join(
            df_gcds,
            df_gic["LocalSystemIdentifier"] == df_gcds["LocalSystemIdentifier"],
            "left_anti"
        )
        
        # Assert - Should only have GIC_11111
        assert df_result.count() == 1
        result = df_result.select("LocalSystemIdentifier").first()[0]
        assert result == "GIC_11111"
    
    def test_union_gcds_and_nongcds_parties(self, spark):
        """Test UNION of GCDS-matched and non-GCDS unique parties."""
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([StructField("LocalSystemIdentifier", StringType(), True)])
        
        # GCDS-matched parties
        gcds_data = [("GIC_12345",), ("RANZ_RANZ_PK001",)]
        df_gcds = spark.createDataFrame(gcds_data, schema)
        
        # Non-GCDS unique parties
        nongcds_data = [("GIC_11111",), ("RANZ_RANZ_PK003",)]
        df_nongcds = spark.createDataFrame(nongcds_data, schema)
        
        # Act - UNION
        df_result = df_gcds.union(df_nongcds)
        
        # Assert
        assert df_result.count() == 4
        identifiers = {row[0] for row in df_result.collect()}
        assert identifiers == {"GIC_12345", "RANZ_RANZ_PK001", "GIC_11111", "RANZ_RANZ_PK003"}


class TestPartyIdentifierJoin:
    """Test final join with Party and Party_SystemIdentifier."""
    
    def test_party_filter_for_gic_and_gcds(self, spark, sample_party_data):
        """Test Party table filtering for GIC and GCDS identifiers."""
        # Arrange
        df_party = sample_party_data
        
        # Act
        df_filtered = df_party.filter(
            col("PartyIdentifier").like("GCDS%") | 
            col("PartyIdentifier").like("GIC%")
        )
        
        # Assert
        count = df_filtered.count()
        assert count == 5  # All records match the filter
        
        # Verify no RANZ-only identifiers
        ranz_count = df_filtered.filter(col("PartyIdentifier").like("RANZ%")).count()
        assert ranz_count == 0
    
    def test_final_join_with_party_preserves_all(self, spark, sample_party_data):
        """Test final left join with Party table preserves all Party records."""
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType
        
        df_party = sample_party_data
        
        schema = StructType([
            StructField("PartyIdentifier", StringType(), True),
            StructField("PartyRoleType", StringType(), True)
        ])
        
        role_data = [
            ("GIC_12345", "Client"),
            ("GCDS_GCDS001", "Prospect")
            # GCDS_GCDS005 has no role data
        ]
        df_party_role = spark.createDataFrame(role_data, schema)
        
        # Act - Left join
        df_result = df_party.join(df_party_role, "PartyIdentifier", "left")
        
        # Assert - All Party records preserved
        assert df_result.count() == df_party.count()
        
        # Verify null handling for parties without role
        gcds005_role = df_result.filter(
            col("PartyIdentifier") == "GCDS_GCDS005"
        ).select("PartyRoleType").first()[0]
        assert gcds005_role is None