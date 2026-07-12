
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os

# ================================================================================
# FIXTURES
# ================================================================================

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables"""
    monkeypatch.setenv('AU_GDP_Defined_Storage_Account', 'edlcorestdauprod0001')
    monkeypatch.setenv('GDP_STORAGE_NAME', 'edlcorestdeuprod0001')
    monkeypatch.setenv('TENANT_ID', '6e93a626-8aca-4dc1-9191-ce291b4b75a1')
    monkeypatch.setenv('GDP_SA_STORAGE_NAME', 'edlcorestdbrprod0001')
    monkeypatch.setenv('ENV', 'preprd')
    monkeypatch.setenv('APP_REG_APP_ID', '1ad6fac9-ea83-41de-9152-f455e2bf9f60')
    return monkeypatch

@pytest.fixture
def mock_spark():
    """Create mock Spark session"""
    spark = Mock()
    spark.read.format.return_value = spark.read
    spark.read.load.return_value = Mock()
    return spark

@pytest.fixture
def mock_dbutils():
    """Create mock dbutils"""
    dbutils = Mock()
    dbutils.fs.ls.return_value = []
    dbutils.widgets.get.return_value = ""
    return dbutils

@pytest.fixture
def sample_dataframe():
    """Create mock DataFrame"""
    df = Mock()
    df.columns = ['ROWID_XREF', 'EDL_ACT_DTS', 'DATA']
    df.count.return_value = 100
    df.withColumn.return_value = df
    df.filter.return_value = df
    df.drop.return_value = df
    df.union.return_value = df
    df.select.return_value = df
    df.createOrReplaceTempView.return_value = None
    return df

# ================================================================================
# TEST CLASSES
# ================================================================================

class TestHelperFunctions:
    """Test helper functions"""
    
    def test_get_base_table_name_xref(self):
        """Test extracting base name from _xref table"""
        # Functions are available from Cell 1 import
        assert get_base_table_name('c_b_party_xref') == 'c_b_party'
        assert get_base_table_name('C_B_PARTY_XREF') == 'c_b_party'
    
    def test_get_base_table_name_hxrf(self):
        """Test extracting base name from _hxrf table"""
        assert get_base_table_name('c_b_party_hxrf') == 'c_b_party'
        assert get_base_table_name('C_B_PARTY_HXRF') == 'c_b_party'
    
    def test_get_base_table_name_no_suffix(self):
        """Test table name without suffix"""
        assert get_base_table_name('c_b_party') == 'c_b_party'
    
    def test_build_base_path_success(self, mock_env_vars):
        """Test building base path with production storage account"""
        result = build_base_path('c_b_party_xref')
        
        assert 'edlcorestdauprod0001' in result
        assert 'c_b_party_xref' in result
        assert 'abfss://mdm-ranz@' in result
    
    def test_build_base_path_lookup_table(self, mock_env_vars):
        """Test building path for lookup table"""
        result = build_base_path('c_lkp_legal_entity_type_xref')
        
        expected = 'abfss://mdm-ranz@edlcorestdauprod0001.dfs.core.windows.net/c_lkp_legal_entity_type_xref'
        assert result == expected

class TestConfiguration:
    """Test configuration and environment"""
    
    def test_environment_variables(self, mock_env_vars):
        """Test environment variables are set"""
        assert os.environ.get('AU_GDP_Defined_Storage_Account') == 'edlcorestdauprod0001'
        assert os.environ.get('ENV') == 'preprd'
        assert os.environ.get('TENANT_ID') == '6e93a626-8aca-4dc1-9191-ce291b4b75a1'
    
    def test_storage_accounts(self, mock_env_vars):
        """Test storage account configuration"""
        ranz_storage = os.environ.get('AU_GDP_Defined_Storage_Account')
        assert ranz_storage == 'edlcorestdauprod0001'
        
        gdp_storage = os.environ.get('GDP_STORAGE_NAME')
        assert gdp_storage == 'edlcorestdeuprod0001'

class TestMainFunctions:
    """Test main load functions"""
    
    def test_merge_live_and_history_success(self, sample_dataframe, mock_env_vars):
        """Test merging LIVE and HISTORY tables"""
        with patch('test_ranz_working.read_ranz_v4_data', return_value=sample_dataframe):
            with patch('test_ranz_working.align_schemas', return_value=(sample_dataframe, sample_dataframe)):
                with patch('test_ranz_working.deduplicate_dataframe', return_value=sample_dataframe):
                    result = merge_live_and_history('c_b_party', '20260420')
        
        assert result is not None
    
    def test_load_tables_success(self, sample_dataframe, mock_env_vars):
        """Test main load function"""
        load_df = ['c_b_party_xref']
        
        with patch('__main__.merge_live_and_history', return_value=sample_dataframe):
            results = load_ranz_v4_rdm_tables(load_df, '20260420')
        
        assert len(results) == 1
        assert results[0]['status'] == 'SUCCESS'
