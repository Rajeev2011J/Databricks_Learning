"""Pytest configuration and shared fixtures for RDM testing."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from unittest.mock import Mock, MagicMock
import os
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder
        .appName("RDM_Party_Role_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def mock_dbutils():
    """Mock Databricks utilities for testing."""
    dbutils_mock = Mock()
    
    # Mock secrets
    secrets_mock = Mock()
    secrets_mock.get.return_value = "mock_secret_value"
    dbutils_mock.secrets = secrets_mock
    
    # Mock widgets
    widgets_data = {"Load_Date": "", "RunType": "daily"}
    
    def widget_get(key):
        return widgets_data.get(key, "")
    
    def widget_text(key, default):
        if key not in widgets_data:
            widgets_data[key] = default
    
    widgets_mock = Mock()
    widgets_mock.get.side_effect = widget_get
    widgets_mock.text.side_effect = widget_text
    dbutils_mock.widgets = widgets_mock
    
    return dbutils_mock


@pytest.fixture
def mock_environment_variables():
    """Mock environment variables for testing."""
    env_vars = {
        "APP_REG_APP_ID": "test-app-id",
        "GDP_STORAGE_NAME": "sagdpdefinedtest",
        "TENANT_ID": "test-tenant-id",
        "GDP_SA_STORAGE_NAME": "sagdpdefinedbrtest",
        "ENV": "test",
        "GDP_NA_STORAGE_NAME": "sagdpdefinednlstest",
        "AU_GDP_Defined_Storage_Account": "sagdpdefinedautest"
    }
    
    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
    
    yield env_vars
    
    # Cleanup
    for key in env_vars.keys():
        os.environ.pop(key, None)


@pytest.fixture
def mock_storage_functions(monkeypatch):
    """Mock storage account authentication functions."""
    def mock_authenticate(*args, **kwargs):
        pass
    
    def mock_read_gdp(*args, **kwargs):
        pass
    
    def mock_save_to_saradar(*args, **kwargs):
        pass
    
    # These will be patched in individual tests
    return {
        "authenticate": mock_authenticate,
        "read_gdp": mock_read_gdp,
        "save": mock_save_to_saradar
    }


@pytest.fixture
def sample_gcds_keystore_data(spark):
    """Sample GCDS KeyStore data for testing."""
    schema = StructType([
        StructField("gcid", StringType(), True),
        StructField("KeyStore_value", StringType(), True),
        StructField("KeyStore_type", StringType(), True)
    ])
    
    data = [
        ("GCDS001", "12345", "GIC"),
        ("GCDS002", "67890", "GIC"),
        ("GCDS003", "RANZ001", "CB RANZ"),
        ("GCDS004", "RANZ002", "CB RANZ"),
        ("GCDS005", "99999", "OTHER")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_gcds_partyrole_data(spark):
    """Sample GCDS PartyRole data for testing."""
    schema = StructType([
        StructField("GCID", StringType(), True),
        StructField("Party_role", StringType(), True),
        StructField("PartyRole-StartDate", StringType(), True),
        StructField("PartyRole-ChangeDate", StringType(), True),
        StructField("Life_cycle_status", StringType(), True)
    ])
    
    data = [
        ("GCDS001", "Client", "2024-01-01", "2024-01-15", "Active"),
        ("GCDS002", "Prospect", "2024-02-01", "2024-02-10", "Active"),
        ("GCDS003", "Client", "2024-03-01", "2024-03-20", "Active"),
        ("GCDS004", "Client", "2024-04-01", None, "Pending"),
        ("GCDS005", "Client", "2024-05-01", "2024-05-15", "Closed")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_gcds_client_data(spark):
    """Sample GCDS Client data for testing."""
    schema = StructType([
        StructField("GCID", StringType(), True),
        StructField("ClientName", StringType(), True)
    ])
    
    data = [
        ("GCDS001", "Client A"),
        ("GCDS002", "Client B"),
        ("GCDS003", "Client C"),
        ("GCDS004", "Client D"),
        ("GCDS005", "Client E")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_gic_pessoa_data(spark):
    """Sample GIC Pessoa data for testing."""
    schema = StructType([
        StructField("COD_INSTITUCIONAL", StringType(), True),
        StructField("NOM_PESSOA", StringType(), True)
    ])
    
    data = [
        ("12345", "GIC Person A"),
        ("67890", "GIC Person B"),
        ("11111", "GIC Person C - No GCDS")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_gic_tipo_cadastro_data(spark):
    """Sample GIC Tipo Cadastro data for testing."""
    schema = StructType([
        StructField("COD_INSTITUCIONAL", StringType(), True),
        StructField("DES_TIPO_CADASTRO", StringType(), True),
        StructField("DTA_CADASTRO", StringType(), True),
        StructField("DTA_EFETIVACAO", StringType(), True),
        StructField("DES_STATUS_TIPO_CADASTRO", StringType(), True)
    ])
    
    data = [
        ("12345", "Cliente PF", "2024-01-01", "2024-01-05", "Ativo"),
        ("67890", "Cliente PJ", "2024-02-01", "2024-02-05", "Ativo"),
        ("11111", "Prospect", "2024-03-01", None, "Pendente")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_ranz_party_data(spark):
    """Sample RANZ Party data for testing."""
    schema = StructType([
        StructField("PKEY_SRC_OBJECT", StringType(), True),
        StructField("SRC_PARTY_ID", StringType(), True),
        StructField("ROWID_XREF", StringType(), True)
    ])
    
    data = [
        ("RANZ_PK001", "RANZ001", "ROW001"),
        ("RANZ_PK002", "RANZ002", "ROW002"),
        ("RANZ_PK003", "RANZ003", "ROW003")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_ranz_contract_role_party_data(spark):
    """Sample RANZ Contract Role Party data for testing."""
    schema = StructType([
        StructField("FK_PARTY_ID", StringType(), True),
        StructField("ROLE_TYPE_CD", StringType(), True),
        StructField("START_DT", StringType(), True),
        StructField("END_DT", StringType(), True),
        StructField("FK_CONTR_ID", StringType(), True)
    ])
    
    data = [
        ("ROW001", "BO", "2024-01-01", "2024-12-31", "CONT001"),
        ("ROW002", "CL", "2024-02-01", None, "CONT002"),
        ("ROW003", "BO", "2024-03-01", "2024-06-30", "CONT003")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_ranz_contract_data(spark):
    """Sample RANZ Contract data for testing."""
    schema = StructType([
        StructField("CONTR_ID", StringType(), True),
        StructField("LIFECYCLE_STATUS_CD", StringType(), True)
    ])
    
    data = [
        ("CONT001", "AC"),
        ("CONT002", "BL"),
        ("CONT003", "CL")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_party_data(spark):
    """Sample Party data for testing."""
    schema = StructType([
        StructField("PartyIdentifier", StringType(), True)
    ])
    
    data = [
        ("GCDS_GCDS001",),
        ("GIC_12345",),
        ("GIC_67890",),
        ("GCDS_GCDS003",),
        ("GCDS_GCDS005",)
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_party_system_identifier_data(spark):
    """Sample Party System Identifier data for testing."""
    schema = StructType([
        StructField("LocalSystemIdentifier", StringType(), True),
        StructField("PartyIdentifier", StringType(), True)
    ])
    
    data = [
        ("GCDS_GCDS001", "GCDS_GCDS001"),
        ("GIC_12345", "GIC_12345"),
        ("GIC_67890", "GIC_67890"),
        ("RANZ_RANZ_PK001", "GCDS_GCDS003"),
        ("GCDS_GCDS005", "GCDS_GCDS005")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def expected_local_system_identifiers():
    """Expected LocalSystemIdentifier values for validation."""
    return {
        "gic_format": "GIC_12345",
        "ranz_format": "RANZ_RANZ_PK001",
        "gcds_format": "GCDS_GCDS005"
    }


@pytest.fixture
def ranz_status_mapping():
    """RANZ lifecycle status mapping for validation."""
    return {
        "AC": "Active Client",
        "BL": "Blocked Client",
        "CL": "Closed Client",
        "DC": "Declined Client",
        "AP": "Applicant Client"
    }


@pytest.fixture
def load_date_today():
    """Today's date in expected format."""
    return datetime.today().strftime('%Y%m%d')