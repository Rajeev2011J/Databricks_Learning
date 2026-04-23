"""Pytest Unit Tests for Party Data Generator

Run with: pytest test_party_data_generator.py -v
"""

import pytest
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, BooleanType
from party_data_generator import (
    get_id_prefixes,
    get_party_types,
    get_cdd_types,
    get_kyc_groups,
    generate_party_df,
    generate_party_address_df,
    generate_party_naics_df,
    generate_party_systemidentifier_df,
    generate_party_altnames_df
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("PartyDataGeneratorTests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_party_ids():
    """Sample party IDs for testing child dataframes."""
    return [
        "GCOB_LEC_12345678",
        "LEGACY2_LE_98765432",
        "GIC_11223344",
        "GCDS_55667788"
    ]


# ===============================================
# Configuration Tests
# ===============================================

class TestConfigurations:
    """Test configuration data correctness."""
    
    def test_id_prefixes_count(self):
        """Test that we have the expected number of ID prefixes."""
        prefixes = get_id_prefixes()
        assert len(prefixes) == 21
        assert all(isinstance(p, str) for p in prefixes)
    
    def test_id_prefixes_format(self):
        """Test that all ID prefixes end with underscore."""
        prefixes = get_id_prefixes()
        assert all(p.endswith('_') for p in prefixes)
    
    def test_party_types_count(self):
        """Test party types list includes None and has expected size."""
        party_types = get_party_types()
        assert len(party_types) == 31
        assert None in party_types
    
    def test_cdd_types_count(self):
        """Test CDD types list includes None and has expected size."""
        cdd_types = get_cdd_types()
        assert len(cdd_types) == 39
        assert None in cdd_types
    
    def test_kyc_groups_count(self):
        """Test KYC groups has expected company names."""
        kyc_groups = get_kyc_groups()
        assert len(kyc_groups) == 31
        assert "SANTANDER" in kyc_groups
        assert "Johnson & Johnson" in kyc_groups


# ===============================================
# Party DataFrame Tests
# ===============================================

class TestPartyDataFrame:
    """Test party_df generation and quality."""
    
    def test_party_df_row_count(self, spark):
        """Test that party_df generates correct number of rows."""
        party_df = generate_party_df(spark, num_records=50)
        assert party_df.count() == 50
    
    def test_party_df_schema_columns(self, spark):
        """Test that party_df has all expected columns."""
        party_df = generate_party_df(spark, num_records=10)
        expected_columns = [
            "PartyIdentifier", "FullLegalName", "DateOfBirth", "FirstName", "MiddleName", "LastName",
            "Party_type", "GlobalClientOwnerName", "IsEligibleForFatcaAssessment", "FatcaClassification",
            "GIIN", "EIN", "FatcaDateOfIssue", "FatcaComments", "IsEligibleForCrsAssessment",
            "CrsClassification", "CrsFormSignedDate", "CrsComments", "LegalForm", "CustomerLifeCycleStatus",
            "IsLatestApprovedVersionOfClient", "FullLegalNameInLocalLanguage", "IsIncorporated",
            "IncorporationNumber", "IncorporationDate", "HasSourceOfWealth", "SanctionsOrExternalWatchlist",
            "InternalWatchlist", "AdverseInformationOrMedia", "StatedFindings", "PEPStatus", "IsTrust",
            "TypeOfTrust", "IsClientRegulated", "RegulatorName", "RegulatorCountry", "HasRecognisedRegulator",
            "IsClientListed", "ExchangeName", "ExchangeCountry", "HasRecognisedExchange",
            "CountryOfTaxResidence", "TinAvailable", "TinOrEquivalent", "TinUnavailabilityReason",
            "ExplanationForTinBeingUnavailable", "SourceOfIdentificationDocument", "IdentificationDocumentNumber",
            "SourceOfVerifiedDocument", "VerifiedDocumentNumber", "CddType", "FIHubIndicator", "W&RORRetail",
            "Party_LeadingPartyIdentifier", "IsRabobankEntity", "HO_reporting_party-FINREP_Code",
            "HO_reporting_party-FINREP", "KYCGroup", "SectorTeam"
        ]
        assert party_df.columns == expected_columns
        assert len(party_df.columns) == 59
    
    def test_party_identifier_format(self, spark):
        """Test that PartyIdentifier follows expected format."""
        party_df = generate_party_df(spark, num_records=20)
        party_ids = [row.PartyIdentifier for row in party_df.select("PartyIdentifier").collect()]
        
        # Should match pattern: PREFIX_NNNNNNNN (prefix + 8 digits)
        valid_prefixes = get_id_prefixes()
        for pid in party_ids:
            assert any(pid.startswith(prefix) for prefix in valid_prefixes), f"Invalid PartyIdentifier: {pid}"
            # Extract numeric part after prefix
            for prefix in valid_prefixes:
                if pid.startswith(prefix):
                    numeric_part = pid[len(prefix):]
                    assert numeric_part.isdigit(), f"Non-numeric part in PartyIdentifier: {pid}"
                    assert len(numeric_part) == 8, f"Wrong length for numeric part in PartyIdentifier: {pid}"
                    break
    
    def test_party_identifier_uniqueness(self, spark):
        """Test that PartyIdentifier values are unique."""
        party_df = generate_party_df(spark, num_records=100)
        total_count = party_df.count()
        distinct_count = party_df.select("PartyIdentifier").distinct().count()
        # Allow small collision rate due to random generation
        assert distinct_count >= total_count * 0.95, "Too many duplicate PartyIdentifiers"
    
    def test_boolean_columns_types(self, spark):
        """Test that boolean columns contain only boolean values."""
        party_df = generate_party_df(spark, num_records=20)
        boolean_columns = [
            "IsEligibleForFatcaAssessment", "IsEligibleForCrsAssessment", "IsIncorporated",
            "HasSourceOfWealth", "IsTrust", "IsClientRegulated", "HasRecognisedRegulator",
            "IsClientListed", "HasRecognisedExchange", "FIHubIndicator", "IsRabobankEntity"
        ]
        
        for col in boolean_columns:
            # Check that column values are boolean (True, False, or None)
            values = [row[col] for row in party_df.select(col).collect()]
            assert all(v in [True, False, None] for v in values), f"Column {col} contains non-boolean values"
    
    def test_ein_format(self, spark):
        """Test that EIN follows format XX-XXXXXXX."""
        party_df = generate_party_df(spark, num_records=20)
        eins = [row.EIN for row in party_df.select("EIN").collect()]
        
        ein_pattern = re.compile(r'^\d{2}-\d{7}$')
        for ein in eins:
            assert ein_pattern.match(ein), f"Invalid EIN format: {ein}"
    
    def test_party_type_values(self, spark):
        """Test that Party_type contains only valid values."""
        party_df = generate_party_df(spark, num_records=50)
        party_types = [row.Party_type for row in party_df.select("Party_type").collect()]
        valid_types = get_party_types()
        
        for pt in party_types:
            assert pt in valid_types, f"Invalid Party_type: {pt}"
    
    def test_kyc_group_values(self, spark):
        """Test that KYCGroup contains only valid company names."""
        party_df = generate_party_df(spark, num_records=50)
        kyc_values = [row.KYCGroup for row in party_df.select("KYCGroup").collect()]
        valid_groups = get_kyc_groups()
        
        for kg in kyc_values:
            assert kg in valid_groups, f"Invalid KYCGroup: {kg}"
    
    def test_name_fields_not_empty(self, spark):
        """Test that FirstName and LastName are never null or empty."""
        party_df = generate_party_df(spark, num_records=30)
        first_names = [row.FirstName for row in party_df.select("FirstName").collect()]
        last_names = [row.LastName for row in party_df.select("LastName").collect()]
        
        assert all(fn is not None and fn != "" for fn in first_names), "FirstName contains null or empty values"
        assert all(ln is not None and ln != "" for ln in last_names), "LastName contains null or empty values"
    
    def test_incorporation_number_format(self, spark):
        """Test that IncorporationNumber follows format INC+5digits."""
        party_df = generate_party_df(spark, num_records=20)
        inc_numbers = [row.IncorporationNumber for row in party_df.select("IncorporationNumber").collect()]
        
        inc_pattern = re.compile(r'^INC\d{5}$')
        for inc_num in inc_numbers:
            assert inc_pattern.match(inc_num), f"Invalid IncorporationNumber format: {inc_num}"


# ===============================================
# Party Address DataFrame Tests
# ===============================================

class TestPartyAddressDataFrame:
    """Test party_address_df generation and quality."""
    
    def test_party_address_row_count(self, spark, sample_party_ids):
        """Test that party_address_df generates correct number of rows."""
        address_df = generate_party_address_df(spark, sample_party_ids, num_records=25)
        assert address_df.count() == 25
    
    def test_party_address_schema(self, spark, sample_party_ids):
        """Test that party_address_df has all expected columns."""
        address_df = generate_party_address_df(spark, sample_party_ids, num_records=10)
        expected_columns = [
            "PartyIdentifier", "AddressSystem", "AddressSystem_ID", "AddressType",
            "HouseNumber", "Street", "City", "Region", "PostalCode", "Country", "CountryCode"
        ]
        assert address_df.columns == expected_columns
        assert len(address_df.columns) == 11
    
    def test_party_address_identifiers_valid(self, spark, sample_party_ids):
        """Test that PartyIdentifier in address_df comes from provided list."""
        address_df = generate_party_address_df(spark, sample_party_ids, num_records=20)
        party_ids = [row.PartyIdentifier for row in address_df.select("PartyIdentifier").collect()]
        
        for pid in party_ids:
            assert pid in sample_party_ids, f"Invalid PartyIdentifier in address_df: {pid}"
    
    def test_postal_code_format(self, spark, sample_party_ids):
        """Test that PostalCode is 5 digits."""
        address_df = generate_party_address_df(spark, sample_party_ids, num_records=20)
        postal_codes = [row.PostalCode for row in address_df.select("PostalCode").collect()]
        
        for pc in postal_codes:
            assert pc.isdigit(), f"PostalCode contains non-digits: {pc}"
            assert len(pc) == 5, f"PostalCode wrong length: {pc}"
    
    def test_address_system_id_format(self, spark, sample_party_ids):
        """Test that AddressSystem_ID follows format ASID+4digits."""
        address_df = generate_party_address_df(spark, sample_party_ids, num_records=15)
        asids = [row.AddressSystem_ID for row in address_df.select("AddressSystem_ID").collect()]
        
        asid_pattern = re.compile(r'^ASID\d{4}$')
        for asid in asids:
            assert asid_pattern.match(asid), f"Invalid AddressSystem_ID format: {asid}"


# ===============================================
# Party NAICS DataFrame Tests
# ===============================================

class TestPartyNAICSDataFrame:
    """Test party_naics_df generation and quality."""
    
    def test_party_naics_row_count(self, spark, sample_party_ids):
        """Test that party_naics_df generates correct number of rows."""
        naics_df = generate_party_naics_df(spark, sample_party_ids, num_records=30)
        assert naics_df.count() == 30
    
    def test_party_naics_schema(self, spark, sample_party_ids):
        """Test that party_naics_df has all expected columns."""
        naics_df = generate_party_naics_df(spark, sample_party_ids, num_records=10)
        expected_columns = [
            "PartyIdentifier", "LocalSystemId", "LocalSystemName", "NAICSCode",
            "NAICSDescription", "PrimaryNaicsFlag", "NAICSPercentage"
        ]
        assert naics_df.columns == expected_columns
        assert len(naics_df.columns) == 7
    
    def test_naics_code_format(self, spark, sample_party_ids):
        """Test that NAICSCode is 6 digits."""
        naics_df = generate_party_naics_df(spark, sample_party_ids, num_records=20)
        naics_codes = [row.NAICSCode for row in naics_df.select("NAICSCode").collect()]
        
        for nc in naics_codes:
            assert nc.isdigit(), f"NAICSCode contains non-digits: {nc}"
            assert len(nc) == 6, f"NAICSCode wrong length: {nc}"
    
    def test_primary_naics_flag_values(self, spark, sample_party_ids):
        """Test that PrimaryNaicsFlag is Y or N."""
        naics_df = generate_party_naics_df(spark, sample_party_ids, num_records=20)
        flags = [row.PrimaryNaicsFlag for row in naics_df.select("PrimaryNaicsFlag").collect()]
        
        for flag in flags:
            assert flag in ['Y', 'N'], f"Invalid PrimaryNaicsFlag: {flag}"
    
    def test_naics_percentage_range(self, spark, sample_party_ids):
        """Test that NAICSPercentage is between 1 and 100."""
        naics_df = generate_party_naics_df(spark, sample_party_ids, num_records=30)
        percentages = [int(row.NAICSPercentage) for row in naics_df.select("NAICSPercentage").collect()]
        
        for pct in percentages:
            assert 1 <= pct <= 100, f"NAICSPercentage out of range: {pct}"


# ===============================================
# Party System Identifier DataFrame Tests
# ===============================================

class TestPartySystemIdentifierDataFrame:
    """Test party_systemidentifier_df generation and quality."""
    
    def test_party_systemidentifier_row_count(self, spark, sample_party_ids):
        """Test that party_systemidentifier_df generates correct number of rows."""
        sysid_df = generate_party_systemidentifier_df(spark, sample_party_ids, num_records=20)
        assert sysid_df.count() == 20
    
    def test_party_systemidentifier_schema(self, spark, sample_party_ids):
        """Test that party_systemidentifier_df has all expected columns."""
        sysid_df = generate_party_systemidentifier_df(spark, sample_party_ids, num_records=10)
        expected_columns = ["PartyIdentifier", "Application", "LocalSystemIdentifier"]
        assert sysid_df.columns == expected_columns
        assert len(sysid_df.columns) == 3
    
    def test_local_system_identifier_format(self, spark, sample_party_ids):
        """Test that LocalSystemIdentifier follows format LSI+6digits."""
        sysid_df = generate_party_systemidentifier_df(spark, sample_party_ids, num_records=15)
        lsids = [row.LocalSystemIdentifier for row in sysid_df.select("LocalSystemIdentifier").collect()]
        
        lsid_pattern = re.compile(r'^LSI\d{6}$')
        for lsid in lsids:
            assert lsid_pattern.match(lsid), f"Invalid LocalSystemIdentifier format: {lsid}"
    
    def test_application_values(self, spark, sample_party_ids):
        """Test that Application contains only valid values."""
        sysid_df = generate_party_systemidentifier_df(spark, sample_party_ids, num_records=20)
        apps = [row.Application for row in sysid_df.select("Application").collect()]
        valid_apps = ["CoreBanking", "CustomerPortal", "MobileApp", "BackOffice"]
        
        for app in apps:
            assert app in valid_apps, f"Invalid Application: {app}"


# ===============================================
# Party Alternative Names DataFrame Tests
# ===============================================

class TestPartyAlternativeNamesDataFrame:
    """Test party_altnames_df generation and quality."""
    
    def test_party_altnames_row_count(self, spark, sample_party_ids):
        """Test that party_altnames_df generates correct number of rows."""
        altnames_df = generate_party_altnames_df(spark, sample_party_ids, num_records=25)
        assert altnames_df.count() == 25
    
    def test_party_altnames_schema(self, spark, sample_party_ids):
        """Test that party_altnames_df has all expected columns."""
        altnames_df = generate_party_altnames_df(spark, sample_party_ids, num_records=10)
        expected_columns = [
            "PartyIdentifier", "SourceApplication", "PartyNameTypeCode",
            "FirstName", "MiddleName", "LastName", "FullName"
        ]
        assert altnames_df.columns == expected_columns
        assert len(altnames_df.columns) == 7
    
    def test_party_name_type_code_values(self, spark, sample_party_ids):
        """Test that PartyNameTypeCode contains only valid values."""
        altnames_df = generate_party_altnames_df(spark, sample_party_ids, num_records=20)
        codes = [row.PartyNameTypeCode for row in altnames_df.select("PartyNameTypeCode").collect()]
        valid_codes = ["Legal", "Trade", "Alias", "Former"]
        
        for code in codes:
            assert code in valid_codes, f"Invalid PartyNameTypeCode: {code}"
    
    def test_first_last_name_not_null(self, spark, sample_party_ids):
        """Test that FirstName and LastName are never null."""
        altnames_df = generate_party_altnames_df(spark, sample_party_ids, num_records=30)
        first_names = [row.FirstName for row in altnames_df.select("FirstName").collect()]
        last_names = [row.LastName for row in altnames_df.select("LastName").collect()]
        
        assert all(fn is not None for fn in first_names), "FirstName contains null values"
        assert all(ln is not None for ln in last_names), "LastName contains null values"
    
    def test_full_name_constructed(self, spark, sample_party_ids):
        """Test that FullName is properly constructed from name parts."""
        altnames_df = generate_party_altnames_df(spark, sample_party_ids, num_records=20)
        rows = altnames_df.select("FirstName", "MiddleName", "LastName", "FullName").collect()
        
        for row in rows:
            name_parts = [row.FirstName, row.MiddleName, row.LastName]
            # Remove None values and join with space
            expected_full_name = ' '.join([p for p in name_parts if p is not None])
            assert row.FullName == expected_full_name, f"FullName mismatch: expected '{expected_full_name}', got '{row.FullName}'"


# ===============================================
# Integration Tests
# ===============================================

class TestIntegration:
    """Test integration between different dataframes."""
    
    def test_child_dataframes_use_parent_ids(self, spark):
        """Test that child dataframes use PartyIdentifiers from party_df."""
        # Generate parent dataframe
        party_df = generate_party_df(spark, num_records=50)
        party_ids = [row.PartyIdentifier for row in party_df.select("PartyIdentifier").collect()]
        
        # Generate child dataframes
        address_df = generate_party_address_df(spark, party_ids, num_records=50)
        naics_df = generate_party_naics_df(spark, party_ids, num_records=50)
        sysid_df = generate_party_systemidentifier_df(spark, party_ids, num_records=50)
        altnames_df = generate_party_altnames_df(spark, party_ids, num_records=50)
        
        # Check that all child dataframe PartyIdentifiers exist in parent
        for child_df in [address_df, naics_df, sysid_df, altnames_df]:
            child_ids = [row.PartyIdentifier for row in child_df.select("PartyIdentifier").collect()]
            for cid in child_ids:
                assert cid in party_ids, f"Child dataframe contains PartyIdentifier not in parent: {cid}"
    
    def test_complete_data_pipeline(self, spark):
        """Test that complete data generation pipeline works end-to-end."""
        num_records = 100
        
        # Generate all dataframes
        party_df = generate_party_df(spark, num_records=num_records)
        party_ids = [row.PartyIdentifier for row in party_df.select("PartyIdentifier").collect()]
        
        address_df = generate_party_address_df(spark, party_ids, num_records=num_records)
        naics_df = generate_party_naics_df(spark, party_ids, num_records=num_records)
        sysid_df = generate_party_systemidentifier_df(spark, party_ids, num_records=num_records)
        altnames_df = generate_party_altnames_df(spark, party_ids, num_records=num_records)
        
        # Verify all dataframes were created successfully
        assert party_df.count() == num_records
        assert address_df.count() == num_records
        assert naics_df.count() == num_records
        assert sysid_df.count() == num_records
        assert altnames_df.count() == num_records
