"""Pytest Test Suite for RDM Test Data Generator

Comprehensive tests for all data generation functions and data quality validation.

Usage:
    pytest test_rdm_data_generator.py -v
    pytest test_rdm_data_generator.py -k TestConfiguration -v
"""

import sys
import os
import pytest
from datetime import datetime
from pyspark.sql import SparkSession

# Setup path
workspace_dir = "/Workspace/Users/net.rajeev@gmail.com"
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

# Import the module
import rdm_data_generator as rdm


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    """Get existing SparkSession."""
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def test_num_records():
    """Small number of records for fast testing."""
    return 100


@pytest.fixture(scope="session")
def faker_data():
    """Generate Faker data for tests."""
    return rdm.generate_faker_data(
        num_first_names=50,
        num_middle_names=30,
        num_last_names=50,
        num_companies=25,
        num_persons=25
    )


@pytest.fixture(scope="session")
def party_df(spark, test_num_records, faker_data):
    """Generate Party DataFrame for tests."""
    df = rdm.generate_party_data(spark, test_num_records, faker_data)
    df.cache()
    return df


@pytest.fixture(scope="session")
def party_identifiers(party_df):
    """Extract party identifiers from Party DataFrame."""
    return [row.PartyIdentifier for row in party_df.select("PartyIdentifier").collect()]


@pytest.fixture(scope="session")
def all_tables(spark, test_num_records):
    """Generate all RDM tables for integration tests."""
    return rdm.generate_all_rdm_data(spark, test_num_records)


# ============================================================================
# TEST CLASS: CONFIGURATION
# ============================================================================

class TestConfiguration:
    """Test configuration constants."""
    
    def test_id_prefixes_count(self):
        assert len(rdm.ID_PREFIXES) == 21
    
    def test_id_prefixes_format(self):
        for prefix in rdm.ID_PREFIXES:
            assert prefix.endswith('_')
    
    def test_party_types_count(self):
        assert len(rdm.PARTY_TYPES) == 31
        assert None in rdm.PARTY_TYPES
    
    def test_cdd_types_count(self):
        assert len(rdm.CDD_TYPES) == 39
        assert None in rdm.CDD_TYPES
    
    def test_local_system_id_prefixes(self):
        assert len(rdm.LOCAL_SYSTEM_ID_PREFIXES) == 5
        assert "" in rdm.LOCAL_SYSTEM_ID_PREFIXES
    
    def test_applications_list(self):
        expected = ["GIC", "KN1-GRAM", "GCOB", "GCDS", "KN1"]
        assert rdm.APPLICATIONS == expected


class TestFakerData:
    """Test Faker data generation."""
    
    def test_faker_data_structure(self, faker_data):
        assert 'first_names' in faker_data
        assert 'middle_names' in faker_data
        assert 'last_names' in faker_data
        assert 'global_owner_names' in faker_data
    
    def test_faker_data_counts(self, faker_data):
        assert len(faker_data['first_names']) == 50
        assert len(faker_data['middle_names']) == 30
        assert len(faker_data['last_names']) == 50
        assert len(faker_data['global_owner_names']) == 50
    
    def test_faker_data_uniqueness(self, faker_data):
        assert len(set(faker_data['first_names'])) >= 45
        assert len(set(faker_data['last_names'])) >= 45
    
    def test_faker_data_non_empty(self, faker_data):
        for name in faker_data['first_names']:
            assert isinstance(name, str) and len(name) > 0


class TestUtilities:
    """Test utility functions."""
    
    def test_add_dates_v3_columns(self, spark):
        test_df = spark.createDataFrame([(1,), (2,)], ["id"])
        result_df = rdm.add_dates_v3(test_df)
        expected_cols = ["id", "EDL_LOAD_DTS", "EDL_ACT_DTS", "EDL_ACT_DTS_UTC", "LOAD_DT"]
        assert result_df.columns == expected_cols
    
    def test_add_dates_v3_formats(self, spark):
        test_df = spark.createDataFrame([(1,)], ["id"])
        result_df = rdm.add_dates_v3(test_df)
        row = result_df.first()
        
        assert len(row.EDL_LOAD_DTS) == 26
        assert row.EDL_LOAD_DTS[10] == ' '
        assert len(row.LOAD_DT) == 8
        assert row.LOAD_DT.isdigit()
    
    def test_add_dates_v3_utc_offset(self, spark):
        test_df = spark.createDataFrame([(1,)], ["id"])
        result_df = rdm.add_dates_v3(test_df)
        row = result_df.first()
        
        local_time = datetime.strptime(row.EDL_LOAD_DTS, '%Y-%m-%d %H:%M:%S.%f')
        utc_time = datetime.strptime(row.EDL_ACT_DTS_UTC, '%Y-%m-%d %H:%M:%S.%f')
        time_diff = (local_time - utc_time).total_seconds()
        assert abs(time_diff - 7200) < 1


class TestPartyData:
    """Test Party DataFrame generation."""
    
    def test_party_row_count(self, party_df, test_num_records):
        assert party_df.count() == test_num_records
    
    def test_party_column_count(self, party_df):
        assert len(party_df.columns) == 59
    
    def test_party_required_columns(self, party_df):
        required_cols = ["PartyIdentifier", "FullLegalName", "FirstName", "LastName", "Party_type"]
        for col in required_cols:
            assert col in party_df.columns
    
    def test_party_identifier_format(self, party_df):
        identifiers = party_df.select("PartyIdentifier").limit(20).collect()
        for row in identifiers:
            pid = row.PartyIdentifier
            assert any(pid.startswith(prefix) for prefix in rdm.ID_PREFIXES)
            numeric_part = ''.join(filter(str.isdigit, pid))
            assert len(numeric_part) == 8
    
    def test_party_identifier_uniqueness(self, party_df):
        total_count = party_df.count()
        unique_count = party_df.select("PartyIdentifier").distinct().count()
        assert total_count == unique_count
    
    def test_party_name_construction(self, party_df):
        sample = party_df.select("FirstName", "MiddleName", "LastName", "FullLegalName").limit(10).collect()
        for row in sample:
            parts = [p for p in [row.FirstName, row.MiddleName, row.LastName] if p is not None]
            expected_name = ' '.join(parts)
            assert row.FullLegalName == expected_name
    
    def test_party_boolean_fields(self, party_df):
        boolean_cols = ["IsIncorporated", "IsTrust", "IsClientRegulated", "IsClientListed"]
        for col in boolean_cols:
            values = party_df.select(col).distinct().collect()
            for row in values:
                assert row[col] in [True, False]
    
    def test_party_ein_format(self, party_df):
        eins = party_df.select("EIN").limit(10).collect()
        for row in eins:
            ein = row.EIN
            assert len(ein) == 10
            assert ein[2] == '-'
            assert ein[:2].isdigit()
            assert ein[3:].isdigit()
    
    def test_party_giin_format(self, party_df):
        giins = party_df.filter(party_df.GIIN.isNotNull()).select("GIIN").limit(10).collect()
        for row in giins:
            giin = row.GIIN
            parts = giin.split('.')
            assert len(parts) == 4
    
    def test_party_tin_format(self, party_df):
        tins = party_df.select("TinOrEquivalent").limit(10).collect()
        for row in tins:
            tin = row.TinOrEquivalent
            assert tin.startswith('TIN')
            assert len(tin) == 11
            assert tin[3:].isdigit()
    
    def test_party_incorporation_number_format(self, party_df):
        inc_nums = party_df.select("IncorporationNumber").limit(10).collect()
        for row in inc_nums:
            inc_num = row.IncorporationNumber
            assert inc_num.startswith('INC')
            assert len(inc_num) == 8
            assert inc_num[3:].isdigit()
    
    def test_party_kyc_group(self, party_df):
        kyc_groups = party_df.select("KYCGroup").distinct().collect()
        kyc_values = [row.KYCGroup for row in kyc_groups]
        assert set(kyc_values).issubset({'G1', 'G2', 'G3', 'G4'})


class TestPartyAddressData:
    """Test Party_Address DataFrame generation."""
    
    def test_party_address_row_count(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_address_data(spark, test_num_records, party_identifiers)
        assert df.count() == test_num_records * 2
    
    def test_party_address_column_count(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_address_data(spark, test_num_records, party_identifiers)
        assert len(df.columns) == 11
    
    def test_party_address_postal_code_format(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_address_data(spark, test_num_records, party_identifiers)
        postal_codes = df.select("PostalCode").limit(10).collect()
        for row in postal_codes:
            pc = row.PostalCode
            assert len(pc) == 6
            assert pc.isdigit()
    
    def test_party_address_system_id_format(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_address_data(spark, test_num_records, party_identifiers)
        asids = df.select("AddressSystem_ID").limit(10).collect()
        for row in asids:
            asid = row.AddressSystem_ID
            assert asid.startswith('ASID')
            assert len(asid) == 8
    
    def test_party_address_foreign_key(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_address_data(spark, test_num_records, party_identifiers)
        address_party_ids = set([row.PartyIdentifier for row in df.select("PartyIdentifier").distinct().collect()])
        party_ids_set = set(party_identifiers)
        assert address_party_ids.issubset(party_ids_set)
    
    def test_party_address_country_mapping(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_address_data(spark, test_num_records, party_identifiers)
        countries = df.select("CountryCode").distinct().collect()
        valid_codes = {"US", "GB", "CH", "SG", "DE", "NL"}
        for row in countries:
            assert row.CountryCode in valid_codes
    
    def test_party_address_required_columns(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_address_data(spark, test_num_records, party_identifiers)
        required_cols = ["PartyIdentifier", "AddressType", "Street", "City", "PostalCode", "Country"]
        for col in required_cols:
            assert col in df.columns


class TestPartyNaicsData:
    """Test Party_Naics DataFrame generation."""
    
    def test_party_naics_row_count(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_naics_data(spark, test_num_records, party_identifiers)
        assert df.count() == int(test_num_records * 1.5)
    
    def test_party_naics_column_count(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_naics_data(spark, test_num_records, party_identifiers)
        assert len(df.columns) == 7
    
    def test_party_naics_code_format(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_naics_data(spark, test_num_records, party_identifiers)
        codes = df.select("NAICSCode").limit(10).collect()
        for row in codes:
            code = row.NAICSCode
            assert len(code) == 6
            assert code.isdigit()
    
    def test_party_naics_primary_flag(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_naics_data(spark, test_num_records, party_identifiers)
        flags = df.select("PrimaryNaicsFlag").distinct().collect()
        flag_values = [row.PrimaryNaicsFlag for row in flags]
        assert set(flag_values) == {"Y", "N"}
    
    def test_party_naics_percentage_range(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_naics_data(spark, test_num_records, party_identifiers)
        percentages = df.filter(df.NAICSPercentage.isNotNull()).select("NAICSPercentage").limit(20).collect()
        for row in percentages:
            pct = int(row.NAICSPercentage)
            assert 1 <= pct <= 100


class TestPartySystemIdentifierData:
    """Test Party_SystemIdentifier DataFrame generation."""
    
    def test_party_systemidentifier_row_count(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_systemidentifier_data(spark, test_num_records, party_identifiers)
        assert df.count() == test_num_records * 2
    
    def test_party_systemidentifier_column_count(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_systemidentifier_data(spark, test_num_records, party_identifiers)
        assert len(df.columns) == 3
    
    def test_party_systemidentifier_lsi_format(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_systemidentifier_data(spark, test_num_records, party_identifiers)
        lsis = df.select("LocalSystemIdentifier").limit(10).collect()
        for row in lsis:
            lsi = row.LocalSystemIdentifier
            assert lsi.startswith('LSI')
            assert len(lsi) == 9
    
    def test_party_systemidentifier_application_values(self, spark, test_num_records, party_identifiers):
        df = rdm.generate_party_systemidentifier_data(spark, test_num_records, party_identifiers)
        apps = df.select("Application").distinct().collect()
        app_values = [row.Application for row in apps]
        assert set(app_values).issubset(set(rdm.APPLICATIONS))


class TestPartyAlternativeNamesData:
    """Test Party_AlternativeNames DataFrame generation."""
    
    def test_party_altnames_row_count(self, spark, test_num_records, party_identifiers, faker_data):
        df = rdm.generate_party_altnames_data(spark, test_num_records, party_identifiers, faker_data)
        assert df.count() == test_num_records
    
    def test_party_altnames_column_count(self, spark, test_num_records, party_identifiers, faker_data):
        df = rdm.generate_party_altnames_data(spark, test_num_records, party_identifiers, faker_data)
        assert len(df.columns) == 7
    
    def test_party_altnames_full_name_construction(self, spark, test_num_records, party_identifiers, faker_data):
        df = rdm.generate_party_altnames_data(spark, test_num_records, party_identifiers, faker_data)
        sample = df.select("FirstName", "MiddleName", "LastName", "FullName").limit(10).collect()
        for row in sample:
            parts = [p for p in [row.FirstName, row.MiddleName, row.LastName] if p is not None]
            expected_name = ' '.join(parts)
            assert row.FullName == expected_name
    
    def test_party_altnames_source_application(self, spark, test_num_records, party_identifiers, faker_data):
        df = rdm.generate_party_altnames_data(spark, test_num_records, party_identifiers, faker_data)
        sources = df.select("SourceApplication").distinct().collect()
        source_values = [row.SourceApplication for row in sources]
        assert set(source_values).issubset(set(rdm.SOURCE_APPLICATIONS))
    
    def test_party_altnames_name_type_code(self, spark, test_num_records, party_identifiers, faker_data):
        df = rdm.generate_party_altnames_data(spark, test_num_records, party_identifiers, faker_data)
        codes = df.select("PartyNameTypeCode").distinct().collect()
        code_values = [row.PartyNameTypeCode for row in codes]
        assert set(code_values).issubset(set(rdm.PARTY_NAME_TYPE_CODES))


class TestIntegration:
    """Test end-to-end data generation pipeline."""
    
    def test_generate_all_rdm_data_returns_five_tables(self, all_tables):
        assert len(all_tables) == 5
        expected_tables = ['Party', 'Party_Address', 'Party_Naics', 'Party_SystemIdentifier', 'Party_AlternativeNames']
        assert set(all_tables.keys()) == set(expected_tables)
    
    def test_all_tables_have_date_columns(self, all_tables):
        date_cols = ["EDL_LOAD_DTS", "EDL_ACT_DTS", "EDL_ACT_DTS_UTC", "LOAD_DT"]
        for table_name, df in all_tables.items():
            for col in date_cols:
                assert col in df.columns
    
    def test_referential_integrity(self, all_tables):
        party_ids = set([row.PartyIdentifier for row in all_tables['Party'].select("PartyIdentifier").collect()])
        child_tables = ['Party_Address', 'Party_Naics', 'Party_SystemIdentifier', 'Party_AlternativeNames']
        for table_name in child_tables:
            child_party_ids = set([row.PartyIdentifier for row in all_tables[table_name].select("PartyIdentifier").distinct().collect()])
            assert child_party_ids.issubset(party_ids)
    
    def test_row_count_multipliers(self, all_tables, test_num_records):
        assert all_tables['Party'].count() == test_num_records
        assert all_tables['Party_Address'].count() == test_num_records * 2
        assert all_tables['Party_Naics'].count() == int(test_num_records * 1.5)
        assert all_tables['Party_SystemIdentifier'].count() == test_num_records * 2
        assert all_tables['Party_AlternativeNames'].count() == test_num_records
    
    def test_no_empty_dataframes(self, all_tables):
        for table_name, df in all_tables.items():
            assert df.count() > 0
    
    def test_consistent_load_date(self, all_tables):
        load_dates = []
        for table_name, df in all_tables.items():
            load_date = df.select("LOAD_DT").first().LOAD_DT
            load_dates.append(load_date)
        assert len(set(load_dates)) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
