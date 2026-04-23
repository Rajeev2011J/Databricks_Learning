"""RDM Test Data Generator Module

This module contains the core data generation logic for RDM V3 test data.
Extracted from RDM_TestData_V3 notebook for testing and reusability.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, IntegerType
from faker import Faker
from dbldatagen import DataGenerator


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

ID_PREFIXES = [
    "GCOB_LEC_", "GCOB_RLEP_", "GCOB_RNPP_", "GCOB_NP_", "GIC_", "GCDS_",
    "LEGACY2_LE_", "LEGACY2_RLE_", "LEGACY2_RNP_", "LEGACY2_NP_", "LEGACY2_NP_NPPC_",
    "LEGACY2_RLE_L2_", "LEGACY2_RLE_NL_", "LEGACY2_RLE_RA_", "LEGACY2_RLE_CCDB_",
    "LEGACY2_RNP_CCDB_", "LEGACY2_RNP_L2_", "LEGACY2_RNP_NL_", "LEGACY2_RNP_RA_",
    "LEGACY2_RNP_RF_", "LEGACY2_LE_RF_"
]

PARTY_TYPES = [
    "Subcontratado", "Órgãos Regulatórios", "Corretora - Interveniente (IF)", "Grupo", "Representante",
    "Funcionário", "Related Legal Entity", "Intragrupo", "Legal Entity", None, "Related Natural Person",
    "Partnership", "Comp/Clearing", "Garantidor", "Outras Partes Relacionadas", "Branch",
    "Gestor/Administrador", "Beneficiário", "IF - Correspondente", "Sub Account", "Sub-fund",
    "IF - Funding/Interbancário", "Foreign Branch", "SACADO", "IF Agente / IF Co-Participante",
    "Natural Person", "Organisation Unit", "Managed Fund", "Fornecedor", "Pagamento à Terceiros",
    "Referência Externa"
]

CDD_TYPES = [
    "(Supra-)national and local governmental authorities", "Mutual Benefit Associations", "Cooperatives",
    "Stichting Administratiekantoor", "Limited liability limited partnership", "Fund / Collective investment scheme",
    None, "Lender Only - Financial Institution", "Limited liability company", "RMA Only", "Non-listed Corporate",
    "Limited Liability Limited Partnership", "Associations", "Government institutions / 100% state-owned enterprises",
    "Limited Partnership", "Limited Liability Company", "Other", "General partnership", "General Partnership",
    "Natural Person In Professional Capacity (NPPC)", "Limited liability partnership",
    "Listed Corporate on a Recognised Exchange", "Business trust", "Non profit association/charity", "Trust",
    "Non-supervised agents / Intermediaries", "Unincorporated business partnerships", "Limited partnership",
    "Majority owned Rabobank subsidiary adhering to all Rabobank Group policies", "Foundations", "Supervised FI",
    "Estate", "Corporation", "Listed Corporate", "SPV", "Lender Only - Fund / Collateral Loan Obligation",
    "Natural Person", "Limited Liability Partnership", "Supervised agent, e.g. Investment / Asset / Fund manager"
]

LOCAL_SYSTEM_ID_PREFIXES = ["NP_NPPC_", "LEC_", "RNP_", "RLEC_", ""]  # Empty string for digits-only

APPLICATIONS = ["GIC", "KN1-GRAM", "GCOB", "GCDS", "KN1"]

SOURCE_APPLICATIONS = ["Legacy2", "GCOB", "GCDS"]

PARTY_NAME_TYPE_CODES = ["Alias", "FullLegalNameInLocalLanguage", "TradeName", "Former"]


# ============================================================================
# FAKER DATA GENERATION
# ============================================================================

def generate_faker_data(num_first_names: int = 200, 
                       num_middle_names: int = 150,
                       num_last_names: int = 200,
                       num_companies: int = 100,
                       num_persons: int = 100) -> Dict[str, List[str]]:
    """Generate realistic names using Faker.
    
    Args:
        num_first_names: Number of unique first names to generate
        num_middle_names: Number of unique middle names to generate
        num_last_names: Number of unique last names to generate
        num_companies: Number of company names to generate
        num_persons: Number of person names to generate
        
    Returns:
        Dictionary with lists of names: first_names, middle_names, last_names, global_owner_names
    """
    fake = Faker(['en_US', 'en_GB', 'es_ES', 'es_MX', 'pt_BR', 'pt_PT', 'fr_FR', 'de_DE', 'it_IT', 'nl_NL'])
    
    return {
        'first_names': [fake.first_name() for _ in range(num_first_names)],
        'middle_names': [fake.first_name() for _ in range(num_middle_names)],
        'last_names': [fake.last_name() for _ in range(num_last_names)],
        'global_owner_names': [fake.company() for _ in range(num_companies)] + 
                             [fake.name() for _ in range(num_persons)]
    }


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def add_dates_v3(sdf: DataFrame) -> DataFrame:
    """Add EDL timestamp columns to DataFrame.
    
    Args:
        sdf: Input DataFrame
        
    Returns:
        DataFrame with added timestamp columns: EDL_LOAD_DTS, EDL_ACT_DTS, EDL_ACT_DTS_UTC, LOAD_DT
    """
    filesavetime = datetime.now()
    EDL_UTC = filesavetime - timedelta(hours=2)
    
    timestamp_str = filesavetime.strftime('%Y-%m-%d %H:%M:%S.%f')
    utc_str = EDL_UTC.strftime('%Y-%m-%d %H:%M:%S.%f')
    date_str = filesavetime.strftime('%Y%m%d')
    
    return (sdf
        .withColumn("EDL_LOAD_DTS", F.lit(timestamp_str))
        .withColumn("EDL_ACT_DTS", F.lit(timestamp_str))
        .withColumn("EDL_ACT_DTS_UTC", F.lit(utc_str))
        .withColumn("LOAD_DT", F.lit(date_str))
    )


# ============================================================================
# PARTY DATA GENERATION
# ============================================================================

def generate_party_data(spark: SparkSession,
                       num_records: int,
                       faker_data: Dict[str, List[str]]) -> DataFrame:
    """Generate Party DataFrame.
    
    Args:
        spark: SparkSession instance
        num_records: Number of records to generate
        faker_data: Dictionary with Faker-generated names
        
    Returns:
        DataFrame with Party data (59 columns)
    """
    party_spec = (
        DataGenerator(spark, rows=num_records, name="party")
        # ID Generation
        .withColumn("id_prefix", StringType(), values=ID_PREFIXES, random=True)
        .withColumn("id_number", StringType(), expr="lpad(cast(floor(rand() * 100000000) as string), 8, '0')")
        .withColumn("PartyIdentifier", StringType(), expr="concat_ws('', id_prefix, id_number)")
        
        # Name Fields
        .withColumn("FirstName", StringType(), values=faker_data['first_names'], random=True)
        .withColumn("MiddleName", StringType(), values=faker_data['middle_names'], random=True, percentNulls=0.3)
        .withColumn("LastName", StringType(), values=faker_data['last_names'], random=True)
        .withColumn("FullLegalName", StringType(), expr="concat_ws(' ', FirstName, MiddleName, LastName)")
        .withColumn("DateOfBirth", StringType(), expr="date_sub(current_date(), cast(rand() * 30000 + 6570 as int))", percentNulls=0.3)
        
        # Classification
        .withColumn("Party_type", StringType(), values=PARTY_TYPES, random=True)
        .withColumn("GlobalClientOwnerName", StringType(), values=faker_data['global_owner_names'], random=True)
        .withColumn("LegalForm", StringType(), values=["Corporation", "Partnership", "Sole Proprietorship", "Limited Liability", "LLC", "Trust"], random=True)
        .withColumn("CustomerLifeCycleStatus", StringType(), values=["Former Prospect", "Prospect", None, "Former Client", "Exit Client", "Active", "Client", "Inactive", "Pending", "Closed", "Dormant"], random=True)
        .withColumn("CddType", StringType(), values=CDD_TYPES, random=True)
        
        # FATCA Fields
        .withColumn("IsEligibleForFatcaAssessment", BooleanType(), random=True)
        .withColumn("FatcaClassification", StringType(), values=["Active NFFE", "Passive NFFE", "Excepted NFFE", "Direct Reporting NFFE", "Exempt", "Participating", "Non-Participating", "Partner Jurisdiction FI"], percentNulls=0.3, random=True)
        .withColumn("GIIN", StringType(), expr="concat(substring(uuid(), 1, 2), '.', substring(md5(cast(rand() as string)), 1, 5), '.', substring(md5(cast(rand() as string)), 1, 2), '.', cast(floor(rand() * 1000) as string))", percentNulls=0.3, random=True)
        .withColumn("EIN", StringType(), expr="concat(cast(floor(rand() * 90 + 10) as string), '-', lpad(cast(floor(rand() * 10000000) as string), 7, '0'))")
        .withColumn("FatcaDateOfIssue", StringType(), expr="date_sub(current_date(), cast(rand() * 3650 as int))", percentNulls=0.3, random=True)
        .withColumn("FatcaComments", StringType(), values=["Compliant", "Pending Documentation", "Exempt due to size", "N/A"], percentNulls=0.3, random=True)
        
        # CRS Fields
        .withColumn("IsEligibleForCrsAssessment", BooleanType(), random=True)
        .withColumn("CrsClassification", StringType(), values=["Financial Institution", "Active NFE", "Passive NFE", "Investment Entity", "Financial Institution", "Government Entity"], random=True)
        .withColumn("CrsFormSignedDate", StringType(), expr="date_sub(current_date(), cast(rand() * 3650 as int))")
        .withColumn("CrsComments", StringType(), values=["Form completed", "Awaiting signature", "Exempt", "N/A"])
        
        # Version & Language
        .withColumn("IsLatestApprovedVersionOfClient", StringType(), values=["Y", "N"], random=True)
        .withColumn("FullLegalNameInLocalLanguage", StringType(), expr="concat_ws(' ', FirstName, MiddleName, LastName)")
        
        # Incorporation
        .withColumn("IsIncorporated", BooleanType(), random=True)
        .withColumn("IncorporationNumber", StringType(), expr="concat('INC', lpad(cast(floor(rand() * 100000) as string), 5, '0'))")
        .withColumn("IncorporationDate", StringType(), expr="date_sub(current_date(), cast(rand() * 20000 as int))")
        .withColumn("HasSourceOfWealth", BooleanType(), random=True)
        
        # Risk & Watchlists
        .withColumn("SanctionsOrExternalWatchlist", StringType(), values=["Clear", "Potential Match", "Confirmed Match", "Under Investigation"], random=True)
        .withColumn("InternalWatchlist", StringType(), values=["None", "High Risk", "Medium Risk", "Low Risk", "Clear", "Watchlisted", "Restricted"], random=True)
        .withColumn("AdverseInformationOrMedia", StringType(), values=["None", "Minor", "Significant", "Critical"], random=True)
        .withColumn("StatedFindings", StringType(), values=["No issues", "Minor concerns", "Requires monitoring", "High risk"], random=True)
        .withColumn("PEPStatus", StringType(), values=["Non-PEP", "PEP", "RCA", "HIO", "No", "Yes", "Family Member", "Former PEP"], random=True)
        
        # Trust
        .withColumn("IsTrust", BooleanType(), random=True)
        .withColumn("TypeOfTrust", StringType(), values=["Revocable", "Irrevocable", "Testamentary", "Living"], random=True)
        
        # Regulation
        .withColumn("IsClientRegulated", BooleanType(), random=True)
        .withColumn("RegulatorName", StringType(), values=["SEC", "FCA", "BaFin", "FINRA", "OCC"], random=True)
        .withColumn("RegulatorCountry", StringType(), values=["USA", "UK", "Germany", "France", "Japan"], random=True)
        .withColumn("HasRecognisedRegulator", BooleanType(), random=True)
        
        # Exchange Listing
        .withColumn("IsClientListed", BooleanType(), random=True)
        .withColumn("ExchangeName", StringType(), values=["NYSE", "NASDAQ", "LSE", "TSE", "HKEX"], random=True)
        .withColumn("ExchangeCountry", StringType(), values=["USA", "UK", "Japan", "Hong Kong", "Germany"], random=True)
        .withColumn("HasRecognisedExchange", BooleanType(), random=True)
        
        # Tax Information
        .withColumn("CountryOfTaxResidence", StringType(), values=["USA", "UK", "Canada", "Australia", "Germany", "France"], random=True)
        .withColumn("TinAvailable", StringType(), values=["Y", "N"], random=True)
        .withColumn("TinOrEquivalent", StringType(), expr="concat('TIN', lpad(cast(floor(rand() * 100000000) as string), 8, '0'))")
        .withColumn("TinUnavailabilityReason", StringType(), values=["Not Required", "Not Issued", "Pending"], random=True)
        .withColumn("ExplanationForTinBeingUnavailable", StringType(), expr="concat('Reason code: ', cast(floor(rand() * 100) as string))")
        
        # Identification
        .withColumn("SourceOfIdentificationDocument", StringType(), values=["Passport", "Drivers License", "National ID"], random=True)
        .withColumn("IdentificationDocumentNumber", StringType(), expr="concat('ID', lpad(cast(floor(rand() * 10000000) as string), 7, '0'))")
        .withColumn("SourceOfVerifiedDocument", StringType(), values=["Bank Statement", "Utility Bill", "Tax Return"], random=True)
        .withColumn("VerifiedDocumentNumber", StringType(), expr="concat('VD', lpad(cast(floor(rand() * 10000000) as string), 7, '0'))")
        
        # Additional Attributes
        .withColumn("FIHubIndicator", BooleanType(), random=True)
        .withColumn("W&RORRetail", StringType(), values=["Wholesale", "Retail"], random=True)
        .withColumn("Party_LeadingPartyIdentifier", StringType(), expr="concat('LPI', lpad(cast(floor(rand() * 1000000) as string), 6, '0'))")
        .withColumn("IsRabobankEntity", BooleanType(), random=True)
        .withColumn("HO_reporting_party-FINREP_Code", StringType(), expr="concat('FINREP', lpad(cast(floor(rand() * 1000) as string), 3, '0'))")
        .withColumn("HO_reporting_party-FINREP", StringType(), values=["FINREP1", "FINREP2", "FINREP3"], random=True)
        .withColumn("KYCGroup", StringType(), values=["Group A", "Group B", "Group C"], random=True)
        .withColumn("SectorTeam", StringType(), values=["Financial Services", "Technology", "Healthcare", "Manufacturing"], random=True)
    )
    
    party_df = party_spec.build()
    
    # Select columns in expected order
    party_df = party_df.select(
        "PartyIdentifier", "FullLegalName", "DateOfBirth", "FirstName", "MiddleName", "LastName", "Party_type",
        "GlobalClientOwnerName", "IsEligibleForFatcaAssessment", "FatcaClassification", "GIIN", "EIN",
        "FatcaDateOfIssue", "FatcaComments", "IsEligibleForCrsAssessment", "CrsClassification",
        "CrsFormSignedDate", "CrsComments", "LegalForm", "CustomerLifeCycleStatus",
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
    )
    
    return party_df


def generate_party_address_data(spark: SparkSession,
                               num_records: int,
                               party_identifiers: List[str]) -> DataFrame:
    """Generate Party_Address DataFrame.
    
    Args:
        spark: SparkSession instance
        num_records: Base number of records (will generate 2x)
        party_identifiers: List of valid PartyIdentifiers
        
    Returns:
        DataFrame with Party_Address data (11 columns)
    """
    party_address_spec = (
        DataGenerator(spark, rows=num_records * 2, name="party_address")
        .withColumn("PartyIdentifier", StringType(), values=party_identifiers, random=True)
        .withColumn("AddressSystem", StringType(), values=["Local", "International", "Postal", "Internal", "External", "Legal", "Mailing"], random=True)
        .withColumn("AddressSystem_ID", StringType(), expr="concat('ASID', lpad(cast(floor(rand() * 10000) as string), 4, '0'))")
        .withColumn("AddressType", StringType(), values=["Residential", "Commercial", "Mailing"], random=True)
        .withColumn("HouseNumber", StringType(), expr="cast(floor(rand() * 9999 + 1) as string)")
        .withColumn("Street", StringType(), expr="concat(upper(substring(md5(cast(rand() as string)), 1, 4)), ' ', upper(substring(md5(cast(rand() as string)), 1, 3)), ' Street')")
        .withColumn("City", StringType(), values=["New York", "London", "Zurich", "Singapore", "Frankfurt", "Amsterdam"], random=True)
        .withColumn("Region", StringType(), values=["NY", "ENG", "ZH", "SG", "HE", "NH"], random=True)
        .withColumn("PostalCode", StringType(), expr="lpad(cast(floor(rand() * 1000000) as int), 6, '0')")
        .withColumn("Country", StringType(), values=["United States", "United Kingdom", "Switzerland", "Singapore", "Germany", "Netherlands"], random=True)
        .withColumn("CountryCode", StringType(), values=["US", "GB", "CH", "SG", "DE", "NL"], random=True)
    )
    
    party_address_df = party_address_spec.build()
    
    return party_address_df.select(
        "PartyIdentifier", "AddressSystem", "AddressSystem_ID", "AddressType", "HouseNumber",
        "Street", "City", "Region", "PostalCode", "Country", "CountryCode"
    )


def generate_party_naics_data(spark: SparkSession,
                             num_records: int,
                             party_identifiers: List[str]) -> DataFrame:
    """Generate Party_Naics DataFrame.
    
    Args:
        spark: SparkSession instance
        num_records: Base number of records (will generate 1.5x)
        party_identifiers: List of valid PartyIdentifiers
        
    Returns:
        DataFrame with Party_Naics data (7 columns)
    """
    party_naics_spec = (
        DataGenerator(spark, rows=int(num_records * 1.5), name="party_naics")
        .withColumn("PartyIdentifier", StringType(), values=party_identifiers, random=True)
        .withColumn("local_system_id_prefix", StringType(), values=LOCAL_SYSTEM_ID_PREFIXES, random=True)
        .withColumn("local_system_id_digits", StringType(), expr="lpad(cast(floor(rand() * 10000) as string), 4, '0')")
        .withColumn("LocalSystemId", StringType(), expr="concat(local_system_id_prefix, local_system_id_digits)")
        .withColumn("LocalSystemName", StringType(), values=APPLICATIONS, random=True)
        .withColumn("NAICSCode", StringType(), expr="lpad(cast(floor(rand() * 1000000) as string), 6, '0')")
        .withColumn("NAICSDescription", StringType(), values=["Finance and Insurance", "Manufacturing", "Retail Trade", "Information Technology", "Healthcare"], random=True)
        .withColumn("PrimaryNaicsFlag", StringType(), values=["Y", "N"], random=True)
        .withColumn("NAICSPercentage", StringType(), expr="cast(floor(rand() * 100 + 1) as string)", percentNulls=0.3)
    )
    
    party_naics_df = party_naics_spec.build()
    
    return party_naics_df.select(
        "PartyIdentifier", "LocalSystemId", "LocalSystemName", "NAICSCode",
        "NAICSDescription", "PrimaryNaicsFlag", "NAICSPercentage"
    )


def generate_party_systemidentifier_data(spark: SparkSession,
                                        num_records: int,
                                        party_identifiers: List[str]) -> DataFrame:
    """Generate Party_SystemIdentifier DataFrame.
    
    Args:
        spark: SparkSession instance
        num_records: Base number of records (will generate 2x)
        party_identifiers: List of valid PartyIdentifiers
        
    Returns:
        DataFrame with Party_SystemIdentifier data (3 columns)
    """
    party_systemidentifier_spec = (
        DataGenerator(spark, rows=num_records * 2, name="party_systemidentifier")
        .withColumn("PartyIdentifier", StringType(), values=party_identifiers, random=True)
        .withColumn("Application", StringType(), values=APPLICATIONS, random=True)
        .withColumn("LocalSystemIdentifier", StringType(), expr="concat('LSI', lpad(cast(floor(rand() * 1000000) as string), 6, '0'))")
    )
    
    party_systemidentifier_df = party_systemidentifier_spec.build()
    
    return party_systemidentifier_df.select(
        "PartyIdentifier", "Application", "LocalSystemIdentifier"
    )


def generate_party_altnames_data(spark: SparkSession,
                                num_records: int,
                                party_identifiers: List[str],
                                faker_data: Dict[str, List[str]]) -> DataFrame:
    """Generate Party_AlternativeNames DataFrame.
    
    Args:
        spark: SparkSession instance
        num_records: Number of records to generate
        party_identifiers: List of valid PartyIdentifiers
        faker_data: Dictionary with Faker-generated names
        
    Returns:
        DataFrame with Party_AlternativeNames data (7 columns)
    """
    party_altnames_spec = (
        DataGenerator(spark, rows=num_records, name="party_altnames")
        .withColumn("PartyIdentifier", StringType(), values=party_identifiers, random=True)
        .withColumn("SourceApplication", StringType(), values=SOURCE_APPLICATIONS, random=True)
        .withColumn("PartyNameTypeCode", StringType(), values=PARTY_NAME_TYPE_CODES, random=True)
        .withColumn("FirstName", StringType(), values=faker_data['first_names'], random=True, percentNulls=0.4)
        .withColumn("MiddleName", StringType(), values=faker_data['middle_names'], random=True, percentNulls=0.4)
        .withColumn("LastName", StringType(), values=faker_data['last_names'], random=True, percentNulls=0.4)
        .withColumn("FullName", StringType(), expr="concat_ws(' ', FirstName, MiddleName, LastName)")
    )
    
    party_altnames_df = party_altnames_spec.build()
    
    return party_altnames_df.select(
        "PartyIdentifier", "SourceApplication", "PartyNameTypeCode",
        "FirstName", "MiddleName", "LastName", "FullName"
    )


# ============================================================================
# MAIN GENERATION PIPELINE
# ============================================================================

def generate_all_rdm_data(spark: SparkSession, num_records: int = 50000) -> Dict[str, DataFrame]:
    """Generate all RDM V3 test data tables.
    
    Args:
        spark: SparkSession instance
        num_records: Number of Party records to generate (other tables use multipliers)
        
    Returns:
        Dictionary with DataFrames:
            - Party: Main party table (num_records)
            - Party_Address: Address data (num_records * 2)
            - Party_Naics: NAICS data (num_records * 1.5)
            - Party_SystemIdentifier: System ID data (num_records * 2)
            - Party_AlternativeNames: Alternative names (num_records)
    """
    # Generate Faker data
    faker_data = generate_faker_data()
    
    # Generate Party data (main table)
    party_df = generate_party_data(spark, num_records, faker_data)
    party_df.cache()
    party_df.count()  # Materialize cache
    
    # Extract party identifiers for child tables
    party_identifiers = [row.PartyIdentifier for row in party_df.select("PartyIdentifier").collect()]
    
    # Generate child tables
    party_address_df = generate_party_address_data(spark, num_records, party_identifiers)
    party_naics_df = generate_party_naics_data(spark, num_records, party_identifiers)
    party_systemidentifier_df = generate_party_systemidentifier_data(spark, num_records, party_identifiers)
    party_altnames_df = generate_party_altnames_data(spark, num_records, party_identifiers, faker_data)
    
    # Add date columns to all tables
    return {
        'Party': add_dates_v3(party_df),
        'Party_Address': add_dates_v3(party_address_df),
        'Party_Naics': add_dates_v3(party_naics_df),
        'Party_SystemIdentifier': add_dates_v3(party_systemidentifier_df),
        'Party_AlternativeNames': add_dates_v3(party_altnames_df)
    }
