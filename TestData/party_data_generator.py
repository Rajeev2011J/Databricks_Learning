"""Party Data Generator Module

Provides functions to generate synthetic party-related data using dbldatagen.
"""

from dbldatagen import DataGenerator
from pyspark.sql.types import StringType, BooleanType, IntegerType
from pyspark.sql.functions import concat_ws, col


def get_id_prefixes():
    """Return list of valid ID prefixes for PartyIdentifier."""
    return [
        "GCOB_LEC_", "GCOB_RLEP_", "GCOB_RNPP_", "GCOB_NP_", "GIC_", "GCDS_",
        "LEGACY2_LE_", "LEGACY2_RLE_", "LEGACY2_RNP_", "LEGACY2_NP_", "LEGACY2_NP_NPPC_",
        "LEGACY2_RLE_L2_", "LEGACY2_RLE_NL_", "LEGACY2_RLE_RA_", "LEGACY2_RLE_CCDB_",
        "LEGACY2_RNP_CCDB_", "LEGACY2_RNP_L2_", "LEGACY2_RNP_NL_", "LEGACY2_RNP_RA_",
        "LEGACY2_RNP_RF_", "LEGACY2_LE_RF_"
    ]


def get_party_types():
    """Return list of valid party types."""
    return [
        "Subcontratado", "Órgãos Regulatórios", "Corretora - Interveniente (IF)", "Grupo", "Representante",
        "Funcionário", "Related Legal Entity", "Intragrupo", "Legal Entity", None, "Related Natural Person",
        "Partnership", "Comp/Clearing", "Garantidor", "Outras Partes Relacionadas", "Branch",
        "Gestor/Administrador", "Beneficiário", "IF - Correspondente", "Sub Account", "Sub-fund",
        "IF - Funding/Interbancário", "Foreign Branch", "SACADO", "IF Agente / IF Co-Participante",
        "Natural Person", "Organisation Unit", "Managed Fund", "Fornecedor", "Pagamento à Terceiros",
        "Referência Externa"
    ]


def get_cdd_types():
    """Return list of valid CDD types."""
    return [
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


def get_kyc_groups():
    """Return list of KYC group company names."""
    return [
        "Scandi Standard AB", "SANTANDER", "China Co-op Group Co., Ltd.", "Ably Resources Limited",
        "Bleckmann Nederland B.V.", "RICEGROWERS LIMITED", "JBS USA FINANCE, INC.", "CKC Capital LLC",
        "BANKINTER SA", "Vermeer", "SFI MARKETS B.V.", "The Hain Celestial Group, Inc.",
        "Artisan Investco B.V.", "CARGOBULL FINANCE HOLDING B.V.", "CCK", "HYPO TIROL BANK AG",
        "Glasvezelnet Noordwest Veluwe B.V.", "CUMBERLAND", "WEBESS01 FinCo Pty Ltd", "SW Power Trading LLC",
        "Beissbarth Automotive Testing Solutions GmbH", "Johnson & Johnson", "Procter & Gamble",
        "General Electric", "Siemens AG", "Toyota Motor Corporation", "Volkswagen Group",
        "Royal Dutch Shell", "BP plc", "ExxonMobil Corporation", "Chevron Corporation"
    ]


def generate_party_df(spark, num_records=100):
    """Generate party dataframe with synthetic data.
    
    Args:
        spark: SparkSession instance
        num_records: Number of records to generate
        
    Returns:
        DataFrame with party data
    """
    id_prefixes = get_id_prefixes()
    party_types = get_party_types()
    cdd_types = get_cdd_types()
    kyc_groups = get_kyc_groups()
    
    party_spec = (DataGenerator(spark, rows=num_records, name="party")
        .withColumn("id_prefix", StringType(), values=id_prefixes, random=True)
        .withColumn("id_number", StringType(), expr="lpad(cast(floor(rand() * 100000000) as string), 8, '0')")
        .withColumn("PartyIdentifier", StringType(), expr="concat_ws('', id_prefix, id_number)")
        .withColumn("FirstName", StringType(), expr="initcap(concat(chr(cast(rand() * 26 + 65 as int)), substring(md5(cast(rand() as string)), 1, cast(rand() * 5 + 3 as int))))")
        .withColumn("MiddleName", StringType(), expr="initcap(concat(chr(cast(rand() * 26 + 65 as int)), substring(md5(cast(rand() as string)), 1, cast(rand() * 4 + 3 as int))))", percentNulls=0.3)
        .withColumn("LastName", StringType(), expr="initcap(concat(chr(cast(rand() * 26 + 65 as int)), substring(md5(cast(rand() as string)), 1, cast(rand() * 6 + 4 as int))))")
        .withColumn("FullLegalName", StringType(), expr="concat_ws(' ', FirstName, MiddleName, LastName)")
        .withColumn("DateOfBirth", StringType(), expr="date_sub(current_date(), cast(rand() * 30000 + 6570 as int))")
        .withColumn("Party_type", StringType(), values=party_types, random=True)
        .withColumn("GlobalClientOwnerName", StringType(), expr="concat(substring(uuid(), 1, 8), ' Inc')")
        .withColumn("IsEligibleForFatcaAssessment", BooleanType(), random=True)
        .withColumn("FatcaClassification", StringType(), values=["Active NFFE", "Passive NFFE", "Excepted NFFE", "Direct Reporting NFFE"], random=True)
        .withColumn("GIIN", StringType(), expr="concat(substring(uuid(), 1, 2), '.', substring(md5(cast(rand() as string)), 1, 5), '.', substring(md5(cast(rand() as string)), 1, 2), '.', cast(floor(rand() * 1000) as string))")
        .withColumn("EIN", StringType(), expr="concat(cast(floor(rand() * 90 + 10) as string), '-', lpad(cast(floor(rand() * 10000000) as string), 7, '0'))")
        .withColumn("FatcaDateOfIssue", StringType(), expr="date_sub(current_date(), cast(rand() * 3650 as int))")
        .withColumn("FatcaComments", StringType(), expr="concat('Review completed on ', date_sub(current_date(), cast(rand() * 365 as int)))")
        .withColumn("IsEligibleForCrsAssessment", BooleanType(), random=True)
        .withColumn("CrsClassification", StringType(), values=["Financial Institution", "Active NFE", "Passive NFE", "Investment Entity"], random=True)
        .withColumn("CrsFormSignedDate", StringType(), expr="date_sub(current_date(), cast(rand() * 3650 as int))")
        .withColumn("CrsComments", StringType(), expr="concat('CRS assessment date: ', date_sub(current_date(), cast(rand() * 730 as int)))")
        .withColumn("LegalForm", StringType(), values=["Corporation", "Partnership", "Sole Proprietorship", "Limited Liability"], random=True)
        .withColumn("CustomerLifeCycleStatus", StringType(), values=["Former Prospect", "Prospect", None, "Former Client", "Exit Client", "Active", "Client"], random=True)
        .withColumn("IsLatestApprovedVersionOfClient", StringType(), values=["Y", "N"], random=True)
        .withColumn("FullLegalNameInLocalLanguage", StringType(), expr="concat_ws(' ', FirstName, MiddleName, LastName)")
        .withColumn("IsIncorporated", BooleanType(), random=True)
        .withColumn("IncorporationNumber", StringType(), expr="concat('INC', lpad(cast(floor(rand() * 100000) as string), 5, '0'))")
        .withColumn("IncorporationDate", StringType(), expr="date_sub(current_date(), cast(rand() * 20000 as int))")
        .withColumn("HasSourceOfWealth", BooleanType(), random=True)
        .withColumn("SanctionsOrExternalWatchlist", StringType(), values=["None", "OFAC", "EU", "UN"], random=True)
        .withColumn("InternalWatchlist", StringType(), values=["None", "High Risk", "Medium Risk", "Low Risk"], random=True)
        .withColumn("AdverseInformationOrMedia", StringType(), values=["None", "Minor", "Significant"], random=True)
        .withColumn("StatedFindings", StringType(), expr="concat('Finding-', substring(uuid(), 1, 8))")
        .withColumn("PEPStatus", StringType(), values=["Non-PEP", "PEP", "RCA", "HIO"], random=True)
        .withColumn("IsTrust", BooleanType(), random=True)
        .withColumn("TypeOfTrust", StringType(), values=["Revocable", "Irrevocable", "Testamentary", "Living"], random=True)
        .withColumn("IsClientRegulated", BooleanType(), random=True)
        .withColumn("RegulatorName", StringType(), values=["SEC", "FCA", "BaFin", "FINRA", "OCC"], random=True)
        .withColumn("RegulatorCountry", StringType(), values=["USA", "UK", "Germany", "France", "Japan"], random=True)
        .withColumn("HasRecognisedRegulator", BooleanType(), random=True)
        .withColumn("IsClientListed", BooleanType(), random=True)
        .withColumn("ExchangeName", StringType(), values=["NYSE", "NASDAQ", "LSE", "TSE", "HKEX"], random=True)
        .withColumn("ExchangeCountry", StringType(), values=["USA", "UK", "Japan", "Hong Kong", "Germany"], random=True)
        .withColumn("HasRecognisedExchange", BooleanType(), random=True)
        .withColumn("CountryOfTaxResidence", StringType(), values=["USA", "UK", "Canada", "Australia", "Germany", "France"], random=True)
        .withColumn("TinAvailable", StringType(), values=["Y", "N"], random=True)
        .withColumn("TinOrEquivalent", StringType(), expr="concat('TIN', lpad(cast(floor(rand() * 100000000) as string), 8, '0'))")
        .withColumn("TinUnavailabilityReason", StringType(), values=["Not Required", "Not Issued", "Pending"], random=True)
        .withColumn("ExplanationForTinBeingUnavailable", StringType(), expr="concat('Reason code: ', cast(floor(rand() * 100) as string))")
        .withColumn("SourceOfIdentificationDocument", StringType(), values=["Passport", "Drivers License", "National ID"], random=True)
        .withColumn("IdentificationDocumentNumber", StringType(), expr="concat('ID', lpad(cast(floor(rand() * 10000000) as string), 7, '0'))")
        .withColumn("SourceOfVerifiedDocument", StringType(), values=["Bank Statement", "Utility Bill", "Tax Return"], random=True)
        .withColumn("VerifiedDocumentNumber", StringType(), expr="concat('VD', lpad(cast(floor(rand() * 10000000) as string), 7, '0'))")
        .withColumn("CddType", StringType(), values=cdd_types, random=True)
        .withColumn("FIHubIndicator", BooleanType(), random=True)
        .withColumn("W&RORRetail", StringType(), values=["Wholesale", "Retail"], random=True)
        .withColumn("Party_LeadingPartyIdentifier", StringType(), expr="concat('LPI', lpad(cast(floor(rand() * 1000000) as string), 6, '0'))")
        .withColumn("IsRabobankEntity", BooleanType(), random=True)
        .withColumn("HO_reporting_party-FINREP_Code", StringType(), expr="concat('FINREP', lpad(cast(floor(rand() * 1000) as string), 3, '0'))")
        .withColumn("HO_reporting_party-FINREP", StringType(), values=["FINREP1", "FINREP2", "FINREP3"], random=True)
        .withColumn("KYCGroup", StringType(), values=kyc_groups, random=True)
        .withColumn("SectorTeam", StringType(), values=["Financial Services", "Technology", "Healthcare", "Manufacturing"], random=True)
    )
    
    party_df = party_spec.build()
    
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


def generate_party_address_df(spark, party_ids, num_records=100):
    """Generate party address dataframe.
    
    Args:
        spark: SparkSession instance
        party_ids: List of PartyIdentifier values
        num_records: Number of records to generate
        
    Returns:
        DataFrame with party address data
    """
    street_names = ["Main Street", "Oak Avenue", "Maple Drive", "Washington Boulevard", "Park Lane",
                    "Broadway", "First Avenue", "Second Street", "Elm Street", "Pine Road"]
    
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
              "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte"]
    
    party_address_spec = (DataGenerator(spark, rows=num_records, name="party_address")
        .withColumn("PartyIdentifier", StringType(), values=party_ids, random=True)
        .withColumn("AddressSystem", StringType(), values=["Local", "International", "Postal"], random=True)
        .withColumn("AddressSystem_ID", StringType(), expr="concat('ASID', lpad(cast(floor(rand() * 10000) as string), 4, '0'))")
        .withColumn("AddressType", StringType(), values=["Residential", "Commercial", "Mailing"], random=True)
        .withColumn("HouseNumber", StringType(), expr="cast(floor(rand() * 9999 + 1) as string)")
        .withColumn("Street", StringType(), values=street_names, random=True)
        .withColumn("City", StringType(), values=cities, random=True)
        .withColumn("Region", StringType(), values=["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"], random=True)
        .withColumn("PostalCode", StringType(), expr="lpad(cast(floor(rand() * 100000) as string), 5, '0')")
        .withColumn("Country", StringType(), values=["USA", "UK", "Japan", "France", "Australia", "Canada"], random=True)
        .withColumn("CountryCode", StringType(), values=["US", "GB", "JP", "FR", "AU", "CA"], random=True)
    )
    
    party_address_df = party_address_spec.build()
    party_address_df = party_address_df.select(
        "PartyIdentifier", "AddressSystem", "AddressSystem_ID", "AddressType", "HouseNumber", "Street", "City", "Region",
        "PostalCode", "Country", "CountryCode"
    )
    
    return party_address_df


def generate_party_naics_df(spark, party_ids, num_records=100):
    """Generate party NAICS dataframe.
    
    Args:
        spark: SparkSession instance
        party_ids: List of PartyIdentifier values
        num_records: Number of records to generate
        
    Returns:
        DataFrame with party NAICS data
    """
    party_naics_spec = (DataGenerator(spark, rows=num_records, name="party_naics")
        .withColumn("PartyIdentifier", StringType(), values=party_ids, random=True)
        .withColumn("LocalSystemId", StringType(), expr="concat('LSID', lpad(cast(floor(rand() * 10000) as string), 4, '0'))")
        .withColumn("LocalSystemName", StringType(), values=["Core Banking", "CRM System", "Trading Platform", "Risk System"], random=True)
        .withColumn("NAICSCode", StringType(), expr="lpad(cast(floor(rand() * 1000000) as string), 6, '0')")
        .withColumn("NAICSDescription", StringType(), values=["Finance and Insurance", "Manufacturing", "Retail Trade", "Information Technology", "Healthcare"], random=True)
        .withColumn("PrimaryNaicsFlag", StringType(), values=["Y", "N"], random=True)
        .withColumn("NAICSPercentage", StringType(), expr="cast(floor(rand() * 100 + 1) as string)")
    )
    
    party_naics_df = party_naics_spec.build()
    party_naics_df = party_naics_df.select(
        "PartyIdentifier", "LocalSystemId", "LocalSystemName", "NAICSCode", "NAICSDescription", "PrimaryNaicsFlag", "NAICSPercentage"
    )
    
    return party_naics_df


def generate_party_systemidentifier_df(spark, party_ids, num_records=100):
    """Generate party system identifier dataframe.
    
    Args:
        spark: SparkSession instance
        party_ids: List of PartyIdentifier values
        num_records: Number of records to generate
        
    Returns:
        DataFrame with party system identifier data
    """
    party_systemidentifier_spec = (DataGenerator(spark, rows=num_records, name="party_systemidentifier")
        .withColumn("PartyIdentifier", StringType(), values=party_ids, random=True)
        .withColumn("Application", StringType(), values=["CoreBanking", "CustomerPortal", "MobileApp", "BackOffice"], random=True)
        .withColumn("LocalSystemIdentifier", StringType(), expr="concat('LSI', lpad(cast(floor(rand() * 1000000) as string), 6, '0'))")
    )
    
    party_systemidentifier_df = party_systemidentifier_spec.build()
    party_systemidentifier_df = party_systemidentifier_df.select(
        "PartyIdentifier", "Application", "LocalSystemIdentifier"
    )
    
    return party_systemidentifier_df


def generate_party_altnames_df(spark, party_ids, num_records=100):
    """Generate party alternative names dataframe.
    
    Args:
        spark: SparkSession instance
        party_ids: List of PartyIdentifier values
        num_records: Number of records to generate
        
    Returns:
        DataFrame with party alternative names data
    """
    party_altnames_spec = (DataGenerator(spark, rows=num_records, name="party_altnames")
        .withColumn("PartyIdentifier", StringType(), values=party_ids, random=True)
        .withColumn("SourceApplication", StringType(), values=["Legacy System", "Migration Source", "External Feed"], random=True)
        .withColumn("PartyNameTypeCode", StringType(), values=["Legal", "Trade", "Alias", "Former"], random=True)
        .withColumn("FirstName", StringType(), expr="initcap(concat(chr(cast(rand() * 26 + 65 as int)), substring(md5(cast(rand() as string)), 1, cast(rand() * 5 + 3 as int))))")
        .withColumn("MiddleName", StringType(), expr="initcap(concat(chr(cast(rand() * 26 + 65 as int)), substring(md5(cast(rand() as string)), 1, cast(rand() * 4 + 3 as int))))", percentNulls=0.4)
        .withColumn("LastName", StringType(), expr="initcap(concat(chr(cast(rand() * 26 + 65 as int)), substring(md5(cast(rand() as string)), 1, cast(rand() * 6 + 4 as int))))")
        .withColumn("FullName", StringType(), expr="concat_ws(' ', FirstName, MiddleName, LastName)")
    )
    
    party_altnames_df = party_altnames_spec.build()
    party_altnames_df = party_altnames_df.select(
        "PartyIdentifier", "SourceApplication", "PartyNameTypeCode", "FirstName", "MiddleName", "LastName", "FullName"
    )
    
    return party_altnames_df
