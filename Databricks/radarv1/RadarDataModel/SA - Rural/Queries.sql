-- GCDS sustainability

"CREATE VIEW [gcds].[vw_GCDS_Sustainability_ETL]
AS SELECT  
	  isnull(Cast([GCID] as int), -1) as [GlobalClientID]

			, cast([Sustainability_Date] as date) as [AssessmentDate]
			, cast([Sustainabiliy_PhotoScore] as nvarchar(2)) as [ClientPhotoScore]

			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]

		FROM [gcds].[GCDS_client_Client];"


-- National identifierr
"CREATE VIEW [gcds].[vw_GCDS_NationalIdentifier_ETL]
AS SELECT  
			 isnull(Cast([GCID] as int), -1) as [GlobalClientID]

			, cast([National_Identifier_Code] as nvarchar(20)) as [NationalIDCode]
			, cast([NationalID-type] as nvarchar(255)) as [NationalIDType]
			, cast([NationalID-value] as nvarchar(100)) as [NationalIDValue]

			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]

		FROM [gcds].[GCDS_client_Client];"

-- RANZ identifiers? (not required)
"CREATE VIEW [gcds].[vw_GCDS_AUNZ_ETL] AS SELECT  
		  isnull(Cast([GCID] as int), -1) as [GlobalClientID]

		, cast([AUNZ-ABNnr] as nvarchar(125)) as [AbnNumber]
		, cast([AUNZ-APRA_code] as nvarchar(125)) as [ApraCode]
		, cast([AUNZ-IRDnr] as nvarchar(125)) as [IrdNumber]
		, cast([AUNZ-RBNZ_code] as nvarchar(125)) as [RbnzCode]

		, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
		, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
		, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]
		FROM [gcds].[GCDS_client_Client];"


-- GCDS Adresses
"CREATE VIEW [gcds].[vw_GCDS_Address_ETL] AS SELECT 
        isnull([GlobalClientID],1) as [GlobalClientID]
      , [City]
      , [Country]
      , [CountryCode]
      , [HouseNumber]
      , [PostCode]
      , [Region]
      , [StreetLine1]
      , [StreetLine2]
      , [StreetLine3]
	  , [AdressType]
      , [EDL_LOAD_DTS]
      , [EDL_ACT_DTS]
      , [IF_LoadDate]

FROM (
	SELECT  
			  cast(isnull([GCID],-1) as int) as [GlobalClientID]
			, cast([Principal_address_city] as [nvarchar](255)) as [City]
			, cast([Principal_address_country] as [nvarchar](255))  as [Country]
			, cast([Principal_address_country_code] as [nvarchar](2))  as [CountryCode]
			, cast([Principal_address_house_nr] as [nvarchar](255))  as [HouseNumber]
			, cast([Principal_address_zipcode] as [nvarchar](255))  as [PostCode]
			, cast([Principal_address_region] as [nvarchar](255))  as [Region]
			, cast([Principal_address_streetLine1] as [nvarchar](255))  as [StreetLine1]
			, cast([Principal_address_streetLine2] as [nvarchar](255))  as [StreetLine2]
			, cast([Principal_address_streetLine3] as [nvarchar](255))  as [StreetLine3]
			, cast('Principal' as [nvarchar](255)) as [AdressType]
			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]
		FROM [gcds].[GCDS_client_Client]

	UNION

	SELECT  
			  cast(isnull([GCID],-1) as int) as [GlobalClientID]
			, cast([Registered_address_city] as [nvarchar](255)) as [City]
			, cast([Registered_address_country] as [nvarchar](255)) as [Country]
			, cast([Registered_address_country_code] as [nvarchar](2)) as [CountryCode]
			, cast([Registered_address_house_nr] as [nvarchar](255)) as [HouseNumber]
			, cast([Registered_address_zipcode] as [nvarchar](255)) as [PostCode]
			, cast([Registered_address_region] as [nvarchar](255)) as [Region]
			, cast([Registered_address_streetLine1] as [nvarchar](255)) as [StreetLine1]
			, cast([Registered_address_streetLine2] as [nvarchar](255)) as [StreetLine2]
			, cast([Registered_address_streetLine3] as [nvarchar](255)) as [StreetLine3]
			, cast('Registered' as [nvarchar](255)) as [AdressType]
			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]

		FROM [gcds].[GCDS_client_Client]

	
	UNION

	SELECT  
			  cast(isnull([GCID],-1) as int) as [GlobalClientID]
			, cast([Residentialaddress_city] as [nvarchar](255)) as [City]
			, cast([Residentialaddress_country] as [nvarchar](255)) as [Country]
			, cast([Residentialaddress_countrycode] as [nvarchar](2)) as [CountryCode]
			, cast([ResidentialAddress_HouseNumber] as [nvarchar](255)) as [HouseNumber]
			, cast([Residentialaddress_zipcode] as [nvarchar](255)) as [PostCode]
			, cast([Residentialaddress_region] as [nvarchar](255)) as [Region]
			, cast([Residentialaddress_streetLine1] as [nvarchar](255)) as [StreetLine1]
			, cast([Residentialaddress_streetLine2] as [nvarchar](255)) as [StreetLine2]
			, cast([Residentialaddress_streetLine3] as [nvarchar](255)) as [StreetLine3]
			, cast('Residential' as [nvarchar](255)) as [AdressType]
			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]
		FROM [gcds].[GCDS_client_Client]
		) a;"


-- GCDS Keystore
"CREATE VIEW [gcds].[vw_GCDS_Keystore_ETL]
AS SELECT  
			 isnull(Cast([GCID] as int), -1) as [GlobalClientID]

			, cast([Bank_code] as nvarchar(255)) as [BankCode]
			, cast([Country_code] as nvarchar(2)) as [CountryCode]
			, cast([KeyStore_type] as nvarchar(100)) as [KeyType]
			, cast([KeyStore_value] as nvarchar(255)) as [KeyValue]
			, cast([Registration_status] as nvarchar(255)) as [RegistrationStatus]
			, cast([Status] as nvarchar(20)) as [Status]

			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]
  FROM [gcds].[GCDS_client_KeyStoreKey];"


-- GCDS Client
"CREATE VIEW [gcds].[vw_GCDS_client_ETL] AS SELECT  
			  isnull(Cast([GCID] as int), -1) as [GlobalClientID]
			, cast([CO-businessline_code] as nvarchar(20)) as [ClientOwnerBusinessLineCode]
			, cast([CO-businessline_description] as nvarchar(255)) as [ClientOwnerBusinessLineDescription]
			, cast([CO-email] as nvarchar(255)) as [ClientOwnerEmail]
			, cast([CO-full_name] as nvarchar(255)) as [ClientOwnerFullName]
			, cast([CO-location] as nvarchar(255)) as [ClientOwnerLocation]
			, cast([CO-name] as nvarchar(255)) as [ClientOwnerName]
			, cast([Commercial_ultimate_parent] as nvarchar(9)) as [CommercialUltimateParent]

			, cast([Country_of_ultimate_CreditRisk] as nvarchar(2)) as [CountryCodeOfUltimateCreditRisk] --reversed as wrong reversed delivered
			, cast([Country_code_of_ultimate_CreditRisk] as nvarchar(100)) as [CountryofUltimateCreditRisk] --reversed as wrong reversed delivered

			, cast([Entity_classification] as nvarchar(100)) as [EntityClassification]
			, cast([Global_ultimate_parent] as nvarchar(9)) as [GlobalUltimateParent]
			, cast([NO_business_relation] as nvarchar(1)) as [NoBusinessRelationIndicator]
			, cast([Party_type] as nvarchar(50)) as [PartyType]
			, cast([Rabobank_entity] as nvarchar(1)) as [RabobankEntityIndicator]
			, cast([SME_indicator] as nvarchar(1)) as [SMEIndicator]
			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]
		FROM [gcds].[GCDS_client_Client];"

-- GCDS RMA
"CREATE VIEW [gcds].[vw_GCDS_RMA_ETL]
AS SELECT  
	  isnull(Cast([GCID] as int), -1) as [GlobalClientID]
	  			, cast([Active] as nvarchar(2)) as [Active]
	  			, cast([BICCODE] as nvarchar(12)) as [BICCODE]
	  			, cast([Branch] as nvarchar(255)) as [Branch]
	  			, cast([Category] as nvarchar(255)) as [Category]

			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]
	FROM [gcds].[GCDS_client_RMA];"


-- GCDS products
"CREATE VIEW [gcds].[vw_GCDS_Products_ETL] AS SELECT  
				isnull(Cast([GCID] as int), -1) as [GlobalClientID]

	  			, cast([Booking_location] as nvarchar(255)) as [BookingLocation]
	  			, cast([BusinessUnit] as nvarchar(255)) as [BusinessUnit]
	  			, cast([OTC_ProductFlag] as nvarchar(1)) as [OTCProductFlag]
	  			, cast([Product_location] as nvarchar(255)) as [ProductLocation]
	  			, cast([Status] as nvarchar(20)) as [Status]
	  			, cast([TypeCode] as nvarchar(255)) as [TypeCode]
	  			, cast([TypeDescription] as nvarchar(255)) as [TypeDescription]
	  			, cast([UsedByChildOnly] as nvarchar(1)) as [UsedByChildOnly]

			, cast([EDL_LOAD_DTS] as [datetime2]) as [EDL_LOAD_DTS]
			, cast([EDL_ACT_DTS] as [datetime2]) as [EDL_ACT_DTS]
			, convert(date, [IF_LoadDate], 23) as [IF_LoadDate]
			FROM [gcds].[GCDS_client_Products];"


-- 