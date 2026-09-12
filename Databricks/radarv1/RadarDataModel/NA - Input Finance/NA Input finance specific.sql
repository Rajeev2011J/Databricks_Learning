-- PALERMO
"CREATE VIEW [GEN].[vw_Palermo_Client_Src] AS SELECT 
	/*
PLEASE SAVE THIS QUERY AS FILE IN 'FEC W&R MI - General\3. MI Framework\2. Datamart\SQL'


Created by Ravi Puli

Log:
2024-04-10 created Ravi Puli
Modified on 2024-05-06 Ravi Puli : Updated GlobalBusinessline and GCO locations
2024-05-07 BvdM adjusted GlobalClientid logic; RUCA uses GCIDs, NYBR also inactive keys (>inactive during 2024)

*/

CAST(PAL.[Customer ID ] AS NVARCHAR(25)) AS LocalSystemId,
	/** Incase client is  NP, masking the [Customer Name] value and updating as Client ID **/
	CAST([Customer Name] AS NVARCHAR(100)) AS ClientName,
	/** Incase NA client is not mapped in GCDS, then [GlobalClientID] is replaced with Local Client Id **/
	CASE 
		WHEN [Branch no] ='RUCA' --RUCA uses GCIDs as clientids, not CIF
			THEN CAST(TRY_CAST(PAL.[Customer ID ] as int) AS NVARCHAR(25))
		WHEN KEYSTORE.[GlobalClientID] IS NOT NULL
			THEN CAST(KEYSTORE.[GlobalClientID] AS NVARCHAR(25))
		WHEN KEYSTORE2.[GlobalClientID] IS NOT NULL
			THEN CAST(KEYSTORE2.[GlobalClientID] AS NVARCHAR(25))
		ELSE CAST(CONCAT (
					'PALEREMO:',
					PAL.[Customer ID ]
					) AS NVARCHAR(25))
		END AS [GlobalClientID],
	CAST(KEYSTORE.[KeyType] AS NVARCHAR(100)) AS SourceSystem,
	--CASE 
	---WHEN GCDS_GCO.[Name] IS NOT NULL and GCDS_GCO.[Name] <>'UNKOWN_USER'
	--THEN CAST(GCDS_GCO.[Name] AS NVARCHAR(255))
	--ELSE CAST(NULL AS NVARCHAR(255))
	--END 
	CAST(CASE WHEN GCDS_GCO.[Name] IS NOT NULL  and GCDS_GCO.[Name]<>'UNKOWN_USER'
	THEN  GCDS_GCO.[Name]  ELSE NULL END AS NVARCHAR(255)) AS [GlobalClientOwnerName],
	CAST(CASE 
		WHEN [Branch no] ='RIL'  THEN 'Canada AGVendor'
		WHEN [Branch no] ='RAF-NLS'  THEN 'RAF Input Finance'
		WHEN [Branch no] ='RAF'  THEN 'RAF Direct Lending'
		WHEN [Branch no] ='RCBR'  THEN 'Rabobank Canada '
		WHEN [Branch no] ='RUCA'  THEN 'Rural Canada'
		WHEN [Branch no] ='NYBR'  THEN 'Rabobank New York' ELSE NULL END
			 AS NVARCHAR(264)) AS [GlobalClientOwnerLocation],
	CAST(CASE 
		WHEN [Branch no] ='RIL'  THEN 'CA'
		WHEN [Branch no] ='RAF-NLS'  THEN 'US'
		WHEN [Branch no] ='RAF'  THEN 'US'
		WHEN [Branch no] ='RCBR'  THEN 'CA'
		WHEN [Branch no] ='RUCA'  THEN 'CA'
		WHEN [Branch no] ='NYBR'  THEN 'US' ELSE NULL
			END AS NVARCHAR(25))  AS GCOLocationCountryCode,
	CAST(NULL AS NVARCHAR(255)) AS [ClientOwnerBusinessLine],
	CASE 
		WHEN GCDS_UP.[RelationshipValue] IS NULL
			THEN PAL.[Ultimate parent ID]
		ELSE GCDS_UP.[RelationshipValue]
		END AS UltimateParentGCID,
	CAST(Gcds_org.[LegalForm] AS NVARCHAR(255)) AS [LegalForm],
	/** Deriving the GlobalBusinessLine logic **/
		CAST(CASE 
		WHEN [Branch no] ='RIL'  THEN 'Lending'
		WHEN [Branch no] ='RAF_NLS'  THEN 'Lending'
		WHEN [Branch no] ='RAF'  THEN 'Global Rural Banking'
		WHEN [Branch no] ='RCBR'  THEN 'Wholesale Corporate'
		WHEN [Branch no] ='RUCA'  THEN 'Global Rural Banking'
		WHEN [Branch no] ='NYBR'  THEN 'Wholesale Corporate' ELSE NULL
			END AS NVARCHAR(255)) AS [GlobalBusinessLine],
	CASE 
	WHEN UPPER([Customer Name]) like '%RABOBANK%' THEN 'Y'
		WHEN GcdsClient.[RabobankEntityIndicator] IS NOT NULL
			THEN CAST(GcdsClient.[RabobankEntityIndicator] AS NVARCHAR(5))
		ELSE CAST('N' AS NVARCHAR(5))
		END AS [Rabobank_entity],
	CAST(CASE 
		WHEN [Branch no] ='RIL'  THEN 'Lending'
		WHEN [Branch no] ='RAF_NLS'  THEN 'Lending'
		WHEN [Branch no] ='RAF'  THEN 'Rural'
		WHEN [Branch no] ='RCBR'  THEN 'Wholesale Corporate'
		WHEN [Branch no] ='RUCA'  THEN 'Rural'
		WHEN [Branch no] ='NYBR'  THEN 'Wholesale Corporate' ELSE NULL
			END AS NVARCHAR(255)) AS [ClientSegment],
	CAST([Primary NAICS value] AS NVARCHAR(255)) AS [IndustryCode],
	CAST(NULL AS DECIMAL(5, 2)) AS [NAICSPercentage],
	CAST([Primary NAICS description] AS NVARCHAR(255)) AS [IndustryCodeType],
	CASE 
		--WHEN GcdsClient.[PartyType] IS NOT NULL
		--	THEN GcdsClient.[PartyType]
			WHEN [Customer type ID]='P' THEN 'Natural Person'
		ELSE CAST('Legal Entity' AS NVARCHAR(50))
		END AS [PartyType],
	CASE 
		WHEN FI.FiHubIndicator = '1'
			THEN 'Y'
		ELSE 'N'
		END AS IS_FI,
	CASE 
		WHEN GcdsPartyRole.[PartyLifeCycleStatus] IS NOT NULL
			THEN CAST(GcdsPartyRole.[PartyLifeCycleStatus] AS NVARCHAR(50))
		ELSE CAST('Client' AS NVARCHAR(50))
		END AS [PartyLifeCycleStatus],
	CASE 
		WHEN [Customer status] = 'Active'
			AND CAST(GcdsPartyRole.[PartyLifeCycleStatus] AS NVARCHAR(50)) = 'Client'
			THEN CAST('Y' AS VARCHAR(1))
		ELSE CAST('N' AS VARCHAR(1))
		END AS [isCurrentClient],
	CAST(ONBOARDLOC.[Onboarding_date] AS DATETIME2) AS [Onboarding_date],
	CAST(ONBOARDLOC.[Offboarding_date] AS DATETIME2) AS [Offboarding_date],
	CAST([Customer Country] AS NVARCHAR(255)) AS [CountryOfRegistration],
	CAST([Customer Country] AS NVARCHAR(2)) AS [CountryCodeOfRegistration],
	CAST('North America' AS NVARCHAR(50)) AS [GlobalReportingRegion],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionAsia],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionE_A],
	CAST('Y' AS NVARCHAR(1)) AS [InvolvedRegionNA],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionSA],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionRANZ],
	/** Deriving the [SIRA_Exclude] logic **/
	CAST(CASE 
			WHEN AetosDpt.Level04Name = 'DLL (GL)'
				THEN 'DLL'
			WHEN GcdsClient.[RabobankEntityIndicator] = 'Y'
				THEN 'Rabobank Entity'
			WHEN GcdsClient.[PartyType] <> 'Natural Person'
				AND GcdsClient.[PartyType] <> 'Legal Entity'
				THEN GcdsClient.[PartyType]
			WHEN (
					GcdsPartyRole.[PartyLifeCycleStatus] IN (
						'Prospect',
						'Former Client',
						'Inactive'
						)
					AND (
						ONBOARDLOC.Offboarding_date < '2023-12-31'
						OR ONBOARDLOC.Offboarding_date IS NULL
						)
					)
				OR (ONBOARDLOC.Onboarding_date >= '2023-12-31')
				THEN 'notaclient20231231'
			ELSE NULL
			END AS NVARCHAR(50)) AS [SIRA_Exclude],
	CAST(GcdsClient.[GlobalClientID] AS INT) AS [GCDSId],
	/** If NA ([CDD Risk Rating]) data is not availble then check and take it from Gcob **/
	CAST(NULL AS NVARCHAR(25)) as  [OverallCalculatedRisk],
	CAST(NULL AS DATETIME2) AS [NextReviewDate],
	NULL AS [GeoRisk],
	GCOBRISK.[EntityTypeRisk],
	GCOBRISK.[StructureRisk],
	GCOBRISK.[SectorRisk],
	GCOBRISK.[ProductRisk],
	GCOBRISK.[PEPRisk],
	GCOBRISK.[TransactionRisk],
	GCOBRISK.[DistributionRisk],
	GCOBRISK.[ThirdPartyRisk],
	GCOBRISK.[AdverseInfoRisk],
	GCOBRISK.[OtherRisk],
	unique_test = COUNT(PAL.[Customer ID ]) OVER (PARTITION BY PAL.[Customer ID ])
--Select  
--Distinct[Branch no]
FROM [RISKSHIELD].[PALERMO_CUSTOMERS_EOY2023] pal

LEFT JOIN (
	SELECT globalclientid,
		keyvalue,
		keytype
	FROM [gcds].[GCDS_Keystore_DWH] k
	WHERE keytype = 'CIF'
		AND record_end IS NULL
		AND (
			[status] IS NULL
			OR [status] = 'active'
			)
	) KEYSTORE
	ON KEYSTORE.keyvalue = cast(cast(pal.[Customer ID] AS INT) AS NVARCHAR(63))

LEFT JOIN (
	SELECT globalclientid,
		keyvalue,
		keytype
	FROM [gcds].[GCDS_Keystore_DWH] k
	WHERE keytype = 'CIF'
		AND record_end IS NULL
		--AND (
		--	[status] IS NULL
		--	OR [status] = 'active'
		--	)
	) KEYSTORE2
	ON KEYSTORE2.keyvalue = cast(cast(pal.[Customer ID] AS INT) AS NVARCHAR(63))

LEFT JOIN
	---Select DISTINCT PartyType from
	gcds.GCDS_client_DWH GcdsClient
	ON GcdsClient.[GlobalClientID] =  CASE 
										WHEN [Branch no] ='RUCA' --RUCA uses GCIDs as clientids, not CIF
											THEN CAST(TRY_CAST(PAL.[Customer ID ] as int) AS NVARCHAR(25))
										WHEN KEYSTORE.[GlobalClientID] IS NOT NULL
											THEN CAST(KEYSTORE.[GlobalClientID] AS NVARCHAR(25))
										WHEN KEYSTORE2.[GlobalClientID] IS NOT NULL
											THEN CAST(KEYSTORE2.[GlobalClientID] AS NVARCHAR(25))
									END
LEFT JOIN
	/** Check and take GCOB Risk data from GCOB view if NA Risk data is not availble **/
	(
	SELECT 'GCOB' AS SourceSystem,
		[SourceSystemId] AS LocalSystemId,
		CAST([GCDSId] AS INT) [GCDSId],
		[OverallCalculatedRisk],
		[NextReviewDate],
		[GeoRisk],
		[EntityTypeRisk],
		[StructureRisk],
		[SectorRisk],
		[ProductRisk],
		[PEPRisk],
		[TransactionRisk],
		[DistributionRisk],
		[ThirdPartyRisk],
		[AdverseInfoRisk],
		[OtherRisk]
	FROM [GEN].[vw_GcobLERiskData_Src]
	WHERE RANK = 1
	) GCOBRISK
	ON GcdsClient.[GlobalClientID] = GCOBRISK.[GCDSId]
LEFT JOIN (
	SELECT [GlobalClientID],
		[RelationshipValue]
	FROM [gcds].[GCDS_P2P_Recursive_DWH]
	WHERE record_end IS NULL
		AND [RelationshipType] = 'Credit'
		AND [FinalLevel] = 1
	) Gcds_UP
	ON GcdsClient.[GlobalClientID] = Gcds_UP.[GlobalClientID]
LEFT JOIN (
	SELECT [GlobalClientID],
		[CDDEntityTypeCode]
	FROM [gcds].[GCDS_CDDRiskAssessment_DWH]
	WHERE Record_end IS NULL
	) GCDS_CDD
	ON GcdsClient.[GlobalClientID] = GCDS_CDD.[GlobalClientID]
LEFT JOIN (
	SELECT [GlobalClientID],
		[NAICSCode],
		CAST([NAICSDescription] AS NVARCHAR(255)) AS [NAICSDescription],
		[NAICSPercentage]
	FROM [gcds].[GCDS_Naics_DWH]
	WHERE Record_End IS NULL
		AND [NAICSType] = 'Primary'
	) GCDS_na
	ON GcdsClient.[GlobalClientID] = GCDS_na.[GlobalClientID]
LEFT JOIN (
	SELECT [GlobalClientID],
		[FullLegalName],
		[LegalForm]
	FROM [gcds].[GCDS_Organisation_DWH]
	WHERE record_end IS NULL
	) GCDS_org
	ON GcdsClient.[GlobalClientID] = GCDS_org.[GlobalClientID]
LEFT JOIN (
	SELECT [GlobalClientID],
		[Sector],
		[SectorCode]
	FROM [gcds].[GCDS_Commercial_DWH]
	WHERE Record_end IS NULL
	) GCDS_COMM
	ON GcdsClient.[GlobalClientID] = GCDS_COMM.[GlobalClientID]
LEFT JOIN (
	SELECT [GlobalClientID],
		[PartyLifeCycleStatus]
	FROM [gcds].[GCDS_PartyRole_DWH]
	WHERE record_end IS NULL
		AND PartyRoleType = 'Customer'
	) GcdsPartyRole
	ON GcdsClient.[GlobalClientID] = GcdsPartyRole.[GlobalClientID]
LEFT JOIN
	/** Take Onboarding and OffBoarding data from GCOB incase NA don't have **/
	(
	SELECT [GlobalClientID],
		MAX([OnboardingDate]) AS Onboarding_date,
		MIN([OffboardingDate]) AS Offboarding_date
	FROM [gcds].[GCDS_OnboardedLocations_DWH]
	WHERE Record_End IS NULL
	GROUP BY [GlobalClientID]
	) ONBOARDLOC
	ON ONBOARDLOC.[GlobalClientID] = GcdsClient.[GlobalClientID]
LEFT JOIN (
	SELECT [GlobalClientID],
		[Location],
		[BusinessLineDescription],
		[Name],
		[record_end]
	FROM [gcds].[GCDS_Manager_DWH]
	WHERE record_end IS NULL
		AND [ManagersType] = 'GlobalClientOwner'
		--and [GlobalClientID]='36451'
	) GCDS_GCO
	ON GcdsClient.[GlobalClientID] = GCDS_GCO.[GlobalClientID]
LEFT JOIN (
	SELECT [GlobalClientID],
		FiHubIndicator
	FROM (
		SELECT FiHubIndicator,
			LECase.GcobId
		FROM (
			SELECT LECase.GcobId,
				MAX(LECase.LegalEntityClientId) AS [LatestId]
			FROM radar.GCOB_LE_Cases LECase
			WHERE LECase.CurrentStatus <> 'Cancelled'
			GROUP BY LECase.GcobId
			) AS LECase
		LEFT JOIN (
			SELECT FiHubIndicator,
				id
			FROM radar.GCOB_LE_Cases
			) AS t4
			ON LECase.LatestId = t4.id
		) GCOBFI
	LEFT JOIN (
		SELECT [GlobalClientID],
			[KeyType],
			GCOBID
		FROM (
			SELECT [GlobalClientID],
				[KeyType],
				KeyValue AS GCOBID,
				ROW_NUMBER() OVER (
					PARTITION BY [GlobalClientID] ORDER BY [GlobalClientID],
						[KeyType],
						[Status]
					) AS 'RANK'
			FROM [gcds].[GCDS_Keystore_DWH]
			WHERE [KeyType] IN ('GCOBID')
				AND (
					STATUS = 'Active'
					OR STATUS IS NULL
					)
				AND record_end IS NULL
			) TAB
		WHERE RANK = 1
		) KeyStore
		ON GCOBFI.[GcobId] = KeyStore.[GCOBID]
	) FI
	ON FI.[GlobalClientID] = GcdsClient.[GlobalClientID]
LEFT JOIN (
	SELECT Cast([Region Name] AS NVARCHAR(50)) [Region Name],
		CAST([Name] AS NVARCHAR(100)) [Name],
		Cast([CountryISO - Country Code] AS NVARCHAR(7)) AS CountryCode
	FROM [GEN].[Aetos_Location_CFO]
	) AetosLoc
	ON AetosLoc.[Name] = GCDS_GCO.[Location]
LEFT JOIN (
	SELECT [GlobalClientID],
		Country,
		CountryCode
	FROM [gcds].GCDS_Address_DWH
	WHERE record_end IS NULL
		AND AdressType = 'Registered'
	) GcdsAdds
	ON GcdsClient.[GlobalClientID] = GcdsAdds.[GlobalClientID]
LEFT JOIN (
	SELECT CAST(Level03Name AS NVARCHAR(255)) AS Level03Name,
		CAST(Level04Name AS NVARCHAR(255)) AS Level04Name,
		CAST(Level06Name AS NVARCHAR(255)) AS Level06Name,
		CAST(Name AS NVARCHAR(255)) AS [Name]
	FROM [GEN].[Aetos_Department_CFO]
	) AetosDpt
	ON GCDS_GCO.[BusinessLineDescription] = AetosDpt.Name
LEFT JOIN (
	SELECT cast([Code] AS NVARCHAR(255)) AS [Code],
		cast([Name] AS NVARCHAR(255)) AS [Name],
		cast([Group Head] AS NVARCHAR(255)) AS [Group Head]
	FROM [GEN].[Aetos_Client_Owning_Departments_CPO_DC]
	WHERE Active = 'True'
	) AetosClientODP
	ON GCDS_GCO.[BusinessLineDescription] = AetosClientODP.[Name]
WHERE GcdsClient.record_end IS NULL;"



-- NLSvf (vendor finance from GDP)