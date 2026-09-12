-- KN1 query?

"CREATE VIEW [GEN].[vw_KN1_ID2023_Src] AS select 
	copa.id_contraparte as KN1_id_2023
	--, null as CPF
	from [KN1].[ADKYC_CONTRAPARTES] copa
		LEFT JOIN [KN1].[ADKYC_CONTRAPARTES_DESATIVADOS] CPDE (NOLOCK) 
		ON COPA.NR_CPF_CNPJ = CPDE.NR_CPF_CNPJ
			AND COPA.DOC_ESTRANGEIRO = CPDE.DOC_ESTRANGEIRO
			AND COPA.ID_GRP_CONTRAPARTE = CPDE.ID_GRP_CONTRAPARTE
			AND CPDE.ID_CONTRAPARTE_DESATIVADO IN
				(SELECT MAX(ID_CONTRAPARTE_DESATIVADO)
				FROM [KN1].[ADKYC_CONTRAPARTES_DESATIVADOS] (NOLOCK)
				WHERE NR_CPF_CNPJ = COPA.NR_CPF_CNPJ
					AND DOC_ESTRANGEIRO = COPA.DOC_ESTRANGEIRO
					AND ID_GRP_CONTRAPARTE = COPA.ID_GRP_CONTRAPARTE)
	 where copa.ID_GRP_CONTRAPARTE not in (4,6)
	  AND COPA.ID_CONTRAPARTE_RENOVACAO = -1 -- Additional row that got added after communication with KN1 team
	  AND (
	  CASE
		  WHEN CPDE.DT_DESATIVACAO <> '19000101'
			   AND CPDE.DT_REATIVACAO = '19000101'
			   AND YEAR(CPDE.DT_DESATIVACAO) <> 2023 THEN 1
		  ELSE 0
	  END) = 0
	  AND copa.CD_SITUACAO =1
	  AND YEAR(COPA.DTHR_FIM) = 2023

union

select 
	max(copa.id_contraparte) as KN1_id_2023
	--, COPA.NR_CPF_CNPJ as CPF
	from [KN1].[ADKYC_CONTRAPARTES] copa
		LEFT JOIN [KN1].[ADKYC_CONTRAPARTES_DESATIVADOS] CPDE (NOLOCK) 
		ON COPA.NR_CPF_CNPJ = CPDE.NR_CPF_CNPJ
			AND COPA.DOC_ESTRANGEIRO = CPDE.DOC_ESTRANGEIRO
			AND COPA.ID_GRP_CONTRAPARTE = CPDE.ID_GRP_CONTRAPARTE
			AND CPDE.ID_CONTRAPARTE_DESATIVADO IN
				(SELECT MAX(ID_CONTRAPARTE_DESATIVADO)
				FROM [KN1].[ADKYC_CONTRAPARTES_DESATIVADOS] (NOLOCK)
				WHERE NR_CPF_CNPJ = COPA.NR_CPF_CNPJ
					AND DOC_ESTRANGEIRO = COPA.DOC_ESTRANGEIRO
					AND ID_GRP_CONTRAPARTE = COPA.ID_GRP_CONTRAPARTE)
	where copa.ID_GRP_CONTRAPARTE not in (4,6)
	  AND COPA.ID_CONTRAPARTE_RENOVACAO = -1 -- Additional row that got added after communication with KN1 team
	  AND (
	  CASE
		  WHEN CPDE.DT_DESATIVACAO <> '19000101'
			   AND CPDE.DT_REATIVACAO = '19000101'
			   AND YEAR(CPDE.DT_DESATIVACAO) <> 2023 THEN 1
		  ELSE 0
	  END) = 0
	  AND copa.CD_SITUACAO =1
	  AND YEAR(COPA.DTHR_FIM) < 2023
	GROUP BY COPA.NR_CPF_CNPJ;"


-- GIC client
"CREATE VIEW [GEN].[vw_GIC_Client_Src]
AS SELECT 
/*
PLEASE SAVE THIS QUERY AS FILE IN 'FEC W&R MI - General\3. MI Framework\2. Datamart\SQL'


Created by Ravi Puli

Log:
2024-04-20 created Ravi Puli

*/


CAST(Person.[COD_INSTITUCIONAL] AS NVARCHAR(25)) AS LocalSystemId,

	/** Incase client is  NP, masking the [Customer Name] value and updating as Client ID **/
	CASE 
		WHEN [SEQ_TIPO_PESSOA] = '2'
			THEN CONCAT (
					'Natural Person',
					Person.[COD_INSTITUCIONAL]
					)
		WHEN [SEQ_TIPO_PESSOA] <> '2'
			THEN Person.[NOM_COMPLETO]
		END AS ClientName,
		
		/** Incase SA client is not mapped in GCDS, then [GlobalClientID] is replaced with Local Client Id **/
	CASE 
		WHEN KEYSTORE.[GlobalClientID] IS NOT NULL
			THEN CAST(KEYSTORE.[GlobalClientID] AS NVARCHAR(25))
		ELSE CAST(CONCAT (
					'GIC:',
					Person.[COD_INSTITUCIONAL]
					) AS NVARCHAR(25))
		END AS [GlobalClientID],
	CAST('GIC' AS NVARCHAR(100)) AS SourceSystem,
	CASE 
		WHEN GCDS_GCO.[Name] IS NOT NULL and GCDS_GCO.[Name] <>'UNKOWN_USER'
			THEN CAST(GCDS_GCO.[Name] AS NVARCHAR(255))
		ELSE CAST([NOM_RM] AS NVARCHAR(255))
		END AS [GlobalClientOwnerName],
	CASE 
		WHEN GCDS_GCO.[Location] IS NOT NULL
			THEN CONCAT (
					'Rabobank ',
					GCDS_GCO.[Location]
					)
		ELSE CAST('Rabobank Brazil' AS NVARCHAR(264))
		END AS [GlobalClientOwnerLocation],
	CASE 
		WHEN AetosLoc.CountryCode IS NOT NULL
			THEN AetosLoc.CountryCode
		ELSE CAST('BR' AS NVARCHAR(7))
		END AS GCOLocationCountryCode,
	CASE 
		WHEN GCDS_GCO.[BusinessLineDescription] IS NOT NULL
			THEN CAST(GCDS_GCO.[BusinessLineDescription] AS NVARCHAR(255))
		ELSE CAST([DES_LINHA_NEGOCIO] AS NVARCHAR(255))
		END AS [ClientOwnerBusinessLine],
	CAST(CASE 
			WHEN GCDS_UP.[RelationshipValue] IS NULL
				THEN GcdsClient.[GlobalClientID]
			ELSE GCDS_UP.[RelationshipValue]
			END AS NVARCHAR(25)) AS UltimateParentGCID,
	CAST(Gcds_org.[LegalForm] AS NVARCHAR(255)) AS [LegalForm],
	/** Deriving the GlobalBusinessLine logic **/
	CAST(CASE 
			WHEN [DES_LINHA_NEGOCIO] = 'RURAL'
				THEN 'Global Rural Banking'
			WHEN [DES_LINHA_NEGOCIO] = 'GFM'
				THEN 'Markets'
			WHEN [DES_LINHA_NEGOCIO] = 'CORPORATE'
				THEN 'Wholesale Corporate'
			WHEN FI.FiHubIndicator = '1'
				AND GCDS_GCO.[BusinessLineDescription] = 'PSP Coverage' --GCDS_COMM.Sector='PSP' 
				THEN 'PSP Coverage'
			WHEN AetosClientODP.[Group Head] IS NOT NULL
				THEN CASE 
						WHEN AetosDpt.Level03Name = 'Trade & Commodity Finance (GD)'
							THEN 'TCF'
						WHEN AetosDpt.Level04Name = 'Corporate Finance Origination (GL)'
							THEN 'CFO'
						WHEN AetosClientODP.[Group Head] = 'SAM'
							THEN 'Core Lending'
						ELSE AetosClientODP.[Group Head]
						END
			WHEN AetosDpt.Level06Name = 'Wholesale'
				THEN CASE 
						WHEN FI.FiHubIndicator = '1'
							AND GCDS_GCO.[BusinessLineDescription] = 'PSP Coverage' --GCDS_COMM.Sector='PSP' 
							THEN 'Wholesale Corporate'
						WHEN FI.FiHubIndicator = '1'
							AND GCDS_GCO.[BusinessLineDescription] != 'PSP Coverage' --GCDS_COMM.Sector IS NULL
							THEN 'Wholesale FI'
						ELSE 'Wholesale Corporate'
						END
			ELSE AetosDpt.Level06Name
			END AS NVARCHAR(255)) AS [GlobalBusinessLine],
	CASE  WHEN UPPER([NOM_RM]) LIKE '%RABOBANK%'  THEN 'Y'
		WHEN GcdsClient.[RabobankEntityIndicator] IS NOT NULL
			THEN CAST(GcdsClient.[RabobankEntityIndicator] AS NVARCHAR(5))
		ELSE CAST('N' AS NVARCHAR(5))
		END AS [Rabobank_entity],
		/** Deriving the Client Segment logic **/
	CAST(CASE 
			WHEN FI.FiHubIndicator = '1'
				AND GCDS_GCO.[BusinessLineDescription] = 'PSP Coverage' -- GCDS_COMM.Sector='PSP' 
				THEN 'Wholesale Corporate'
			WHEN  GCDS_GCO.[BusinessLineDescription] ='FI Relationship Management'  
			    THEN 'Wholesale FI'
			WHEN FI.FiHubIndicator = '1'
				AND (
					GCDS_GCO.[BusinessLineDescription] != 'PSP Coverage'
					OR GCDS_GCO.[BusinessLineDescription] IS NULL
					) -- GCDS_COMM.Sector IS NULL
				THEN 'Wholesale FI'
			WHEN [DES_LINHA_NEGOCIO] = 'RURAL'
				THEN 'Rural'
			--WHEN [DES_LINHA_NEGOCIO] = 'GFM'
			--	THEN 'Markets'
			WHEN [DES_LINHA_NEGOCIO] = 'CORPORATE'
				THEN 'Wholesale Corporate'
			WHEN AetosDpt.Level06Name IS NOT NULL
				THEN CASE 
						WHEN AetosDpt.Level06Name = 'Wholesale'
							THEN 'Wholesale Corporate'
						WHEN AetosDpt.Level06Name = 'Treasury'
							THEN 'Wholesale FI'
						ELSE AetosDpt.Level06Name
						END
			WHEN AetosDpt.Level06Name = 'Wholesale'
				THEN 'Wholesale Corporate'
			WHEN [DES_LINHA_NEGOCIO] = 'RURAL'
				THEN 'Rural'
			ELSE AetosDpt.Level06Name
			END AS NVARCHAR(255)) AS [ClientSegment],
	CAST(GCDS_na.[NAICSCode] AS NVARCHAR(255)) AS IndustryCode,
	GCDS_na.[NAICSPercentage] AS [NAICSPercentage],
	CAST(GCDS_na.[NAICSDescription] AS NVARCHAR(255)) AS IndustryCodeType,
	CASE 
		WHEN GcdsClient.[PartyType] IS NOT NULL
			THEN GcdsClient.[PartyType]
		ELSE CAST(CASE 
					WHEN [SEQ_TIPO_PESSOA] = '1'
						THEN 'Legal Entity'
					WHEN [SEQ_TIPO_PESSOA] = '2'
						THEN 'Natural Person'
					ELSE NULL
					END AS NVARCHAR(50))
		END AS [PartyType],
	CASE 
		WHEN FI.FiHubIndicator = '1'
			THEN 'Y'
		ELSE 'N'
		END AS IS_FI,
	CAST(CASE 
			WHEN GcdsPartyRole.[PartyLifeCycleStatus] IS NOT NULL
				THEN GcdsPartyRole.[PartyLifeCycleStatus]
				WHEN GcdsPartyRole.[PartyLifeCycleStatus] ='Cliente'
				THEN 'Client'
			ELSE Relation.DES_TIPO_CADASTRO
			END AS NVARCHAR(50)) AS [PartyLifeCycleStatus],
	CAST(CASE 
			WHEN Relation.[DES_STATUS_TIPO_CADASTRO] = 'Ativo'
				THEN 'Y'
			WHEN GcdsPartyRole.[PartyLifeCycleStatus] = 'Client'
				THEN 'Y'
			ELSE 'N'
			END AS VARCHAR(1)) AS [isCurrentClient],
	ONBOARDLOC.[Onboarding_date] AS [Onboarding_date],
	ONBOARDLOC.[Offboarding_date] AS [Offboarding_date],

	-- if country not filled in gcds is added by GIC
	CAST(CASE 
		WHEN GcdsAdds.Country is not null
			THEN GcdsAdds.Country 
		ELSE EBXCountryCodes.[SHORT_NAME_EN]
		END AS  NVARCHAR(255))  AS [CountryOfRegistration],
	--CAST(GcdsAdds.Country AS NVARCHAR(255)) AS [CountryOfRegistration],
	-- if country not filled in gcds is added by GIC
	CASE 
		WHEN GcdsAdds.CountryCode is not null
			THEN CAST(GcdsAdds.CountryCode AS NVARCHAR(2)) 
		ELSE BRCountryList.[COD_COUNTRY_RISK_MTGT]
		END AS [CountryCodeOfRegistration],
	--CAST(GcdsAdds.CountryCode AS NVARCHAR(2)) AS [CountryCodeOfRegistration],



	CAST('South America' AS NVARCHAR(50)) AS [GlobalReportingRegion],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionAsia],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionE_A],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionNA],
	CAST('Y' AS NVARCHAR(1)) AS [InvolvedRegionSA],
	CAST('N' AS NVARCHAR(1)) AS [InvolvedRegionRANZ],
		/** Deriving the [SIRA_Exclude] logic **/ 
	CASE 
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
		END AS [SIRA_Exclude],
		CAST(CASE 
               WHEN SEC.NAICS IS NOT NULL   THEN SEC.SancationCircumventionReason 
		       ELSE NULL END AS NVARCHAR(2)) AS  SanctionsCircumvention,
	CAST(GcdsClient.[GlobalClientID] AS INT) AS [GCDSId],
 [OverallCalculatedRisk],
[OverallCalculatedRiskDiscription],
[OverallRecalculatedRisk],
[OverallRecalculatedRiskDiscription],
[OverallValidatedRisk],
[OverallValidatedRiskDiscription],
	--CAST([OverallCalculatedRisk] as NVARCHAR(25)) AS [OverallCalculatedRisk],
	CAST([NextReviewDate] AS DATETIME2) AS [NextReviewDate],
	[GeoRisk],
	[GeoRiskDiscription],
	[EntityTypeRisk],
	[EntityTypeRiskDiscription],
	[StructureRisk],
	[StructureRiskDiscription],
	[SectorRisk],
	[SectorRiskDiscription],
	[ProductRisk],
	[ProductRiskDiscription],
	[PEPRisk],
	CAST([PEPRiskDiscription] AS NVARCHAR(25)) AS [PEPRiskDiscription],
	[TransactionRisk],
	[TransactionRiskDiscription],
	[DistributionRisk],
	[DiscriptionDiscription],
	[ThirdPartyRisk],
	[ThirdPartyRiskDiscription],
	CAST(NULL AS INT) AS [AdverseInfoRisk],[AdverseInfoRiskDiscription],
	[OtherRisk],[OtherRiskDiscription],
	unique_test = COUNT(Person.[COD_INSTITUCIONAL]) OVER (PARTITION BY Person.[COD_INSTITUCIONAL])
FROM [GIC].[GIC_RDL_PESSOA] Person
LEFT JOIN
/** Join the SA data with GCDS to retrive Uniq Globalclient ID **/(
	SELECT [GlobalClientID],
		[KeyType],
		KeyValue
	FROM (
		SELECT [GlobalClientID],
			[KeyType],
			KeyValue,
			ROW_NUMBER() OVER (
				PARTITION BY [GlobalClientID] ORDER BY [GlobalClientID],
					[KeyType],
					[Status]
				) AS 'RANK'
		FROM [gcds].[GCDS_Keystore_DWH]
		WHERE [KeyType] IN ('GIC')
			AND (
				STATUS = 'Active'
				OR STATUS IS NULL
				)
			AND record_end IS NULL
		) TAB
	WHERE RANK = 1
	) KeyStore
	ON KeyStore.KeyValue = Person.[COD_INSTITUCIONAL]
-- Join to get country from GIC
LEFT JOIN (SELECT [COD_INSTITUCIONAL], [SEQ_PAIS] FROM [GIC].[GIC_RDL_PESSOA_ENDERECOS]
WHERE [FLG_CORRESPONDENCIA] = 'Sim') Adres
	ON Person.[COD_INSTITUCIONAL] = Adres.[COD_INSTITUCIONAL]
LEFT JOIN [GIC].[CAD_PAIS] BRCountryList
	ON Adres.[SEQ_PAIS] = BRCountryList.[SEQ_PAIS]
LEFT JOIN [ebx].[EBX_Countries_ISO3166COUNTRYCODES] EBXCountryCodes
	ON BRCountryList.[COD_COUNTRY_RISK_MTGT] = EBXCountryCodes.[ALPHA_2_CODE]
-- End of joins for Country from GIC
LEFT JOIN [GIC].[GIC_RDL_PESSOA_LINHA_NEGOCIO] ORG
	ON Person.[COD_INSTITUCIONAL] = ORG.[COD_INSTITUCIONAL]
LEFT JOIN [GIC].[GIC_RDL_PESSOA_JURIDICA_IDENTIFICACAO] OrgName
	ON Person.[COD_INSTITUCIONAL] = OrgName.[COD_INSTITUCIONAL]
LEFT JOIN [GIC].[GIC_RDL_PESSOA_TIPO_CADASTRO] Relation
	ON Relation.[COD_INSTITUCIONAL] = Person.[COD_INSTITUCIONAL]
LEFT JOIN gcds.GCDS_client_DWH GcdsClient
	ON KeyStore.[GlobalClientID] = GcdsClient.[GlobalClientID]
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
LEFT OUTER JOIN
  [GEN].[vw_SanctionsCircumvention_Src] SEC  ON 
     SEC.NAICS=GCDS_na.[NAICSCode]

LEFT JOIN (
	SELECT [GlobalClientID],
		[FullLegalName],
		[LegalForm]
	FROM [gcds].[GCDS_Organisation_DWH]
	WHERE record_end IS NULL
	) GCDS_org
	ON GcdsClient.[GlobalClientID] = GCDS_org.[GlobalClientID] -- and GCDS_cl.record_end IS NULL
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
		--and [GlobalClientID]='497484'
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
LEFT JOIN [GEN].[vw_KN1RiskData_Src] RISK
	ON RISK.[LocalSystemId] = Person.[COD_INSTITUCIONAL]
WHERE Relation.DES_TIPO_CADASTRO = 'Cliente'
	AND Relation.[DES_STATUS_TIPO_CADASTRO] = 'Ativo'
	AND GcdsClient.record_end IS NULL;"


-- KN1 AllRisks
"CREATE VIEW [GEN].[vw_KN1AllRisks_Src] AS SELECT
     CAST('GIC' AS NVARCHAR(25)) AS LocalSystem
	,CAST(GICPerson.[COD_INSTITUCIONAL] AS NVARCHAR(25)) AS [LocalSystemId]
	,CAST(KEYSTORE.[GlobalClientID] AS NVARCHAR(25)) AS [GlobalClientID]
	,CAST(KN1Case.[CaseId_KN1] AS NVARCHAR(25)) AS [CaseId] --KN1 id
	,CAST(KN1Case.[AdverseInfo] AS NVARCHAR(255)) AS [AdverseInfo]
	,CAST(KN1Case.[ComplexStructure] AS NVARCHAR(255)) AS [ComplexStructure]
	,CAST(GICEntityType.[DES_TIPO_SOCIEDADE] AS NVARCHAR(255)) AS [EntityType]
	,CAST(KN1Case.[FaceToFaceContact] AS NVARCHAR(255)) AS [FaceToFaceContact]
	,CAST(KN1Case.[IssuesBearerShares] AS NVARCHAR(255)) AS [IssuesBearerShares]
	,CAST(KN1Case.[NomineeShareholders] AS NVARCHAR(255)) AS [NomineeShareholders]
	,CAST(null AS NVARCHAR(255)) AS [CddtypeName] -- Need to check this
	,CASE WHEN GICPerson.[SEQ_TIPO_PESSOA] = '2' THEN '1' ELSE '0' END AS[isNaturalPerson]
	,KN1Case.[IsTrust]
	,CAST(null AS NVARCHAR(25)) AS [IsClientRegulated] -- Need to check this
	,CAST(null AS NVARCHAR(25)) AS [IsClientListed] -- Need to check this
	,CAST(NULL AS NVARCHAR(25)) AS	CommonUBOinOrganizationalLayers
	,CAST(NULL AS NVARCHAR(255)) AS HoldsTpaAccounts
	,CAST(NULL AS NVARCHAR(25)) AS CDDPreviousRating
FROM ( SELECT [GlobalClientID],	[KeyType], KeyValue	FROM ( SELECT [GlobalClientID],[KeyType], KeyValue, ROW_NUMBER() OVER ( PARTITION BY [GlobalClientID] ORDER BY [GlobalClientID], [KeyType], [Status] ) AS 'RANK' FROM [gcds].[GCDS_Keystore_DWH] WHERE [KeyType] IN ('GIC')	AND ( STATUS = 'Active'	OR STATUS IS NULL) AND record_end IS NULL) TAB	WHERE RANK = 1) KEYSTORE --GCDS Select from view [vw_GIC_Client_Src]
INNER JOIN [GIC].[GIC_RDL_PESSOA] GICPerson -- Join GIC ID
	ON KEYSTORE.KeyValue = GICPerson.[COD_INSTITUCIONAL]
LEFT JOIN [GEN].[vw_KN1_Cases_Src] KN1Case -- Join KN1 Case Src
	ON GICPerson.[COD_INSTITUCIONAL] = KN1Case.[LocalSystemId_GIC]
LEFT JOIN [GIC].[GIC_RDL_PESSOA_JURIDICA_IDENTIFICACAO] GICOrgName -- Join to get entity type id
	ON GICPerson.[COD_INSTITUCIONAL] = GICOrgName.[COD_INSTITUCIONAL]
LEFT JOIN [GIC].[TIPO_SOCIEDADE] GICEntityType -- Join to get entity type name
	ON GICOrgName.[SEQ_TIPO_SOCIEDADE] = GICEntityType.[SEQ_TIPO_SOCIEDADE];"

-- GIC - GRAM Allrisks


-- KN1 raw?
"CREATE VIEW [GEN].[vw_KN1_raw]
AS Select  *

FROM (
--Create KN1
SELECT Distinct
	copa.CD_SITUACAO
	,COPA.ID_CONTRAPARTE 'ID_KN2'
    ,rtrim(CPCO.CD_CONTRAPARTE) 'CST02'
    ,COPA.NM_CONTRAPARTE 'CST03'
    , CASE WHEN (SELECT CPPC.ID_CONTRAPARTE FROM [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] CPPC (NOLOCK) LEFT JOIN [KN1].[ADRBB_QUESTOES] DOQU (NOLOCK) ON DOQU.ID_QUESTAO = CPPC.ID_QUESTAO WHERE DOQU.CD_QUESTAO = 'Q7' AND CPPC.CD_RESPOSTA = 'Sim' AND CPPC.ID_CONTRAPARTE = COPA.ID_CONTRAPARTE) IS NULL THEN 'N' ELSE 'Y' END 'CST09'
    ,CASE WHEN COPA.DT_RENOVACAO = '1900-01-01 00:00:00.000' THEN isnull(CONVERT(VARCHAR,CPAN.DT_RENOVACAO, 120), '') ELSE CONVERT(VARCHAR, COPA.DT_RENOVACAO, 120) END 'CST13'
    ,'N' 'RSI01'
    ,'N' 'RSI02'
    ,CASE WHEN (SELECT CPPC.ID_CONTRAPARTE FROM [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] CPPC WITH (NOLOCK) LEFT JOIN [KN1].[ADRBB_QUESTOES] DOQU WITH (NOLOCK) ON DOQU.ID_QUESTAO = CPPC.ID_QUESTAO WHERE CPPC.ID_CONTRAPARTE = COPA.ID_CONTRAPARTE AND DOQU.CD_QUESTAO = 'Q292' AND CPPC.CD_RESPOSTA = 'Sim') IS NULL THEN 'N' ELSE 'Y' END 'RSI03'
    ,CASE WHEN (SELECT CPPC.ID_CONTRAPARTE FROM [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] CPPC WITH (NOLOCK) LEFT JOIN [KN1].[ADRBB_QUESTOES] DOQU WITH (NOLOCK) ON DOQU.ID_QUESTAO = CPPC.ID_QUESTAO WHERE CPPC.ID_CONTRAPARTE = COPA.ID_CONTRAPARTE AND DOQU.CD_QUESTAO = 'Q5' AND CPPC.CD_RESPOSTA = '9') IS NULL THEN 'N' ELSE 'Y' END 'RSI04'
    ,CASE WHEN (SELECT CPPC.ID_CONTRAPARTE FROM [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] CPPC (NOLOCK) LEFT JOIN [KN1].[ADRBB_QUESTOES] DOQU (NOLOCK) ON DOQU.ID_QUESTAO = CPPC.ID_QUESTAO WHERE DOQU.CD_QUESTAO = 'Q8' AND CPPC.CD_RESPOSTA = 'Sim' AND CPPC.ID_CONTRAPARTE = COPA.ID_CONTRAPARTE) IS NULL THEN 'N' ELSE 'Y' END 'RSI06'
    ,CASE WHEN (SELECT CPPC.ID_CONTRAPARTE FROM [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] CPPC (NOLOCK) LEFT JOIN [KN1].[ADRBB_QUESTOES] DOQU (NOLOCK) ON DOQU.ID_QUESTAO = CPPC.ID_QUESTAO WHERE DOQU.CD_QUESTAO IN ('Q42', 'Q77') AND CPPC.CD_RESPOSTA = 'Sim' AND CPPC.ID_CONTRAPARTE = COPA.ID_CONTRAPARTE) IS NOT NULL THEN 'face-to-face' ELSE CASE WHEN (SELECT CPPC.ID_CONTRAPARTE FROM [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] CPPC (NOLOCK) LEFT JOIN [KN1].[ADRBB_QUESTOES] DOQU (NOLOCK) ON DOQU.ID_QUESTAO = CPPC.ID_QUESTAO WHERE DOQU.CD_QUESTAO IN ('Q42', 'Q77') AND CPPC.CD_RESPOSTA = 'Não' AND CPPC.ID_CONTRAPARTE = COPA.ID_CONTRAPARTE) IS NOT NULL THEN 'no-face-to-face' ELSE '' END END 'RSI07'
    ,CASE WHEN (RISA.NM_RISCO = '' OR RISA.NM_RISCO IS NULL) THEN RISAA.NM_RISCO ELSE RISA.NM_RISCO END 'CDD01'
    ,CASE WHEN CPDE.DT_DESATIVACAO <> '19000101' AND CPDE.DT_REATIVACAO = '19000101' THEN 'Desativado' ELSE 'Ativo' END AS FLAG_ATIVO
    ,CASE WHEN CPDE.DT_DESATIVACAO <> '19000101' AND CPDE.DT_REATIVACAO = '19000101' THEN CONVERT(VARCHAR, CPDE.DT_DESATIVACAO, 120) ELSE '1900-01-01' END AS DT_DESATIVACAO
    ,CPST.NM_SITUACAO 'SIT_CDD'
    ,ABA.DE_PONTUACAO_ABA 'RISCO_TIPO_EMPRESA'
	,CASE WHEN STRK.DE_PONTUACAO_ABA IS NOT NULL THEN STRK.DE_PONTUACAO_ABA ELSE '' END AS TotalStructureRisk
	,CASE WHEN INFAD.CD_RESPOSTA IS NOT NULL THEN 'Y' ELSE 'N' END AS AdverseInformationRisk
	,CASE WHEN COPANT.DTHR_FIM IS NOT NULL THEN COPANT.DTHR_FIM ELSE '' END AS PreviousReviewDate
    ,CASE WHEN RISANT.NM_RISCO IS NOT NULL THEN RISANT.NM_RISCO ELSE '' END AS CDDPreviousRating
	--INTO [GEN].[KN1_SIRA_DM_KN1_2023]
FROM [KN1].[ADKYC_CONTRAPARTES] COPA
	LEFT JOIN [KN1].[ADKYC_CONTRAPARTES] COPANT (NOLOCK) 
		ON COPANT.CD_SITUACAO = 1
		AND COPANT.DTHR_FIM <> '19000101'
		AND COPANT.ID_CONTRAPARTE_RENOVACAO <> -1
		AND COPANT.ID_CONTRAPARTE < COPA.ID_CONTRAPARTE
		AND COPANT.ID_CONTRAPARTE_RENOVACAO = COPA.ID_CONTRAPARTE
	LEFT JOIN [KN1].[ADKYC_RISCOS] RISANT (NOLOCK) 
		ON COPANT.CD_RISCO_ATRIBUIDO = RISANT.CD_RISCO
	LEFT JOIN [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] INFAD (NOLOCK) 
		ON COPA.ID_CONTRAPARTE = INFAD.ID_CONTRAPARTE
		AND INFAD.ID_QUESTAO in('47') and INFAD.CD_RESPOSTA = 2
	LEFT JOIN [KN1].[ADRBB_CONTRAPARTES_PONTUACOES_ABAS] STRK (NOLOCK) 
		ON COPA.ID_CONTRAPARTE = STRK.ID_CONTRAPARTE
		AND STRK.CD_DOMINIO_QUESTAO in('D3','D25','D26','D27','D97')
	LEFT JOIN [KN1].[ADRBB_CONTRAPARTES_PONTUACOES_ABAS] ABA (NOLOCK) 
		ON COPA.ID_CONTRAPARTE = ABA.ID_CONTRAPARTE
		AND ABA.CD_DOMINIO_QUESTAO = 'D17'
	INNER JOIN [KN1].[ADKYC_CONTRAPARTES_COMPL] CPCO (NOLOCK) 
		ON COPA.ID_CONTRAPARTE = CPCO.ID_CONTRAPARTE
	LEFT JOIN [KN1].[ADKYC_RISCOS] RISA (NOLOCK) 
		ON COPA.CD_RISCO_ATRIBUIDO = RISA.CD_RISCO
	LEFT JOIN [KN1].[ADKYC_CONTRAPARTES_KYC] CPKY (NOLOCK) 
		ON CPCO.CD_CONTRAPARTE = CPKY.CD_CONTRAPARTE
	LEFT JOIN [KN1].[ADKYC_CONTRAPARTES] CPAN (NOLOCK) 
		ON CPAN.ID_CONTRAPARTE_RENOVACAO = COPA.ID_CONTRAPARTE
	LEFT JOIN [KN1].[ADKYC_RISCOS] RISAA (NOLOCK) 
		ON CPAN.CD_RISCO_ATRIBUIDO = RISAA.CD_RISCO
	LEFT JOIN [KN1].[ADKYC_SITUACOES] CPST (NOLOCK) 
		ON CPST.CD_SITUACAO = COPA.CD_SITUACAO
	LEFT JOIN [KN1].[ADKYC_CONTRAPARTES_DESATIVADOS] CPDE (NOLOCK) 
		ON COPA.NR_CPF_CNPJ = CPDE.NR_CPF_CNPJ
		AND COPA.DOC_ESTRANGEIRO = CPDE.DOC_ESTRANGEIRO
		AND COPA.ID_GRP_CONTRAPARTE = CPDE.ID_GRP_CONTRAPARTE
		AND CPDE.ID_CONTRAPARTE_DESATIVADO IN (
			SELECT MAX(ID_CONTRAPARTE_DESATIVADO)
			FROM [KN1].[ADKYC_CONTRAPARTES_DESATIVADOS] (NOLOCK)
			WHERE NR_CPF_CNPJ = COPA.NR_CPF_CNPJ
				AND DOC_ESTRANGEIRO = COPA.DOC_ESTRANGEIRO
				AND ID_GRP_CONTRAPARTE = COPA.ID_GRP_CONTRAPARTE
			)
WHERE COPA.ID_CONTRAPARTE IN ( SELECT [KN1_id_2023] FROM [GEN].[vw_KN1_ID2023_Src] )
) KN1;"


-- non gcob sector
"CREATE VIEW [GEN].[vw_NonGcobSector_src]
AS SELECT
    CAST('GIC' AS NVARCHAR(10)) AS SourceSystem,
    CAST([LocalsystemId] AS NVARCHAR(25)) AS [LocalsystemId],
	CAST([IndustryCode] AS NVARCHAR(255)) AS [SectorCode],
	CAST(GlobalClientId AS NVARCHAR(25)) AS GlobalClientId,
	CAST('Y' AS NVARCHAR(1)) AS [PrimaryNICSCodeFlag],
	cast(getdate()-1 as date) as ActualDate
FROM [GEN].[vw_GIC_Client_Src]
WHERE [IndustryCode] IS NOT NULL

UNION

/** North America Sector Data **/
SELECT 
    CAST('RAF' AS NVARCHAR(10)) AS SourceSystem,
    CAST([LocalsystemId] AS NVARCHAR(25)) AS [LocalsystemId],
	CAST([IndustryCode] AS NVARCHAR(255)) AS [SectorCode],
	CAST(GlobalClientId AS NVARCHAR(25)) AS GlobalClientId,
	CAST('Y' AS NVARCHAR(1)) AS [PrimaryNICSCodeFlag],
	cast(getdate()-1 as date) as ActualDate
FROM [GEN].[vw_RAF_Client_Src]
WHERE [IndustryCode] IS NOT NULL

UNION

/** Australia and New Zealand Sector Data **/
SELECT 
     CAST('RANZ' AS NVARCHAR(10)) AS SourceSystem,
    CAST([LocalsystemId] AS NVARCHAR(25)) AS [LocalsystemId],
	CAST([IndustryCode] AS NVARCHAR(255)) AS [SectorCode],
	CAST(GlobalClientId AS NVARCHAR(25)) AS GlobalClientId,
	CAST('Y' AS NVARCHAR(1)) AS [PrimaryNICSCodeFlag],
	cast(getdate()-1 as date) as ActualDate
FROM [GEN].[vw_RANZ_Client_Src]
WHERE [IndustryCode] IS NOT NULL;"


-- KN1 NAICS specific
"CREATE VIEW [GEN].[vw_KN1_NAICS_Src] AS SELECT 
GIC.[LocalSystemID] as [LocalClientID]
,KN1_NAICS.NAIC as SectorCode
,GIC.[GlobalClientID]
FROM (
	SELECT DISTINCT
		COPA.ID_CONTRAPARTE as [KN1_id]
	    ,rtrim(CPCO.CD_CONTRAPARTE) as [GIC_id]
		,CASE WHEN (NAIC.DE_COB_NAICS IS NULL AND COPA.ID_GRP_CONTRAPARTE IN (2, 976, 39)) THEN '424510' WHEN (NAIC.DE_COB_NAICS IS NULL AND COPA.ID_GRP_CONTRAPARTE IN (40)) THEN '525910' WHEN (NAIC.DE_COB_NAICS IS NULL AND COPA.ID_GRP_CONTRAPARTE IN (37, 38)) THEN '424910' ELSE NAIC.CD_COB_NAICS END AS [NAIC]
	FROM [KN1].[ADKYC_CONTRAPARTES] COPA
		INNER JOIN [KN1].[ADKYC_CONTRAPARTES_COMPL] CPCO (NOLOCK) 
			ON COPA.ID_CONTRAPARTE = CPCO.ID_CONTRAPARTE
		LEFT JOIN [KN1].[ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS] ACQR (NOLOCK) 
			ON COPA.ID_CONTRAPARTE = ACQR.ID_CONTRAPARTE
			AND ACQR.ID_QUESTAO IN (12, 390)
		LEFT JOIN [KN1].[ADRBB_COB_NAICS] NAIC (NOLOCK) 
			ON NAIC.CD_COB_NAICS = ACQR.CD_RESPOSTA
	WHERE  COPA.ID_CONTRAPARTE IN (SELECT [KN1_id_2023] FROM [GEN].[vw_KN1_ID2023_Src])
	) KN1_NAICS
LEFT JOIN [GEN].[vw_GIC_Client_Src] GIC
	ON GIC.[LocalSystemID] = KN1_NAICS.GIC_id
WHERE NAIC is not null;"


-- KN1 Risknew
"CREATE VIEW [GEN].[vw_KN1_RiskNew_Src] AS Select  
 [LocalSystemId]
, InstanceId
, SourceSystem
, SOurceSystemReferenceID
, InstanceCalculationId
, CalculatedRiskrating.RiskModelRiskLevelId as OverallCalculatedRisk
, CalculatedRisk as OverallCalculatedRiskDescription 
, ReCalculatedRiskrating.RiskModelRiskLevelId as OverallRecalculatedRisk
, ReCalculatedRisk as OverallRecalculatedRiskDescription  
, OverallValidatedRiskrating.RiskModelRiskLevelId as OverallValidatedRisk
, OverallValidatedRisk as OverallValidatedRiskDescription 
, AdverseInforating.RiskModelRiskLevelId as AdverseInfoRisk
, [Adverse Info] as AdverseInfoRiskDescription
, DistributionChannelrating.RiskModelRiskLevelId as DistributionRisk
, [Distribution Channel] as DistributionRiskDescription
, [Entity Type] as EntityTypeRiskDescription
, EntityTyperating.RiskModelRiskLevelId as EntityTypeRisk
, [Geographical] as GeoRiskDescription
, Geographicalrating.RiskModelRiskLevelId as GeoRisk
, [Other] as OtherRiskDescription
, Otherrating.RiskModelRiskLevelId as OtherRisk
, [PEP] as PEPRiskDescription
, PEPrating.RiskModelRiskLevelId as PEPRisk
, [Products and Services] As ProductRiskDescription
, ProductsandServicesrating.RiskModelRiskLevelId as ProductRisk
, [Sector] As SectorRiskDescription
, Sectorrating.RiskModelRiskLevelId as SectorRisk
, [Structure] as StructureRiskDescription
, Structurerating.RiskModelRiskLevelId as StructureRisk
, [Third Party] as ThirdPartyRiskDescription
, ThirdPartyrating.RiskModelRiskLevelId as ThirdPartyRisk
, [Transaction] as TransactionRiskDescription
, Transactionrating.RiskModelRiskLevelId as TransactionRisk
		from 
(SELECT * 
FROM (
     SELECT
		  InstanceId, SourceSystem, SOurceSystemReferenceID, InstanceCalculationId, CalculatedRisk, ReCalculatedRisk
		, [Adverse Info], [Distribution Channel], [Entity Type]
		, [Geographical], [Other], [PEP], [Products and Services]
		, [Sector], [Structure], [Third Party], [Transaction]
	FROM (
			SELECT InstanceID ,SourceSystem, SOurceSystemReferenceID, InstanceCalculationId, CalculatedRisk, ReCalculatedRisk
				, Category
				, CalculatedCategoryRisk--select InstanceCalculationId, count(*)
				FROM [gcob].[GCOB_RM_dbo_InstanceCalculationCategory] icc --group by InstanceCalculationId
				INNER JOIN (SELECT InstanceId, a.ID , CalculatedRisk, ReCalculatedRisk, b.SourceSystem, b.SOurceSystemReferenceID
							FROM 
						 [gcob].[GCOB_RM_dbo_InstanceCalculation]  a
							INNER JOIN [gcob].[GCOB_RM_dbo_Instance] b ON a.InstanceID = b.id
							LEFT OUTER JOIN (SELECT id, Replace(Name,' Risk','') AS CalculatedRisk
										FROM [gcob].[GCOB_RM_dbo_RiskLevel]) CRL ON CRL.id = a.[CalculatedRiskLevelId]
							LEFT OUTER JOIN (SELECT id, Replace(Name,' Risk','') AS ReCalculatedRisk
										FROM [gcob].[GCOB_RM_dbo_RiskLevel]) RCRL ON RCRL.id = a.[ReCalculatedRiskLevelId]
							WHERE expired IS NULL) ic ON icc.InstanceCalculationId = ic.id
				INNER JOIN (SELECT id, Name AS Category 
							FROM [gcob].[GCOB_RM_dbo_Category]) ctg			ON ctg.Id = icc.CategoryId
				--LEFT OUTER JOIN [gcob].[GCOB_RM_dbo_InstanceCategory] icat	ON icat.CategoryId = icc.CategoryId AND icat.InstanceId = icc.InstanceCalculationId
				LEFT OUTER JOIN (SELECT id, Replace(Name,' Risk','') AS CalculatedCategoryRisk
							FROM [gcob].[GCOB_RM_dbo_RiskLevel]) Rl			ON Rl.id=icc.[CalculatedRiskLevelId]
				WHERE Category IS NOT NULL
				  AND CalculatedCategoryRisk IS NOT NULL
				AND SOurceSystem = '3d73640e-ba60-4528-ac73-8bfc2f0f5de5'
				---AND  SOurceSystemReferenceID like '%OCDD%'
	) x --all categories and risk as rows
	PIVOT (
		  Max(CalculatedCategoryRisk)
		  FOR [Category] IN (
					[Adverse Info], [Distribution Channel], [Entity Type]
					, [Geographical], [Other], [PEP], [Products and Services]
					, [Sector], [Structure], [Third Party], [Transaction]
						)--End IN
			) Sub--End PIVOT
	) RSK
	
	) RiskData

	LEFT JOIN (


SELECT COPA.[ID_CONTRAPARTE] as [CASE ID]--,RISF.[NM_RISCO] AS [ValidatedRisk]

,CASE WHEN RISI.[NM_RISCO] = 'Baixo' THEN 'Low'

WHEN RISI.[NM_RISCO] = 'Médio' THEN 'Medium'

WHEN RISI.[NM_RISCO] = 'Alto' THEN 'High'

ELSE RISI.[NM_RISCO] END AS OverallValidatedRisk

,rtrim(CPCO.CD_CONTRAPARTE) AS [LocalSystemId]

FROM [KN1].[ADKYC_CONTRAPARTES] as COPA

LEFT JOIN [KN1].[ADKYC_RISCOS] RISI 
ON COPA.[CD_RISCO_ATRIBUIDO] = RISI.[CD_RISCO]

LEFT JOIN [KN1].[ADKYC_CONTRAPARTES_COMPL] CPCO (NOLOCK) 
		ON COPA.ID_CONTRAPARTE = CPCO.ID_CONTRAPARTE

	) OCDD ON RIskdata.SOurceSystemReferenceID = OCDD.[CASE ID]
LEFT JOIN
--Select * from 
[gcob].[GCOB_CS_case_RiskModelRiskLevel] CalculatedRiskrating  ON
CalculatedRiskrating.DisplayName=RiskData.CalculatedRisk
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] ReCalculatedRiskrating  ON
ReCalculatedRiskrating.DisplayName=RiskData.ReCalculatedRisk
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] OverallValidatedRiskrating  ON
OverallValidatedRiskrating.DisplayName=OCDD.OverallValidatedRisk
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] AdverseInforating  ON
AdverseInforating.DisplayName=RiskData.[Adverse Info]
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] DistributionChannelrating  ON
DistributionChannelrating.DisplayName=RiskData.[Distribution Channel]
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] EntityTyperating  ON
EntityTyperating.DisplayName=RiskData.[Entity Type]
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] Geographicalrating  ON
Geographicalrating.DisplayName=RiskData.[Geographical]
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] Otherrating  ON
Otherrating.DisplayName=RiskData.Other
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] PEPrating  ON
PEPrating.DisplayName=RiskData.PEP
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] ProductsandServicesrating  ON
ProductsandServicesrating.DisplayName=RiskData.[Products and Services]
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] Sectorrating  ON
Sectorrating.DisplayName=RiskData.Sector
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] Structurerating  ON
Structurerating.DisplayName=RiskData.Structure
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] ThirdPartyrating  ON
ThirdPartyrating.DisplayName=RiskData.[Third Party]
LEFT JOIN
[gcob].[GCOB_CS_case_RiskModelRiskLevel] Transactionrating  ON
Transactionrating.DisplayName=RiskData.[Transaction]

				where RIskdata.SOurceSystem = '3d73640e-ba60-4528-ac73-8bfc2f0f5de5';"



-- GIC PEP UBO
"CREATE VIEW [GEN].[vw_GICPepUbo_Src] AS Select 
CAST(  CASE 
           WHEN CL.[GlobalclientID] IS NOT NULL
					THEN CL.[GlobalclientID] 
				      ELSE CONCAT ('GIC:',GIC_ID) END AS NVARCHAR(25)) AS [CaseID],
CAST(CONCAT ('GIC:',GIC_ID )  AS NVARCHAR(25))  AS  [LocalSystemId],
CAST(        CASE 
				WHEN  CL.[GlobalclientID] IS NOT NULL
					THEN CL.[GlobalclientID]
				ELSE CONCAT (
					'GIC:',
					GIC_ID
					)  END AS NVARCHAR(25)) AS [GlobalclientID],

CAST(NULL AS INT) AS [MaxClientStructureSnapshotId],
CAST(CONCAT('GIC:',GIC_ID) AS NVARCHAR(255))  AS [ParentIdentity],
CAST(CONCAT('GIC:',GIC_ID) AS NVARCHAR(17))  AS [ParentIdentity_v2],
CAST(NULL AS NVARCHAR(255))  AS [ChildIdentity],
CAST(NULL AS INT)  AS [Relationshipid],
CAST(NULL AS INT)  AS [Parenttype],
CAST(NULL AS INT)  AS [UboThroughReasonReferenceId],
CAST(NULL AS NVARCHAR(100))  AS [UboReason],
 CAST(NULL AS NVARCHAR(5)) AS [IsUbo],
CAST(NULL AS INT) AS [PoliticallyExposedPersonStatusId],
CAST(NULL AS NVARCHAR(50)) AS [PEPStatus],
CAST(
      CASE 
	      WHEN NameofPEP IS NOT NULL 
		        THEN 'Y' 
				    ELSE 'N' END AS NVARCHAR(9))  AS [IsPEP],
CAST(NULL AS INT) AS [PEP_CountryReferenceId],
CAST(NULL AS NVARCHAR(100)) AS [PEP_Nationality],
CAST(NULL AS NVARCHAR(5)) AS [PEP_Nationality_IsoCode],
CAST(NULL AS NVARCHAR(5)) AS [CORRUPTIONRISK],
[CountryOfRegistration],
[CountryCodeOfRegistration] from
[GEN].[KN1_PEPs_30_Apr_2024] PEP
INNER JOIN
[GEN].[vw_GIC_Client_Src] CL ON
CL.LocalSystemId=PEP.GIC_ID;"
