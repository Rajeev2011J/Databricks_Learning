-- RANZ client risks (T24 customers - Riskshield)

"CREATE VIEW [GEN].[vw_RANZ_AllRisks_Src] AS SELECT 
/*
Created by: Ravi Puli ravi.puli@rabobank.nl
Goal: to have an overview of All Risk data from RANZ for the SIRA and DNB questionnaire.
Log:
2024-02-15 Created Ravi Puli

*/
     CAST('T24' AS NVARCHAR(25)) AS LocalSystem,
   CAST(CONCAT (
			CASE 
				WHEN RANZ.[Country Risk ID] IS NOT NULL
					THEN RANZ.[Country Risk ID]
				ELSE RANZ.[Customer Country]
				END,
			RANZ.[Customer ID]
			) AS NVARCHAR(25))  AS LocalSystemId,
	CAST(CASE 
			WHEN KEYSTORE.[GlobalClientID] IS NOT NULL
				THEN CONVERT(NVARCHAR(35), KEYSTORE.[GlobalClientID])
			ELSE CONCAT (
					'RANZ:',
					CONCAT (
						CASE 
							WHEN RANZ.[Country Risk ID] IS NOT NULL
								THEN RANZ.[Country Risk ID]
							ELSE RANZ.[Customer Country]
							END,
						RANZ.[Customer ID]
						)
					)
			END AS NVARCHAR(25)) AS [GlobalClientID]
	,CAST(CONCAT (
			CASE 
				WHEN RANZ.[Country Risk ID] IS NOT NULL
					THEN RANZ.[Country Risk ID]
				ELSE RANZ.[Customer Country]
				END,
			RANZ.[Customer ID]
			) AS NVARCHAR(25))  AS CaseId
	,CAST(NULL AS NVARCHAR(255)) AS AdverseInfo
	,CAST(NULL AS NVARCHAR(255)) AS ComplexStucture
	,CAST(CASE 
	    WHEN RISK.[CDD_CLIENT_TYPE] = 'BUSI'
			THEN 'Legal Entity'
        WHEN RISK.[CDD_CLIENT_TYPE] = 'PART'
			THEN 'Partnership'
	    WHEN RISK.[CDD_CLIENT_TYPE] = 'ASSO'
			THEN 'Other Body Corporate'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'INDV'
			THEN 'Natural Person'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'DIYS'
			THEN 'Super Fund'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'INTM'
			THEN 'Intermediary'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'JOIN'
			THEN 'Joint'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'COMP'
			THEN 'Company'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'MINO'
			THEN 'Minor'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'SMSF'
			THEN 'Self Managed Super Fund'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'TRUS'
			THEN 'Trust' 
			ELSE RISK.[CDD_CLIENT_TYPE] END 
	AS NVARCHAR(255)) AS EntityType
	,CAST(NULL AS NVARCHAR(255)) AS  FaceToFaceContact
	,CAST(CASE WHEN RISK.[ISSUES_BEARER_SHARES]='Y' THEN 'Yes'
	          WHEN RISK.[ISSUES_BEARER_SHARES]='N' THEN 'No' ELSE NULL END AS NVARCHAR(255)) AS IssuesBearerShares
	,CAST(CASE WHEN RISK.[NOMINEE_SHAREHOLDERS] ='Y' THEN 'Yes'
	           WHEN  RISK.[NOMINEE_SHAREHOLDERS] ='N' THEN 'No' ELSE NULL END AS NVARCHAR(255)) AS NomineeShareholders
	,CAST(
	  CASE 
	    WHEN RISK.[CDD_CLIENT_TYPE] = 'BUSI'
			THEN 'Legal Entity'
        WHEN RISK.[CDD_CLIENT_TYPE] = 'PART'
			THEN 'Partnership'
	    WHEN RISK.[CDD_CLIENT_TYPE] = 'ASSO'
			THEN 'Other Body Corporate'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'INDV'
			THEN 'Natural Person'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'DIYS'
			THEN 'Super Fund'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'INTM'
			THEN 'Intermediary'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'JOIN'
			THEN 'Joint'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'COMP'
			THEN 'Company'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'MINO'
			THEN 'Minor'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'SMSF'
			THEN 'Self Managed Super Fund'
		WHEN RISK.[CDD_CLIENT_TYPE] = 'TRUS'
			THEN 'Trust' 
			ELSE RISK.[CDD_CLIENT_TYPE] END 
	AS NVARCHAR(255))  AS CddtypeName
	,CASE 
		WHEN RISK.[CDD_CLIENT_TYPE] = 'INDV'
			THEN 1
		ELSE 0
		END AS isNaturalPerson
	,CASE 
		WHEN RISK.[CDD_CLIENT_TYPE] = 'TRUS'
			THEN 1
		ELSE 0
		END AS IsTrust
	,CAST(NULL AS NVARCHAR(25)) AS IsClientRegulated
	,CAST(NULL AS NVARCHAR(25)) AS  IsClientListed
    ,CAST([COMMON_UBO_IN_ORGANIZATIONAL_LAYERS] AS NVARCHAR(25)) AS	CommonUBOinOrganizationalLayers
	,CAST([HOLDS_TPA_ACCOUNTS] AS NVARCHAR(255)) AS HoldsTpaAccounts
	,CAST(NULL AS NVARCHAR(25)) AS CDDPreviousRating
 
	FROM [RISKSHIELD].[T24_Customers_EOY2023] RANZ

		/** Join the RANZ data with GCDS to retrive Uniq Globalclient ID **/
LEFT JOIN (
	SELECT *
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
		WHERE [KeyType] IN (
				'T24 AUZ',
				'CB RANZ',
				'T24'
				)
			AND (
				STATUS = 'Active'
				OR STATUS IS NULL
				)
			AND record_end IS NULL
		) TAB
	WHERE RANK = 1
	) KEYSTORE
	ON RANZ.[Customer ID] = KEYSTORE.KeyValue
/** MDM (RANZ RISK) Data ***/
LEFT JOIN (
	SELECT *
	FROM (
		SELECT MDM.*,
			COUNT(*) OVER (
				PARTITION BY PARTY_ID ORDER BY CONTRACT_ID DESC ROWS UNBOUNDED PRECEDING
				) AS DUP
		FROM [MDM].[MDM_WITH_PEPUBO] MDM 
		) MDM
	WHERE DUP = 1
	) RISK
	ON RISK.CONTRACT_ID = RANZ.[Customer ID]
		AND RANZ.[Customer Country] = TRIM(REPLACE(SUBSTRING(RISK.[CLIENT_BUSINESS_LINE], 1, CHARINDEX('_', [CLIENT_BUSINESS_LINE] + '_') - 1), 'RANZ', ''));"


-- RANZ new view
"CREATE VIEW [GEN].[vw_RANZ_RiskNew_Src]
AS WITH GCDSKEY  AS (
				SELECT [GlobalClientID] as GCDSId,
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
					WHERE [KeyType] IN ('T24 AUZ',
				'CB RANZ',
				'T24')
						AND (
							STATUS = 'Active'
							OR STATUS IS NULL
							)
						AND record_end IS NULL
					) TAB
				WHERE RANK = 1
				) --KeyStore
				---ON GCOBFI.[GcobId] = KeyStore.[GCOBID]
 Select 
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
,CAST(GCDSId AS NVARCHAR(25)) AS GCDSId
,ROW_NUMBER () OVER (PARTITION BY [LocalSystemId],SOurceSystemReferenceID ORDER BY SOurceSystemReferenceID ) AS RANK
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
				--AND SOurceSystem = '565a6d04-8552-4898-9175-c26327420c79'
				and SOurceSystemReferenceID like '%OCDD%'
	) x --all categories and risk as rows
	PIVOT (
		  Max(CalculatedCategoryRisk)
		  FOR [Category] IN (
					[Adverse Info], [Distribution Channel], [Entity Type]
					, [Geographical], [Other], [PEP], [Products and Services]
					, [Sector], [Structure], [Third Party], [Transaction]
						)--End IN
			) Sub--End PIVOT
	) RSK) RiskData

	LEFT JOIN (

	SELECT 
		CASE WHEN b.ClientID IS NULL THEN 0 ELSE 1 END AS LatestCase, 'CaseSummary' AS CaseSource, 'RANZ'AS RegionSource
		, [Case ID],
			CASE WHEN Country ='New Zealand' THEN 
               CONCAT('NZ',[Client ID]) ELSE  CONCAT('AU',[Client ID]) END  as  [LocalSystemId]
		, [Client Name],[Review Type], REPLACE([New Risk Rating]  ,'Risk','')  AS [OverallValidatedRisk],[Client Type], Branch--, Region
	---Select * 
	FROM [OCDD].[OCDD_Case_Summary] a
	--where  [Case ID] like '%98735%'
	LEFT JOIN (SELECT Max([Created Date/Time]) as LatestCaseDAte,
	[CLient ID] AS ClientID
				FROM [OCDD].[OCDD_Case_Summary]
				GROUP BY [CLient ID]
		) b ON a.[Client ID] = b.ClientId AND b.LatestCaseDAte = a.[Created Date/Time]
	UNION
	SELECT --c.client_status, 
		CASE WHEN b.ClientID IS NULL THEN 0 ELSE 1 END AS LatestCase, 'CaseAU' AS CaseSource, 'RANZ'AS RegionSOurce
		, [Case ID],
			CASE WHEN Country ='New Zealand' THEN 
               CONCAT('NZ',[Client ID]) ELSE  CONCAT('AU',[Client ID]) END  as  [LocalSystemId]
		, [Client Name],[Review Type], REPLACE([Risk Rating] ,'Risk','') AS [OverallValidatedRisk],NULL AS [Client Type], Branch--, Region
		--	,* 
		--Select *
	FROM [OCDD].[OCDD_Resolved_Case_Report_AU] a
	--where isnull(Completed , '9999-12-31') > @DateofData
		---AND Created  <= @DateofData 
	--where  [Case ID] like '%98735%'
	LEFT JOIN (SELECT Max([Completed On]) as LatestCaseDAte, [CLient ID] AS ClientID
				FROM [OCDD].[OCDD_Resolved_Case_Report_AU]
				GROUP BY [CLient ID]
		) b ON a.[Client ID] = b.ClientId AND b.LatestCaseDAte = a.[Completed On]
	UNION
	SELECT --c.client_status, 
		--CLIENT_ID Localsystemid,
		CASE WHEN CLIENT_ID IS NULL THEN 0 ELSE 1 END AS LatestCase, 'CaseAU' AS CaseSource, 'RANZ'AS RegionSOurce
		, [Case_ID] AS [CASE ID], 
			CASE WHEN Country ='New Zealand' THEN 
               CONCAT('NZ',[Client_ID]) ELSE  CONCAT('AU',[Client_ID]) END  as  [LocalSystemId], [Client_Name] AS [CLIENT NAME],[Review_Type] AS [Review Type], [Risk_Rating] AS [New Risk Rating],NULL AS [Client_Type], NULL AS Branch--, Region
		--	,* 
		--Select *
		FROM
		[ros].[CDD_Resolved_Report] a
	LEFT JOIN (SELECT Max([Completed_On]) as LatestCaseDAte, [CLient_ID] AS ClientID
				FROM [ros].[CDD_Resolved_Report]
				GROUP BY [CLient_ID]
		) b ON a.[Client_ID] = b.ClientId AND b.LatestCaseDAte = a.[Completed_On]

	) OCDD ON RIskdata.SOurceSystemReferenceID = OCDD.[CASE ID]

	LEFT JOIN GCDSKEY GCDS ON
	GCDS.GCOBID=OCDD.LocalSystemId
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

				where RIskdata.SOurceSystem = '565a6d04-8552-4898-9175-c26327420c79';"


-- new style

-- PEP UBOs
"CREATE VIEW [GEN].[vw_RANZPepUbo_Src] AS Select
/*
Created by: Ravi Puli ravi.puli@rabobank.nl
Goal: to have an overview of UBOs and PEPs from GCOB for the SIRA and DNB questionnaire.
Log:
2024-02-15 Created Ravi Puli

*/
CAST(CONCAT (
			CASE 
				WHEN RANZ.[Country Risk ID] IS NOT NULL
					THEN RANZ.[Country Risk ID]
				ELSE RANZ.[Customer Country]
				END,
			RANZ.[Customer ID]
			) AS NVARCHAR(255)) AS [CaseID],
CAST(CONCAT (
			CASE 
				WHEN RANZ.[Country Risk ID] IS NOT NULL
					THEN RANZ.[Country Risk ID]
				ELSE RANZ.[Customer Country]
				END,
			RANZ.[Customer ID]
			) AS NVARCHAR(255)) AS  [LocalSystemId],
CAST([GlobalclientID] AS NVARCHAR(25)) AS [GlobalclientID],

CAST(NULL AS INT) AS [MaxClientStructureSnapshotId],
CAST(NULL AS NVARCHAR(255))  AS [ParentIdentity],
CAST(NULL AS NVARCHAR(17))  AS [ParentIdentity_v2],
CAST(NULL AS NVARCHAR(255))  AS [ChildIdentity],
CAST(NULL AS INT)  AS [Relationshipid],
CAST(NULL AS INT)  AS [Parenttype],
CAST(NULL AS INT)  AS [UboThroughReasonReferenceId],
CAST(NULL AS NVARCHAR(100))  AS [UboReason],
 CAST(RELATION_SHIP_CODE AS NVARCHAR(5)) AS [IsUbo],
CAST(NULL AS INT) AS [PoliticallyExposedPersonStatusId],
CAST(NULL AS NVARCHAR(50)) AS [PEPStatus],
CAST(PEP_EVALUATION_DESC AS NVARCHAR(9))  AS [IsPEP],
CAST(NULL AS INT) AS [PEP_CountryReferenceId],
CAST(NULL AS NVARCHAR(100)) AS [PEP_Nationality],
CAST(NULL AS NVARCHAR(5)) AS [PEP_Nationality_IsoCode],
CAST(NULL AS NVARCHAR(5)) AS [CORRUPTIONRISK],
[Country of Residence] AS [ResidentialAddressCountry],
[Country of Residence] AS [ResidentialAddressIsoCode]

FROM
(SELECT * FROM
(
SELECT [GlobalClientID],
RANZ.* , ROW_NUMBER() OVER (
				PARTITION BY [GlobalClientID],[Customer ID],[Country Risk ID] ORDER BY [GlobalClientID],[Customer ID],[Country Risk ID] ) AS UNIQ
				--Select * 
				FROM [RISKSHIELD].[T24_Customers_EOY2023] RANZ
				--where  [Customer ID]='17601501'
LEFT JOIN
 (
	SELECT *
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
		WHERE [KeyType] IN (
				'T24 AUZ',
				'CB RANZ',
				'T24'
				)  --AND [GlobalClientID]='2848878'
			AND (
				STATUS = 'Active'
				OR STATUS IS NULL
				)
			AND record_end IS NULL
		) TAB

	) KEYSTORE
	ON CAST(TRIM(RANZ.[Customer ID]) AS NVARCHAR(25)) = CAST(TRIM(KEYSTORE.KeyValue) AS NVARCHAR(25))
	---where CAST(TRIM(RANZ.[Customer ID]) AS NVARCHAR(25))= '000024SP'
	
) RANZ 
/** MDM (RANZ RISK) Data ***/
INNER JOIN (
	SELECT *
	FROM (
		SELECT CASE
		     WHEN PEP_EVALUATION_DESC ='Immediate family member of a PEP' THEN 'PEP'
			  WHEN PEP_EVALUATION_DESC ='Close associate of a PEP' THEN 'PEP'
			   WHEN PEP_EVALUATION_DESC ='PEP' THEN 'PEP'
			  WHEN PEP_EVALUATION_DESC ='Not a PEP' THEN NULL END AS PEP_EVALUATION_DESC,
			  RELATION_SHIP_CODE,
			  CLIENT_BUSINESS_LINE,
			  CONTRACT_ID,
			COUNT(*) OVER (
				PARTITION BY CONTRACT_ID ORDER BY CONTRACT_ID DESC ROWS UNBOUNDED PRECEDING
				) AS DUP
		FROM [MDM].[MDM_WITH_PEPUBO] MDM 
		where  (PEP_EVALUATION_DESC<> 'Not a PEP' OR RELATION_SHIP_CODE='UBO'
		) 
		   --and  [CLIENT_STATUS]='Active Client'  and PEP_EVALUATION_DESC<> 'Not a PEP' 
		   ----OR (RELATION_SHIP_CODE='UBO' and [CLIENT_STATUS]='Active Client') 
		) MDM
	WHERE DUP = 1
	

	) MDM
	ON MDM.CONTRACT_ID = RANZ.[Customer ID]
		AND RANZ.[Customer Country] = TRIM(REPLACE(SUBSTRING(MDM.[CLIENT_BUSINESS_LINE], 1, CHARINDEX('_', [CLIENT_BUSINESS_LINE] + '_') - 1), 'RANZ', ''))
)RANZ  where 
UNIQ='1';"
