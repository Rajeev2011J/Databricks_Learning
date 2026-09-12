-- base risks
"CREATE VIEW [GEN].[base_risks]
AS SELECT CountryCode, RiskType, RiskValue, CountryName
FROM (
SELECT COUNTRYCODE
      ,[MoneyLaunderingRisk]
      ,[TerrorismFinancingRisk]
      ,[SanctionsRisk]
      ,[CorruptionRisk]
      ,[TaxIntegrityRisk]
      ,[ECNonCooperativeJurisdictions]
      ,[HighFATFRisk]
      ,[ECHighRisk]
      ,[TotalitarianRegimes]
      ,[CountryName]
FROM GEN.vw_EbxGlobalCountryCamsRiskList_src			
) AS SourceTable
UNPIVOT (
RiskValue FOR RiskType IN (
      [MoneyLaunderingRisk]
      ,[TerrorismFinancingRisk]
      ,[SanctionsRisk]
      ,[CorruptionRisk]
      ,[TaxIntegrityRisk]
      ,[ECNonCooperativeJurisdictions]
      ,[HighFATFRisk]
      ,[ECHighRisk]
      ,[TotalitarianRegimes]
)
) AS UnpivotedData;"


-- Accounts products

"CREATE VIEW [GEN].[vw_AllAccountsandProducts_Src] AS SELECT 
       [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,[ProductEndDate]
	  ,cast('2023-12-31' as date) as ActualDate

FROM (

Select [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,[ProductEndDate]
  FROM   [GEN].[vw_PalermoNaAccountsandProducts_Src]
Union ALL 
Select [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,[ProductEndDate]
 FROM [GEN].[vw_RANZAccountsandProducts_Src] 
 UNION ALL
 Select  [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,[ProductEndDate] FROM 
[GEN].[vw_FlexcubeAPSAccountsandProducts_Src]
Union ALL
Select  [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,[ProductEndDate] from 
[GEN].[vw_FlexcubeBRAAccountsandProducts_Src]
Union ALL
Select [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,NULL AS [ProductEndDate] From 
[GEN].[vw_FlexcubeCBTAccountsandProducts_Src]
Union ALL
Select  [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,[ProductEndDate] FROM
[GEN].[vw_FlexcubeEUAccountsandProducts_Src] 
UNION ALL
Select 
 [GlobalclientID]
      ,[AssessmentUnitName]
      ,[LocalClientID]
      ,[SourceSystem]
      ,[ProductGroup]
      ,[ProductType]
      ,[ProductCode]
      ,[ProductName]
      ,[ProductRiskLocal]
      ,[CreditLineLimit]
      ,[AccountNumber]
      ,[DataDeliveryDate]
      ,[ProductLocation]
      ,[BookingEntity]
      ,[ProductStatus]
      ,[ProductStartDate]
      ,[ProductEndDate]
FROM   [GEN].[vw_SiebelAccountsandProducts_Src]) PROD;"


-- STAK part to derive Trust or STAKs
"CREATE VIEW [GEN].[vw_DNB_GR17] AS SELECT

/*
SAVE QUERY IN ""DNB Questionnaire 2024\GR Questions\SQL""

Created by: Barend van der Meulen
Goal: to have an overview of the characteristics asked by the DNB (Dutch authorities)
Log:
	2024-04-12 Created based ON last years query
	2024-05-02 igv: I do a ""union"" to add the data from SA. The union is on the 'top' table, look for the comment of its start and it can be removed if SA data come in from the Client vw.
	2024-05-03 Adjusted SA & GCOB query to have the category-logic in only one place. Added RANZ & RAF Input Finance data
	2024-05-07 Added Trust in Name, LP in name, added GR number in ColumnName

DNB GR17:
GR.17	""Please specify the number of customers to which one of the following characteristics apply.
This concerns the customers of your branche offices/susidiaries/representative offices.

Reference date: 31-12-2023
Note: please fill in all fields in this table, or fill in '""""0""""""
GR.17.01	Trust (anglo-saxon)
GR.17.02	Foundation or other similar foreign legal form
GR.17.03	Trust office foundation (STAK)
GR.17.04	Limited partnership (CV)
GR.17.05	Limited liability partnership (LLP) and/or Limited Partnership (LP)
GR.17.06	Nominee shareholder
GR.17.07	Bearer shares


*/


		a.[LocalSystemId]
      , a.[SourceSystem]
      , a.[GlobalClientID]

	  , a.[ClientLifeCycleStatus]
	  , a.[ClientName]
	  , a.[LegalForm]
      , a.[CddtypeName]
      , a.[EntityType]
      , a.[PartyType]

      , a.[CurrentCompletedLegalEntityClientId]
      , a.[CaseID]
      , a.[LegalEntityClientId]
      , a.[CaseReviewType]
      , a.[CurrentStatus]
	  , a.[NextReviewDate]
      , a.[ScheduledCompletionDate]
      , a.ReviewCounter

	  --, ReviewCounterca
	  --, ReviewCountercaeq

	  -- GR.17.01	Trust (anglo-saxon)
	  , a.TrustInName
	  , a.TrustInCddType
	  , a.TrustInLegalForm
	  , a.TrustInEntityType
	  --, a.TrustCDDType
	  , a.[IsTrust_ori]
	  , CASE 
			WHEN a.TrustInName = 1 or a.TrustInCddType = 1 or a.TrustInLegalForm = 1 or a.TrustInEntityType = 1 or a.[IsTrust_ori] = 1 
				THEN 1 
			ELSE 0 
		END AS [GR.17.01 IsTrust]

	  -- GR.17.02 Stichting (Foundation)
	  , a.StInName
	  , a.StInLegalForm
	  , a.StCDDType
	  , CASE 
			WHEN a.StakInName = 1 or a.StakCDDType = 1 
				THEN 0
			WHEN a.StInName = 1 or a.StInLegalForm = 1 or a.StCDDType = 1 
				THEN 1 
			ELSE 0 
		END AS [GR.17.02 IsStichting]

	  -- GR.17.03 Stichting Administratiekantoor (STAK) (Trust Office Foundation)
	  , a.StakInName
	  , a.StakCDDType --was added 14/08/2023. Still empty at 15/04/2024
	  , CASE WHEN a.StakInName = 1 or a.StakCDDType = 1 
				THEN 1 
			 ELSE 0 
		END AS [GR.17.03 IsSTAK]

	  -- GR.17.04 CV
	  , a.CvInLegalForm
	  , a.CvInLegalForm as [GR.17.04 CV]

	  -- GR.17.05	Limited liability partnership (LLP) and/or Limited Partnership (LP)
	  , a.LlpInName
	  , a.LlpInLegalForm
	  , a.LllpInCddType
	  , a.LlpInEntityType
	  , CASE
			WHEN a.CvInLegalForm = 1 
				THEN 0
			WHEN a.LlpInName = 1 or a.LlpInLegalForm = 1 or a.LllpInCddType = 1 or a.LlpInEntityType = 1 
				THEN 1 
			ELSE 0 
		END AS [GR.17.05 isLLPandorLP]

	  --GR.17.06	Nominee shareholder
	  , a.NomineeShareholders
	  , a.isNomineeShareholders as [GR.17.06 NomineeShareholders]

	  --GR.17.07	Bearer shares
	  , a.IssuesBearerShares
	  , a.BearersharesEntityType
	  , CASE 
			WHEN a.IsIssuesBearerShares = 1 or a.BearersharesEntityType = 1 
				THEN 1 
			ELSE 0 
		END AS [GR.17.07 IsBearerShares]
	
FROM (

	SELECT  B.[LocalSystemId]
		  , B.[SourceSystem]
		  , B.[GlobalClientID]

		  , B.[ClientName]
		  , B.[LegalForm]
		  , B.[CddtypeName]
		  , B.[EntityType]
		  , B.[PartyType]

		  , B.[ClientLifeCycleStatus]
		  , B.CurrentCompletedLegalEntityClientId
		  , B.[CaseID]
		  , B.[LegalEntityClientId]
		  , B.[CaseReviewType]
		  , B.[CurrentStatus]
		  , B.[ScheduledCompletionDate]
		  , B.[NextReviewDate]
		  , B.ReviewCounter


		  -- GR.17.01	Trust (anglo-saxon)
		  , CASE WHEN B.[ClientName] like '%trust%' THEN 1 ELSE 0 END AS TrustInName
		  , CASE WHEN B.[CddtypeName] like '%trust%' THEN 1 ELSE 0 END AS TrustInCddType
		  , CASE WHEN B.[LegalForm] = 'TRUST' THEN 1 ELSE 0 END AS TrustInLegalForm
		  , CASE WHEN B.[EntityType] like '%trust%' or B.[EntityType] in ('Self Managed Super Fund', 'Super Fund') THEN 1 ELSE 0 END AS TrustInEntityType
		  --, CASE WHEN B.[CddtypeName] = 'Trust' THEN 1 ELSE 0 END AS TrustCDDType
		  , B.[IsTrust] AS isTrust_ori

		  -- GR.17.02 Stichting (Foundation)
		  , CASE WHEN B.[ClientName] like '%stichting%' or B.[ClientName] like '%foundation%' THEN 1 ELSE 0 END AS StInName
		  , CASE WHEN B.[LegalForm] in ('ST','STG.','Stichting') THEN 1 ELSE 0 END AS StInLegalForm
		  , CASE WHEN B.[CddtypeName] = 'Foundations' THEN 1 ELSE 0 END AS StCDDType

		  -- GR.17.03 Stichting Administratiekantoor (STAK) (Trust Office Foundation)
		  , CASE WHEN B.[ClientName] like 'stak %' or B.[ClientName] like '%Stichting Administratie%' THEN 1 ELSE 0 END AS StakInName
		  , CASE WHEN B.[CddtypeName] = 'Stichting Administratiekantoor' THEN 1 ELSE 0 END AS StakCDDType --was added 14/08/2023. Still empty at 15/04/2024

		  -- GR.17.04 CV
		  , CASE WHEN B.[LegalForm] in (
									  'C.V.'
									  , 'COMVN.'
									) THEN 1 
				 ELSE 0 
			END AS CvInLegalForm

		  -- GR.17.05	Limited liability partnership (LLP) and/or Limited Partnership (LP)
		  , CASE  WHEN B.[ClientName] like '%LLP%' 
					or B.[ClientName] like '%LP' 
					or B.[ClientName] like '%L.L.P.%' 
					or B.[ClientName] like '%L.P.%' 
					or (B.[ClientName] like '%partn%' and B.[ClientName] like '%lim%')
					THEN 1 ELSE 0 END AS LlpInName
		  , CASE WHEN B.[LegalForm] in (
									  'LP'
									, 'LP (Limited Partnership)'
									, 'Limited partnership'
									, 'L.P.'
									, 'LLP'
									, 'LLP (Limited liability partnership)'
										) THEN 1 
				 ELSE 0 END AS LlpInLegalForm
		  , CASE WHEN B.[CddtypeName] in (
									    'Limited Liability Limited Partnership'
									  , 'Limited Liability Partnership'
									  , 'Limited Partnership'
										) THEN 1 
				  ELSE 0 END AS LllpInCddType
		  , CASE WHEN B.[EntityType] in ('Limited Liab Partnership') THEN 1 ELSE 0 END AS LlpInEntityType

		  --GR.17.06	Nominee shareholder
		  , CASE 
				WHEN B.[NomineeShareholders] = 'Yes' or B.[NomineeShareholders] = 'Y' THEN 1 
				WHEN B.[NomineeShareholders] = 'No' or B.[NomineeShareholders] = 'N' or B.[NomineeShareholders] is NULL THEN 0 
				ELSE B.[NomineeShareholders]
			END AS IsNomineeShareholders
		 , B.NomineeShareholders

		  --GR.17.07	Bearer shares
		  , CASE 
				WHEN B.[IssuesBearerShares] = 'Yes' or B.[IssuesBearerShares] = 'Y' THEN 1 
				WHEN B.[IssuesBearerShares] = 'No' or B.[IssuesBearerShares] = 'N' or B.[IssuesBearerShares] is NULL THEN 0 
				ELSE B.[IssuesBearerShares]
		    END AS IsIssuesBearerShares
		  , B.IssuesBearerShares
		  , CASE WHEN B.[EntityType] = 'Legal entity that issued bearer shares (excluding companies listed ON a recognized stock exchange)' THEN 1 ELSE 0 END AS BearersharesEntityType

		  , RN = ROW_NUMBER()OVER(PARTITION BY B.[GlobalClientID] ORDER BY CASE WHEN B.[SourceSystem] = 'GCOB' THEN 1 ELSE 2 END ASC)

	  FROM (

			SELECT  CAST(ac.[LocalSystemId] AS NVARCHAR(63)) AS [LocalSystemId]
				  , ac.[SourceSystem]
				  , CAST(ac.[GlobalClientID] AS NVARCHAR(63)) AS [GlobalClientID]

				  , ac.[ClientName]
				  , ac.[LegalForm]
				  , gcob_leprcl.[CddtypeName]
				  , ar.[EntityType]
				  , ac.[PartyType]

				  , gcob_ca.[ClientLifeCycleStatus]
				  , gcob_cl.[CompletedId] AS CurrentCompletedLegalEntityClientId
				  , CAST(gcob_ca.[CaseID] AS NVARCHAR(63)) AS [CaseID]
				  , CAST(gcob_ca.[LegalEntityClientId] AS NVARCHAR(63)) AS [LegalEntityClientId]
				  , gcob_ca.[CaseReviewType]
				  , gcob_ca.[CurrentStatus]
				  , gcob_ca.[ScheduledCompletionDate]
				  , gcob_ca.[NextReviewDate]
				  , gcob_ca_eq.ReviewCounter - gcob_ca.ReviewCounter AS ReviewCounter
				  , NULL AS isTrust
				  , ar.[NomineeShareholders]
				  , ar.[IssuesBearerShares]


				  FROM [GEN].[vw_Client_Src] ac

				  LEFT JOIN (
						SELECT
							  [GcobId]
							, [CompletedId]
							, [LastFullReviewID]
						FROM [gcob].[vw_LE_Client]
						--WHERE gcobid = 17053 --completedid = 73584
						) gcob_cl
					ON CAST(gcob_cl.[GcobId] AS NVARCHAR(63)) = ac.[LocalSystemId]
					AND ac.[SourceSystem] = 'GCOB'

				  LEFT JOIN (
						SELECT
							  [caseID]
							, [GcobId]
							, [ClientLifeCycleStatus]
							, [CaseReviewType]
							, [LegalEntityClientId]
							, [NextReviewDate]
							, [FinalDecisionDate]
							, [ScheduledCompletionDate]
							, [CurrentStatus]
							, ReviewCounter = ROW_NUMBER()OVER(PARTITION BY [GcobId] ORDER BY [LegalEntityClientId] desc)
						FROM [gcob].[vw_LE_Cases]
						WHERE [CurrentStatus] != 'Cancelled'
						--and [GcobId] = 17053 --caseid 73337 gcobid 17053 [LegalEntityClientId] 73584
			
						) gcob_ca
					ON CAST(gcob_ca.[GcobId] AS NVARCHAR(63)) = ac.[LocalSystemId]
					AND ac.[SourceSystem] = 'GCOB'

				  LEFT JOIN (
						SELECT
							  [GcobId]
							, [LegalEntityClientId]
							, ReviewCounter = ROW_NUMBER()OVER(PARTITION BY [GcobId] ORDER BY [LegalEntityClientId] desc)
						FROM [gcob].[vw_LE_Cases]
						WHERE [CurrentStatus] != 'Cancelled'
						--and [LegalEntityClientId] = 73584
						) gcob_ca_eq
					ON gcob_ca_eq.[LegalEntityClientId] = gcob_cl.[CompletedId]
					--AND ac.[SourceSystem] = 'GCOB'

					LEFT JOIN (
						SELECT 
							  pvt.*
						FROM (
								SELECT 
										t1.GcobId
									, t1.LegalEntityClientId
									, CASE 
											WHEN t2.QuestionCode = 'STR-G3.1' THEN 'STR4Layers'
											WHEN t2.QuestionCode like '%ENT-Q1%' THEN 'EntityType'
											WHEN t2.QuestionCode like 'STR%' AND t2.[Text] like '%increased tax integrity risk%' THEN 'StructureTax'
											WHEN t2.QuestionCode like 'STR%' AND (LEFT(t2.[Text],2) ='b)'  OR t2.[Text] ='Is a trust, trust office or equivalent entity part of the organisational structure?') THEN 'StructureTCSP'
											WHEN t2.QuestionCode = 'STR-G3.13' THEN 'StructureOverlyComplex'
											WHEN t2.QuestionCode = 'DIST-G1.1' THEN 'Face2Face Contact'
											WHEN t2.QuestionCode in ('ADVR-Q2','ADVR-Q1') THEN 'AdverseInfo'
											WHEN t2.QuestionCode in ('STR-G3.12' , 'STR-Q1') THEN 'IssuesBearerShares'
											WHEN t2.QuestionCode in ('201CS13','STR-G3.9' ) THEN 'NomineeShareholders'

											--WHEN t2.QuestionCode in ('STR-G3.12' , 'STR-Q1') THEN t2.HasMaterialRiskQuestion
										END AS [Question]
 
										, t1.[Answertext]
										--,t2.HasMaterialRiskQuestion
										FROM radar.Gcob_LE_RiskmodelAnswers t1
										LEFT JOIN [gcob].[GCOB_RM_dbo_Question] t2 ON t1.questionid = t2.id
 
									) AS p1 
							PIVOT(MAX([Answertext]) 
							FOR Question  IN ([STR4Layers],[StructureTax], [StructureTCSP], [StructureOverlyComplex],[Face2Face Contact], [AdverseInfo],[IssuesBearerShares] ,[NomineeShareholders],[EntityType])) AS PVT
		 
					 ) ar
					 ON ar.LegalEntityClientId = GCOB_ca.[LegalEntityClientId]

				  LEFT JOIN gcob.LegalEntity_Party_CDD_Risk_Category_Level gcob_leprcl
				  ON gcob_leprcl.[CaseId] = gcob_ca.[caseID]

				  UNION

				  --------- BRAZIL

				  SELECT
						CAST(GIC.[LocalSystemId] AS NVARCHAR(63)) AS [LocalSystemId]
					  , 'GIC_KN1' AS [SourceSystem]
					  , CAST(GIC.[GlobalClientID] AS NVARCHAR(63)) AS [GlobalClientID]

					  , GIC.[ClientName]
					  , GIC.[LegalForm]
					  , NULL AS [CddtypeName]
					  , NULL AS [EntityType]
					  , GIC.[PartyType]

					  , GIC.[PartyLifeCycleStatus] AS [ClientLifeCycleStatus]
					  , KN1.[CaseId_KN1] AS [CurrentCompletedLegalEntityClientId] --To be reviewed
					  , CAST(KN1.[CaseId_KN1] AS NVARCHAR(63)) AS [CaseID]
					  , KN1.[CaseId_KN1] AS [LegalEntityClientId] --To be reviewed
					  , NULL AS [CaseReviewType]
					  , 'Completed' AS [CurrentStatus]
					  , NULL AS [ScheduledCompletionDate]
					  , KN1.[NextReviewDate]
					  , 0 AS ReviewCounter -- Assign due to having available only the latest completed case
					  , KN1.[IsTrust] AS [IsTrust]
					  , KN1.[NomineeShareholders]
					  , KN1.[IssuesBearerShares]

				  FROM [GEN].[vw_GIC_Client_Src] GIC

				  LEFT JOIN [GEN].[vw_KN1_Cases_Src] KN1
				  ON GIC.[LocalSystemId] = KN1.[LocalSystemId_GIC]

				  UNION

					---------RANZ --'NZ6905884'

				  SELECT
						RANZ_cl.[LocalSystemId]
					  , RANZ_cl.[SourceSystem]
					  , RANZ_cl.[GlobalClientID]

					  , RANZ_cl.[ClientName]
					  , RANZ_cl.[LegalForm]
					  , RANZ_ar.[CddtypeName]
					  , RANZ_ar.[EntityType]
					  , RANZ_cl.[PartyType]

					  , RANZ_cl.[PartyLifeCycleStatus] AS [ClientLifeCycleStatus]
					  , NULL AS CurrentCompletedLegalEntityClientId
					  , RANZ_ar.[CaseID]
					  , NULL AS [LegalEntityClientId]
					  , NULL AS [CaseReviewType]
					  , NULL AS [CurrentStatus]
					  , NULL AS [ScheduledCompletionDate]
					  , NULL AS [NextReviewDate]
					  , 0 AS ReviewCounter
					  , RANZ_ar.[IsTrust] AS isTrust
					  , RANZ_ar.[NomineeShareholders]
					  , RANZ_ar.[IssuesBearerShares]

				  FROM [GEN].[vw_RANZ_Client_Src] AS RANZ_cl --285581

				  INNER JOIN [GEN].[vw_RANZ_AllRisks_Src] AS RANZ_ar
				  on RANZ_cl.[LocalSystemId] = RANZ_ar.[LocalSystemId]
				  and RANZ_cl.[SourceSystem] = RANZ_ar.[LocalSystem]

				  UNION

				  SELECT  
						RAF.[LocalSystemId]
					  , RAF.[SourceSystem]
					  , RAF.[GlobalClientID]

					  , RAF.[ClientName]
					  , RAF.[LegalForm]
					  , NULL AS [CddtypeName]
					  , NULL AS [EntityType]
					  , RAF.[PartyType]

					  , RAF.[PartyLifeCycleStatus] AS [ClientLifeCycleStatus]
					  , NULL AS CurrentCompletedLegalEntityClientId
					  , NULL AS [CaseID]
					  , NULL AS [LegalEntityClientId]
					  , NULL AS [CaseReviewType]
					  , NULL AS [CurrentStatus]
					  , NULL AS [ScheduledCompletionDate]
					  , NULL AS [NextReviewDate]
					  , 0 AS ReviewCounter
					  , NULL AS isTrust
					  , NULL AS [NomineeShareholders]
					  , NULL AS [IssuesBearerShares]

				    FROM [GEN].[vw_RAF_Client_Src] RAF

					) B

			

			) A

	WHERE RN = 1;"
