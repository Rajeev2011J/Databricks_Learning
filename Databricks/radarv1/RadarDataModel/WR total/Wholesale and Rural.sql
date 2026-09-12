-- To link all to a target Data model
-- ClientStatic
-- Client Risk & Heatmap
-- BusinessActivities (Sectors and naics)
-- Hierarchy (PEP / UBO / GNS related)
-- Products
-- Transactions
-- 

-- vw_AllRisks_Src
"CREATE VIEW [GEN].[vw_AllRisks_Src] AS SELECT * FROM (
SELECT ALLRISK.*
      , ROW_NUMBER () OVER (PARTITION BY [GlobalClientID] ORDER BY CASE WHEN Localsystem ='GCOB' THEN 1 ELSE 2 END ) 'RANK' 
	 , cast(getdate()-1 as date) as ActualDate
  FROM  (   Select
[LocalSystem]
      ,[LocalSystemId]
      ,[GlobalClientID]
      ,[CaseId]
      ,[AdverseInfo]
      ,CAST (NULL AS NVARCHAR(255)) AS [ComplexStructure]
      ,[EntityType]
      ,[FaceToFaceContact]
      ,[IssuesBearerShares]
      ,[NomineeShareholders]
      ,[CddtypeName]
      ,[isNaturalPerson]
      ,[IsTrust]
      ,[IsClientRegulated]
      ,[IsClientListed]
      ,[CommonUBOinOrganizationalLayers]
      ,[HoldsTpaAccounts]
      ,[CDDPreviousRating]
	  FROM
	  [GEN].[vw_RANZ_AllRisks_Src]

	  UNION

Select [LocalSystem]
      ,[LocalSystemId]
      ,[GlobalClientID]
      ,[CaseId]
      ,[AdverseInfo]
     ,CAST (NULL AS NVARCHAR(255)) AS [ComplexStructure]
      ,[EntityType]
      ,[FaceToFaceContact]
      ,[IssuesBearerShares]
      ,[NomineeShareholders]
      ,[CddtypeName]
      ,[isNaturalPerson]
      ,[IsTrust]
      ,[IsClientRegulated]
      ,[IsClientListed]
      ,[CommonUBOinOrganizationalLayers]
      ,[HoldsTpaAccounts]
      ,[CDDPreviousRating]
	  FROM [GEN].[vw_KN1AllRisks_Src]

	  UNION

	  Select [LocalSystem]
      ,[LocalSystemId]
      ,[GlobalClientID]
      ,[CaseId]
      ,[AdverseInfo]
     ,[ComplexStucture]
      ,[EntityType]
      ,[FaceToFaceContact]
      ,[IssuesBearerShares]
      ,[NomineeShareholders]
      ,[CddtypeName]
      ,[isNaturalPerson]
      ,[IsTrust]
      ,[IsClientRegulated]
      ,[IsClientListed]
      ,[CommonUBOinOrganizationalLayers]
      ,[HoldsTpaAccounts]
      ,[CDDPreviousRating]
	  FROM 

[GEN].[vw_GcobLEClientAllRisks_Src]  
) ALLRISK )RISK where RANK=1;"


-- NP mapping?
"CREATE VIEW [GEN].[vw_GCDS-GCOBNP_mapping] AS SELECT

/*

Created by Barend van der Meulen
as a tryout to map Natural persons between GCDS and GCOBNP

2024-04-23

*/
          countGCOB = case when GCOBNP.gcobid is null then 0 else DENSE_RANK() OVER (PARTITION BY case when GCDS.[GlobalClientID] is not null then GCOBNP.gcobid end ORDER BY GCDS.[GlobalClientID] ASC) end + DENSE_RANK() OVER (PARTITION BY GCOBNP.gcobid ORDER BY case when GCDS.[GlobalClientID] is not null and GCOBNP.gcobid is not null then GCDS.[GlobalClientID] end DESC) -1 -- 0  when only NULL, 1 when only 1 unique GCDS.[GlobalClientID]
        , countGCDS = case when GCDS.[GlobalClientID] is null then 0 else DENSE_RANK() OVER (PARTITION BY case when GCOBNP.gcobid is not null then GCDS.[GlobalClientID] end ORDER BY GCOBNP.gcobid ASC) end + DENSE_RANK() OVER (PARTITION BY GCDS.[GlobalClientID] ORDER BY case when GCOBNP.gcobid is not null and GCDS.[GlobalClientID] is not null then GCOBNP.gcobid end DESC) -1 -- 0  when only NULL, 1 when only 1 unique GCOBNP.gcobid

        , GCDS.[GlobalClientID]
        , GCDS.[KeyType]
        , GCDS.[KeyValue]
        , GCDS.[Status]
		--, GCDS.[PersonName]
		, GCOBNP.gcobid
	    --, GCOBNP.[name]
        , GCOBNP.[SystemTypeReferenceId]
        , GCOBNP.[ValueOfIdentifier]
	    --, GCOBNP.[LastName]

--, case 
--					when len(GCOBNP.[LastName_Simplified]) = len(GCDS.[PersonName_Simplified])
--						then GCOBNP.[LastName_Simplified]
--					when len(GCOBNP.[LastName_Simplified]) < len(GCDS.[PersonName_Simplified])
--						then right(GCDS.[PersonName_Simplified], len(GCOBNP.[LastName_Simplified]))
--					when len(GCDS.[PersonName_Simplified]) < len(GCOBNP.[LastName_Simplified])
--						then right(GCOBNP.[LastName_Simplified], len(GCDS.[PersonName_Simplified]))
--				end as bla

--, 				case 
--					when len(GCOBNP.[LastName_Simplified]) = len(GCDS.[PersonName_Simplified])
--						then GCDS.[PersonName_Simplified]
--					when len(GCOBNP.[LastName_Simplified]) < len(GCDS.[PersonName_Simplified])
--						then GCOBNP.[LastName_Simplified]
--					when len(GCDS.[PersonName_Simplified]) < len(GCOBNP.[LastName_Simplified])
--						then GCDS.[PersonName_Simplified]
--				end as blabla

		, case
			when case 
					when len(GCOBNP.[LastName_Simplified]) = len(GCDS.[PersonName_Simplified])
						then GCOBNP.[LastName_Simplified]
					when len(GCOBNP.[LastName_Simplified]) < len(GCDS.[PersonName_Simplified])
						then right(GCDS.[PersonName_Simplified], len(GCOBNP.[LastName_Simplified]))
					when len(GCDS.[PersonName_Simplified]) < len(GCOBNP.[LastName_Simplified])
						then right(GCOBNP.[LastName_Simplified], len(GCDS.[PersonName_Simplified]))
				end
					= 
				case 
					when len(GCOBNP.[LastName_Simplified]) = len(GCDS.[PersonName_Simplified])
						then GCDS.[PersonName_Simplified]
					when len(GCOBNP.[LastName_Simplified]) < len(GCDS.[PersonName_Simplified])
						then GCOBNP.[LastName_Simplified]
					when len(GCDS.[PersonName_Simplified]) < len(GCOBNP.[LastName_Simplified])
						then GCDS.[PersonName_Simplified]
				end
					then 0
				else 1
			end as diffName

FROM (
		SELECT 
				-- ks.[RDR_ID]
				ks.[GlobalClientID]
		--      , ks.[BankCode]
		--      , ks.[CountryCode]
				, ks.[KeyType]
				, case
					when ks.[KeyType] = 'ACBS Chile onshore' then 'ACBS'
					when ks.[KeyType] in ('SBWRR', 'SIEBEL') then 'OLI ClientID (Siebel ID)'
					else ks.[KeyType]
				end as mappedKey
			  ,ks .[KeyValue]
			  --,ks .[RegistrationStatus]
				,ks .[Status]
				, np.[PersonName]
				,		replace(
						replace(
						replace(
						replace(
						lower(np.[PersonName])
						, ',', '')
						, '.', '')
						, ' ', '')
						, '-', '')
						Collate SQL_Latin1_General_CP1253_CI_AI as PersonName_Simplified
		  FROM [gcds].[GCDS_Keystore_DWH] ks

		  inner join [gcds].[GCDS_PartyRole_DWH] pr
		  on pr.[GlobalClientID] = ks.[GlobalClientID]

		  inner join [gcds].[GCDS_Client_DWH] cl
		  on cl.[GlobalClientID] = ks.[GlobalClientID]

		  inner join [gcds].[GCDS_NaturalPerson_DWH] np
		  on np.[GlobalClientID] = ks.[GlobalClientID]

		  where ks.record_end is null
		  and pr.record_end is null
		  and cl.record_end is null
		  and np.record_end is null
		  and pr.[PartyLifeCycleStatus] = 'client'
		  and cl.[PartyType] = 'natural person'
		  and (ks.[Status] is null or ks.[Status] = 'active')
  ) GCDS

  INNER JOIN (
		  SELECT 
				cl.gcobid
		  --    ,ide.[NaturalPersonClientId]
			  ,ref.[name]
			  , case 
					when ref.[name] = 'AFS Relationship Number' then 'AFS'
					when ref.[name] = 'Atlas Utrecht' then 'Atlas UTC'
					when ref.[name] = 'SalesForce Client ID (nCino)' then 'NCINOID'
					ELSE  ref.[name]
				end as mappedkey
			  ,ide.[SystemTypeReferenceId]
			  ,ide.[ValueOfIdentifier]
			  --,ide.[DateAdded]
			  --,ide.[EDL_LOAD_DTS]
			  , npc.[LastName]
			  ,			replace(
						replace(
						replace(
						replace(
						lower(npc.[LastName])
						, ',', '')
						, '.', '')
						, ' ', '')
						, '-', '')
						Collate SQL_Latin1_General_CP1253_CI_AI as LastName_Simplified
		  FROM [gcob].[GCOB_CS_NaturalPerson_NaturalPersonClientIdentifier] ide

		  inner join [gcob].[GCOB_CS_NaturalPerson_SystemTypeReference] ref
		  ON ide.[SystemTypeReferenceId] = ref.[SystemTypeReferenceId]

		  inner JOIN [radar].[NP_Client] cl
		  ON cl.[CompletedId] = ide.[NaturalPersonClientId]

		  LEFT JOIN [gcob].[GCOB_CS_NaturalPerson_NaturalPersonClient] npc
		  ON npc.[GcobId] = cl.[GcobId]

 ) GCOBNP
 on GCOBNP.[ValueOfIdentifier] = GCDS.[KeyValue]
 AND GCOBNP.mappedkey = GCDS.mappedkey;"
