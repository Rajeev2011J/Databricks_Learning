# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a dataobject to capture the clientownerTypes and their locations and Businessline details.
# MAGIC
# MAGIC #### Authors
# MAGIC - Prasad.Gadidala@rabobank.com
# MAGIC
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC - Reading GCOB,GCDS,GIC dataobjects from GDP
# MAGIC - Fetching the client details and thier locations across different dimensions from different SourceSystems
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
# MAGIC | Rajeev Kumar  | 26-Nov-2025 | 14162593 | 1 GCO per party within Party_ClientOwnership 
# MAGIC | Hariharan AK  | 15-Dec-2025 | 14482013 | System preference for Clientbusinessline code and Clientbusinessline code name has been updated as GCDS
# MAGIC |Sowmya| 26-May-2025| 16072135 | Updated the system preference GCDS-> GCOB->L2->GIC->ncino->others. Modfied the 1 clientowner per party logic and this logic is applied to GCO only|
# MAGIC |Abhishek Jaiswal| 04-June-2026| 16314898 | Include RANZ Data for Party Client Ownership data object

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# COMMAND ----------

# DBTITLE 1,Importing modules from RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ["APP_REG_APP_ID"]
ReadStorage = os.environ["GDP_STORAGE_NAME"]
TenantId = os.environ["TENANT_ID"]
GIC_ReadStorage = os.environ["GDP_SA_STORAGE_NAME"]
environment = os.environ["ENV"]
radar_datamodel_version_number = 3
RANZ_ReadStorage = os.environ['AU_GDP_Defined_Storage_Account']

# COMMAND ----------

# DBTITLE 1,Defining Load_Date and RunType
dbutils.widgets.text("Load_Date", "")
dbutils.widgets.text("RunType", "daily")  # default is daily

Load_Date = dbutils.widgets.get("Load_Date")
RunType = dbutils.widgets.get("RunType").lower()
if Load_Date:
    RunType="historical"
    
if not Load_Date:
    Load_Date = datetime.today().strftime('%Y%m%d')

print(f"Running for Load Date: {Load_Date}")

# COMMAND ----------

# DBTITLE 1,Defining the dataobject name
party_ClientOwnership_dataobject = "Party_ClientOwnership"

# COMMAND ----------

# DBTITLE 1,Defining the SARADAR Storage account
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(SARADAR)
authenticate_storage_account(RANZ_ReadStorage)

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
# Derive the date for which data has to be processed from GDP
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Reading GCDS DataObjects from GDP
# List of datasets from GDP
gcds_df = pd.DataFrame(
    {
        "definedDatasetname": [
            "client_Client",
            "client_KeyStoreKey",
            "client_PartyRole",
            "client_Products",
            "client_ClientOwnersLocal",
            "client_ClientOwnersProduct"
        ]
    }
)

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading GCOB CaseService Dataobjects from GDP
# List of datasets from GDP
load_df = [
            "party_case_client_details",
            "party_AllPartyDetails",
            "party_local_client_Owners",
            "party_products_and_services",
            "party_allcountries"
        ]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# DBTITLE 1,Reading GIC Dataobjects from GDP
# List of datasets from GDP
load_df = pd.DataFrame({"GDPname": [
'pessoa_linha_negocio'
,'pessoa'
,'vwgic_rdl_pessoa_linha_negocio'
,'vwgic_rdl_pessoa_tipo_cadastro'
,'pessoa_email'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df = [
'Legacy2_case_client_details'
,'Legacy2_products_and_services'
,'Legacy2_local_client_Owners'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read RANZ data from GDP defined layer
load_df = [
    'c_b_party_xref',
    'c_b_contract_xref',
    #'c_b_business_line_xref',
    'c_b_contr_rol_party'
]
# Run the load
load_ranz_v4_rdm_tables(load_df, Load_Date)

# COMMAND ----------

from pyspark.sql.functions import explode, array, lit
# Define the mapping between DepartmentCode and BusinessLevelNameL4
GroupbyNameandcode = {
'Global Projects & Other (GL)': ['ABCBK', 'ALPPH', 'BETPH', 'BIOMA', 'BRAPH', 'BRODE', 'CBTGC', 'CBTGG', 'CBTGL', 'CBTGT', 'CBTRC', 'CBTRG', 'CBTRL', 'CBTRT', 'COMPH', 'DELPH', 'ENRIO', 'FGHFG', 'FINAC', 'FINAG', 'FINPH', 'FLINT', 'FOUPH', 'FRTBI', 'FRTBR', 'GAMPH', 'HOARI', 'HOARR', 'HOARW', 'HOFGS', 'LIMOS', 'MADWS', 'MAJRR', 'MISCS', 'MISWS', 'MODAD', 'MSRER', 'MUPPH', 'MUTPR', 'OTRUR', 'OTWHS', 'OVEPH', 'PFTBR', 'PLWAR', 'PREAS', 'PRYIT', 'PYIGS', 'PYIRW', 'PYRRR', 'RADWS', 'RAJRR', 'RCVRN', 'REGPR', 'REGTP', 'REGUA', 'RESNG', 'RISPH', 'RSTGS', 'SDL04', 'SDL06', 'SMSDL', 'SOLMA', 'SOMAN', 'SPREP', 'TAERR', 'TAXEQ', 'TEWSA', 'VATRE', 'WESRW'],
'Deleted Department4': ['ABFCS', 'ATHLO', 'BEAme', 'BISII', 'CCCON', 'CCCOR', 'CCCRR', 'CCKRM', 'CCPYI', 'CCSSC', 'CCWSA', 'CIWSA', 'COFHX', 'Condu', 'EQUTX', 'FGHba', 'FRIBA', 'GLFAR', 'GMCGS', 'IASTD', 'LLIQB', 'LSYNX', 'MMTRD', 'OTHRX', 'OTRNG', 'PPLRR', 'PPLWS', 'PRCGS', 'REBRA', 'RESCC', 'RFLPD', 'RSLEG', 'SECUX', 'VERRN', 'cmser', 'mmsw', 'pracc', 'repoo', 'screl', 'svpmg', 'trmtm'],
'Value Chain Finance (GL)': ['ABFFA', 'ASBAF', 'ASBAM', 'SUFNA', 'TCPIP'],
'Trade & Commodity Finance (GL)': ['ABLEN', 'FIEMA', 'STFIN', 'STFMA', 'TCASL', 'TCCSG', 'TCESL', 'TCFAG', 'TCFCA', 'TCFCO', 'TCFEN', 'TCFFA', 'TCFFI', 'TCFMA', 'TCFME', 'TCFTF', 'TCMSL', 'TCRDI', 'TCSUG'],
'Core Lending (GL)': ['ACFIN', 'ADVIS', 'AFFAS', 'CAPCA', 'CCFAC', 'CCLAC', 'CCSEC', 'CCSIG', 'COLEN', 'CRALN', 'EFFAS', 'EXFIN', 'FIGFA', 'GCSMA', 'GLPGM', 'GRFIN', 'LENNC', 'LFMAN', 'LFROF', 'LPGFA', 'LPGME', 'OFABS', 'OFBSF', 'RECFC', 'RECFN', 'REFWR', 'RMSFA', 'RMSFI', 'RMSOC', 'RMSTM', 'SFMAN', 'SRFIN', 'TCFEF', 'TMIKC', 'TMINL', 'TPREM', 'TRSER'],
'Support (GL)': ['ADMOO', 'ALMRI', 'ALRIS', 'APOLL', 'ASCGS', 'BUREM', 'CARFA', 'CASMT', 'CBITT', 'CBKMS', 'CBSAO', 'CBSCR', 'CBSCS', 'CBSDE', 'CBSFF', 'CBSLA', 'CBSLS', 'CBSMO', 'CBSMS', 'CBSRD', 'CBSSA', 'CBSTS', 'CCRES', 'CDMRU', 'CDMWH', 'CHOST', 'CLSES', 'CMTCR', 'COBFI', 'COBFL', 'COBLC', 'COBRU', 'COBTM', 'COBWH', 'CODPR', 'COFCC', 'COMSV', 'CORIT', 'CORSB', 'CORSP', 'CPANZ', 'CPCBD', 'CPCBS', 'CPCNC', 'CPCRP', 'CPDCI', 'CPDDC', 'CPDED', 'CPEIN', 'CPEMT', 'CPFBR', 'CPFCP', 'CPFGW', 'CPFIM', 'CPITS', 'CPKYC', 'CPLPP', 'CPMFS', 'CPMMR', 'CPMMT', 'CPMRC', 'CPMSC', 'CPMTA', 'CPMTR', 'CPNAM', 'CPOBM', 'CPODE', 'CPOMT', 'CPPSE', 'CPRVS', 'CPTRV', 'CPTSF', 'CRADM', 'CRANA', 'CRBIN', 'CRISP', 'CRISR', 'CROWR', 'CRSPB', 'DACOP', 'DAMAN', 'DBAAA', 'DBSTR', 'DCWEN', 'DECBS', 'DLGRM', 'DPMGT', 'DPSUP', 'DRSUP', 'EDIWS', 'ENNNN', 'ESSIN', 'ESUSY', 'EUASU', 'EXPAT', 'FAASP', 'FACSE', 'FECOT', 'FINRP', 'FIRIC', 'FJAAR', 'FJACL', 'FJADC', 'FJAFF', 'FJAOA', 'FJAVC', 'FJMAN', 'FJSSE', 'FMDAD', 'FMMDI', 'FMSCD', 'FMSDE', 'FMSMS', 'FMSOP', 'FMSTR', 'FOBYT', 'FOSIT', 'FSASU', 'FSPGS', 'FWMNT', 'GAUDT', 'GBFEE', 'GBFSP', 'GBFSU', 'GBFTP', 'GBOTH', 'GBPPO', 'GBSCB', 'GBSNM', 'GBSPR', 'GBSSM', 'GBSTC', 'GCOMP', 'GCONT', 'GCRRK', 'GCSWE', 'GEMIN', 'GFACR', 'GFTIN', 'GLEGL', 'GLFIN', 'GLOPS', 'GLUDC', 'GMGBD', 'GMKRK', 'GNMGT', 'GPTAX', 'GRCON', 'GRCRC', 'GRECA', 'GREFM', 'GREPS', 'GRERO', 'GRMGD', 'GRMRA', 'GRMRI', 'GRMRO', 'GRMRP', 'GRMRR', 'HUMRS', 'INNAC', 'INNCR', 'INNDC', 'INNOV', 'INTSE', 'INVRE', 'IRRCR', 'IRSOP', 'ISDII', 'ISDIN', 'ITACC', 'ITILO', 'ITPRJ', 'ITSEC', 'ITSPM', 'IWOSU', 'KRMCR', 'KYCBR', 'KYCGL', 'KYCMS', 'MAABL', 'MAAIN', 'MIDWR', 'MKTNG', 'MODER', 'MODRS', 'MOEGS', 'MOFXM', 'MORAB', 'MTAPA', 'OCOFI', 'OCSDE', 'OCSFL', 'OCSGM', 'OCSKY', 'OCSMS', 'OITIN', 'OLSUP', 'OPCON', 'OPERB', 'OPGCS', 'OPINI', 'OPMAS', 'OPRCO', 'OPSCS', 'OPSLO', 'OPTSU', 'OSIRR', 'OSIWH', 'PALBL', 'PALPR', 'PCSBL', 'PMGCS', 'PMVDV', 'POFGS', 'PORTF', 'PWSER', 'QCBBA', 'QCBIN', 'QFMBA', 'QFMIN', 'QFRBA', 'QFRIN', 'QUAMA', 'RBSAP', 'RBSCC', 'RBSFC', 'RBSLD', 'RBSLP', 'RBSMS', 'RESCH', 'RESEN', 'RETIT', 'RGMAM', 'RGMAS', 'RGMEU', 'RIMVA', 'RMPRO', 'RRBMA', 'RSCON', 'RSKMT', 'RSKTI', 'RSMAR', 'RSMOF', 'RTLIN', 'SEMAN', 'SERVI', 'SFSBL', 'SIANZ', 'SIBAS', 'SIBRA', 'SICDD', 'SICDM', 'SICDS', 'SICPR', 'SICPS', 'SIFTR', 'SIGWW', 'SIKYC', 'SILPP', 'SINAM', 'SMMNT', 'SOIAM', 'SOSII', 'SPASM', 'STRMT', 'SYFIN', 'SYLOA', 'SYSET', 'SYSIN', 'SYSMM', 'SYSMP', 'SYSMS', 'SYSMT', 'SYSRI', 'SYSRP', 'SYSRW', 'SYSSC', 'SYSTR', 'SYSWR', 'TCFRM', 'TELEC', 'TELEE', 'TPCRD', 'TPEDE', 'TPFIR', 'TPFMM', 'TPRAB', 'TRSBL', 'TSYIN', 'TWOCA', 'ULNIX', 'WCMSP', 'WINDO', 'WRAVM', 'WRDAL', 'WRDDS', 'WRDMW', 'WRDQA', 'WRSIA', 'WSBAP', 'WSBLO', 'WSBLP', 'WSBPM', 'WSBTS', 'WSCRM', 'WSFCC'],
'Retail NL (GL)': ['AFBAN', 'BIZNE', 'DUTDE', 'GROBA', 'IDBBC', 'IDBCC', 'IDBCH', 'IDBCI', 'IDBDM', 'IDBHC', 'IDBIT', 'IDBMC', 'IDBMG', 'IDBMH', 'IDBOH', 'IDBOP', 'IDBRH', 'IDBRM', 'ORNAB', 'OTHRN', 'RAFAC', 'RAFOU', 'RALEA', 'RNIDB', 'ROBCO', 'SMAFI', 'VBNED'],
'Wholesale Other (GL)': ['AIRPA', 'AIRPO', 'BETAV', 'CCSUP', 'CFIRI', 'CRCMO', 'FACOM', 'GCCER', 'GCCMA', 'GCSNC', 'GUSTC', 'IFOOD', 'IKTCL', 'INFGP', 'INNWS', 'NEUND', 'PROEQ', 'PRPRM', 'PSPCO', 'RADPO', 'REGAF', 'RSMAN', 'SWTCH', 'YORKS'],
'Markets (GL)': ['ALAIP', 'BCVAA', 'BCVAL', 'BCVAP', 'BEINS', 'BODER', 'BOTRA', 'CALBC', 'CASES', 'CLPSO', 'CMFIG', 'CMFRM', 'COCTR', 'COMMB', 'COMNM', 'COMPD', 'CORCL', 'COSAM', 'CRTAD', 'CRTAE', 'DCMCO', 'DCMFI', 'DEALC', 'DERCC', 'DRIAD', 'ECOMM', 'EMARK', 'ENVFP', 'EQMGT', 'EQRSH', 'EQUTY', 'ETSAL', 'FICLO', 'FIGCA', 'FIGEM', 'FIGMA', 'FILSL', 'FININ', 'FIRES', 'FIRMT', 'FISAL', 'FISAS', 'FISMA', 'FISTR', 'FIXIS', 'FMMAN', 'FNENG', 'FVAAA', 'FXOIS', 'FXOPT', 'FXTEC', 'GEDEQ', 'GEDFD', 'GFMCO', 'GFMRE', 'GFMRP', 'GFMSU', 'GOVBO', 'GPRSK', 'GRCER', 'HYBRI', 'INGRB', 'LECAM', 'LOSYN', 'MARCO', 'MBDAD', 'MCRNR', 'MCRUR', 'MEDTF', 'MKXVA', 'MMDER', 'MMSWA', 'MOMAR', 'MPSTC', 'MRGDT', 'MTSNC', 'MUDEB', 'NLCRD', 'NWISS', 'OSMAN', 'PMMAN', 'PREXD', 'PRLTR', 'PROMA', 'PROSI', 'PRPLA', 'PRPLI', 'PRTMA', 'RATES', 'RCGMA', 'REGSA', 'REGSR', 'REGSW', 'RELVT', 'REMED', 'REMFC', 'RETMA', 'RICMP', 'RISKR', 'RISSO', 'RITMM', 'RITRM', 'RMAFI', 'RMRMI', 'RSAME', 'RSASI', 'RSAUS', 'RTTRD', 'SALMA', 'SCOFI', 'SCSOL', 'SCTNC', 'SECNC', 'SECSA', 'SECUR', 'SIRRI', 'SPFIN', 'STIRL', 'STRBK', 'STRUC', 'STRUM', 'SUCAE', 'SUCAM', 'TAMCM', 'TPEQY', 'TPGFM', 'TREPO', 'TREVI', 'TRGSL', 'TRSCT', 'TSABE', 'WCGRM', 'WEDER'],
'Rabo Investments (GL)': ['ALSFU', 'AMFUN', 'ANGIS', 'ARBOR', 'ARLBR', 'ARLON', 'ARLOP', 'BEVII', 'BFFGT', 'BFGEN', 'BGLIN', 'BLUES', 'BNLSU', 'BORSK', 'BOULD', 'BOULT', 'BPCOI', 'BRIGH', 'BRVPF', 'BTHRA', 'BUWCO', 'CAVFV', 'COVEN', 'CYRTE', 'DGFDF', 'DICOI', 'DIVCA', 'DPEJC', 'EFUND', 'EMRLD', 'FUGBO', 'FUGFA', 'FUGLS', 'FUNBO', 'FUNCO', 'FUNLS', 'GIBOF', 'GILBO', 'GILEM', 'GILHC', 'GILHS', 'GILHV', 'HAVOA', 'HPREQ', 'IFAFN', 'IFAFU', 'INNIN', 'JABCI', 'JABFT', 'JABFU', 'LANGH', 'LIMVE', 'LVKND', 'MBOGF', 'MBOGT', 'NENDI', 'NORDA', 'NORDI', 'NORDT', 'OTCOI', 'PAIMI', 'PASCH', 'PHRCP', 'PROHF', 'PRVEQ', 'PSPFI', 'PSPHO', 'PSPII', 'PTPNS', 'RAFAR', 'RAPCO', 'RAPST', 'RCIMT', 'RCISP', 'RDINL', 'REEEP', 'RPEDG', 'RPEMI', 'RPERA', 'RPERC', 'RPERI', 'RPERV', 'RPFAF', 'RRGFD', 'SCUPS', 'SHFTI', 'SOFFI', 'SUBDE', 'SUCOI', 'SUMEQ', 'SUPFI', 'SUPII', 'TETFF', 'THUJA', 'WATLI'],
'Client Coverage (GL)': ['BNLCE', 'BNLCO', 'BNLCR', 'BNLEF', 'BNLHT', 'BNLNC', 'BNREF', 'CASAD', 'COLAC', 'COLFA', 'ETCOR', 'ETDEV', 'ETSPC', 'ETTAM', 'ETTRA', 'FASPC', 'FATRA', 'OTCOR', 'OTHDV', 'OTHTH', 'SEREB', 'SPOCO'],
'Treasury (GL)': ['BSMAN', 'CAPIS', 'CAPTR', 'CLOST', 'COINV', 'COLIN', 'CORLB', 'CSACO', 'CSBRI', 'EXLIQ', 'FATRG', 'FUCUA', 'GFCOT', 'GLLBO', 'GLMGT', 'GLOLB', 'IBLBT', 'IDBMB', 'INLBO', 'IRRMG', 'LBBMB', 'MANST', 'MBSEC', 'MMSEC', 'MMSTC', 'MUGIC', 'ONEAL', 'PRCOL', 'PRMET', 'REPAS', 'REPOF', 'RFOUR', 'ROPAR', 'RUOBO', 'SEACC', 'SEFSP', 'STBVP', 'STEFU', 'STRIN', 'TEYEX', 'TMAIN', 'TPOTR', 'TRCOO', 'TRGCC', 'TRGOT', 'TRGPY', 'TRQRM', 'VFMAN', 'VISTA', 'VLTFU', 'WLMOB'],
'Sustainability (GL)': ['BUSDE', 'COSOR', 'GFSYT', 'MANSU', 'POLRR'],
'Corporate Finance (GL)': ['CAECM', 'COFIN', 'CORAD', 'FASBA', 'MAFRI', 'MAOTH', 'REMFO', 'RFSIN', 'RSFSS'],
'Capital W&R (GL)': ['CAPTL', 'CAPTW', 'CFALO', 'CFALW', 'CRIRR'],
'Specialized Lending (GL)': ['CASTR'],
'COO & Support (GL)': ['CCBMT', 'CCBSU', 'CCDCI', 'CCDCO', 'CCKYC', 'CFANZ', 'CFBRA', 'CFCPC', 'CFFCP', 'CFFIN', 'CFGWW', 'CFLPP', 'CFNAM', 'CFOCD', 'CFTRV', 'COSER', 'CRCNC', 'CRFBR', 'CRIES', 'CRMET', 'CRMFS', 'CRMPS', 'CRRVS', 'CRTMR', 'CRTMT', 'CRTSF', 'CRYRI', 'CRYRP', 'CRYSS', 'CRYTR', 'ENGIN', 'FJFFS', 'FJOAS', 'INFIN', 'INSER', 'ITIVV', 'MASUP', 'RTBMN', 'RTWRM', 'SYSCB', 'SYSFM', 'SYSFO', 'SYSLS', 'SYSMR', 'SYSWC', 'SYSWI', 'WOSER'],
'Local Bank Book (GL)': ['CETHE'],
'Colesco (GL)': ['CLCAD', 'CLCSL', 'RBPRD'],
'DLL (GL)': ['DELAL'],
'International Direct Banking (GL)': ['DIBAN', 'IBDII', 'IDBIB', 'INREB'],
'Wholesale Management (GL)': ['DIWHS', 'GPCMA', 'GPRCO', 'PLWHO', 'WCIMA', 'WCNMA', 'WHMGT'],
'Other (GL)': ['ENRON', 'FACOT', 'FIGIF', 'FINAD', 'GRADJ', 'HOADJ', 'HOAMO', 'IASCC', 'LEGIT', 'PYIOT', 'RECFI', 'T1CAP', 'TRPOS'],
'Rural Lending (GL)': ['FARMA', 'RUTRE', 'UCRUB'],
'Other Rural (GL)': ['GFARM', 'INTFI', 'PLRUR', 'RFMIS', 'RRBAM', 'RUDIG', 'RURVI'],
'Global Project Implementation (GL)': ['GPIMP'],
'Grootbedrijf (GL)': ['GRBEC', 'GRECT', 'GROBE', 'PBLSE'],
'Global Retail Banking (GL)': ['GRBER', 'MONAR', 'PPJAY', 'SMEIN', 'TRANG', 'UCBAL', 'UCBAM', 'UCBBB'],
'International Services (GL)': ['INPAS', 'INTES'],
'Obvion (GL)': ['OBMTM'],
'Other Clients and Portfolio Management (GL)': ['OCAPM'],
'Other Wholesale Lending (GL)': ['OWLE'],
'Project Finance (GL)': ['PFFAS', 'PJFIN', 'REIMA'],
'W&R Portfolio Management (GL)': ['PORMA'],
'Input Finance (GL)': ['UCBUB'],
'No department specified': ['UNSD'],
'Unspecified': ['Unspecified'],
'Wholesale Segment Strategy (GL)': ['WHSES']
}
#list comprehension
data = [(business_line, code.strip()) for business_line, codes in GroupbyNameandcode.items() for code in codes]
# Create Spark DataFrame
df = spark.createDataFrame(data, ["BusinessLineName", "Code"])
# Create the temp view
df.createOrReplaceTempView('Aetosrefrencedata')

# COMMAND ----------

from pyspark.sql.functions import explode, array, lit

# Define the mapping between CountryISO and LocationCode
mapping = {
    'AR': ['ARG'],
    'HK': ['HKG', 'ASIA','ASRM'],
    'US': ['ATL', 'DAL', 'USANA','CHI','NARM','NEY','RAF','RDS','RNA','SAF','RSEC'],
    'AU': ['AUS', 'AUSA'],
    'CN': ['BEI', 'CHN', 'CHNDBU', 'SHA', 'SHAU'],
    'BE': ['BEL'],
    'BR': ['BRA', 'BRS', 'USASA'],
    'CA': ['CAN','CARUR'],
    'CL': ['CHL'],
    'CW': ['CUW'],
    'DE': ['DEU'],
    'ES': ['ESP'],
    'FR': ['FRA'],
    'GB': ['GBR','EURA', 'LOG', 'LONR'],
    'NL': ['GLOA', 'HOFE', 'HOFH','HOFA', 'RTN','UOTH','UPE', 'UTR', 'UTRA', 'URF', 'URFO', 'URMB','UTG','FOUND'],
    'HU': ['HUN'],
    'IE': ['IAC', 'IRL'],
    'ID': ['IDN'],
    'IN': ['IND', 'REA'],
    'IT': ['ITA'],
    'JP': ['JPN'],
    'KE': ['KEN'],
    'KR': ['KOR'],
    'MX': ['MEX', 'MEXS'],
    'MY': ['MYS'],
    'NZ': ['NZL'],
    'PE': ['PER'],
    'PL': ['POL', 'PBG'],
    'RU': ['RUS','ZAO'],
    'SG': ['SGP', 'SGPDBU'],
    'TW': ['TAI'],
    'TH': ['THA'],
    'TR': ['TUR', 'TURAS'],
    'VN': ['VNM'],
    'ZA': ['ZAF']
}

# Convert the mapping dictionary to a list of tuples
data = [(iso, loc) for iso, locs in mapping.items() for loc in locs]
# Create Spark DataFrame
df = spark.createDataFrame(data, ["CountryISO", "LocationCode"])
df.createOrReplaceTempView('ISOCode_static_table')
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Transformation to get Party_ClientOwnership dataObject

# COMMAND ----------

# DBTITLE 1,Updating GCOB Case Client Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select
# MAGIC   *,
# MAGIC   case
# MAGIC     when ClientType = 'Legal Entity' then 'Legal Entity'
# MAGIC     when
# MAGIC       ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     then
# MAGIC       'Natural Person'
# MAGIC     else null
# MAGIC   end as Party_type,
# MAGIC   Case
# MAGIC     when ClientType = 'Legal Entity' then concat('GCOB_LEC_', GcobId)
# MAGIC     when
# MAGIC       ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     then
# MAGIC       concat('GCOB_NP_NPPC_', GcobId)
# MAGIC   End as LocalSystemIdentifier
# MAGIC from
# MAGIC   party_case_client_details
# MAGIC where
# MAGIC   CaseStatusName <> 'Cancelled'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Fetching all GCOB Client Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails_Row As
# MAGIC select distinct
# MAGIC   p.GlobalClientOwnerEmail,
# MAGIC   p.GlobalClientOwnerName,
# MAGIC   p.GlobalClientOwnerLocation,
# MAGIC   p.UniquePartyId,
# MAGIC   p.gcdsid,
# MAGIC   p.GcobId,
# MAGIC   p.PartyId,
# MAGIC   p.CaseId,
# MAGIC   case
# MAGIC     when p.ClientType = 'LegalEntityClient' then 'Legal Entity'
# MAGIC     when
# MAGIC       p.ClientType in (
# MAGIC         'NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)'
# MAGIC       )
# MAGIC     then
# MAGIC       'Natural Person'
# MAGIC     when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC     when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person'
# MAGIC     else null
# MAGIC   end as Party_type,
# MAGIC   CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.BusinessLineName as BusinessLine,
# MAGIC   lco.EmailAddress as LocalClientOwnerEmail,
# MAGIC   lco.LocalClientOwnerName,
# MAGIC   lco.Location as LocalClientOwnerLocation,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC       PARTITION BY p.gcobid, p.ClientType
# MAGIC       ORDER BY
# MAGIC         CASE
# MAGIC           WHEN p.ClientLifeCycleStatus = 'Client' THEN 1
# MAGIC           ELSE 2
# MAGIC         END ASC,
# MAGIC         c.CaseCompletedDate DESC
# MAGIC     ) AS ROWNUM
# MAGIC from
# MAGIC   party_AllPartyDetails p
# MAGIC     Inner join Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC     LEFT JOIN party_local_client_Owners lco
# MAGIC       on c.Sourceclient = lco.Sourceclient
# MAGIC     LEFT JOIN party_products_and_services pp
# MAGIC       on c.Sourceclient = pp.Sourceclient
# MAGIC where
# MAGIC   Status = 'Live'

# COMMAND ----------

# DBTITLE 1,Fetching all GCOB Party Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllParty As
# MAGIC with gcob as 
# MAGIC (
# MAGIC select distinct
# MAGIC   p.GlobalClientOwnerEmail,
# MAGIC   p.GlobalClientOwnerName,
# MAGIC   p.GlobalClientOwnerLocation,
# MAGIC   p.UniquePartyId,
# MAGIC   p.gcdsid,
# MAGIC   p.CaseId,
# MAGIC   case
# MAGIC     when p.ClientType = 'LegalEntityClient' then 'Legal Entity'
# MAGIC     when
# MAGIC       p.ClientType in (
# MAGIC         'NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)'
# MAGIC       )
# MAGIC     then
# MAGIC       'Natural Person'
# MAGIC     when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC     when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person'
# MAGIC     else null
# MAGIC   end as Party_type,
# MAGIC   CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.BusinessLineName as BusinessLine,
# MAGIC   lco.EmailAddress as LocalClientOwnerEmail,
# MAGIC   lco.LocalClientOwnerName,
# MAGIC   lco.Location as LocalClientOwnerLocation,
# MAGIC   c.SalesforceClientID_nCino,
# MAGIC   c.gcobid as ncino_gcobid,
# MAGIC   p.gcobid
# MAGIC   ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as CaseStatusName
# MAGIC from
# MAGIC   party_AllPartyDetails p
# MAGIC     LEFT JOIN Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC     LEFT JOIN party_local_client_Owners lco
# MAGIC       on c.Sourceclient = lco.Sourceclient
# MAGIC where
# MAGIC   Status = 'Live'
# MAGIC   --and (p.IsLatestApprovedVersionofclient = 'True' or p.IsLatestApprovedVersionofclient is null )
# MAGIC )
# MAGIC select * from gcob
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Logic to have Unique GCOB ids
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails As
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Gcob_AllPartyDetails_Row
# MAGIC

# COMMAND ----------

# DBTITLE 1,Legacy2 All Party Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = TRUE and GcobCaseId is null and Value is null and GcobId like 'RA:%';
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2 AS
# MAGIC select distinct 
# MAGIC c.clientId,SalesforceClientID_nCino
# MAGIC ,CASE
# MAGIC   WHEN c.IsClient = 'true' and  c.ClientTypeId = 1 THEN concat('LE_', GcobId)
# MAGIC   WHEN c.IsClient = 'true' and c.ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN c.IsClient = 'false' and c.ClientTypeId = 1 THEN concat('RLE_', GcobId)
# MAGIC   WHEN c.IsClient = 'false' and c.ClientTypeId in (2,3) THEN concat('RNP_', GcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC ,case when c.ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when c.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when c.ClientType = 'Related Legal Entity' then 'Related Legal Entity' 
# MAGIC       when c.ClientType = 'Related Natural Person' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,concat('LEGACY2_',Legacy2_Identifier) as LocalSystemIdentifier
# MAGIC ,c.GlobalClientOwner as GlobalClientOwnerName
# MAGIC ,c.GlobalClientOwnerEmail
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,c.BusinessLineName as BusinessLine
# MAGIC ,lco.LocalOwnerEmailAddress as LocalClientOwnerEmail
# MAGIC ,lco.LocalClientOwnerName
# MAGIC ,lco.LocalClientOwnerCountry as LocalClientOwnerLocation
# MAGIC from Legacy2_client c
# MAGIC LEFT JOIN Legacy2_local_client_Owners lco on c.ClientId = lco.ClientId
# MAGIC     LEFT JOIN Legacy2_products_and_services pp on c.ClientId = pp.ClientId
# MAGIC where ClientTypeId in (1,2,3)

# COMMAND ----------

# DBTITLE 1,Fetching GIC Client information
# MAGIC %sql
# MAGIC Create or replace temporary view GIC_ClientDetails As
# MAGIC With BusinessLine as
# MAGIC (
# MAGIC select distinct 
# MAGIC vng.COD_INSTITUCIONAL
# MAGIC ,vng.DES_LINHA_NEGOCIO as BusinessLine
# MAGIC From vwgic_rdl_pessoa_linha_negocio vng 
# MAGIC LEFT JOIN vwgic_rdl_pessoa_tipo_cadastro cad on cad.SEQ_PESSOA = vng.SEQ_PESSOA 
# MAGIC where vng.DTA_DESATIVACAO is null
# MAGIC and cad.SEQ_TIPO_CADASTRO = 1 
# MAGIC and SEQ_STATUS_TIPO_CADASTRO in (1,3)
# MAGIC ),
# MAGIC ActiveEmails as 
# MAGIC (  
# MAGIC  SELECT 
# MAGIC  pe.SEQ_PESSOA,
# MAGIC  pe.DES_EMAIL,
# MAGIC  ROW_NUMBER() OVER (
# MAGIC     PARTITION BY pe.SEQ_PESSOA 
# MAGIC     ORDER BY pe.FLG_PREFERENCIAL DESC, pe.DES_EMAIL ASC
# MAGIC     ) AS rn
# MAGIC  FROM pessoa_email pe
# MAGIC  WHERE pe.DTA_DESATIVACAO IS NULL  -- Only active emails
# MAGIC )
# MAGIC select distinct
# MAGIC   p.COD_INSTITUCIONAL,
# MAGIC   CASE
# MAGIC     WHEN p.SEQ_TIPO_PESSOA = '2' THEN 'Natural Person'
# MAGIC     else 'Legal Entity'
# MAGIC   END AS Party_type,
# MAGIC   CONCAT('GIC_', p.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC   ,'Rabobank Brazil' as GlobalClientOwnerLocation
# MAGIC   ,gco.NOM_COMPLETO as GlobalClientOwnerName
# MAGIC   ,ae.DES_EMAIL as GlobalClientOwnerEmail
# MAGIC   ,b.BusinessLine
# MAGIC from
# MAGIC   pessoa p
# MAGIC   LEFT JOIN PESSOA_LINHA_NEGOCIO ng on p.SEQ_PESSOA = ng.SEQ_PESSOA
# MAGIC   LEFT JOIN pessoa gco  on ng.SEQ_PESSOA_RM = gco.SEQ_PESSOA
# MAGIC   --LEFT JOIN pessoa_email e on gco.SEQ_PESSOA = e.SEQ_PESSOA
# MAGIC   LEFT JOIN ActiveEmails ae on gco.SEQ_PESSOA = ae.SEQ_PESSOA AND ae.rn = 1 -- Take only one email per person
# MAGIC   LEFT JOIN BusinessLine b on p.COD_INSTITUCIONAL = b.COD_INSTITUCIONAL 

# COMMAND ----------

# DBTITLE 1,Fetching GCDS Client information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCDS_Clients AS
# MAGIC SELECT DISTINCT
# MAGIC   k.KeyStore_value AS identifier,
# MAGIC   k.KeyStore_type,
# MAGIC   c.GCID,
# MAGIC   c.Party_type,
# MAGIC   c.`Global_CO-name` AS GlobalClientOwnerName,
# MAGIC   c.`Global_CO-email` AS GlobalClientOwnerEmail,
# MAGIC   c.`Global_CO-location` AS GlobalClientOwnerLocation,
# MAGIC   iso.LocationCode AS GlobalClientOwnerLocationCode,
# MAGIC   iso.CountryISO AS GlobalClientOwnerLocationCountryISOCode,
# MAGIC   c.`Global_CO-businessline_code` AS GlobalClientOwnerBusinesslinecode,
# MAGIC   ar.BusinessLineName AS GlobalClientOwnerBusinessLineName,
# MAGIC   lc.`Local_CO-Name` AS LocalClientOwnerName,
# MAGIC   lc.`Local_CO-email` AS LocalClientOwnerEmail,
# MAGIC   lc.`Local_CO-location` AS LocalClientOwnerLocation,
# MAGIC   iso1.LocationCode  AS LocalClientOwnerLocationCode,
# MAGIC   iso1.CountryISO AS LocalClientOwnerLocationCountryISOCode,
# MAGIC   lc.`Local_CO-businessline` AS LocalClientOwnerBusinesslinecode,
# MAGIC   ar1.BusinessLineName AS LocalClientOwneBusinessLineName,
# MAGIC   pc.`Local_CO-name` AS ProdcutClientOwnerName,
# MAGIC   pc.`Local_CO-email` AS ProdcutClientOwnerEmail,
# MAGIC   pc.`Local_CO-location` AS ProdcutClientOwnerLocation,
# MAGIC   iso2.LocationCode  AS ProdcutClientOwnerLocationCode,
# MAGIC   iso2.CountryISO AS ProdcutClientOwnerCountryISOLocationCode,
# MAGIC   pc.`Local_CO-businessline` AS ProdcutClientOwneBusinesslinecode,
# MAGIC   ar2.BusinessLineName AS ProdcutClientOwnerBusinessLineName,
# MAGIC   c.`RM-email` AS RelationMangerEmail,
# MAGIC   c.`RM-name` AS RelationMangerName,
# MAGIC   c.`RM-location` AS RelationMangerLocation,
# MAGIC   iso3.LocationCode AS RelationMangerLocationCode,
# MAGIC   iso3.CountryISO as RelationMangerLocationCountryISOCode,
# MAGIC   c.`RM-businessline_code` AS RelationMangerBusinesslinecode,
# MAGIC   ar3.BusinessLineName AS RelationMangerBusinessLineName
# MAGIC FROM client_KeyStoreKey k
# MAGIC INNER JOIN client_Client c ON c.GCID = k.GCID
# MAGIC LEFT JOIN client_ClientOwnersLocal lc ON lc.GCID = c.GCID
# MAGIC LEFT JOIN client_ClientOwnersProduct pc ON pc.GCID = c.GCID
# MAGIC LEFT JOIN Aetosrefrencedata ar ON ar.code = c.`Global_CO-businessline_code`
# MAGIC LEFT JOIN Aetosrefrencedata ar1 ON ar1.code = lc.`Local_CO-businessline`
# MAGIC LEFT JOIN Aetosrefrencedata ar2 ON ar2.code = pc.`Local_CO-businessline`
# MAGIC LEFT JOIN Aetosrefrencedata ar3 ON ar3.code = c.`RM-businessline_code`
# MAGIC LEFT join ISOCode_static_table iso on iso.LocationCode=c.`Global_CO-location_code`
# MAGIC LEFT join ISOCode_static_table iso1 on iso1.LocationCode=lc.`Local_CO-location_code`
# MAGIC LEFT join ISOCode_static_table iso2 on iso2.LocationCode=pc.`Local_CO-Location_code`
# MAGIC LEFT join ISOCode_static_table iso3 on iso3.LocationCode=c.`RM-location_code`
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Fetching RANZ Client information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RANZ_Clients AS
# MAGIC select distinct
# MAGIC CONCAT('RANZ_',cbp.PKEY_SRC_OBJECT) LocalSystemIdentifier
# MAGIC ,cbp.SRC_PARTY_ID
# MAGIC ,Ranz_Contract.ACCNT_MGR as LocalClientOwnerEmail
# MAGIC ,Ranz_Contract.RANZG_BR_CD as LocalClientOwnerLocationCode
# MAGIC ,Case when Ranz_Contract.LOB_CD = 'RD' then 'ROS' else Ranz_Contract.LOB_CD end as LocalClientOwnerBusinessLineCode
# MAGIC ,Case 
# MAGIC     when Ranz_Contract.LOB_CD = 'CB' then 'Country Banking'
# MAGIC     when Ranz_Contract.LOB_CD = 'RD' then 'Rabo Online Savings'
# MAGIC     else null
# MAGIC end as LocalClientOwnerBusinessLineName
# MAGIC --,b.BUSINESS_LINE_DISP as LocalClientOwnerBusinessLineName
# MAGIC FROM c_b_party as cbp
# MAGIC INNER JOIN c_b_contr_rol_party as crp
# MAGIC     ON crp.FK_PARTY_ID = cbp.ROWID_XREF
# MAGIC INNER JOIN c_b_contract Ranz_Contract on Ranz_Contract.CONTR_ID=crp.FK_CONTR_ID
# MAGIC --lEFT JOIN C_B_BUSINESS_LINE b ON Ranz_Contract.LOB_CD=b.BUS_LINE_CD

# COMMAND ----------

# DBTITLE 1,get the all Clients details for Parties present in GCDS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCDS_ClientsOwners AS
# MAGIC WITH BaseCoverage AS (
# MAGIC   SELECT
# MAGIC     CASE 
# MAGIC       WHEN gcds.identifier = gcob.GcobId AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID'
# MAGIC         THEN gcob.LocalSystemIdentifier
# MAGIC       WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person'
# MAGIC         THEN ncino.LocalSystemIdentifier
# MAGIC       WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC'
# MAGIC         THEN gic.LocalSystemIdentifier
# MAGIC       WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC         THEN l2.LocalSystemIdentifier
# MAGIC       WHEN gcds.identifier = ranz.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ'
# MAGIC         THEN ranz.LocalSystemIdentifier
# MAGIC       ELSE CONCAT('GCDS_', gcds.GCID)
# MAGIC     END AS LocalSystemIdentifier,
# MAGIC       CASE WHEN gcds.identifier = gcob.GcobId AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID'
# MAGIC        THEN 'GCOB'
# MAGIC        WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person'
# MAGIC        THEN 'GCOB'
# MAGIC        WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC'
# MAGIC        THEN 'GIC'
# MAGIC        WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC        THEN 'Legacy2'
# MAGIC        WHEN gcds.identifier = ranz.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ'
# MAGIC         THEN 'RANZ'
# MAGIC        Else 'GCDS'
# MAGIC   END AS Application,
# MAGIC     COALESCE(gcds.GlobalClientOwnerName,gcob.GlobalClientOwnerName,l2.GlobalClientOwnerName, gic.GlobalClientOwnerName, ncino.GlobalClientOwnerName ) AS GlobalClientOwnerName,
# MAGIC     COALESCE(gcds.GlobalClientOwnerEmail,gcob.GlobalClientOwnerEmail,l2.GlobalClientOwnerEmail, gic.GlobalClientOwnerEmail,ncino.GlobalClientOwnerEmail ) AS GlobalClientOwnerEmail,
# MAGIC     --COALESCE(gcob.GlobalClientOwnerLocation, ncino.GlobalClientOwnerLocation, gic.GlobalClientOwnerLocation, l2.GlobalClientOwnerLocation, gcds.GlobalClientOwnerLocation) AS GlobalClientOwnerLocation,
# MAGIC     gcds.GlobalClientOwnerLocationCode,
# MAGIC     gcds.GlobalClientOwnerLocationCountryISOCode,
# MAGIC     gcds.GlobalClientOwnerBusinesslinecode,
# MAGIC     gcds.GlobalClientOwnerBusinessLineName,
# MAGIC     gcds.LocalClientOwnerName,
# MAGIC     COALESCE(gcds.LocalClientOwnerEmail,ranz.LocalClientOwnerEmail) as LocalClientOwnerEmail,
# MAGIC     --gcds.LocalClientOwnerLocation,
# MAGIC     COALESCE(gcds.LocalClientOwnerLocationCode,ranz.LocalClientOwnerLocationCode) as LocalClientOwnerLocationCode,
# MAGIC     gcds.LocalClientOwnerLocationCountryISOCode,
# MAGIC     COALESCE(gcds.LocalClientOwnerBusinesslinecode,ranz.LocalClientOwnerBusinessLineCode) as LocalClientOwnerBusinesslinecode,
# MAGIC     COALESCE(gcds.LocalClientOwneBusinessLineName,ranz.LocalClientOwnerBusinessLineName) as LocalClientOwneBusinessLineName,
# MAGIC     gcds.ProdcutClientOwnerName,
# MAGIC     gcds.ProdcutClientOwnerEmail,
# MAGIC     --gcds.ProdcutClientOwnerLocation,
# MAGIC     gcds.ProdcutClientOwnerLocationCode,
# MAGIC     gcds.ProdcutClientOwnerCountryISOLocationCode,
# MAGIC     gcds.ProdcutClientOwneBusinesslinecode,
# MAGIC     gcds.ProdcutClientOwnerBusinessLineName,
# MAGIC     gcds.RelationMangerEmail,
# MAGIC     gcds.RelationMangerName,
# MAGIC     --gcds.RelationMangerLocation,
# MAGIC     gcds.RelationMangerLocationCode,
# MAGIC     gcds.RelationMangerLocationCountryISOCode,
# MAGIC     gcds.RelationMangerBusinesslinecode,
# MAGIC     gcds.RelationMangerBusinessLineName
# MAGIC   FROM GCDS_Clients gcds
# MAGIC     LEFT JOIN Gcob_AllPartyDetails gcob
# MAGIC       ON CAST(gcds.identifier AS STRING) = CAST(gcob.GcobId AS STRING)
# MAGIC       AND gcob.Party_type = 'Legal Entity'
# MAGIC       AND gcds.KeyStore_type = 'GCOBID'
# MAGIC     LEFT JOIN Gcob_CaseClientDetails c
# MAGIC       ON CAST(c.SalesforceClientID_nCino AS STRING) = gcds.identifier
# MAGIC       AND gcds.KeyStore_type = 'NCINOID'
# MAGIC     LEFT JOIN Gcob_AllPartyDetails ncino
# MAGIC       ON CAST(c.ClientId AS STRING)= CAST(ncino.PartyId AS STRING)
# MAGIC       AND ncino.Party_type = 'Natural Person'
# MAGIC     LEFT JOIN GIC_ClientDetails gic
# MAGIC       ON gcds.identifier = CAST( gic.COD_INSTITUCIONAL AS STRING)
# MAGIC       AND gcds.KeyStore_type = 'GIC'
# MAGIC     LEFT JOIN Legacy2 l2 
# MAGIC       ON CAST(l2.SalesforceClientID_nCino AS STRING) = gcds.identifier 
# MAGIC       AND l2.Party_type = gcds.Party_type
# MAGIC       AND gcds.KeyStore_type = 'NCINOID'
# MAGIC     LEFT JOIN RANZ_Clients ranz 
# MAGIC       ON gcds.identifier = CAST(ranz.SRC_PARTY_ID AS STRING)
# MAGIC       and gcds.KeyStore_type = 'CB RANZ' 
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   LocalSystemIdentifier,
# MAGIC   Application,
# MAGIC   GlobalClientOwnerName,
# MAGIC   GlobalClientOwnerEmail,
# MAGIC   --GlobalClientOwnerLocation,
# MAGIC   GlobalClientOwnerLocationCode,
# MAGIC   GlobalClientOwnerLocationCountryISOCode,
# MAGIC   GlobalClientOwnerBusinesslinecode,
# MAGIC   GlobalClientOwnerBusinessLineName,
# MAGIC   LocalClientOwnerName,
# MAGIC   LocalClientOwnerEmail,
# MAGIC   --LocalClientOwnerLocation,
# MAGIC   LocalClientOwnerLocationCode,
# MAGIC   LocalClientOwnerLocationCountryISOCode,
# MAGIC   LocalClientOwnerBusinesslinecode,
# MAGIC   LocalClientOwneBusinessLineName,
# MAGIC   ProdcutClientOwnerName,
# MAGIC   ProdcutClientOwnerEmail,
# MAGIC   --ProdcutClientOwnerLocation,
# MAGIC   ProdcutClientOwnerLocationCode,
# MAGIC   ProdcutClientOwnerCountryISOLocationCode,
# MAGIC   ProdcutClientOwneBusinesslinecode,
# MAGIC   ProdcutClientOwnerBusinessLineName,
# MAGIC   RelationMangerEmail,
# MAGIC   RelationMangerName,
# MAGIC   --RelationMangerLocation,
# MAGIC   RelationMangerLocationCode,
# MAGIC   RelationMangerLocationCountryISOCode,
# MAGIC   RelationMangerBusinesslinecode,
# MAGIC   RelationMangerBusinessLineName
# MAGIC FROM BaseCoverage
# MAGIC

# COMMAND ----------

# DBTITLE 1,GCOB Clientowners data for Non GCDS Parties
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_Clientowners_NonGCDS AS
# MAGIC WITH GCOB AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'GCOB' AS Application,
# MAGIC     t1.GlobalClientOwnerName,
# MAGIC     t1.GlobalClientOwnerEmail,
# MAGIC     --t1.GlobalClientOwnerLocation,
# MAGIC     NULL AS GlobalClientOwnerLocationCode,
# MAGIC     NULL AS GlobalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS GlobalClientOwnerBusinesslinecode,
# MAGIC     t1.BusinessLine AS GlobalClientOwnerBusinessLineName,
# MAGIC     t1.LocalClientOwnerName,
# MAGIC     t1.LocalClientOwnerEmail,
# MAGIC    -- t1.LocalClientOwnerLocation,
# MAGIC     NULL AS LocalClientOwnerLocationCode,
# MAGIC     Null as LocalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS LocalClientOwnerBusinesslinecode,
# MAGIC     NULL AS LocalClientOwneBusinessLineName,
# MAGIC     NULL AS ProductClientOwnerName,
# MAGIC     NULL AS ProductClientOwnerEmail,
# MAGIC     --NULL AS ProductClientOwnerLocation,
# MAGIC     NULL AS ProductClientOwnerLocationCode,
# MAGIC     NULL AS ProdcutClientOwnerCountryISOLocationCode,
# MAGIC     NULL AS ProductClientOwneBusinesslinecode,
# MAGIC     NULL AS ProductClientOwnerBusinessLineName,
# MAGIC     NULL AS RelationMangerEmail,
# MAGIC     NULL AS RelationMangerName,
# MAGIC     --NULL AS RelationMangerLocation,
# MAGIC     NULL AS RelationMangerLocationCode,
# MAGIC     Null as RelationMangerLocationCountryISOCode,
# MAGIC     NULL AS RelationMangerBusinesslinecode,
# MAGIC     NULL AS RelationMangerBusinessLineName,
# MAGIC     t1.Party_Type,
# MAGIC     t1.CaseId
# MAGIC   FROM Gcob_AllParty t1
# MAGIC   WHERE t1.Party_Type <> 'Natural Person'
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'GCOB' AS Application,
# MAGIC     t1.GlobalClientOwnerName,
# MAGIC     t1.GlobalClientOwnerEmail,
# MAGIC     --t1.GlobalClientOwnerLocation,
# MAGIC     NULL AS GlobalClientOwnerLocationCode,
# MAGIC     NUll as GlobalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS GlobalClientOwnerBusinesslinecode,
# MAGIC     t1.BusinessLine AS GlobalClientOwnerBusinessLineName,
# MAGIC     t1.LocalClientOwnerName,
# MAGIC     t1.LocalClientOwnerEmail,
# MAGIC     --t1.LocalClientOwnerLocation,
# MAGIC     NULL AS LocalClientOwnerLocationCode,
# MAGIC     NULL AS LocalClientOwnerCountryISOLocationCode,
# MAGIC     NULL AS LocalClientOwnerBusinesslinecode,
# MAGIC     NULL AS LocalClientOwneBusinessLineName,
# MAGIC     NULL AS ProductClientOwnerName,
# MAGIC     NULL AS ProductClientOwnerEmail,
# MAGIC     --NULL AS ProductClientOwnerLocation,
# MAGIC     NULL AS ProductClientOwnerLocationCode,
# MAGIC     NULL AS ProductClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS ProductClientOwneBusinesslinecode,
# MAGIC     NULL AS ProductClientOwnerBusinessLineName,
# MAGIC     NULL AS RelationMangerEmail,
# MAGIC     NULL AS RelationMangerName,
# MAGIC     --NULL AS RelationMangerLocation,
# MAGIC     NULL AS RelationMangerLocationCode,
# MAGIC     NULL AS RelationMangerLocationCountryISOCode,
# MAGIC     NULL AS RelationMangerBusinesslinecode,
# MAGIC     NULL AS RelationMangerBusinessLineName,
# MAGIC     t1.Party_Type,
# MAGIC     t1.CaseId
# MAGIC   FROM Gcob_AllParty t1
# MAGIC   WHERE t1.Party_Type = 'Natural Person'
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   LocalSystemIdentifier,
# MAGIC   Application,
# MAGIC   GlobalClientOwnerName,
# MAGIC   GlobalClientOwnerEmail,
# MAGIC   --GlobalClientOwnerLocation,
# MAGIC   GlobalClientOwnerLocationCode,
# MAGIC   GlobalClientOwnerLocationCountryISOCode,
# MAGIC   GlobalClientOwnerBusinesslinecode,
# MAGIC   GlobalClientOwnerBusinessLineName,
# MAGIC   LocalClientOwnerName,
# MAGIC   LocalClientOwnerEmail,
# MAGIC   --LocalClientOwnerLocation,
# MAGIC   LocalClientOwnerLocationCode,
# MAGIC   LocalClientOwnerLocationCountryISOCode,
# MAGIC   LocalClientOwnerBusinesslinecode,
# MAGIC   LocalClientOwneBusinessLineName,
# MAGIC   ProductClientOwnerName,
# MAGIC   ProductClientOwnerEmail,
# MAGIC   --ProductClientOwnerLocation,
# MAGIC   ProductClientOwnerLocationCode,
# MAGIC   ProdcutClientOwnerCountryISOLocationCode,
# MAGIC   ProductClientOwneBusinesslinecode,
# MAGIC   ProductClientOwnerBusinessLineName,
# MAGIC   RelationMangerEmail,
# MAGIC   RelationMangerName,
# MAGIC   --RelationMangerLocation,
# MAGIC   RelationMangerLocationCode,
# MAGIC   RelationMangerLocationCountryISOCode,
# MAGIC   RelationMangerBusinesslinecode,
# MAGIC   RelationMangerBusinessLineName,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY LocalSystemIdentifier, Party_Type
# MAGIC     ORDER BY CaseId DESC
# MAGIC   ) AS ROWNUM
# MAGIC FROM GCOB

# COMMAND ----------

# DBTITLE 1,ClientsOwners data of Sources not in GCDS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonGCDS_Clientowners AS
# MAGIC WITH NonGCDS AS (
# MAGIC   -- From GCOB_Coverage_NonGCDS
# MAGIC   SELECT
# MAGIC     LocalSystemIdentifier,
# MAGIC     Application,
# MAGIC     GlobalClientOwnerName,
# MAGIC     GlobalClientOwnerEmail,
# MAGIC     --GlobalClientOwnerLocation,
# MAGIC     GlobalClientOwnerLocationCode,
# MAGIC     GlobalClientOwnerLocationCountryISOCode,
# MAGIC     GlobalClientOwnerBusinesslinecode,
# MAGIC     GlobalClientOwnerBusinessLineName,
# MAGIC     LocalClientOwnerName,
# MAGIC     LocalClientOwnerEmail,
# MAGIC     --LocalClientOwnerLocation,
# MAGIC     LocalClientOwnerLocationCode,
# MAGIC     LocalClientOwnerLocationCountryISOCode,
# MAGIC     LocalClientOwnerBusinesslinecode,
# MAGIC     LocalClientOwneBusinessLineName,
# MAGIC     ProductClientOwnerName,
# MAGIC     ProductClientOwnerEmail,
# MAGIC     --ProductClientOwnerLocation,
# MAGIC     ProductClientOwnerLocationCode,
# MAGIC     ProdcutClientOwnerCountryISOLocationCode,
# MAGIC     ProductClientOwneBusinesslinecode,
# MAGIC     ProductClientOwnerBusinessLineName,
# MAGIC     RelationMangerEmail,
# MAGIC     RelationMangerName,
# MAGIC     --RelationMangerLocation,
# MAGIC     RelationMangerLocationCode,
# MAGIC     RelationMangerLocationCountryISOCode,
# MAGIC     RelationMangerBusinesslinecode,
# MAGIC     RelationMangerBusinessLineName,
# MAGIC     ROWNUM
# MAGIC   FROM GCOB_Clientowners_NonGCDS
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   -- From GIC_ClientDetails
# MAGIC   SELECT DISTINCT
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'GIC' AS Application,
# MAGIC     t1.GlobalClientOwnerName,
# MAGIC     t1.GlobalClientOwnerEmail,
# MAGIC     --t1.GlobalClientOwnerLocation,
# MAGIC     NULL AS GlobalClientOwnerLocationCode,
# MAGIC     NULL AS GlobalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS GlobalClientOwnerBusinesslinecode,
# MAGIC     NULL AS GlobalClientOwnerBusinessLineName,
# MAGIC     t1.GlobalClientOwnerName AS LocalClientOwnerName,
# MAGIC     t1.GlobalClientOwnerEmail AS LocalClientOwnerEmail,
# MAGIC     --t1.GlobalClientOwnerLocation AS LocalClientOwnerLocation,
# MAGIC     NULL AS LocalClientOwnerLocationCode,
# MAGIC     NULL AS LocalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS LocalClientOwnerBusinesslinecode,
# MAGIC     NULL AS LocalClientOwneBusinessLineName,
# MAGIC     NULL AS ProductClientOwnerName,
# MAGIC     NULL AS ProductClientOwnerEmail,
# MAGIC     --NULL AS ProductClientOwnerLocation,
# MAGIC     NULL AS ProductClientOwnerLocationCode,
# MAGIC     NULL AS ProdcutClientOwnerCountryISOLocationCode,
# MAGIC     NULL AS ProductClientOwneBusinesslinecode,
# MAGIC     NULL AS ProductClientOwnerBusinessLineName,
# MAGIC     NULL AS RelationMangerEmail,
# MAGIC     NULL AS RelationMangerName,
# MAGIC     --NULL AS RelationMangerLocation,
# MAGIC     NULL AS RelationMangerLocationCode,
# MAGIC     NULL AS RelationMangerLocationCountryISOCode,
# MAGIC     NULL AS RelationMangerBusinesslinecode,
# MAGIC     NULL AS RelationMangerBusinessLineName,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY t1.COD_INSTITUCIONAL, t1.Party_Type
# MAGIC       ORDER BY t1.COD_INSTITUCIONAL DESC
# MAGIC     ) AS ROWNUM
# MAGIC   FROM GIC_ClientDetails t1
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   -- From Legacy2
# MAGIC   SELECT DISTINCT
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'Legacy2' AS Application,
# MAGIC     t1.GlobalClientOwnerName,
# MAGIC     t1.GlobalClientOwnerEmail,
# MAGIC     --t1.GlobalClientOwnerLocation,
# MAGIC     NULL AS GlobalClientOwnerLocationCode,
# MAGIC     NULL AS GlobalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS GlobalClientOwnerBusinesslinecode,
# MAGIC     NULL AS GlobalClientOwnerBusinessLineName,
# MAGIC     t1.LocalClientOwnerName,
# MAGIC     t1.LocalClientOwnerEmail,
# MAGIC    -- t1.LocalClientOwnerLocation,
# MAGIC     NULL AS LocalClientOwnerLocationCode,
# MAGIC     NULL AS LocalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS LocalClientOwnerBusinesslinecode,
# MAGIC     NULL AS LocalClientOwneBusinessLineName,
# MAGIC     NULL AS ProductClientOwnerName,
# MAGIC     NULL AS ProductClientOwnerEmail,
# MAGIC     --NULL AS ProductClientOwnerLocation,
# MAGIC     NULL AS ProductClientOwnerLocationCode,
# MAGIC     NULL AS ProdcutClientOwnerCountryISOLocationCode,
# MAGIC     NULL AS ProductClientOwneBusinesslinecode,
# MAGIC     NULL AS ProductClientOwnerBusinessLineName,
# MAGIC     NULL AS RelationMangerEmail,
# MAGIC     NULL AS RelationMangerName,
# MAGIC     --NULL AS RelationMangerLocation,
# MAGIC     NULL AS RelationMangerLocationCode,
# MAGIC     NULL AS RelationMangerLocationCountryISOCode,
# MAGIC     NULL AS RelationMangerBusinesslinecode,
# MAGIC     NULL AS RelationMangerBusinessLineName,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY t1.Legacy2_Identifier
# MAGIC       ORDER BY t1.clientid DESC
# MAGIC     ) AS ROWNUM
# MAGIC   FROM Legacy2 t1
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   -- From RANZ
# MAGIC   SELECT DISTINCT
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'RANZ' AS Application,
# MAGIC     NULL as GlobalClientOwnerName,
# MAGIC     NULL as GlobalClientOwnerEmail,
# MAGIC     NULL AS GlobalClientOwnerLocationCode,
# MAGIC     NULL AS GlobalClientOwnerLocationCountryISOCode,
# MAGIC     NULL AS GlobalClientOwnerBusinesslinecode,
# MAGIC     NULL AS GlobalClientOwnerBusinessLineName,
# MAGIC     NULL AS LocalClientOwnerName,
# MAGIC     t1.LocalClientOwnerEmail,
# MAGIC     t1.LocalClientOwnerLocationCode,
# MAGIC     NULL AS LocalClientOwnerLocationCountryISOCode,
# MAGIC     t1.LocalClientOwnerBusinessLineCode,
# MAGIC     t1.LocalClientOwnerBusinessLineName as LocalClientOwneBusinessLineName,
# MAGIC     NULL AS ProductClientOwnerName,
# MAGIC     NULL AS ProductClientOwnerEmail,
# MAGIC     NULL AS ProductClientOwnerLocationCode,
# MAGIC     NULL AS ProdcutClientOwnerCountryISOLocationCode,
# MAGIC     NULL AS ProductClientOwneBusinesslinecode,
# MAGIC     NULL AS ProductClientOwnerBusinessLineName,
# MAGIC     NULL AS RelationMangerEmail,
# MAGIC     NULL AS RelationMangerName,
# MAGIC     NULL AS RelationMangerLocationCode,
# MAGIC     NULL AS RelationMangerLocationCountryISOCode,
# MAGIC     NULL AS RelationMangerBusinesslinecode,
# MAGIC     NULL AS RelationMangerBusinessLineName,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY t1.LocalSystemIdentifier
# MAGIC       ORDER BY t1.SRC_PARTY_ID DESC
# MAGIC     ) AS ROWNUM
# MAGIC   FROM RANZ_Clients t1
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   LocalSystemIdentifier,
# MAGIC   Application,
# MAGIC   GlobalClientOwnerName,
# MAGIC   GlobalClientOwnerEmail,
# MAGIC   --GlobalClientOwnerLocation,
# MAGIC   GlobalClientOwnerLocationCode,
# MAGIC   GlobalClientOwnerLocationCountryISOCode,
# MAGIC   GlobalClientOwnerBusinesslinecode,
# MAGIC   GlobalClientOwnerBusinessLineName,
# MAGIC   LocalClientOwnerName,
# MAGIC   LocalClientOwnerEmail,
# MAGIC   --LocalClientOwnerLocation,
# MAGIC   LocalClientOwnerLocationCode,
# MAGIC   LocalClientOwnerLocationCountryISOCode,
# MAGIC   LocalClientOwnerBusinesslinecode,
# MAGIC   LocalClientOwneBusinessLineName,
# MAGIC   ProductClientOwnerName,
# MAGIC   ProductClientOwnerEmail,
# MAGIC   --ProductClientOwnerLocation,
# MAGIC   ProductClientOwnerLocationCode,
# MAGIC   ProdcutClientOwnerCountryISOLocationCode,
# MAGIC   ProductClientOwneBusinesslinecode,
# MAGIC   ProductClientOwnerBusinessLineName,
# MAGIC   RelationMangerEmail,
# MAGIC   RelationMangerName,
# MAGIC   --RelationMangerLocation,
# MAGIC   RelationMangerLocationCode,
# MAGIC   RelationMangerLocationCountryISOCode,
# MAGIC   RelationMangerBusinesslinecode,
# MAGIC   RelationMangerBusinessLineName
# MAGIC FROM NonGCDS
# MAGIC WHERE ROWNUM = 1

# COMMAND ----------

# MAGIC %skip
# MAGIC # to check if there are any data-type conversion errors before attempting to save
# MAGIC display(spark.sql('select * from NonGCDS_Clientowners limit 5'))

# COMMAND ----------

# DBTITLE 1,Comparing GCDS and Non GCDS data
# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_UniqueClients As
# MAGIC select * 
# MAGIC from NonGCDS_Clientowners a
# MAGIC left anti join GCDS_ClientsOwners b
# MAGIC on a.LocalSystemIdentifier = b.LocalSystemIdentifier
# MAGIC

# COMMAND ----------

# DBTITLE 1,Combining GCDS and Non GCDS details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Clientowners AS
# MAGIC SELECT * FROM GCDS_ClientsOwners
# MAGIC UNION
# MAGIC select * from NonGCDS_UniqueClients

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_Clientowners = spark.table("Clientowners")

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_party_Clientowners = add_party_identifier(df_party_Clientowners, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Create table from dataframe
df_party_Clientowners.createOrReplaceTempView('Party_Clientowners')

# COMMAND ----------

# DBTITLE 1,Attribute based on system preference
# MAGIC %sql
# MAGIC Create or replace temporary view SystemPrefAttribute As
# MAGIC WITH ranked_data AS (
# MAGIC SELECT distinct
# MAGIC PartyIdentifier,
# MAGIC GlobalClientOwnerName,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'GCDS' THEN 1
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'GCOB' THEN 2
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'Legacy2' THEN 3
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'GIC' THEN 4
# MAGIC   ELSE 10 END 
# MAGIC ) AS rn_GCO,
# MAGIC GlobalClientOwnerLocationCode,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GlobalClientOwnerLocationCode IS NOT NULL AND Application = 'GCDS' THEN 1
# MAGIC   WHEN GlobalClientOwnerLocationCode IS NOT NULL AND Application = 'GCOB' THEN 2
# MAGIC   WHEN GlobalClientOwnerLocationCode IS NOT NULL AND Application = 'Legacy2' THEN 3
# MAGIC   WHEN GlobalClientOwnerLocationCode IS NOT NULL AND Application = 'GIC' THEN 4
# MAGIC   ELSE 10 END
# MAGIC ) AS rn_GlobalClientOwnerLocation,
# MAGIC GlobalClientOwnerBusinesslinecode,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GlobalClientOwnerBusinesslinecode IS NOT NULL AND Application = 'GCDS' THEN 1
# MAGIC   WHEN GlobalClientOwnerBusinesslinecode IS NOT NULL AND Application = 'GCOB' THEN 2
# MAGIC   WHEN GlobalClientOwnerBusinesslinecode IS NOT NULL AND Application = 'Legacy2' THEN 3
# MAGIC   WHEN GlobalClientOwnerBusinesslinecode IS NOT NULL AND Application = 'GIC' THEN 4
# MAGIC   ELSE 10 END
# MAGIC ) AS rn_GlobalClientOwnerBusinesslinecode,
# MAGIC GlobalClientOwnerBusinessLineName,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GlobalClientOwnerBusinessLineName IS NOT NULL AND Application = 'GCDS' THEN 1
# MAGIC   WHEN GlobalClientOwnerBusinessLineName IS NOT NULL AND Application = 'GCOB' THEN 2
# MAGIC   WHEN GlobalClientOwnerBusinessLineName IS NOT NULL AND Application = 'Legacy2' THEN 3
# MAGIC   WHEN GlobalClientOwnerBusinessLineName IS NOT NULL AND Application = 'GIC' THEN 4
# MAGIC   ELSE 10 END
# MAGIC ) AS rn_GlobalClientOwnerBusinessLineName
# MAGIC -- --BusinessLineName,
# MAGIC -- ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC --   CASE
# MAGIC --   WHEN BusinessLine IS NOT NULL AND Application = 'GCOB' THEN 1
# MAGIC --   WHEN BusinessLine IS NOT NULL AND Application = 'Legacy2' THEN 2
# MAGIC --   WHEN BusinessLine IS NOT NULL AND Application = 'GIC' THEN 3
# MAGIC --   WHEN BusinessLine IS NOT NULL AND Application = 'GCDS' THEN 4
# MAGIC --   ELSE 10 END
# MAGIC -- ) AS rn_BusinessLine,
# MAGIC -- Application
# MAGIC FROM Party_Clientowners
# MAGIC )
# MAGIC
# MAGIC select distinct 
# MAGIC PartyIdentifier
# MAGIC ,MAX(CASE WHEN rn_GCO = 1 THEN GlobalClientOwnerName END) AS GlobalClientOwnerName
# MAGIC ,MAX(CASE WHEN rn_GlobalClientOwnerLocation = 1 THEN GlobalClientOwnerLocationCode END) AS GlobalClientOwnerLocationCode
# MAGIC ,MAX(CASE WHEN rn_GlobalClientOwnerBusinesslinecode = 1 THEN GlobalClientOwnerBusinesslinecode END) AS GlobalClientOwnerBusinesslinecode
# MAGIC ,MAX(CASE WHEN rn_GlobalClientOwnerBusinessLineName = 1 THEN GlobalClientOwnerBusinessLineName END) AS GlobalClientOwnerBusinessLineName
# MAGIC
# MAGIC --,MAX(CASE WHEN rn_BusinessLine = 1 THEN BusinessLine END) AS BusinessLine
# MAGIC from ranked_data
# MAGIC where PartyIdentifier is not null
# MAGIC group by PartyIdentifier

# COMMAND ----------

# DBTITLE 1,Get email for GCO
# MAGIC %sql
# MAGIC Create or replace temporary view GCO_Email As
# MAGIC select distinct
# MAGIC c.PartyIdentifier,
# MAGIC c.Application,
# MAGIC a.GlobalClientOwnerName,
# MAGIC c.GlobalClientOwnerEmail
# MAGIC from Party_ClientOwners c
# MAGIC Inner join SystemPrefAttribute a on c.PartyIdentifier = a.PartyIdentifier and c.GlobalClientOwnerName = a.GlobalClientOwnerName
# MAGIC where c.GlobalClientOwnerName is not null

# COMMAND ----------

# DBTITLE 1,Combine all data with correct attribute
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Party_Clientowner AS
# MAGIC SELECT
# MAGIC   c.PartyIdentifier,
# MAGIC   --c.Application,
# MAGIC   a.GlobalClientOwnerName,
# MAGIC   e.GlobalClientOwnerEmail,
# MAGIC   c.GlobalClientOwnerLocationCode,
# MAGIC   c.GlobalClientOwnerLocationCountryISOCode,
# MAGIC   a.GlobalClientOwnerBusinesslinecode,
# MAGIC   a.GlobalClientOwnerBusinessLineName,
# MAGIC   c.LocalClientOwnerName,
# MAGIC   c.LocalClientOwnerEmail,
# MAGIC   c.LocalClientOwnerLocationCode,
# MAGIC   c.LocalClientOwnerLocationCountryISOCode,
# MAGIC   c.LocalClientOwnerBusinesslinecode,
# MAGIC   c.LocalClientOwneBusinessLineName,
# MAGIC   c.ProdcutClientOwnerName,
# MAGIC   c.ProdcutClientOwnerEmail,
# MAGIC   c.ProdcutClientOwnerLocationCode,
# MAGIC   c.ProdcutClientOwnerCountryISOLocationCode,
# MAGIC   c.ProdcutClientOwneBusinesslinecode,
# MAGIC   c.ProdcutClientOwnerBusinessLineName,
# MAGIC   c.RelationMangerEmail,
# MAGIC   c.RelationMangerName,
# MAGIC   c.RelationMangerLocationCode,
# MAGIC   c.RelationMangerLocationCountryISOCode,
# MAGIC   c.RelationMangerBusinesslinecode,
# MAGIC   c.RelationMangerBusinessLineName
# MAGIC FROM Party_ClientOwners c
# MAGIC LEFT JOIN GCO_Email e 
# MAGIC   ON c.PartyIdentifier = e.PartyIdentifier
# MAGIC LEFT JOIN SystemPrefAttribute a 
# MAGIC   ON c.PartyIdentifier = a.PartyIdentifier

# COMMAND ----------

# DBTITLE 1,Union different client owners  types into single query
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW allClientOwners AS
# MAGIC SELECT DISTINCT
# MAGIC   PartyIdentifier,
# MAGIC   'GlobalClientOwner' AS ClientOwnerType,
# MAGIC   GlobalClientOwnerName AS ClientOwnerName,
# MAGIC   GlobalClientOwnerEmail AS ClientOwnerUPN,
# MAGIC   GlobalClientOwnerLocationCode AS ClientOwnerLocationCode,
# MAGIC   GlobalClientOwnerLocationCountryISOCode AS ClientOwnerLocationCountryISOCode,
# MAGIC   GlobalClientOwnerBusinesslinecode AS ClientOwnerBusinessLineCode,
# MAGIC   GlobalClientOwnerBusinessLineName AS ClientOwnerBusinessLineName
# MAGIC FROM Party_Clientowner
# MAGIC --WHERE GlobalClientOwnerName IS NOT NULL AND GlobalClientOwnerEmail IS NOT NULL 
# MAGIC --AND GlobalClientOwnerEmail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   PartyIdentifier,
# MAGIC   'LocalClientOwner' AS ClientOwnerType,
# MAGIC   LocalClientOwnerName AS ClientOwnerName,
# MAGIC   LocalClientOwnerEmail AS ClientOwnerUPN,
# MAGIC   LocalClientOwnerLocationCode AS ClientOwnerLocationCode,
# MAGIC   LocalClientOwnerLocationCountryISOCode AS ClientOwnerLocationCountryISOCode,
# MAGIC   LocalClientOwnerBusinesslinecode AS ClientOwnerBusinessLineCode,
# MAGIC   LocalClientOwneBusinessLineName AS ClientOwnerBusinessLineName
# MAGIC FROM Party_Clientowner
# MAGIC --WHERE LocalClientOwnerEmail IS NOT NULL --AND LocalClientOwnerName IS NOT NULL
# MAGIC --AND LocalClientOwnerEmail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   PartyIdentifier,
# MAGIC   'ProductClientOwner' AS ClientOwnerType,
# MAGIC   ProdcutClientOwnerName AS ClientOwnerName,
# MAGIC   ProdcutClientOwnerEmail AS ClientOwnerUPN,
# MAGIC   ProdcutClientOwnerLocationCode AS ClientOwnerLocationCode,
# MAGIC   ProdcutClientOwnerCountryISOLocationCode AS ClientOwnerLocationCountryISOCode,
# MAGIC   ProdcutClientOwneBusinesslinecode AS ClientOwnerBusinessLineCode,
# MAGIC   ProdcutClientOwnerBusinessLineName AS ClientOwnerBusinessLineName
# MAGIC FROM Party_Clientowner
# MAGIC --WHERE ProdcutClientOwnerName IS NOT NULL AND ProdcutClientOwnerEmail IS NOT NULL
# MAGIC --AND ProdcutClientOwnerEmail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   PartyIdentifier,
# MAGIC   'RelationshipManager' AS ClientOwnerType,
# MAGIC   RelationMangerName AS ClientOwnerName,
# MAGIC   RelationMangerEmail AS ClientOwnerUPN,
# MAGIC   RelationMangerLocationCode AS ClientOwnerLocation,
# MAGIC   RelationMangerLocationCountryISOCode AS ClientOwnerLocationCountryISOCode,
# MAGIC   RelationMangerBusinesslinecode AS ClientOwnerBusinessLineCode,
# MAGIC   RelationMangerBusinessLineName AS ClientOwnerBusinessLineName
# MAGIC FROM Party_Clientowner
# MAGIC --WHERE RelationMangerName IS NOT NULL AND RelationMangerEmail IS NOT NULL
# MAGIC --AND RelationMangerEmail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATING A FINAL DATAFRAME BY COMBINING ALL THE INFORMATION FROM DIFFERENT SOURCESYSTEMS AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT

# COMMAND ----------

# DBTITLE 1,create Dataframe  for the temp table
df_final_party_Clientowners = spark.table("allClientOwners")

# COMMAND ----------

# MAGIC %skip
# MAGIC # to check if there are any data-type conversion errors before attempting to save
# MAGIC display(df_final_party_Clientowners.limit(100))
# MAGIC

# COMMAND ----------

# DBTITLE 1,Define window partition by PartyIdentifier and ClientOwnerName
window_spec = Window.partitionBy("PartyIdentifier", "ClientOwnerName").orderBy("ClientOwnerUPN")

# COMMAND ----------

# DBTITLE 1,Add row number and filter only first row per group
df_unique_clientowners_gco = (
    df_final_party_Clientowners
    .filter(col('ClientOwnerType')=='GlobalClientOwner')
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_final_unique_clientowners=(df_final_party_Clientowners.filter(col('ClientOwnerType')!='GlobalClientOwner')).union(df_unique_clientowners_gco)

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account

if RunType == "historical":
    save_to_saradar_storage_account(df_final_unique_clientowners, party_ClientOwnership_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_final_unique_clientowners, party_ClientOwnership_dataobject, radar_datamodel_version_number, environment)

# COMMAND ----------


