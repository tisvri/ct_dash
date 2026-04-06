import pandas as pd # type: ignore
import glob
import ast

CACHE_DIR = "clinicaltrials_pages"

files = sorted(glob.glob(f"{CACHE_DIR}/*.parquet"))
df_final = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

columns_to_select = [
    'protocolSection.identificationModule.nctId',
    'protocolSection.identificationModule.officialTitle',
    'protocolSection.statusModule.startDateStruct.date',
    'protocolSection.statusModule.studyFirstPostDateStruct.date',
    'protocolSection.statusModule.primaryCompletionDateStruct.date',
    'protocolSection.sponsorCollaboratorsModule.leadSponsor.name',
    'protocolSection.sponsorCollaboratorsModule.leadSponsor.class',
    'protocolSection.conditionsModule.conditions',
    'protocolSection.designModule.studyType',
    'protocolSection.designModule.phases',
    'protocolSection.designModule.enrollmentInfo.count',
    'protocolSection.armsInterventionsModule.interventions',
    'protocolSection.contactsLocationsModule.locations',
    'protocolSection.sponsorCollaboratorsModule.collaborators',
    'protocolSection.statusModule.overallStatus'
]

df_estudos = df_final.to_parquet("studies.parquet", index=False)