import pandas as pd # type: ignore
import glob
import ast

CACHE_DIR = "clinicaltrials_pages"

files = sorted(glob.glob(f"{CACHE_DIR}/*.parquet"))
df_final = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

map_columns = {
    'protocolSection.identificationModule.nctId': 'NCT Number',
    'protocolSection.identificationModule.officialTitle': 'Study Title',
    'protocolSection.statusModule.startDateStruct.date': 'Start Date',
    'protocolSection.statusModule.studyFirstPostDateStruct.date': 'First Posted',
    'protocolSection.statusModule.primaryCompletionDateStruct.date': 'Completion Date',
    'protocolSection.sponsorCollaboratorsModule.leadSponsor.name': 'Sponsor',
    'protocolSection.sponsorCollaboratorsModule.leadSponsor.class': 'Funder Type',
    'protocolSection.conditionsModule.conditions': 'Conditions',
    'protocolSection.designModule.studyType': 'Study Type',
    'protocolSection.designModule.phases': 'Phases',
    'protocolSection.designModule.enrollmentInfo.count': 'Enrollment',
    'protocolSection.armsInterventionsModule.interventions': 'Intervention Type',
    'protocolSection.contactsLocationsModule.locations': 'Locations',
    'protocolSection.sponsorCollaboratorsModule.collaborators': 'Collaborators',
    'protocolSection.statusModule.overallStatus': 'Study Status'
}

df_final = df_final.rename(columns=map_columns)

df_estudos = df_final.rename(columns=map_columns)
df_estudos = df_final.reindex(columns=map_columns.values())

# col = 'Intervention Type'

# def parse_cell(x):
#     try:
#         if isinstance(x, str):
#             x = ast.literal_eval(x)
#         if isinstance(x, list) and len(x) > 0:
#             return x[0]
#         return {}
#     except:
#         return {}

# intervention_df = pd.json_normalize(
#     df_final[col].apply(parse_cell)
# ).add_prefix('Intervention_')

# df_estudos = pd.concat(
#     [df_final.reset_index(drop=True),
#      intervention_df.reset_index(drop=True)],
#     axis=1
# )
# # df_estudos = df_estudos.astype(str)
# # df_estudos = df_estudos.drop(columns=['Intervention/Intervention Type'])

for date_col in ['Start Date', 'First Posted', 'Completion Date']:
    df_estudos[date_col] = pd.to_datetime(df_estudos[date_col], errors='coerce')


# Tratamento e modelagem da coluna Conditions
df_estudos['Conditions'] = df_estudos['Conditions'].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)

df_estudos['Conditions'] = df_estudos['Conditions'].str.join(', ')
df_estudos['Conditions'] = (
    df_estudos['Conditions']
    .str.strip()
    .str.replace(r'\s+', ' ', regex=True)
    .str.title()
)

# Tratamento e modelagem da coluna Phases
df_estudos['Phases'] = df_estudos['Phases'].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)

df_estudos['Phases'] = df_estudos['Phases'].str.join(', ')

# Tratamento e modelagem das colunas com datas para este formato.
df_estudos['Ano_Inicio'] = df_estudos['Start Date'].dt.year.astype('Int64')
df_estudos = df_estudos[df_estudos['Ano_Inicio'] >= 2020]
df_estudos['Ano_Posted'] = df_estudos['First Posted'].dt.year.astype('Int64')

# salvar dataset final
df_estudos.to_parquet("studies.parquet", index=False)