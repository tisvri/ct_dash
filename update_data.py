# import pandas as pd # type: ignore
# import glob
# import ast

# CACHE_DIR = "clinicaltrials_pages"

# files = sorted(glob.glob(f"{CACHE_DIR}/*.parquet"))

# if files:
#     df_final = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
# else:
#     df_final = pd.DataFrame()

# columns_to_select = [
#     'protocolSection.identificationModule.nctId',
#     'protocolSection.identificationModule.officialTitle',
#     'protocolSection.statusModule.startDateStruct.date',
#     'protocolSection.statusModule.studyFirstPostDateStruct.date',
#     'protocolSection.statusModule.primaryCompletionDateStruct.date',
#     'protocolSection.sponsorCollaboratorsModule.leadSponsor.name',
#     'protocolSection.sponsorCollaboratorsModule.leadSponsor.class',
#     'protocolSection.conditionsModule.conditions',
#     'protocolSection.designModule.studyType',
#     'protocolSection.designModule.phases',
#     'protocolSection.designModule.enrollmentInfo.count',
#     'protocolSection.armsInterventionsModule.interventions',
#     'protocolSection.contactsLocationsModule.locations',
#     'protocolSection.sponsorCollaboratorsModule.collaborators',
#     'protocolSection.statusModule.overallStatus'
# ]

# df_estudos = df_final.reindex(columns=columns_to_select)

# df_estudos = df_final.to_parquet("studies.parquet", index=False)

import pandas as pd
import glob

CACHE_DIR = "clinicaltrials_pages"

print("Procurando arquivos parquet...")

files = sorted(glob.glob(f"{CACHE_DIR}/*.parquet"))

if not files:
    print("Nenhum arquivo encontrado.")
    df_final = pd.DataFrame()
else:
    print(f"{len(files)} arquivos encontrados.")

    df_final = pd.concat(
        (pd.read_parquet(f) for f in files),
        ignore_index=True
    )

print("Total de registros:", len(df_final))

# ── Colunas necessárias para o dashboard ─────────────────────
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

# ── Seleciona apenas colunas necessárias ─────────────────────
df_estudos = df_final.reindex(columns=columns_to_select)

# ── Remove duplicatas (muito comum na API) ───────────────────
df_estudos = df_estudos.drop_duplicates()

# ── Otimiza tipo numérico ───────────────────────────────────
df_estudos['protocolSection.designModule.enrollmentInfo.count'] = pd.to_numeric(
    df_estudos['protocolSection.designModule.enrollmentInfo.count'],
    errors='coerce'
).astype('float32')

# ── Salva dataset final ─────────────────────────────────────
output_file = "studies.parquet"

df_estudos.to_parquet(
    output_file,
    index=False,
    compression="brotli"
)

# ── Informações finais ──────────────────────────────────────
size_mb = round(df_estudos.memory_usage(deep=True).sum() / 1024**2, 2)

print("Arquivo gerado:", output_file)
print("Linhas:", len(df_estudos))
print("Tamanho estimado:", size_mb, "MB")