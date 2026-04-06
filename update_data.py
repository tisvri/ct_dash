# import pandas as pd
# import glob

# CACHE_DIR = "clinicaltrials_pages"

# print("Procurando arquivos parquet...")

# files = sorted(glob.glob(f"{CACHE_DIR}/*.parquet"))

# if not files:
#     print("Nenhum arquivo encontrado.")
#     df_final = pd.DataFrame()
# else:
#     print(f"{len(files)} arquivos encontrados.")

#     df_final = pd.concat(
#         (pd.read_parquet(f) for f in files),
#         ignore_index=True
#     )

# print("Total de registros:", len(df_final))

# # ── Colunas necessárias para o dashboard ─────────────────────
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

# # ── Seleciona apenas colunas necessárias ─────────────────────
# df_estudos = df_final.reindex(columns=columns_to_select)

# # ── Remove duplicatas (muito comum na API) ───────────────────
# df_estudos = df_estudos.drop_duplicates()

# # ── Otimiza tipo numérico ───────────────────────────────────
# df_estudos['protocolSection.designModule.enrollmentInfo.count'] = pd.to_numeric(
#     df_estudos['protocolSection.designModule.enrollmentInfo.count'],
#     errors='coerce'
# ).astype('float32')

# # ── Salva dataset final ─────────────────────────────────────
# output_file = "studies.parquet"

# df_estudos.to_parquet(
#     output_file,
#     index=False,
#     compression="brotli"
# )

# # ── Informações finais ──────────────────────────────────────
# size_mb = round(df_estudos.memory_usage(deep=True).sum() / 1024**2, 2)

# print("Arquivo gerado:", output_file)
# print("Linhas:", len(df_estudos))
# print("Tamanho estimado:", size_mb, "MB")

import requests
import pandas as pd
import time
import os

# ── Configurações da API ────────────────────────────────────────────────────
BASE_URL = "https://clinicaltrials.gov/api/v2/studies"

FIELDS = [
    "protocolSection.identificationModule.nctId",
    "protocolSection.identificationModule.officialTitle",
    "protocolSection.statusModule.startDateStruct.date",
    "protocolSection.statusModule.studyFirstPostDateStruct.date",
    "protocolSection.statusModule.primaryCompletionDateStruct.date",
    "protocolSection.sponsorCollaboratorsModule.leadSponsor.name",
    "protocolSection.sponsorCollaboratorsModule.leadSponsor.class",
    "protocolSection.conditionsModule.conditions",
    "protocolSection.designModule.studyType",
    "protocolSection.designModule.phases",
    "protocolSection.designModule.enrollmentInfo.count",
    "protocolSection.armsInterventionsModule.interventions",
    "protocolSection.contactsLocationsModule.locations",
    "protocolSection.sponsorCollaboratorsModule.collaborators",
    "protocolSection.statusModule.overallStatus",
]

# Busca estudos com esses status
STATUS_FILTER = ["NOT_YET_RECRUITING", "RECRUITING"]

PAGE_SIZE = 1000
OUTPUT_FILE = "studies.parquet"


def fetch_all_studies():
    """Busca todos os estudos da API do ClinicalTrials.gov paginando automaticamente."""
    all_studies = []
    next_page_token = None
    page = 1

    status_query = "|".join(STATUS_FILTER)

    while True:
        print(f"  Buscando página {page}...")

        params = {
            "format": "json",
            "pageSize": PAGE_SIZE,
            "fields": ",".join(FIELDS),
            "filter.overallStatus": status_query,
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        try:
            response = requests.get(BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"  Erro na requisição (página {page}): {e}")
            print("  Tentando novamente em 10 segundos...")
            time.sleep(10)
            continue

        studies = data.get("studies", [])
        all_studies.extend(studies)
        print(f"  {len(studies)} estudos recebidos | Total acumulado: {len(all_studies)}")

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            print("  Última página alcançada.")
            break

        page += 1
        time.sleep(0.3)  # Respeita rate limit da API

    return all_studies


def normalize_studies(studies):
    """Normaliza a lista de estudos (JSON aninhado) para um DataFrame flat."""
    rows = []
    for study in studies:
        row = {}
        protocol = study.get("protocolSection", {})

        id_mod = protocol.get("identificationModule", {})
        row["protocolSection.identificationModule.nctId"] = id_mod.get("nctId")
        row["protocolSection.identificationModule.officialTitle"] = id_mod.get("officialTitle")

        status_mod = protocol.get("statusModule", {})
        row["protocolSection.statusModule.overallStatus"] = status_mod.get("overallStatus")
        row["protocolSection.statusModule.startDateStruct.date"] = (
            status_mod.get("startDateStruct", {}) or {}
        ).get("date")
        row["protocolSection.statusModule.studyFirstPostDateStruct.date"] = (
            status_mod.get("studyFirstPostDateStruct", {}) or {}
        ).get("date")
        row["protocolSection.statusModule.primaryCompletionDateStruct.date"] = (
            status_mod.get("primaryCompletionDateStruct", {}) or {}
        ).get("date")

        sponsor_mod = protocol.get("sponsorCollaboratorsModule", {})
        lead = sponsor_mod.get("leadSponsor", {}) or {}
        row["protocolSection.sponsorCollaboratorsModule.leadSponsor.name"] = lead.get("name")
        row["protocolSection.sponsorCollaboratorsModule.leadSponsor.class"] = lead.get("class")
        row["protocolSection.sponsorCollaboratorsModule.collaborators"] = str(
            sponsor_mod.get("collaborators", [])
        )

        cond_mod = protocol.get("conditionsModule", {})
        row["protocolSection.conditionsModule.conditions"] = str(
            cond_mod.get("conditions", [])
        )

        design_mod = protocol.get("designModule", {})
        row["protocolSection.designModule.studyType"] = design_mod.get("studyType")
        row["protocolSection.designModule.phases"] = str(
            design_mod.get("phases", [])
        )
        enroll = design_mod.get("enrollmentInfo", {}) or {}
        row["protocolSection.designModule.enrollmentInfo.count"] = enroll.get("count")

        arms_mod = protocol.get("armsInterventionsModule", {})
        row["protocolSection.armsInterventionsModule.interventions"] = str(
            arms_mod.get("interventions", [])
        )

        loc_mod = protocol.get("contactsLocationsModule", {})
        locations = loc_mod.get("locations", [])
        # Serializa lista de países para facilitar filtro no dashboard
        countries = list({
            loc.get("country", "") for loc in locations if loc.get("country")
        })
        row["protocolSection.contactsLocationsModule.locations"] = ", ".join(countries)

        rows.append(row)

    return pd.DataFrame(rows)


def main():
    print("=" * 55)
    print("Iniciando atualização dos dados do ClinicalTrials.gov")
    print("=" * 55)

    print(f"\nFiltro de status: {', '.join(STATUS_FILTER)}")
    print("Buscando estudos na API...\n")

    studies = fetch_all_studies()

    if not studies:
        print("\nNenhum estudo retornado pela API. Abortando.")
        return

    print(f"\nTotal de estudos brutos: {len(studies)}")
    print("Normalizando dados...")

    df = normalize_studies(studies)

    # Otimizações
    df = df.drop_duplicates(subset="protocolSection.identificationModule.nctId")
    df["protocolSection.designModule.enrollmentInfo.count"] = pd.to_numeric(
        df["protocolSection.designModule.enrollmentInfo.count"], errors="coerce"
    ).astype("float32")

    df.to_parquet(OUTPUT_FILE, index=False, compression="brotli")

    size_mb = round(os.path.getsize(OUTPUT_FILE) / 1024**2, 2)
    print(f"\nArquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas:         {len(df)}")
    print(f"Tamanho:        {size_mb} MB")
    print("\nConcluído com sucesso!")


if __name__ == "__main__":
    main()