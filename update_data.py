import requests
import pandas as pd
import time
import os

# ── Configurações da API ────────────────────────────────────────────────────
BASE_URL = "https://clinicaltrials.gov/api/v2/studies"

# CORRIGIDO: o parâmetro 'fields' da API v2 espera os nomes curtos das peças
# (ex: "NCTId", "BriefTitle"), não o caminho aninhado completo
# ("protocolSection.identificationModule.nctId"). Os nomes longos continuam
# sendo usados só localmente, para nomear as colunas do DataFrame final.
FIELDS = [
    "NCTId",
    "OfficialTitle",
    "StartDate",
    "StudyFirstPostDate",
    "PrimaryCompletionDate",
    "LeadSponsorName",
    "LeadSponsorClass",
    "Condition",
    "StudyType",
    "Phase",
    "EnrollmentCount",
    "InterventionType",
    "InterventionName",
    "LocationCountry",
    "Collaborator",
    "OverallStatus",
]

# Busca estudos com esses status
STATUS_FILTER = ["NOT_YET_RECRUITING", "RECRUITING"]

# NOVO: filtra direto na API por Start Date >= 2020, já que é isso que o
# dashboard usa mesmo — evita baixar e depois descartar estudos antigos.
DATA_INICIO_MINIMA = "2020-01-01"
FILTRO_DATA = f"AREA[StartDate]RANGE[{DATA_INICIO_MINIMA},MAX]"

PAGE_SIZE = 1000
OUTPUT_FILE = "studies.parquet"
MAX_TENTATIVAS_POR_PAGINA = 5  # NOVO: evita loop infinito de retry


def fetch_all_studies():
    """Busca todos os estudos da API do ClinicalTrials.gov paginando automaticamente."""
    all_studies = []
    next_page_token = None
    page = 1

    # CORRIGIDO: filter.overallStatus é separado por VÍRGULA, não por pipe.
    # Com "|" a API rejeita o parâmetro (400) porque o valor combinado não
    # bate com nenhum status válido — e como isso não é erro de rede, o
    # retry antigo ficava tentando pra sempre sem nunca dar certo.
    status_query = ",".join(STATUS_FILTER)

    # NOVO: pega o total esperado antes de paginar, pra podermos validar no
    # final se batemos perto disso (e não aceitar silenciosamente um
    # resultado truncado).
    total_esperado = None
    try:
        resp_count = requests.get(
            BASE_URL,
            params={
                "format": "json",
                "filter.overallStatus": status_query,
                "filter.advanced": FILTRO_DATA,
                "pageSize": 1,
                "countTotal": "true",
            },
            timeout=60,
        )
        resp_count.raise_for_status()
        total_esperado = resp_count.json().get("totalCount")
        print(f"  Total de estudos reportado pela API para esse filtro: {total_esperado}")
    except requests.exceptions.RequestException as e:
        print(f"  Aviso: não consegui obter o totalCount antecipadamente ({e}).")

    while True:
        print(f"  Buscando página {page}...")

        params = {
            "format": "json",
            "pageSize": PAGE_SIZE,
            "fields": ",".join(FIELDS),
            "filter.overallStatus": status_query,
            "filter.advanced": FILTRO_DATA,
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        tentativa = 0
        while True:
            tentativa += 1
            try:
                response = requests.get(BASE_URL, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                # NOVO: distingue erro de parâmetro (4xx — não adianta tentar
                # de novo) de erro transitório de rede/servidor (5xx/timeout).
                status_code = getattr(e.response, "status_code", None)
                if status_code is not None and 400 <= status_code < 500:
                    print(f"  Erro {status_code} de parâmetro na página {page}: {e}")
                    print(f"  Resposta da API: {getattr(e.response, 'text', '')[:500]}")
                    raise SystemExit(
                        "Abortando: erro de parâmetro não se resolve tentando de novo. "
                        "Confira filter.overallStatus / fields."
                    )
                if tentativa >= MAX_TENTATIVAS_POR_PAGINA:
                    raise SystemExit(
                        f"Abortando: falharam {MAX_TENTATIVAS_POR_PAGINA} tentativas "
                        f"seguidas na página {page} ({e})."
                    )
                print(f"  Erro na requisição (página {page}, tentativa {tentativa}): {e}")
                print("  Tentando novamente em 10 segundos...")
                time.sleep(10)

        studies = data.get("studies", [])
        all_studies.extend(studies)
        print(f"  {len(studies)} estudos recebidos | Total acumulado: {len(all_studies)}")

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            print("  Última página alcançada.")
            break

        page += 1
        time.sleep(0.3)  # Respeita rate limit da API

    # NOVO: alerta (sem abortar) se ficamos muito abaixo do total esperado —
    # normalmente sinal de paginação cortada por algum motivo.
    if total_esperado is not None and len(all_studies) < 0.95 * total_esperado:
        print(
            f"  ⚠️  Aviso: coletamos {len(all_studies)} de {total_esperado} esperados "
            f"({len(all_studies) / total_esperado:.1%}). Algo pode ter cortado a paginação."
        )

    return all_studies, total_esperado


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

    studies, total_esperado = fetch_all_studies()

    if not studies:
        print("\nNenhum estudo retornado pela API. Abortando (parquet antigo mantido).")
        return

    print(f"\nTotal de estudos brutos: {len(studies)}")
    print("Normalizando dados...")

    df = normalize_studies(studies)

    # Otimizações
    df = df.drop_duplicates(subset="protocolSection.identificationModule.nctId")
    df["protocolSection.designModule.enrollmentInfo.count"] = pd.to_numeric(
        df["protocolSection.designModule.enrollmentInfo.count"], errors="coerce"
    ).astype("float32")

    # NOVO: freio de segurança — não sobrescreve um parquet bom existente com
    # um resultado suspeito de estar truncado.
    if total_esperado is not None and len(df) < 0.90 * total_esperado:
        print(
            f"\n❌ Abortando gravação: só {len(df)} de {total_esperado} estudos "
            f"esperados ({len(df) / total_esperado:.1%}). Mantendo o "
            f"'{OUTPUT_FILE}' anterior para não perder dados."
        )
        return

    df.to_parquet(OUTPUT_FILE, index=False, compression="brotli")

    size_mb = round(os.path.getsize(OUTPUT_FILE) / 1024**2, 2)
    print(f"\nArquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas:         {len(df)}")
    print(f"Tamanho:        {size_mb} MB")
    print("\nConcluído com sucesso!")


if __name__ == "__main__":
    main()