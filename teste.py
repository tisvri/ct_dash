import requests

BASE_URL = "https://clinicaltrials.gov/api/v2/studies"

testes = {
    "A) fields com nomes LONGOS (dot-notation, como no script antigo)": {
        "fields": "protocolSection.identificationModule.nctId,protocolSection.statusModule.overallStatus",
    },
    "B) fields com nomes CURTOS (NCTId, OverallStatus)": {
        "fields": "NCTId,OverallStatus",
    },
    "C) sem parametro fields (baseline)": {},
}

for nome, extra_params in testes.items():
    params = {
        "format": "json",
        "pageSize": 2,
        "filter.overallStatus": "NOT_YET_RECRUITING,RECRUITING",
        "filter.advanced": "AREA[StartDate]RANGE[2020-01-01,MAX]",
    }
    params.update(extra_params)
    print(f"\n{nome}")
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        print(f"  status HTTP: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            studies = data.get("studies", [])
            print(f"  qtd estudos retornados: {len(studies)}")
            if studies:
                # mostra as chaves de protocolSection do primeiro estudo p/ ver o que veio
                ps = studies[0].get("protocolSection", {})
                print(f"  modulos presentes no protocolSection: {list(ps.keys())}")
                import json
                print(f"  tamanho aproximado (bytes) do 1o estudo: {len(json.dumps(studies[0]))}")
        else:
            print(f"  resposta: {r.text[:400]}")
    except requests.exceptions.RequestException as e:
        print(f"  ERRO: {e}")