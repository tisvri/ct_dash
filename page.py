import requests
import pandas as pd
import time
import os
import glob
import numpy as np
import ast
import streamlit as st
import plotly.express as px
import re

#TODO ── Configuração da página ──────────────────────────────────────────────────
st.set_page_config(page_title="Estudos Abertos no Clinical Trials", layout="wide")
color_sequence = ['#EC0E73', '#041266', '#00A1E0', '#C830A0', '#61279E']
logo_path = "Logo svri texto preto.png"

if os.path.exists(logo_path):
    st.sidebar.image(logo_path, width='stretch')

CACHE_TTL_HORAS = 12
@st.cache_data(ttl=CACHE_TTL_HORAS * 3600)
def carregar_dados():
    if os.path.exists("studies.parquet"):
        return pd.read_parquet("studies.parquet")
    else:
        return pd.DataFrame()

df_estudos = carregar_dados()
if df_estudos.empty:
    st.warning("Dados ainda não carregados. Aguarde a atualização automática.")
    st.stop()
st.write("Colunas df_escopo:", df_estudos.columns.tolist())
df_estudos = df_estudos.drop_duplicates(subset="NCT Number")
df_filtrado = df_estudos.copy()

st.title("_ClinicalTrials.Gov_: Indicadores de Estudos em Andamento", text_alignment="center")
st.subheader("Série Temporal: 2020 - Atual")

multi = '''Este dashboard compila os estudos extraídos da API da plataforma ClinicalTrials.gov e que têm Status em andamento, sendo eles: :red[**Not Yet Recruiting e Recruiting**].  
Elaboramos este material para auxilio no acesso a indicadores chave de pesquisa clínica mundial. Á esquerda, é possivel verificar diversas formas de filtro dos dados disponiveis e que podem ser usados de forma simultanêa. A série historica escolhida é a partir dos estudos iniciados (**Start Date**) a partir de **2020**.  
Dúvidas ou problemas técnicos, por favor entre em contato com o **Time de BI-SVRI**.
'''
st.markdown(multi)

#TODO ── Sidebar: filtros ────────────────────────────────────────────────────────
with st.sidebar:
    def limpar_filtros():
        st.session_state["filtro_sponsor"] = []
        st.session_state["filtro_condicao"] = []
        st.session_state["filtro_status"] = 'Selecionar Todos'
        st.session_state["filtro_fase"] = 'Selecionar Todos'
        st.session_state["filtro_funder"] = 'Selecionar Todos'
        st.session_state["filtro_ano_inicio"] = 'Selecionar Todos'
        st.session_state["filtro_ano_posted"] = 'Selecionar Todos'
        st.session_state["filtro_escopo"] = "🌍 Mundo"
        # st.session_state[key] = 0
    st.button("🗑️ Limpar filtros", on_click=limpar_filtros)

    patrocinadores = st.multiselect(
        "**Patrocinadores**",
        sorted(df_estudos["Sponsor"].dropna().unique()),
        key="filtro_sponsor"
    )

    condicao = st.multiselect(
        "**Comorbidade:**",
        sorted(df_estudos['Conditions'].dropna().unique()),
        key="filtro_condicao"
    )

    status_estudos = st.selectbox(
        "**Status do Estudo**",
        ['Selecionar Todos'] + sorted(df_estudos['Study Status'].dropna().unique()),
        index=0,
        key="filtro_status"
    )

    fase_estudo = st.selectbox(
        "**Fases:**",
        ['Selecionar Todos'] + sorted(df_estudos['Phases'].dropna().unique()),
        index=0,
        key="filtro_fase"
    )

    tipo_financiamento = st.selectbox(
        "**Tipo de Financiamento:**",
        ['Selecionar Todos'] + sorted(df_estudos['Funder Type'].dropna().unique()),
        index=0,
        key="filtro_funder"
    )

    ano_inicio_min, ano_inicio_max = st.sidebar.slider(
    "**Ano de início do estudo:**",
    min_value=int(df_estudos['Ano_Inicio'].min()),
    max_value=int(df_estudos['Ano_Inicio'].max()),
    value=(int(df_estudos['Ano_Inicio'].min()), int(df_estudos['Ano_Inicio'].max()))
    )

    ano_posted_min, ano_posted_max = st.sidebar.slider(
        "**Ano de publicação:**",
        min_value=int(df_estudos['Ano_Posted'].min()),
        max_value=int(df_estudos['Ano_Posted'].max()),
        value=(int(df_estudos['Ano_Posted'].min()), int(df_estudos['Ano_Posted'].max()))
    )


    if patrocinadores:
        df_filtrado = df_filtrado[df_filtrado['Sponsor'].isin(patrocinadores)]

    if status_estudos != 'Selecionar Todos':
        df_filtrado = df_filtrado[df_filtrado['Study Status'] == status_estudos]

    if condicao:
        pattern = '|'.join(map(re.escape, condicao))
        df_filtrado = df_filtrado[df_filtrado['Conditions'].str.contains(pattern, case=False, na=False)]

    if fase_estudo != 'Selecionar Todos':
        df_filtrado = df_filtrado[df_filtrado['Phases'] == fase_estudo]

    if tipo_financiamento != 'Selecionar Todos':
        df_filtrado = df_filtrado[df_filtrado['Funder Type'] == tipo_financiamento]

    df_filtrado = df_filtrado[
        (df_filtrado['Ano_Inicio'].between(ano_inicio_min, ano_inicio_max)) &
        (df_filtrado['Ano_Posted'].between(ano_posted_min, ano_posted_max))
    ]

#TODO ── Escopo Mundo / Brasil ───────────────────────────────────────────────────
escopo = st.radio(
    "**Estudos:**", ["🌍 Mundo", "🇧🇷 Brasil"],
    horizontal=True, key="filtro_escopo"
)

if escopo == "🌍 Mundo":
    df_escopo = df_filtrado
else:
    df_escopo = df_filtrado[df_filtrado["Locations"].str.contains(
        r"\b(?:brazil|brasil)\b", case=False, na=False)]

#TODO ── Métricas ────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    st.metric("**TOTAL DE ESTUDOS:**", df_escopo["NCT Number"].nunique())
with col2:
    total_brasil = df_escopo[df_escopo["Locations"].str.contains(
        r"\b(?:brazil|brasil)\b", case=False, na=False)]["NCT Number"].nunique()
    st.metric("**TOTAL DE ESTUDOS NO BRASIL:**", total_brasil)

#TODO ── Agregações para gráficos ────────────────────────────────────────────────
df_top10 = (df_escopo.groupby('Sponsor')['NCT Number'].nunique()
            .reset_index(name='num_estudos')
            .sort_values('num_estudos', ascending=False).head(20))

df_top10_comorbidade = (df_escopo.groupby('Conditions')['NCT Number'].nunique()
                        .reset_index(name='num_estudos')
                        .sort_values('num_estudos', ascending=False).head(20))

df_contagem_fase = (df_escopo.groupby('Phases')['NCT Number'].nunique()
                    .reset_index(name='num_estudos'))

# df_estudos['Start Date'] = pd.to_datetime(df_estudos['Start Date'], errors='coerce')
# df_estudos['First Posted'] = pd.to_datetime(df_estudos['First Posted'], errors='coerce')
st.write("Colunas df_escopo:", df_escopo.columns.tolist())
st.write("Primeiras linhas df_escopo:", df_escopo.head())

df_escopo['Ano_Start'] = df_estudos['Start Date'].dt.year
df_escopo['Ano_Posted'] = df_estudos['First Posted'].dt.year

df_intervention = (df_escopo.groupby('Intervention_type')['NCT Number'].nunique()
            .reset_index(name='num_estudos')
            .sort_values('num_estudos', ascending=False)) 

df_start = df_escopo.dropna(subset=['Ano_Start']).groupby('Ano_Start')['NCT Number'].nunique().reset_index(name='num_estudos').sort_values('Ano_Start')

df_posted = df_escopo.dropna(subset=['Ano_Posted']).groupby('Ano_Posted')['NCT Number'].nunique().reset_index(name='num_estudos').sort_values('Ano_Posted')


#TODO ── Gráficos ────────────────────────────────────────────────────────────────

fig_start = px.line(df_start, x='Ano_Start', y='num_estudos', markers=True, text='num_estudos')
fig_start.update_traces(textposition='top center', line=dict(color='#041266'), marker=dict(color='#EC0E73', size=8))
fig_start.update_layout(xaxis_title='',yaxis_title='',uniformtext_minsize=8,
    uniformtext_mode='hide', xaxis={'dtick':1}, yaxis={'showticklabels':False})
st.subheader('Estudos por Ano de Início')
st.plotly_chart(fig_start, use_container_width=True)

fig_posted = px.line(df_posted, x='Ano_Posted', y='num_estudos', markers=True, text='num_estudos')
fig_posted.update_traces(textposition='top center', line=dict(color='#EC0E73'), marker=dict(color='#041266', size=8))
fig_posted.update_layout(xaxis_title='',yaxis_title='',uniformtext_minsize=8,
    uniformtext_mode='hide', xaxis={'dtick':1}, yaxis={'showticklabels':False})
st.subheader('Estudos por Ano de Publicação')
st.plotly_chart(fig_posted, use_container_width=True)

ful1,ful2 = st.columns([1,1])

with ful1:
    fig = px.bar(df_top10, x="num_estudos", y="Sponsor", color='Sponsor',
                 color_discrete_sequence=color_sequence, orientation='h',
                 text='num_estudos', height=400)
    fig.update_traces(texttemplate='%{text:.0f}', textposition='outside',
                        textfont=dict(size=18), cliponaxis=False, showlegend=False)
    fig.update_layout(xaxis_title='', yaxis_title='', uniformtext_minsize=6,
                        uniformtext_mode='hide', yaxis=dict(tickfont=dict(size=10)),
                        xaxis=dict(showticklabels=False))
    st.subheader('Quantidade de estudos por empresa')
    st.write('O gráfico abaixo mostra as empresas com maior número de estudos:')
    st.plotly_chart(fig, use_container_width=True)

    fig = px.bar(df_top10_comorbidade, x="num_estudos", y="Conditions", color='Conditions',
                 color_discrete_sequence=color_sequence, text='num_estudos', height=400)
    fig.update_traces(texttemplate='%{text:.0f}', textposition='outside',
                        textfont=dict(size=18), cliponaxis=False, showlegend=False)
    fig.update_layout(xaxis_title='', yaxis_title='', uniformtext_minsize=6,
                        uniformtext_mode='hide', yaxis=dict(tickfont=dict(size=10)),
                        xaxis=dict(showticklabels=False))
    st.subheader('Comorbidades com mais estudos')
    st.write('O gráfico abaixo mostra as principais comorbidades com maiores números de estudos:')
    st.plotly_chart(fig, use_container_width=True)

with ful2:
    fig = px.bar(df_contagem_fase, x="Phases", y="num_estudos", color='Phases',
                 color_discrete_sequence=color_sequence, text='num_estudos', height=400)
    fig.update_traces(texttemplate='%{text:.0f}', textposition='outside',
                        cliponaxis=False, showlegend=False)
    fig.update_layout(xaxis_title='', yaxis_title='', uniformtext_minsize=8,
                        uniformtext_mode='hide', yaxis=dict(showticklabels=False))
    st.subheader('Estudos por Fase')
    st.write('O gráfico abaixo mostra a quantidade de estudos por fase:')
    st.plotly_chart(fig, use_container_width=True)


    fig = px.bar(df_intervention, y='num_estudos', x='Intervention_type', color='Intervention_type',
                 color_discrete_sequence=color_sequence, text='num_estudos', height=400)
    fig.update_traces(texttemplate='%{text:.0f}', textposition='outside',
                        cliponaxis=False, showlegend=False)
    fig.update_layout(xaxis_title='', yaxis_title='', uniformtext_minsize=8, uniformtext_mode='hide',
                        xaxis={'categoryorder': 'category ascending'},
                        yaxis=dict(showticklabels=False))
    st.subheader('Tipo de Intervenção')
    st.write('O gráfico abaixo mostra a quantidade de estudos por tipo de intervenção:')
    st.plotly_chart(fig, use_container_width=True)


#TODO ── Modelagem da Tabela Exibida no Projeto ──────────────────────────────────────────────────────────────────
df_tabela = df_escopo.drop(columns=['Locations','Collaborators','Intervention_armGroupLabels', 'Intervention_name', 'Intervention_description','Intervention_otherNames', 'Ano_Start', 'Ano_Posted', 'Ano_Inicio'])
df_tabela = df_tabela.rename(columns={'Intervention_type': 'Tipo de Intervenção',
                                      'Study Title': 'Título do Estudo',
                                      'Start Date': 'Início do Estudo',
                                      'First Posted': 'Primeira Publicação',
                                      'Completion Date': 'Conclusão do Estudo',
                                      'Sponsor': 'Patrocinador',
                                      'Funder_type': 'Tipo de Financiamento',
                                      'Conditions': 'Comorbidades',
                                      'Study Type': 'Tipo de Estudo',
                                      'Enrollment': 'Recrutamento',
                                      'Study Status': 'Status de Estudo',
                                      'Phases': 'Fases'})


#Tratando as colunas com None (tipo string/object)
cols_texto = df_tabela.select_dtypes(include=['object']).columns
df_tabela[cols_texto] = df_tabela[cols_texto].fillna('Não informado')

#Tratando as colunas com Nan/None (tipo Date)
cols_data = df_tabela.select_dtypes(include=['datetime64[ns]']).columns
for col in cols_data:
    df_tabela[col] = df_tabela[col].dt.strftime('%d/%m/%Y').fillna('Não informado')

df_tabela["NCT Number"] = "https://clinicaltrials.gov/study/" + df_tabela["NCT Number"]

st.dataframe(
    df_tabela.head(1000),
    column_config={
        "NCT Number": st.column_config.LinkColumn(
            "NCT Number",
            display_text=r"https://clinicaltrials.gov/study/(.*)"
        )
    }
)

#st.write(df_filtrado[['Start Date','Ano_Inicio']].head())

# st.title('Projeto BI-SVRI')
st.markdown("Desenvolvido por [Science Valley Research Institute](https://svriglobal.com/)")