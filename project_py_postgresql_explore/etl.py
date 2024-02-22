import pandas as pd
import json

path_form = "Cadastro_CRE_Ser_Goias.csv"
path_diretor = "DADOS COMPLETOS DOS DIRETORES-SEDUC.csv"
path_regional = "Dados para cadastro Sagres_REGIONAIS_mod.csv"
path_seduc = "Dados para cadastro Sagres_SEDUC.csv"
path_tutor = "Dados para cadastro Sagres_TUTORES.csv"
path_prof = "relatorio_sagres_professores_2024_02_19.csv"
path_json = "project_py_postgresql_explore\cpfs_fixed.json"

#
def rename_df(df: pd.DataFrame, cols: dict):
    return df.rename(columns=cols)

def reads():
    formulario = pd.read_csv(path_form)
    # print(path_form)

    diretor = pd.read_csv(path_diretor)
    # print(diretor)

    regional = pd.read_csv(path_regional)
    # print(regional)

    seduc = pd.read_csv(path_seduc)
    # print(seduc)

    tutor = pd.read_csv(path_tutor, sep=";")
    # print(tutor)
###########################################################################
###########################################################################
###########################################################################
# Opening JSON file
with open(path_json) as my_file:
    data = json.load(my_file)
    print(my_file.read())

#create dataframe with all cpfs fixeds
base = pd.DataFrame(data)
#print(base)
###########################################################################
###########################################################################
###########################################################################
formulario = pd.read_csv(path_form)
colunas={
    'Informe seu CPF (Somente números)': 'CPF',
    'Nome Completo (Sem abreviação)': 'NOME_COMPLETO_formulario'
}
df_formulario = rename_df(formulario,colunas)
df_formulario['CPF'] = df_formulario['CPF'].astype(str).str.strip() 
df_formulario['CPF'] = df_formulario['CPF'].astype(str).str.zfill(11)
df_formulario['cout_CPF'] = df_formulario['CPF'].str.len()
df_formulario['NOME_COMPLETO_formulario'] = df_formulario['NOME_COMPLETO_formulario'].str.upper()
df_formulario['origem_formulario'] = "formulario"
#print(df_formulario.query('cout_CPF_formulario != 11'))
# MERGES
df_merge = base.merge(df_formulario, how='left', on='CPF')
df_merge = df_merge.filter(items=['CPF', 'NOME_COMPLETO_formulario','origem_formulario'])
#print(df_merge)
###########################################################################
###########################################################################
###########################################################################
diretor = pd.read_csv(path_diretor) 
colunas={
    'NOME DO DIRETOR': 'NOME_COMPLETO_diretor'
}
df_diretor = rename_df(diretor,colunas)
df_diretor['CPF'] = df_diretor['CPF'].astype(str).str.strip()
df_diretor.replace({'CPF': r'\D'}, {'CPF': ""}, regex=True, inplace=True)
#diretor['CPF'] = diretor['CPF'].astype(str).str.zfill(11)
df_diretor['count_CPF'] = df_diretor['CPF'].str.len()
df_diretor['NOME_COMPLETO_diretor'] = df_diretor['NOME_COMPLETO_diretor'].str.upper()
df_diretor['origem_diretor'] = "diretor"
#print(df_diretor.query('count_CPF == 11')[['CPF','count_CPF']])
# MERGES
df_merge = df_merge.merge(df_diretor, how='left', on='CPF')
df_merge = (
    df_merge
    .filter(
        items=[
            'CPF',
            'NOME_COMPLETO_formulario','origem_formulario',
            'NOME_COMPLETO_diretor','origem_diretor'
        ]
    )
)
###########################################################################
###########################################################################
###########################################################################
tutor = pd.read_csv(path_tutor, sep=";") 
colunas={
    'TUTOR EDUCACIONAL': 'NOME_COMPLETO_tutor'
}
df_tutor = rename_df(tutor,colunas)
df_tutor['CPF'] = tutor['CPF'].astype(str).str.strip()
df_tutor.replace({'CPF': r'\D'}, {'CPF': ""}, regex=True, inplace=True)
df_tutor['CPF'] = df_tutor['CPF'].astype(str).str.zfill(11)
df_tutor['count_CPF'] = df_tutor['CPF'].str.len()
df_tutor['NOME_COMPLETO_tutor'] = df_tutor['NOME_COMPLETO_tutor'].str.upper()
df_tutor['origem_tutor'] = "tutor"
#print(df_tutor.query('count_CPF == 11')[['CPF','count_CPF']])
# MERGES
df_merge = df_merge.merge(df_tutor, how='left', on='CPF')
df_merge = (
    df_merge
    .filter(
        items=[
            'CPF',
            'NOME_COMPLETO_formulario','origem_formulario',
            'NOME_COMPLETO_diretor','origem_diretor',
            'NOME_COMPLETO_tutor','origem_tutor'
        ]
    )
)
###########################################################################
###########################################################################
###########################################################################
prof = pd.read_csv(path_prof, sep=";", encoding="ISO-8859-1") 
colunas={
    'NOME': 'NOME_COMPLETO_prof'
}
df_prof = rename_df(prof,colunas)
df_prof['CPF'] = prof['CPF'].astype(str).str.strip()
df_prof.replace({'CPF': r'\D'}, {'CPF': ""}, regex=True, inplace=True)
df_prof['CPF'] = df_prof['CPF'].astype(str).str.zfill(11)
df_prof['count_CPF'] = df_prof['CPF'].str.len()
df_prof['NOME_COMPLETO_prof'] = df_prof['NOME_COMPLETO_prof'].str.upper()
df_prof['origem_prof'] = "prof"
#print(df_prof.query('count_CPF == 11')[['CPF','count_CPF']])
# MERGES
df_merge = df_merge.merge(df_prof, how='left', on='CPF')
df_merge = (
    df_merge
    .filter(
        items=[
            'CPF',
            'NOME_COMPLETO_formulario','origem_formulario',
            'NOME_COMPLETO_diretor','origem_diretor',
            'NOME_COMPLETO_tutor','origem_tutor',
            'NOME_COMPLETO_prof','origem_prof'
        ]
    )
)
###########################################################################
###########################################################################
###########################################################################
regional = pd.read_csv(path_regional, sep=";", encoding="ISO-8859-1") 
colunas={
    'NOME': 'NOME_COMPLETO_regional'
}
df_regional = rename_df(regional,colunas)
df_regional['CPF'] = regional['CPF'].astype(str).str.strip()
df_regional.replace({'CPF': r'\D'}, {'CPF': ""}, regex=True, inplace=True)
df_regional['CPF'] = df_regional['CPF'].astype(str).str.zfill(11)
df_regional['count_CPF'] = df_regional['CPF'].str.len()
df_regional['NOME_COMPLETO_regional'] = df_regional['NOME_COMPLETO_regional'].str.upper()
df_regional['origem_regional'] = "regional"
#print(df_regional.query('count_CPF != 11')[['CPF','count_CPF']])
# MERGES
df_merge = df_merge.merge(df_regional, how='left', on='CPF')
df_merge = (
    df_merge
    .filter(
        items=[
            'CPF',
            'NOME_COMPLETO_formulario','origem_formulario',
            'NOME_COMPLETO_diretor','origem_diretor',
            'NOME_COMPLETO_tutor','origem_tutor',
            'NOME_COMPLETO_prof','origem_prof',
            'NOME_COMPLETO_regional','origem_regional'
        ]
    )
)
#print(df_merge.query('CPF == "92336655187"')) 
#print(df_merge.loc[295]) #92336655187 , diretor
#print(df_merge.query('CPF == "71081470178"')) #80,373
#print(df_merge.loc[80]) #92336655187 , nenhum
#print(df_merge.loc[373]) #92336655187 , nenhum
#print(df_merge.query('CPF == "00549289151"'))#273,274,275
#print(df_merge.loc[273]) #92336655187 , prof
#print(df_merge.loc[274]) #92336655187 , prof
#print(df_merge.loc[82]) #89653831100

df_merge.to_csv("consolidado_01.csv", sep=";", encoding="ISO-8859-1", index=False, header=True)