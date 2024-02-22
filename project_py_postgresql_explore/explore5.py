import pandas as pd
path_prof = "relatorio_sagres_professores_2024_02_19.csv"
prof = pd.read_csv(path_prof, sep=";", encoding="ISO-8859-1") 
path_aluno = "relatorio_sagres_alunos_2024_02_19.csv"
aluno = pd.read_csv(path_aluno, sep=";", encoding="ISO-8859-1") 
#Yuri Marcellos Pereira de Azevedo
#
def rename_df(df: pd.DataFrame, cols: dict):
    return df.rename(columns=cols)
colunas={
    '[NOME]': 'NOME',
    'MUNICÍPIO': 'MUNICIPIO',
    '.[COORD. REGIONAL]':'REGIONAL',
    '[CÓDIGO MEC]': 'INEP',
    '[SÉRIE]': 'SERIE',
    '[TURMA]': 'TURMA',
}
aluno = rename_df(aluno,colunas)
#print(aluno.columns)
#print(prof.query("NOME.str.contains('YURI')")[['NOME','CPF']])#unique()
#print(aluno.query("MUNICIPIO.str.contains('FRIA')")['MUNICIPIO'].unique())#)#
#print(aluno.query("MUNICIPIO.str.contains('FRIA')")['ESCOLA'].unique())#)#
print(aluno.query("ESCOLA.str.contains('FRIA')")['INEP'].unique())#52043533
print(aluno.query("INEP == 52043533")['MATRICULA'].unique())#




