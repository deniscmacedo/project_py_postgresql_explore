import pandas as pd
file_name_alunos = 'relatorio_sagres_alunos_2024_02_21'
read_path = f'{file_name_alunos}.csv'
alunos = pd.read_csv(read_path, delimiter=';', encoding="ISO-8859-1", on_bad_lines='skip')
cols = alunos.columns.to_list()
cols = [
    'REGIONAL','MUNICIPIO','CODIGO_MEC','ESCOLA',
    'CARACTERISTICA','MODALIDADE','NIVEL','DEP_ADM',
    'CONVENIO','CODIGO_COMPOSICAO','COMPOSICAO',
    'SERIE','CODIGO_TURMA','TURMA','TURNO','MATRICULA',
    'CPF','NOME','IDADE'
]
c1 = type(cols)
c2 = type(alunos)

alunos.columns = cols
c3 = alunos.groupby('SERIE')[['MATRICULA','NOME']].count()
print(c3)