import psycopg2
import pandas as pd
import json
import os
from  datetime import datetime
#
import re

cpf = [
'050.894.611-55','011.798.601-10','99399016153','58939873149','644.560.211-87',
'798.878.561-34','90861116100','434.652.261-00','55594263187','012.121.271-88',
'854.928.331-20','586 464 421 87','011.443.301-14','575 522 901 53','81341164187',
'96179287104','796364031-04','526.923.881-00','619.250.475-04','73356182153',
'95445951120','80176216120','955.905.871-15','712.798.811 00','032.366.811-95',
'382 689 301 87','769 178 431 72','877.610.921-68','016.569.401-75',
'039.453.791-21','706.014.761-66','793.045.751-68','61889997153','52067548115',
'71081470178','915.309.971-00','896.538.311-00','050.894.611-55','011.798.601-10',
'576.877.301-00','864.000.611-34','591.925.771-72','006.755.521-76',
'846.923.831-00','949.087.451-53','008.464.251-30','016.245.071-07',
'004.488.293-98','003.990.251-03','816.962.991-87','037.093.761-90',
'484.508.631-04','020.397.401-80','051.828.531-65','041.573.441-03',
'048.300.861-30','031.058.781-65','867.106.721-15','003.732.421-74',
'003.397.571-08','526.905.201-68','924.006.391-91','009.349.371-17',
'402.115.681-04','052.688.761-04','010.285.811-06','758.317.261-04',
'979.464.191-04','041.404.711-74','838.070.981-00','853.369.471-72',
'612.403.201-59','030.41.666-70','872.379.031-87','774.953.901-82',
'476.970.081-49','883.499.341-15','009.594.121-58','706.020.491-13',
'624.517.071-00','414.757.551-04','440.868.491-00','414.738.501-00',
'59148969168','59784814153','80002285134','795.901.521-04','982.150.431-00',
'030.976.661-33','892.060.701-00','014.099.021-61','852.940.391-68',
'947.939.801-04','005.241.241-59','070.319.731-20','039.975.601-96',
'032.194.521-24','017.413.911-05','807.741.131.20','031.185.131-28',
'847.116.391-87','246.572.488-63','932.331.401-34','601.682.851-34',
'968.750.891-49','371.066.861-15','560.712.131-34','394.602.071-20',
'948.968.361-20','748.790.901-82','029.198.211-56','803.994.951-34',
'006.172.521-81','831.313.881-53','040.724.471-96','596.709.241-20',
'955.484.241-49','574.898.661-20','034.789.581-64','034.563.891-32',
'800.035.751-87','498.728.001-97','821.953.401-00','758.546.861-04  ',
'048.545.911-61','352.949.651-00','005.492.891-51','888849701-30','84086556120',
'057.532.731-65','64092526172','470.249.681-68','002.573.231-55','309.731.471-72',
'520.554.341.87','368.354.438-47','700.707.331-04','923.366.551-87','020.397.071-31','869.971.651-68','041.640.801-08','800.460.611-34','050.949.801-01','029.678.731-01','756.295.181-00','015.307.751-40','829.972.921-15','002.745.821-04','028.090.141-08','050.736.041-99','622.971.471-04','046.223.061-90','032.196.611-22','816.288.031-34','853.988.961-72','869.444.521-20','520.523.701-53','780.178.151-15','253.896.611-68','031.759.511-39','001.980.061-44','843.486.821-00','520.523.701-53','576.701.881-20','79225268149','770.636.511-53 ','71660097134','008.728.351-46','71081470178','031.879.316-42 ','81977964168','50936581115','80834957191']
cpf_novo = []
for _ in cpf:
    numeros = re.findall(r'\d', _)
    numeros_str = ''.join(numeros)
    cpf_novo.append(numeros_str)
data = dict()
#
# output_folder = f'out_{datetime.now().strftime('%Y_%m_%d')}'
# if not os.path.exists(output_folder):
#     os.makedirs(output_folder)
# print("ABC3")
#
path_json = f"project_py_postgresql_explore\creds.json"
# path_out = f"project_py_postgresql_reports\{output_folder}\visao_seduc.csv"
# Opening JSON file
with open(path_json) as my_file:
    data = json.load(my_file)
    print(my_file.read())
print(data)
#acesso ao BD sergoias
con=psycopg2.connect(
    dbname=data["dbname"], 
    host=data["host"], 
    port=data["port"], 
    user=data["user"], 
    password=data["password"]
)
tupla = tuple(cpf_novo)
#tupla = ('6º Ano','7º Ano','8º Ano','9º Ano')
params = {'value': tupla}
query_total = f"""
    select uu.username,up.full_name ,uar.access_at 
    from sergoias.users_user uu
    left join sergoias.users_person up on up.id = uu.person_id 
    inner join sergoias.users_accessrecord uar on uar.user_id = uu.id
    where uu.username in {tupla}
""" 
df_total = pd.read_sql_query(query_total,con)

#
con.close()
df_lista2 = df_total.groupby(['username', 'full_name']).agg(list)#.apply(lambda x: x.to_numpy()).
df_lista2.to_csv("acessos_agg.csv", sep=",", encoding="utf-8", index=True, header=True)
#print(df_total.describe())
print(df_lista2.head())
#
query_acessos = """

"""
# df_acessos = pd.read_sql_query(query_acessos,con)
# con.close()
# print(df_acessos.head())
#
# result = pd.merge(df_total, df_acessos, on="Nome da Regional")
# result = (
#     result
#     .assign(Alcance = lambda df: 100*df['Estudantes com Primeiro Acesso']/df['Total de Estudantes'])
#     .sort_values(by=['Alcance'], ascending=False)
# )
#result.to_csv("visao_seduc2.csv", sep=",", encoding="utf-8", index=False, header=True)
#
#print(result.head())

