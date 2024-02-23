import pandas as pd
import json
import psycopg2

path_json = "project_py_postgresql_explore\explore_cpfs2.json"
# Opening JSON file
with open(path_json) as my_file:
    data = json.load(my_file)
    #print(my_file.read())
#print(data['CPF'])
tupla = tuple(data['CPF'])
#
path_json = "project_py_postgresql_explore\creds.json"
# path_out = f"project_py_postgresql_reports\{output_folder}\visao_seduc.csv"
# Opening JSON file
with open(path_json) as my_file:
    data = json.load(my_file)
    print(my_file.read())
#print(data)
#acesso ao BD sergoias
con=psycopg2.connect(
    dbname=data["dbname"], 
    host=data["host"], 
    port=data["port"], 
    user=data["user"], 
    password=data["password"]
)
#tupla = tuple(cpf_novo)
#tupla = ('6º Ano','7º Ano','8º Ano','9º Ano')
params = {'value': tupla}
query_user = f"""
    select 
        uu.username as "Matricula",
        up.full_name as "Nome_Completo", 
        to_char(uu.date_joined, 'DD/MM/YYYY HH24:MI') as "Data_Cadastro",
        rr."name" as "Regional", 
        ii."name" as "Escola"
    from sergoias.users_user uu
    left join sergoias.users_person up on up.id = uu.person_id
    left join sergoias.institutions_institution_users iiu on iiu.user_id = uu.id
    left join sergoias.institutions_institution ii on ii.id = iiu.institution_id 
    left join sergoias.regionals_regional rr on rr.id = ii.regional_id     
    where uu.username in {tupla}
"""
query_acesso = f"""
    select uu.username,up.full_name,uar.access_at 
    from sergoias.users_user uu
    left join sergoias.users_person up on up.id = uu.person_id 
    inner join sergoias.users_accessrecord uar on uar.user_id = uu.id
    where uu.username in {tupla}
""" 
query_netescola = f"""
    select to_char(nn.access_at, 'DD/MM/YYYY HH24:MI'), url_response->>'login' as login
    from sergoias.netescola_netescolaintegrationlog nn
    where nn.url_response->>'login' in {tupla}
"""    

df = pd.read_sql_query(query_user,con)
con.close()
#
print(df)#