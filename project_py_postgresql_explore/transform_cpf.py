import pandas as pd
import json
import re

path_json_in = "project_py_postgresql_explore\cpfs.json"
path_json_out = "project_py_postgresql_explore\cpfs_fixed.json"

# Opening JSON file
with open(path_json_in) as my_file:
    data = json.load(my_file)
    print(my_file.read())
print(type(data))

lista = data["lista"]
print(type(lista))

cpf_novo = []
for _ in lista:
    numeros = re.findall(r'\d', _)
    numeros_str = ''.join(numeros)
    cpf_novo.append(numeros_str)
print(cpf_novo)
#
new_dict = {}
new_dict['cpf'] = cpf_novo
with open(path_json_out, 'w') as json_file: #, encoding ='utf8'
    json.dump(new_dict, json_file, ensure_ascii = True) 