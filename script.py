import pandas as pd
import numpy as np
import datetime
from epiweeks import Week
import os
import locale


#Para que las fechas estén en español
locale.setlocale(locale.LC_TIME, 'es_MX.UTF-8')

#***************************************************************************
claves_estatales = {'01': 'Aguascalientes','02': 'Baja California','03': 'Baja California Sur','04': 'Campeche','05': 'Coahuila','06': 'Colima','07': 'Chiapas','08': 'Chihuahua','09': 'Ciudad de México','10': 'Durango', '11': 'Guanajuato','12': 'Guerrero','13': 'Hidalgo','14': 'Jalisco','15': 'Estado de México','16': 'Michoacán','17': 'Morelos','18': 'Nayarit','19': 'Nuevo León','20': 'Oaxaca','21': 'Puebla','22': 'Querétaro', '23': 'Quintana Roo','24': 'San Luis Potosí','25': 'Sinaloa','26': 'Sonora','27': 'Tabasco','28': 'Tamaulipas','29': 'Tlaxcala','30': 'Veracruz','31': 'Yucatán','32': 'Zacatecas'}
claves_estatales_2 = {v: k for k, v in claves_estatales.items()}

file1 = 'data_in.csv'
#datos = pd.read_csv(file1,nrows=25000)
datos = pd.read_csv(file1)


#*********************************************************************
def str_to_date(str):
	return datetime.datetime.strptime(str,"%Y-%m-%d")


#*********************************************************************
data0 = pd.DataFrame(datos, columns = ['fecha_recoleccion','estado','variante_oms','tipo_variante'])
data0 = data0[(  data0['fecha_recoleccion'].apply(lambda x: str_to_date(x)) >= datetime.datetime(2021, 1, 3, 0, 0) )]
data0.reset_index(drop=True, inplace=True)

data0['fecha_recoleccion'] = data0['fecha_recoleccion'].apply(lambda x: Week.fromdate(str_to_date(x)).week)
data0['estado'] = data0['estado'].apply(lambda x: claves_estatales_2[x])

data0.rename(columns={'fecha_recoleccion': 'SE', 'estado': 'cve_ent'}, inplace=True)

data_voc = pd.DataFrame(data0[(data0['tipo_variante'] == 'VOC')], columns = ['SE','cve_ent','variante_oms'])
data_voi = pd.DataFrame(data0[(data0['tipo_variante'] == 'VOI')], columns = ['SE','cve_ent','variante_oms'])


#*********************************************************************
def var_count(data_frame):
	#Cuenta las variantes principales
	variantes = pd.DataFrame(data_frame, columns = ['variante_oms']).to_numpy().flatten()
	unique, counts = np.unique(variantes, return_counts=True)
	x = dict(zip(unique, counts))
	var_count = {k: v for k, v in sorted(x.items(), key=lambda item: item[1],reverse=True)}
	top_var = [k for k in var_count.keys()]
	return top_var


def SemEpid(df,nn):
	#regresa un diccionario 'dic' a partir del dataframe 'df'. El dic contiene las claves de entidad y no. de contagios en la semana epidem 'nn' 
	df = df.groupby(['SE','cve_ent']).size().reset_index(name='count')
	df = pd.DataFrame( df[(df['SE'] == nn)],columns =['cve_ent','count']).set_index('cve_ent')['count'].to_dict()
	dic = {k:df.get(k) if k in df.keys() else 0 for k in claves_estatales.keys()  }
	return dic


def DF_variantes(dataframe,lista):
	#Crea lista de dataframes con las variantes específicadas en 'lista'
	DF_var_list= list()
	n = len(lista)
	for i in range(n):
		df  = pd.DataFrame( dataframe[(dataframe['variante_oms'] == lista[i] )], columns = ['SE','cve_ent','variante_oms'])
		DF_var_list.append(df)
	return DF_var_list


#*********************************************************************
Sem_max = 26

def print_json(dataframe,lista,filex):
 
	df_list = DF_variantes(dataframe,lista)

	print('[',file=filex)
	for k in range(1,Sem_max+1):

		se = '{0}{1}'.format('SE', str(k).zfill(2)) 
		date_ini = Week(2021,k).startdate().strftime('%d/%b/%Y')
		date_end = Week(2021,k).enddate().strftime('%d/%b/%Y')
		
		print('\t{',file=filex)
		print('\t\t"se": "{}",'.format(se),file=filex)
		print('\t\t"fecha_1": "{}",'.format(date_ini),file=filex)
		print('\t\t"fecha_2": "{}",'.format(date_end),file=filex)
		print('\t\t"cve_ent": "00",',file=filex)
		print('\t\t"ent": "Nacional",',file=filex)

		for i in range(len(lista)):
			x = SemEpid(df_list[i],k)
			xx = sum(x.values())
			print('\t\t"{}": {},'.format( lista[i],  xx ),file=filex)

		if(k==Sem_max):
			print('\t}',file=filex)
		else:
			print('\t},',file=filex)
	print(']',file=filex)


#*********************************************************************
top_voi = var_count(data_voi)
top_voc = var_count(data_voc)
top_var = top_voc+top_voi


#*********************************************************************
filename_voi = os.path.join('data_out.json')
file_voi = open(filename_voi,'w')

print_json(data0,top_var,file_voi)