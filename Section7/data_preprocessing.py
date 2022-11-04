import json
import pandas as pd
import os

with open('json파일 경로.json', 'r', encoding='utf-8-sig') as json_file:
    json_data = json.load(json_file)

def country_name_convert(row):
    if row['Country_Region'] in json_data:
        return json_data[row['Country_Region']]
    return row['Country_Region']

def create_dateframe(filename):

    doc = pd.read_csv(PATH + filename, encoding='utf-8-sig')
    try:
        doc = doc[['Country_Region', 'Confirmed']]  
    except:
        doc = doc[['Country/Region', 'Confirmed']] 
        doc.columns = ['Country_Region', 'Confirmed']
    doc = doc.dropna(subset=['Confirmed'])    
    doc['Country_Region'] = doc.apply(country_name_convert, axis=1)  
    doc = doc.astype({'Confirmed': 'int64'}) 
    doc = doc.groupby('Country_Region').sum() 

    date_column = filename.split(".")[0].lstrip('0').replace('-', '/') 
    doc.columns = [date_column]
    return doc


def generate_dateframe_by_path(PATH):

    file_list, csv_list = os.listdir(PATH), list()
    first_doc = True
    for file in file_list:
        if file.split(".")[-1] == 'csv':
            csv_list.append(file)
    csv_list.sort()
    
    for file in csv_list:
        doc = create_dateframe(file)
        if first_doc:
            final_doc, first_doc = doc, False
        else:
            final_doc = pd.merge(final_doc, doc, how='outer', left_index=True, right_index=True)

    final_doc = final_doc.fillna(0)
    return final_doc

PATH = '데이터들이 저장된 파일 경로'
doc = generate_dateframe_by_path(PATH)
doc = doc.astype('int64')
doc.to_csv("COVID-19-master/final_df.csv")