from os import path
from glob import glob 
from multiprocessing import Pool
import tabula
import pandas as pd
import pickle


COLUNAS = ["data", "local", "tipo_leito", "total", "ocupados", "percentual_ocupados"]
PATH = "C:\\Users\\jmess\\Documents\\Workspace\\Data_Science\\data\\dados_ocupacao\\"

def processar_tipo_1_3(novos_dados, data, dados):
    dados.columns = dados.loc[0]
    dados.drop([0, len(dados.index) - 1], axis=0, inplace=True) 

    dados["local"] = dados["TIPO DE LEITO"]  

    for i in range(1, (len(dados.index) // 3) + 1):
        dados.iloc[(3 * (i - 1)) + 1: 3*i, 0] = dados.iloc[(3 * (i - 1)), 0]
        dados.iloc[(3 * (i - 1)), 4] = 'Estado'

    novos_dados.tipo_leito = dados.iloc[:,0]
    novos_dados.total = dados.iloc[:,1]
    novos_dados.ocupados = dados.iloc[:,2]
    novos_dados.percentual_ocupados = dados.iloc[:,3]
    novos_dados.local = dados.local
    novos_dados.data = data      

    return novos_dados


def save(l):
    with open("list_files", "wb") as fp:
        pickle.dump(l, fp)


def load():
    with open("list_files", "rb") as fp:
        b = pickle.load(fp)

    return b


def processar_pdf(path):
    df = tabula.read_pdf(path, pages=1, multiple_tables=True,
        area=[
            [21.20, 499.59, 30.87, 545.72],
            [29.38, 358.98, 191.58, 579.20],
            [40.16, 489.9, 60.81, 549.65],
            [57.5, 357.63, 179.39, 599.37],
            [28, 498.20, 40.55, 547.32],
            [39, 350.86, 202.03, 582.29],
            [28.64, 483.32, 42.78, 536.16],
            [40.55, 337.47, 191.61, 581.55],
            [19.71, 496.72, 30.13, 546.57],
            [30.13, 355.33, 158.13, 580.80],
            [18.97, 501.18, 30.88, 548.06],
            [30.88, 358.56, 193.10, 583.78],
            [29.39, 494.48, 42.04, 545.08],
            [39.06, 344.91, 206.50, 581.55],
            [19.71, 499.69, 32.37, 547.32],
            [29.39, 357.56, 201.29, 580.80]
        ])

    novos_dados = pd.DataFrame(columns=COLUNAS)
    
    if len(df) > 1 and df[1].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_tipo_1_3(novos_dados, df[0].columns[0], df[1])
    elif len(df) > 2 and df[2].columns[1] == 'POR TIPO DE LEITO':
        data = df[1].columns[0]

        dados  = df[2]

        if pd.isna(dados.iloc[2,0]):
            dados.drop([0, 1, 2, len(dados.index) - 1], axis=0, inplace=True)
        else:
            dados.drop([0, 1, len(dados.index) - 1], axis=0, inplace=True)

        total = []
        ocupados = []
        for i in dados.iloc[:,1]:
            split_list = i.split(' ')
            total.append(int(split_list[0])) 
            ocupados.append(int(split_list[1]))
    
        novos_dados.tipo_leito = dados.iloc[:,0]
        novos_dados.total = total
        novos_dados.ocupados = ocupados
        novos_dados.percentual_ocupados = dados.iloc[:,2]
        novos_dados.data = data
    elif len(df) > 4 and df[4].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_tipo_1_3(novos_dados, df[3].columns[0], df[4])
    elif len(df) > 5 and df[5].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_tipo_1_3(novos_dados, df[3].columns[0], df[5])
    elif len(df) > 6 and df[6].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_tipo_1_3(novos_dados, df[5].columns[0], df[6])
    elif len(df) > 13 and df[13].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_tipo_1_3(novos_dados, df[12].columns[0], df[13])
    elif len(df) > 12 and df[12].columns[1] == 'POR TIPO DE LEITO':
        dados = df[12]
        dados.drop(dados.columns[0], axis=1, inplace=True)
        novos_dados = processar_tipo_1_3(novos_dados, df[11].columns[0], dados)
    elif len(df) > 11 and df[11].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_tipo_1_3(novos_dados, df[10].columns[0], df[11])
    elif len(df) > 16 and df[16].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_tipo_1_3(novos_dados, df[15].columns[0], df[16])
    else:
        print(f'Error: {path}')
        return novos_dados

    list = load()
    list.append(path)
    save(list)    

    return novos_dados


def main():
    dados = pd.DataFrame(columns=COLUNAS)

    try:
        list = load()
    except Exception:
        list = glob(path.join(path.relpath(PATH), "**", "*.pdf"), recursive=True)
        save(list)
    
    with Pool(4) as p:
        for i in p.map(processar_pdf, list):
            dados = dados.append(i, ignore_index=True)

    dados.to_csv('dados.csv')


def test():
    resultado = processar_pdf('test.pdf')
    print(resultado)


if __name__ == '__main__':
    main()
