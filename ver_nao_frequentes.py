import pandas as pd

input_file = input("Digite o path da planilha: ")

output_file = 'nao_frequentes.xlsx'

df = pd.read_excel(input_file)

if 'Não Frequentes' not in df.columns:
    print("A coluna 'Não Frequentes' não foi encontrada na planilha.")
else:
    if (df['Não Frequentes'] == 0).all():
        df.to_excel(output_file, index=False)
        print(f"Todos os registros têm 'Não Frequentes' igual a 0. Planilha salva como '{output_file}'.")
    else:
        print("Nem todos os registros têm 'Não Frequentes' igual a 0. Nenhuma planilha foi gerada.")
