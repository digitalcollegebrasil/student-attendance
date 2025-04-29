import pandas as pd

input_file = input("Digite o path da planilha: ")

output_file = 'nao_frequentes.xlsx'

df = pd.read_excel(input_file)

if 'Não Frequentes' not in df.columns:
    print("A coluna 'Não Frequentes' não foi encontrada na planilha.")
else:
    df_filtrado = df[df['Não Frequentes'] == 0]

    df_filtrado.to_excel(output_file, index=False)
    print(f"{len(df_filtrado)} registros com 'Não Frequentes' igual a 0 foram salvos em '{output_file}'.")
