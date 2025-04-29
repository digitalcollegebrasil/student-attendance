import pandas as pd

input_file = input("Digite o path da planilha: ")

live_output = 'live.xlsx'

df = pd.read_excel(input_file)

if 'Nome' not in df.columns:
    print("A coluna necessária ('Nome') não foi encontrada na planilha.")
else:
    df = df[df['Nome'].astype(str).str.len() >= 3]

    df_live = df[df['Nome'].astype(str).str[2].str.upper() == 'L']

    df_live.to_excel(live_output, index=False)
    print(f"{len(df_live)} registros com a terceira letra do nome sendo 'L' foram salvos em '{live_output}'.")
