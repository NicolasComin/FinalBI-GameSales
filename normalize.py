import pandas as pd

df = pd.read_csv("dataset/Video Games Sales (1980-2024) - Raw.csv")

df = df.drop(columns=['img', 'last_update'])

#Converter para numeros
sales_cols = ['total_sales', 'na_sales', 'jp_sales', 'pal_sales', 'other_sales']
for col in sales_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Remover linhas onde vendas sao zero e NaN
df = df[
    (df[sales_cols].fillna(0).sum(axis=1)) > 0
]

num_cols = sales_cols + ['critic_score']

df['critic_score'] = pd.to_numeric(df['critic_score'], errors='coerce')

# Paronizacao de valor das notas de 10 pra 100
df.loc[df['critic_score'] <= 10, 'critic_score'] *= 10

group_cols = ['title', 'genre', 'publisher', 'developer']

#Agrupar jogos de diferentes plataformas
df_grouped = df.groupby(group_cols).agg({
    'console': lambda x: ', '.join(sorted(x.dropna().unique())),
    'total_sales': 'sum',
    'na_sales': 'sum',
    'jp_sales': 'sum',
    'pal_sales': 'sum',
    'other_sales': 'sum',
    'critic_score': 'mean',  
    'release_date': 'min'    
}).reset_index()

# Evite nota nula
df_grouped['critic_score'] = df_grouped['critic_score'].fillna(0)

# Formata data
df_grouped['release_date'] = pd.to_datetime(df_grouped['release_date'], dayfirst=True)

# Ordenar por vendas
df_grouped = df_grouped.sort_values(by='total_sales', ascending=False)

# Adiciona coluna de numero de plataformas 
df_grouped['num_platforms'] = df_grouped['console'].apply(lambda x: len(x.split(', ')))

# Salvar resultado
df_grouped.to_csv("dataset/dataset_tratado.csv", index=False, float_format="%.2f")

print("Normalização finalizada!")