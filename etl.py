import pandas as pd
from banco.ConectaDB import engine

df = pd.read_csv("dataset/Video Games Sales (1980-2024) - Raw.csv")

# Remover colunas desnecessárias
df = df.drop(columns=["img", "last_update"])

# Converter colunas de vendas para numérico
sales_cols = ["total_sales", "na_sales", "jp_sales", "pal_sales", "other_sales"]
for col in sales_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Remover jogos sem nenhuma venda registrada
df = df[df[sales_cols].fillna(0).sum(axis=1) > 0]

# Converter e padronizar critic_score para 0-100
df["critic_score"] = pd.to_numeric(df["critic_score"], errors="coerce")
df.loc[df["critic_score"] <= 10, "critic_score"] *= 10

# Agrupar jogos que aparecem em múltiplas plataformas
group_cols = ["title", "genre", "publisher", "developer"]

df = df.groupby(group_cols).agg(
    console=("console", lambda x: ", ".join(sorted(x.dropna().unique()))),
    total_sales=("total_sales", "sum"),
    na_sales=("na_sales", "sum"),
    jp_sales=("jp_sales", "sum"),
    pal_sales=("pal_sales", "sum"),
    other_sales=("other_sales", "sum"),
    critic_score=("critic_score", "mean"),
    release_date=("release_date", "min"),
).reset_index()

# Converter todos os valores menores ou iguais a Zero para Null
df["critic_score"] = df["critic_score"].where(df["critic_score"] > 0, other=None)

# Converter data de lançamento
df["release_date"] = pd.to_datetime(df["release_date"], dayfirst=True, errors="coerce")

# Remover registros sem data 
df = df[df["release_date"].notna()].copy()

# Adicionar coluna de número de plataformas
df["num_platforms"] = df["console"].apply(lambda x: len(x.split(", ")))

# Ordenar por total de vendas
df = df.sort_values(by="total_sales", ascending=False)

df.to_sql(
    "staging_games",
    engine,
    if_exists="replace",
    index=False
)

print("ETL finalizado!")
