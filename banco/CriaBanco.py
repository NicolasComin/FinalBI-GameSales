from sqlalchemy import text
from ConectaDB import engine

sql = """
CREATE TABLE IF NOT EXISTS dim_genero (
    id_genero SERIAL PRIMARY KEY,
    nome_genero TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_publisher (
    id_publisher SERIAL PRIMARY KEY,
    nome_publisher TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_developer (
    id_developer SERIAL PRIMARY KEY,
    nome_developer TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_data (
    id_data SERIAL PRIMARY KEY,
    data_lancamento DATE UNIQUE,
    dia INT,
    mes INT,
    trimestre INT,
    ano INT
);

CREATE TABLE IF NOT EXISTS fato_vendas (
    id SERIAL PRIMARY KEY,
    titulo TEXT,
    id_genero INT REFERENCES dim_genero(id_genero),
    id_publisher INT REFERENCES dim_publisher(id_publisher),
    id_developer INT REFERENCES dim_developer(id_developer),
    id_data INT REFERENCES dim_data(id_data),
    consoles TEXT,
    num_plataformas INT,
    total_sales NUMERIC,
    na_sales NUMERIC,
    jp_sales NUMERIC,
    pal_sales NUMERIC,
    other_sales NUMERIC,
    critic_score NUMERIC
);
"""

with engine.connect() as conn:
    conn.execute(text(sql))
    conn.commit()

print("Tabelas criadas com sucesso!")
