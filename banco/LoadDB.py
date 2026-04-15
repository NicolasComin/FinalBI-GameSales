from ConectaDB import engine
from sqlalchemy import text

with engine.connect() as conn:

    print("Carregando dimensão de gêneros...")
    conn.execute(text("""
        INSERT INTO dim_genero (nome_genero)
        SELECT DISTINCT genre
        FROM staging_games
        WHERE genre IS NOT NULL
        ON CONFLICT DO NOTHING
    """))

    print("Carregando dimensão de publishers...")
    conn.execute(text("""
        INSERT INTO dim_publisher (nome_publisher)
        SELECT DISTINCT publisher
        FROM staging_games
        WHERE publisher IS NOT NULL
        ON CONFLICT DO NOTHING
    """))

    print("Carregando dimensão de developers...")
    conn.execute(text("""
        INSERT INTO dim_developer (nome_developer)
        SELECT DISTINCT developer
        FROM staging_games
        WHERE developer IS NOT NULL
        ON CONFLICT DO NOTHING
    """))

    print("Carregando dimensão de datas...")
    conn.execute(text("""
        INSERT INTO dim_data (data_lancamento, dia, mes, trimestre, ano)
        SELECT DISTINCT
            release_date,
            EXTRACT(DAY FROM release_date),
            EXTRACT(MONTH FROM release_date),
            EXTRACT(QUARTER FROM release_date),
            EXTRACT(YEAR FROM release_date)
        FROM staging_games
        WHERE release_date IS NOT NULL
        ON CONFLICT DO NOTHING
    """))

    print("Carregando tabela fato...")
    conn.execute(text("""
        INSERT INTO fato_vendas (
            titulo,
            id_genero,
            id_publisher,
            id_developer,
            id_data,
            consoles,
            num_plataformas,
            total_sales,
            na_sales,
            jp_sales,
            pal_sales,
            other_sales,
            critic_score
        )
        SELECT
            s.title,
            g.id_genero,
            p.id_publisher,
            d.id_developer,
            dt.id_data,
            s.console,
            s.num_platforms,
            s.total_sales,
            s.na_sales,
            s.jp_sales,
            s.pal_sales,
            s.other_sales,
            s.critic_score
        FROM staging_games s
        JOIN dim_genero g
            ON g.nome_genero = s.genre
        JOIN dim_publisher p
            ON p.nome_publisher = s.publisher
        JOIN dim_developer d
            ON d.nome_developer = s.developer
        JOIN dim_data dt
            ON dt.data_lancamento = s.release_date
    """))

    conn.commit()

print("Carga do Data Warehouse finalizada!")
