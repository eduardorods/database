from sqlalchemy import text

from database import engine
from models import Base


def init_db() -> None:
    """Ativa a extensão pgvector e cria todas as tabelas definidas nos modelos."""
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
        print("Extensão pgvector verificada/ativada.")

    Base.metadata.create_all(bind=engine)
    print("Tabelas criadas com sucesso:")
    for table in Base.metadata.sorted_tables:
        print(f"  - {table.name}")


if __name__ == "__main__":
    init_db()
