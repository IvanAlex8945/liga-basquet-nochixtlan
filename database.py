"""
database.py — Configuración de base de datos SQLite + SQLAlchemy
Liga Municipal de Basquetbol de Nochixtlán
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from models import Base

DATABASE_URL = "sqlite:///liga_nochixtlan.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False,
)


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    """
    Activa foreign keys y WAL mode en cada nueva conexión SQLite.
    WAL mejora la concurrencia de lectura/escritura con Streamlit.
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """
    Crea todas las tablas si no existen.

    Migraciones seguras para bases de datos existentes (idempotentes):
      • Índice único anti-duplicados en matches (incluye game_number desde v4).
      • Columna is_test en seasons (v3).
      • Columnas phase, playoff_round, game_number en matches (v4 - Liguilla).
    """
    Base.metadata.create_all(bind=engine)

    with engine.connect() as conn:
        # ── v3: columna is_test ──────────────────────────────────────────
        try:
            conn.execute(text(
                "ALTER TABLE seasons ADD COLUMN is_test BOOLEAN NOT NULL DEFAULT 0"
            ))
        except Exception:
            pass

        # ── v4: columnas de liguilla ─────────────────────────────────────
        for col_sql in [
            "ALTER TABLE matches ADD COLUMN phase       VARCHAR(20) NOT NULL DEFAULT 'Fase Regular'",
            "ALTER TABLE matches ADD COLUMN playoff_round VARCHAR(20)",
            "ALTER TABLE matches ADD COLUMN game_number INTEGER NOT NULL DEFAULT 1",
        ]:
            try:
                conn.execute(text(col_sql))
            except Exception:
                pass   # columna ya existe

        # ── Índice único: ahora incluye game_number ──────────────────────
        # Borrar el índice anterior (sin game_number) si existe
        try:
            conn.execute(text("DROP INDEX IF EXISTS uq_match_per_jornada"))
        except Exception:
            pass
        # Recrear con game_number
        conn.execute(text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_match_per_jornada
            ON matches (season_id, jornada, home_team_id, away_team_id, game_number)
            """
        ))
        conn.commit()


def get_db():
    """Generador de sesión. Uso: with get_db() as db: ..."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
