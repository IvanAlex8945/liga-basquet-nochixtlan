"""
database.py — Configuración de base de datos SQLite + SQLAlchemy
Liga Municipal de Basquetbol de Nochixtlán
"""
import streamlit as st
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from models import Base

# 1. Obtener la URL de conexión (Prioridad: Nube en Secrets > Local)
# NOTA: Cambié "liguilla_nochixtlan.db" por "liga_nochixtlan.db" asumiendo que es tu archivo local actual.
DATABASE_URL = st.secrets.get(
    "DATABASE_URL", "sqlite:///liga_nochixtlan.db"
)

# 2. Ajuste CRÍTICO para PostgreSQL (Neon exige postgresql:// en lugar de postgres://)
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# 3. Configuración del motor de base de datos
# Aplicamos check_same_thread SOLO si estamos usando el archivo SQLite local
if "sqlite" in DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False,
    )
else:
    # Si es Neon (PostgreSQL), no necesitamos connect_args
    engine = create_engine(DATABASE_URL, echo=False)


# 4. Optimizaciones (PRAGMAS) SOLO para SQLite
if "sqlite" in DATABASE_URL:
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
    Nota: En Neon, Base.metadata.create_all ya creará la versión final de las tablas.
    Los ALTER TABLE fallarán silenciosamente, lo cual es el comportamiento deseado.
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
            "ALTER TABLE matches ADD COLUMN phase VARCHAR(20) NOT NULL DEFAULT 'Fase Regular'",
            "ALTER TABLE matches ADD COLUMN playoff_round VARCHAR(20)",
            "ALTER TABLE matches ADD COLUMN game_number INTEGER NOT NULL DEFAULT 1",
        ]:
            try:
                conn.execute(text(col_sql))
            except Exception:
                pass   # columna ya existe

        # ── Índice único: ahora incluye game_number ──────────────────────
        try:
            conn.execute(text("DROP INDEX IF EXISTS uq_match_per_jornada"))
        except Exception:
            pass

        try:
            conn.execute(text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_match_per_jornada
                ON matches (season_id, jornada, home_team_id, away_team_id, game_number)
                """
            ))
        except Exception:
            pass

        try:
            conn.commit()
        except Exception:
            pass


def get_db():
    """Generador de sesión. Uso: with get_db() as db: ..."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
