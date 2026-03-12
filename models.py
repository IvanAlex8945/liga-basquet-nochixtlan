"""
models.py — Modelos de Base de Datos
Liga Municipal de Basquetbol de Nochixtlán

Diseño clave para traspasos:
  - PlayerMatchStat guarda (player_id, team_id) en cada partido.
  - Al sumar estadísticas de un jugador para su equipo ACTUAL, se filtra
    por team_id = equipo_actual del jugador.
  - Esto garantiza que al cambiar de equipo, las stats anteriores no
    "contaminen" las nuevas — sin necesidad de borrar nada.
"""

from sqlalchemy import (
    Column, Integer, String, Boolean, Date, DateTime,
    ForeignKey, Text, Float, UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()


# ---------------------------------------------------------------------------
# TEMPORADA
# ---------------------------------------------------------------------------
class Season(Base):
    """
    Una temporada por categoría.

    Regla de negocio:
      - Solo puede haber UNA temporada con is_active=True por categoría.
      - is_test=True identifica torneos de prueba/sandbox que NO mezclan
        datos con temporadas reales. El Gestor de Temporadas aplica esta
        distinción visualmente en el historial.
    """
    __tablename__ = "seasons"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    name         = Column(String(100), nullable=False)   # "Torneo Apertura 2026"
    category     = Column(String(30),  nullable=False)   # "Libre", "Veteranos", etc.
    year         = Column(Integer,     nullable=False)
    is_active    = Column(Boolean,     default=True)
    is_test      = Column(Boolean,     default=False)    # True = torneo de prueba
    created_at   = Column(DateTime,    default=datetime.utcnow)

    teams   = relationship("Team",  back_populates="season",
                           cascade="all, delete-orphan")
    matches = relationship("Match", back_populates="season",
                           cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# EQUIPO
# ---------------------------------------------------------------------------
class Team(Base):
    """
    Equipo inscrito en una temporada/categoría.
    status: "Activo" | "Dado de Baja"
    """
    __tablename__ = "teams"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    name             = Column(String(100), nullable=False)
    category         = Column(String(30),  nullable=False)
    season_id        = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    status           = Column(String(20),  default="Activo")     # Activo / Dado de Baja
    permissions_used = Column(Integer,     default=0)            # Permisos (máx 3)
    defaults_count   = Column(Integer,     default=0)            # WO acumulados (≥4 → baja)
    created_at       = Column(DateTime,    default=datetime.utcnow)

    season       = relationship("Season", back_populates="teams")
    players      = relationship("Player", back_populates="team",
                                foreign_keys="Player.team_id")
    home_matches = relationship("Match", back_populates="home_team",
                                foreign_keys="Match.home_team_id")
    away_matches = relationship("Match", back_populates="away_team",
                                foreign_keys="Match.away_team_id")


# ---------------------------------------------------------------------------
# JUGADOR
# ---------------------------------------------------------------------------
class Player(Base):
    """
    Jugador registrado en la liga.
    - team_id apunta al equipo ACTUAL (None si está sin equipo).
    - joined_team_date registra cuándo se unió al equipo actual
      (sirve para filtrar sus stats post-traspaso).
    - is_active=False cuando se da de baja de un equipo pero no se transfiere.
    """
    __tablename__ = "players"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    name             = Column(String(120), nullable=False)
    number           = Column(Integer,  nullable=True)           # Dorsal
    category         = Column(String(30),  nullable=False)
    team_id          = Column(Integer, ForeignKey("teams.id"), nullable=True)
    is_active        = Column(Boolean,     default=True)
    joined_team_date = Column(Date,        nullable=True)        # Fecha alta en equipo actual
    created_at       = Column(DateTime,    default=datetime.utcnow)

    team             = relationship("Team", back_populates="players",
                                    foreign_keys=[team_id])
    match_stats      = relationship("PlayerMatchStat", back_populates="player")
    transfer_history = relationship("TransferHistory",  back_populates="player",
                                    foreign_keys="TransferHistory.player_id")


# ---------------------------------------------------------------------------
# PARTIDO
# ---------------------------------------------------------------------------
class Match(Base):
    """
    Partido entre dos equipos.
    status: "Programado" | "Jugado" | "WO Local" | "WO Visitante" |
            "WO Doble" | "Pendiente" | "Cancelado"

    phase:         "Fase Regular" | "Liguilla"
    playoff_round: "Cuartos" | "Semifinal" | "Final"  (solo si phase=="Liguilla")
    game_number:   1, 2 o 3 dentro de una serie al mejor de 3

    La UniqueConstraint original cubre solo Fase Regular. Los partidos de
    liguilla pueden repetir la misma combinación en game_number distinto,
    por lo que el índice único incluye también game_number.
    """
    __tablename__ = "matches"
    __table_args__ = (
        UniqueConstraint(
            "season_id", "jornada", "home_team_id", "away_team_id",
            "game_number",
            name="uq_match_per_jornada",
        ),
    )

    id             = Column(Integer, primary_key=True, autoincrement=True)
    season_id      = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    home_team_id   = Column(Integer, ForeignKey("teams.id"),   nullable=False)
    away_team_id   = Column(Integer, ForeignKey("teams.id"),   nullable=False)
    venue          = Column(String(80),  nullable=True)
    jornada        = Column(Integer,     nullable=False)
    vuelta         = Column(Integer,     default=1)
    phase          = Column(String(20),  default="Fase Regular")   # Fase Regular | Liguilla
    playoff_round  = Column(String(20),  nullable=True)            # Cuartos | Semifinal | Final
    game_number    = Column(Integer,     default=1)                # Juego 1, 2 o 3 de la serie
    scheduled_date = Column(DateTime,    nullable=True)
    played_date    = Column(DateTime,    nullable=True)
    status         = Column(String(25),  default="Programado")
    home_score     = Column(Integer,     nullable=True)
    away_score     = Column(Integer,     nullable=True)
    notes          = Column(Text,        nullable=True)

    season       = relationship("Season", back_populates="matches")
    home_team    = relationship("Team", foreign_keys=[home_team_id],
                                back_populates="home_matches")
    away_team    = relationship("Team", foreign_keys=[away_team_id],
                                back_populates="away_matches")
    player_stats = relationship("PlayerMatchStat", back_populates="match",
                                cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# ESTADÍSTICAS DE JUGADOR POR PARTIDO
# ---------------------------------------------------------------------------
class PlayerMatchStat(Base):
    """
    Estadísticas de un jugador en un partido específico.
    IMPORTANTE: team_id refleja el equipo con el que JUGÓ ese partido.
    Esto permite filtrar stats por equipo actual después de un traspaso.
    Solo se rastrean: puntos, triples, faltas (NO asistencias de canasta).
    """
    __tablename__ = "player_match_stats"

    id        = Column(Integer, primary_key=True, autoincrement=True)
    match_id  = Column(Integer, ForeignKey("matches.id"),  nullable=False)
    player_id = Column(Integer, ForeignKey("players.id"),  nullable=False)
    team_id   = Column(Integer, ForeignKey("teams.id"),    nullable=False)
    played    = Column(Boolean, default=True)   # Asistencia al partido (presencia)
    points    = Column(Integer, default=0)
    triples   = Column(Integer, default=0)
    fouls     = Column(Integer, default=0)

    match  = relationship("Match",  back_populates="player_stats")
    player = relationship("Player", back_populates="match_stats")
    team   = relationship("Team")


# ---------------------------------------------------------------------------
# HISTORIAL DE TRASPASOS
# ---------------------------------------------------------------------------
class TransferHistory(Base):
    """
    Registro auditabe de cada traspaso/alta/baja de jugador.
    Al hacer un traspaso:
      1. Se crea un registro aquí.
      2. Se actualiza player.team_id y player.joined_team_date.
      3. Las PlayerMatchStat antiguas quedan intactas (con el team_id anterior),
         por lo que NO aparecen en las stats del nuevo equipo.
    """
    __tablename__ = "transfer_history"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    player_id     = Column(Integer, ForeignKey("players.id"),   nullable=False)
    from_team_id  = Column(Integer, ForeignKey("teams.id"),     nullable=True)
    to_team_id    = Column(Integer, ForeignKey("teams.id"),     nullable=True)
    transfer_date = Column(DateTime, default=datetime.utcnow)
    reason        = Column(String(200), nullable=True)

    player    = relationship("Player", back_populates="transfer_history",
                             foreign_keys=[player_id])
    from_team = relationship("Team", foreign_keys=[from_team_id])
    to_team   = relationship("Team", foreign_keys=[to_team_id])
