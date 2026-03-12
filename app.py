"""
app.py — Aplicación de Producción v2
Liga Municipal de Basquetbol de Nochixtlán
Stack: Python · Streamlit · SQLite · SQLAlchemy

Módulos:
  📊 Público       — Tabla de Posiciones, Líderes (con Récords), Calendario
  🔐 Admin         — Captura data_editor, Gestión CRUD, Calendario admin
  📡 Vista Streamer — comentada (activar descomentando page_streamer)

Uso: streamlit run app.py
"""

# ════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ════════════════════════════════════════════════════════════════════════════
import hashlib
import os
from contextlib import contextmanager
from datetime import datetime, date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import func
from sqlalchemy.orm import Session

from database import SessionLocal, init_db
from models import (
    Base, Season, Team, Player, Match,
    PlayerMatchStat, TransferHistory,
)

# ════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN GLOBAL
# ════════════════════════════════════════════════════════════════════════════

ADMIN_PASSWORD = st.secrets.get("password_admin", "admin123")
ADMIN_HASH = hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest()

CATEGORIES = ["Libre", "Veteranos", "Femenil", "3ra"]
VENUES = ["Cancha Bicentenario", "Cancha Techada", "Cancha III"]

MAX_ROSTER = 12
MAX_PERMISSIONS = 3
MAX_DEFAULTS_BAJA = 4

# Día de la semana por categoría (0=Lunes ... 6=Domingo)
CATEGORY_WEEKDAY = {
    "Libre":     3,   # Jueves
    "Veteranos": 4,   # Viernes
    "Femenil":   4,   # Viernes
    "3ra":       5,   # Sábado
}
WEEKDAY_NAMES = {
    0: "Lunes", 1: "Martes", 2: "Miércoles",
    3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo",
}


# ════════════════════════════════════════════════════════════════════════════
#  UTILIDADES GENERALES
# ════════════════════════════════════════════════════════════════════════════

@contextmanager
def get_db():
    """Sesión SQLAlchemy como context manager seguro."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def short_name(full_name: str) -> str:
    """
    Convierte nombre completo a 'PrimerNombre PrimerApellido'.
    Ejemplos:
      'Carlos Alberto García Martínez' → 'Carlos García'
      'María López'                    → 'María López'
      'Juan'                           → 'Juan'
    """
    if not full_name:
        return "—"
    parts = full_name.strip().split()
    if len(parts) <= 2:
        return full_name.strip()
    # Formato: Nombre [Nombre2] Apellido1 [Apellido2]
    # Retorna primer nombre + primer apellido (tercer token)
    return f"{parts[0]} {parts[2]}"


def next_weekday_date(from_date: date, weekday: int) -> date:
    """
    Devuelve la próxima fecha que caiga en `weekday` (0=Lun, 6=Dom)
    a partir de from_date. Si from_date ya es ese día, devuelve from_date.
    """
    days_ahead = weekday - from_date.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return from_date + timedelta(days=days_ahead)


def active_season(db: Session, category: str):
    return (
        db.query(Season)
        .filter(Season.category == category, Season.is_active == True)
        .first()
    )


def season_selector(db: Session, category: str, key_prefix: str = ""):
    """
    Devuelve la Season seleccionada para vistas públicas.
    Muestra la activa por defecto; ofrece un expander para ver históricas.
    """
    seasons = (
        db.query(Season)
        .filter(Season.category == category)
        .order_by(Season.is_active.desc(), Season.created_at.desc())
        .all()
    )
    if not seasons:
        return None
    # Activa primero
    season_map = {
        f"{'🟢 ' if s.is_active else '📦 '}"
        f"{s.name}"
        f"{' [PRUEBA]' if s.is_test else ''}": s
        for s in seasons
    }
    # Si solo hay una, devolverla directamente
    if len(seasons) == 1:
        return seasons[0]
    active = next((s for s in seasons if s.is_active), seasons[0])
    labels = list(season_map.keys())
    default_idx = next(
        (i for i, s in enumerate(seasons) if s.is_active), 0
    )
    with st.expander("📜 Cambiar temporada", expanded=False):
        sel_label = st.selectbox(
            "Temporada", labels,
            index=default_idx,
            key=f"{key_prefix}_season_hist",
            label_visibility="collapsed",
        )
        return season_map[sel_label]
    return active


def active_teams(db: Session, season_id: int):
    return (
        db.query(Team)
        .filter(Team.season_id == season_id, Team.status == "Activo")
        .order_by(Team.name)
        .all()
    )


def roster_count(db: Session, team_id: int) -> int:
    return (
        db.query(func.count(Player.id))
        .filter(Player.team_id == team_id, Player.is_active == True)
        .scalar()
    ) or 0


# ════════════════════════════════════════════════════════════════════════════
#  LÓGICA: TABLA DE POSICIONES
# ════════════════════════════════════════════════════════════════════════════

def calculate_standings(db: Session, season_id: int) -> pd.DataFrame:
    """
    Calcula tabla de posiciones.
    Sistema de puntuación de la liga:
      • Partido Ganado  (PG) = 3 puntos de liga
      • Partido Perdido (PP) = 1 punto de liga
      • Default / WO         = 1 punto de liga (equipo que recibe WO)
      • WO Doble             = 0 puntos para ambos
    Orden: Pts de liga DESC, Diferencia de Puntos DESC (1er desempate).
    """
    teams = db.query(Team).filter(Team.season_id == season_id).all()
    if not teams:
        return pd.DataFrame()

    matches = (
        db.query(Match)
        .filter(
            Match.season_id == season_id,
            Match.phase == "Fase Regular",          # ← Liguilla no da puntos
            Match.status.in_([
                "Jugado", "WO Local", "WO Visitante", "WO Doble"
            ]),
        )
        .all()
    )

    stats: dict = {
        t.id: {"Equipo": t.name, "PJ": 0, "PG": 0, "PP": 0,
               "WO": 0, "PF": 0, "PC": 0}
        for t in teams
    }

    for m in matches:
        h, a = m.home_team_id, m.away_team_id
        hs, as_ = (m.home_score or 0), (m.away_score or 0)

        if m.status == "Jugado":
            for tid in (h, a):
                stats[tid]["PJ"] += 1
            stats[h]["PF"] += hs
            stats[h]["PC"] += as_
            stats[a]["PF"] += as_
            stats[a]["PC"] += hs
            if hs > as_:
                stats[h]["PG"] += 1
                stats[a]["PP"] += 1
            elif as_ > hs:
                stats[a]["PG"] += 1
                stats[h]["PP"] += 1
            else:
                # Empate: ambos ganan
                stats[h]["PG"] += 1
                stats[a]["PG"] += 1

        elif m.status == "WO Local":
            # Local pierde por WO → suma PP (1 pt); visitante gana (3 pts)
            stats[h]["PJ"] += 1
            stats[h]["WO"] += 1
            stats[h]["PC"] += 20
            stats[a]["PJ"] += 1
            stats[a]["PG"] += 1
            stats[a]["PF"] += 20

        elif m.status == "WO Visitante":
            # Visitante pierde por WO → suma PP (1 pt); local gana (3 pts)
            stats[a]["PJ"] += 1
            stats[a]["WO"] += 1
            stats[a]["PC"] += 20
            stats[h]["PJ"] += 1
            stats[h]["PG"] += 1
            stats[h]["PF"] += 20

        elif m.status == "WO Doble":
            # Ambos WO → 0 pts para los dos
            stats[h]["PJ"] += 1
            stats[h]["WO"] += 1
            stats[a]["PJ"] += 1
            stats[a]["WO"] += 1

    # Calcular puntos de liga: PG*3 + PP*1 + WO*1 (WO propio como perdida)
    rows = []
    for s in stats.values():
        pts = s["PG"] * 3 + s["PP"] * 1 + s["WO"] * 1
        rows.append({**s, "DP": s["PF"] - s["PC"], "Pts": pts})

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["Pts", "DP"], ascending=[
                        False, False]).reset_index(drop=True)
    df.insert(0, "#", range(1, len(df) + 1))
    return df[["#", "Equipo", "PJ", "PG", "PP", "WO", "PF", "PC", "DP", "Pts"]]


# ════════════════════════════════════════════════════════════════════════════
#  LÓGICA: LÍDERES ESTADÍSTICOS
# ════════════════════════════════════════════════════════════════════════════

def get_top_scorers(db: Session, season_id: int, limit: int = 10,
                    phase: str | None = None) -> pd.DataFrame:
    """
    Top anotadores. Solo suma stats con el equipo ACTUAL del jugador.
    phase: None = todas | "Fase Regular" | "Liguilla"
    Columnas: Pos | Jugador (corto) | Equipo | PTS
    """
    season = db.query(Season).filter(Season.id == season_id).first()
    if not season:
        return pd.DataFrame()

    players = (
        db.query(Player)
        .filter(
            Player.category == season.category,
            Player.is_active == True,
            Player.team_id.isnot(None),
        )
        .all()
    )
    rows = []
    for p in players:
        q = (
            db.query(
                func.sum(PlayerMatchStat.points).label("pts"),
                func.count(PlayerMatchStat.id).label("gp"),
            )
            .join(Match, Match.id == PlayerMatchStat.match_id)
            .filter(
                PlayerMatchStat.player_id == p.id,
                PlayerMatchStat.team_id == p.team_id,
                Match.season_id == season_id,
                PlayerMatchStat.played == True,
            )
        )
        if phase:
            q = q.filter(Match.phase == phase)
        agg = q.first()
        pts = agg.pts or 0
        if pts > 0:
            rows.append({
                "Jugador": short_name(p.name),
                "Equipo":  p.team.name if p.team else "—",
                "PTS":     pts,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values("PTS", ascending=False).head(
        limit).reset_index(drop=True)
    df.insert(0, "Pos", range(1, len(df) + 1))
    return df[["Pos", "Jugador", "Equipo", "PTS"]]


def get_top_triples(db: Session, season_id: int, limit: int = 10,
                    phase: str | None = None) -> pd.DataFrame:
    """
    Top tripleros. Solo stats del equipo ACTUAL.
    phase: None = todas | "Fase Regular" | "Liguilla"
    Columnas: Pos | Jugador (corto) | Equipo | 3PT | Pts de 3
    """
    season = db.query(Season).filter(Season.id == season_id).first()
    if not season:
        return pd.DataFrame()

    players = (
        db.query(Player)
        .filter(
            Player.category == season.category,
            Player.is_active == True,
            Player.team_id.isnot(None),
        )
        .all()
    )
    rows = []
    for p in players:
        q = (
            db.query(func.sum(PlayerMatchStat.triples).label("trp"))
            .join(Match, Match.id == PlayerMatchStat.match_id)
            .filter(
                PlayerMatchStat.player_id == p.id,
                PlayerMatchStat.team_id == p.team_id,
                Match.season_id == season_id,
                PlayerMatchStat.played == True,
            )
        )
        if phase:
            q = q.filter(Match.phase == phase)
        agg = q.first()
        trp = agg.trp or 0
        if trp > 0:
            rows.append({
                "Jugador":  short_name(p.name),
                "Equipo":   p.team.name if p.team else "—",
                "3PT":      trp,
                "Pts de 3": trp * 3,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values("3PT", ascending=False).head(
        limit).reset_index(drop=True)
    df.insert(0, "Pos", range(1, len(df) + 1))
    return df[["Pos", "Jugador", "Equipo", "3PT", "Pts de 3"]]


def get_record_points(db: Session, season_id: int):
    """
    Récord de puntos en un solo partido de la temporada.
    Devuelve string formateado o None.
    """
    result = (
        db.query(PlayerMatchStat, Player, Team, Match)
        .join(Player, Player.id == PlayerMatchStat.player_id)
        .join(Team,   Team.id == PlayerMatchStat.team_id)
        .join(Match,  Match.id == PlayerMatchStat.match_id)
        .filter(Match.season_id == season_id, PlayerMatchStat.played == True)
        .order_by(PlayerMatchStat.points.desc())
        .first()
    )
    if not result or result[0].points == 0:
        return None
    stat, player, team, match = result
    return (
        f"RÉCORD: **{short_name(player.name)}**. "
        f"Equipo **{team.name}**  —  "
        f"**{stat.points} Puntos**  —  JORNADA {match.jornada}"
    )


def get_record_triples(db: Session, season_id: int):
    """
    Récord de triples en un solo partido de la temporada.
    Devuelve string formateado o None.
    """
    result = (
        db.query(PlayerMatchStat, Player, Team, Match)
        .join(Player, Player.id == PlayerMatchStat.player_id)
        .join(Team,   Team.id == PlayerMatchStat.team_id)
        .join(Match,  Match.id == PlayerMatchStat.match_id)
        .filter(Match.season_id == season_id, PlayerMatchStat.played == True)
        .order_by(PlayerMatchStat.triples.desc())
        .first()
    )
    if not result or result[0].triples == 0:
        return None
    stat, player, team, match = result
    return (
        f"RÉCORD: **{short_name(player.name)}**. "
        f"Equipo **{team.name}**  —  "
        f"**{stat.triples} Triples**  —  JORNADA {match.jornada}"
    )


# ════════════════════════════════════════════════════════════════════════════
#  LÓGICA: CALENDARIO / ROUND-ROBIN
# ════════════════════════════════════════════════════════════════════════════

def generate_round_robin_schedule(teams: list) -> list:
    """
    Algoritmo de rotación circular — Round-Robin Doble.
    Retorna lista de (equipo_local, equipo_visitante, num_jornada).

    Garantías anti-duplicado:
      1. Cada par (home, away) aparece exactamente UNA vez en v1 y
         (away, home) exactamente UNA vez en v2.
      2. Un set `seen` rechaza cualquier colisión antes de agregarla,
         como segunda capa de defensa además del UniqueConstraint de BD.
    """
    if len(teams) < 2:
        return []

    t = list(teams)
    if len(t) % 2 != 0:
        t.append(None)   # bye ficticio para número impar de equipos

    rounds = len(t) - 1
    half = len(t) // 2
    v1: list = []
    seen_v1: set = set()  # (home_id, away_id) para dedup

    for r in range(rounds):
        for i in range(half):
            home = t[i]
            away = t[len(t) - 1 - i]
            if home is None or away is None:
                continue
            key = (home.id, away.id)
            if key not in seen_v1:
                seen_v1.add(key)
                v1.append((home, away, r + 1))
        # Rotación circular: t[0] fijo, rotar el resto hacia adelante
        t = [t[0]] + [t[-1]] + t[1:-1]

    # Segunda vuelta: invertir roles local/visitante
    seen_v2: set = set()
    v2: list = []
    for home, away, jorn in v1:
        key = (away.id, home.id)
        if key not in seen_v2:
            seen_v2.add(key)
            v2.append((away, home, jorn + rounds))

    return v1 + v2


def playoff_eligible_players(db: Session, team: Team, season_id: int) -> list:
    total = (
        db.query(func.count(Match.id))
        .filter(
            Match.season_id == season_id,
            Match.status == "Jugado",
            (Match.home_team_id == team.id) | (Match.away_team_id == team.id),
        )
        .scalar()
    ) or 0
    threshold = (total // 2) + 1

    result = []
    for p in db.query(Player).filter(
        Player.team_id == team.id, Player.is_active == True
    ).all():
        gp = (
            db.query(func.count(PlayerMatchStat.id))
            .join(Match, Match.id == PlayerMatchStat.match_id)
            .filter(
                PlayerMatchStat.player_id == p.id,
                PlayerMatchStat.team_id == team.id,
                Match.season_id == season_id,
                PlayerMatchStat.played == True,
            )
            .scalar()
        ) or 0
        result.append({
            "Jugador":    p.name,
            "#":          p.number,
            "PJ":         gp,
            "Requeridos": threshold,
            "Elegible":   "✅" if gp >= threshold else "❌",
        })
    return sorted(result, key=lambda x: x["Elegible"])


# ════════════════════════════════════════════════════════════════════════════
#  AUTENTICACIÓN / SESIÓN
# ════════════════════════════════════════════════════════════════════════════

def is_admin() -> bool:
    return st.session_state.get("authenticated", False)


def login_widget() -> None:
    """Widget de login discreto en la barra lateral."""
    st.sidebar.markdown("---")
    if is_admin():
        st.sidebar.success("🔓 Admin activo")
        if st.sidebar.button("Cerrar sesión", key="logout_btn"):
            st.session_state["authenticated"] = False
            st.session_state["page"] = "📊 Tabla de Posiciones"
            st.rerun()
    else:
        with st.sidebar.expander("🔐 Acceso Admin", expanded=False):
            pwd = st.text_input(
                "Contraseña",
                type="password",
                key="login_pwd",
                label_visibility="collapsed",
                placeholder="Contraseña…",
            )
            if st.button("Ingresar", key="login_btn"):
                if hashlib.sha256(pwd.encode()).hexdigest() == ADMIN_HASH:
                    st.session_state["authenticated"] = True
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta.")


# ════════════════════════════════════════════════════════════════════════════
#  PÁGINA: TABLA DE POSICIONES
# ════════════════════════════════════════════════════════════════════════════

def page_standings() -> None:
    st.title("🏀 Liga Municipal de Basquetbol — Nochixtlán")
    st.subheader("📊 Tabla de Posiciones")

    cat = st.selectbox("Categoría", CATEGORIES, key="pub_cat_stand")

    with get_db() as db:
        season = season_selector(db, cat, key_prefix="stand")
        if not season:
            st.warning("No hay temporada activa para esta categoría.")
            return
        sname = f"{season.name}{' 🧪' if season.is_test else ''}"
        st.caption(
            f"📋 Temporada: **{sname}** — {season.category} {season.year}")
        df = calculate_standings(db, season.id)

    if df.empty:
        st.info("Aún no hay partidos registrados.")
        return

    def highlight_leader(row):
        if row["#"] == 1:
            return ["background-color: #2a2400; color: #FFD700"] * len(row)
        return [""] * len(row)

    st.dataframe(
        df.style.apply(highlight_leader, axis=1),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        "PJ=Jugados · PG=Ganados · PP=Perdidos · WO=Defaults · "
        "PF=Pts a Favor · PC=Pts en Contra · DP=Diferencia · "
        "Pts=Pts de Liga (PG×3 + PP×1 + WO×1)"
    )


# ════════════════════════════════════════════════════════════════════════════
#  PÁGINA: LÍDERES ESTADÍSTICOS
# ════════════════════════════════════════════════════════════════════════════

def page_leaders() -> None:
    st.title("🥇 Líderes Estadísticos")
    cat = st.selectbox("Categoría", CATEGORIES, key="pub_cat_lead")

    phase_label = st.radio(
        "📊 Mostrar estadísticas de:",
        ["Fase Regular", "Liguilla", "Ambas fases"],
        horizontal=True,
        key="lead_phase",
    )
    phase_filter: str | None = (
        None if phase_label == "Ambas fases" else phase_label
    )

    with get_db() as db:
        season = season_selector(db, cat, key_prefix="lead")
        if not season:
            st.warning("No hay temporada activa.")
            return
        sname = f"{season.name}{' 🧪' if season.is_test else ''}"
        st.caption(
            f"📋 Temporada: **{sname}** — {season.category} {season.year}")
        sid = season.id
        df_score = get_top_scorers(db, sid, phase=phase_filter)
        df_triple = get_top_triples(db, sid, phase=phase_filter)
        rec_pts = get_record_points(db, sid)
        rec_trp = get_record_triples(db, sid)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏆 Top 10 Anotadores")
        if not df_score.empty:
            st.dataframe(df_score, use_container_width=True, hide_index=True)
        else:
            st.info("Sin datos aún.")
        if rec_pts:
            st.info(f"🏆 {rec_pts}")

    with col2:
        st.subheader("🎯 Top 10 Tripleros")
        if not df_triple.empty:
            st.dataframe(df_triple, use_container_width=True, hide_index=True)
        else:
            st.info("Sin datos aún.")
        if rec_trp:
            st.info(f"🎯 {rec_trp}")


# ════════════════════════════════════════════════════════════════════════════
#  PÁGINA: CALENDARIO PÚBLICO
# ════════════════════════════════════════════════════════════════════════════

def page_calendar_public() -> None:
    st.title("📅 Calendario de Juegos")
    cat = st.selectbox("Categoría", CATEGORIES, key="pub_cat_cal")

    dia = WEEKDAY_NAMES[CATEGORY_WEEKDAY[cat]]
    st.caption(f"📌 Categoría **{cat}** juega los **{dia}s**")

    with get_db() as db:
        season = season_selector(db, cat, key_prefix="pub_cal")
        if not season:
            st.warning("No hay temporada activa.")
            return
        sname = f"{season.name}{' 🧪' if season.is_test else ''}"
        st.caption(
            f"📋 Temporada: **{sname}** — {season.category} {season.year}")
        matches = (
            db.query(Match)
            .filter(Match.season_id == season.id)
            .order_by(Match.jornada, Match.scheduled_date)
            .all()
        )
        if not matches:
            st.info("Aún no hay partidos programados.")
            return

        rows = []
        for m in matches:
            ht = db.query(Team).get(m.home_team_id)
            at = db.query(Team).get(m.away_team_id)
            fecha = m.scheduled_date.strftime(
                "%d/%m/%Y") if m.scheduled_date else "—"

            if m.status == "Pendiente":
                hora = "⏳ Pendiente"
                cancha = "Por reprogramar"
                resultado = "Pendiente por reprogramar"
            elif m.status == "Cancelado":
                hora = "—"
                cancha = "—"
                resultado = "🚫 Cancelado"
            elif m.status == "Jugado":
                hora = m.scheduled_date.strftime(
                    "%H:%M") if m.scheduled_date else "—"
                cancha = m.venue or "—"
                resultado = f"{m.home_score} - {m.away_score}"
            elif m.status in ("WO Local", "WO Visitante", "WO Doble"):
                hora = m.scheduled_date.strftime(
                    "%H:%M") if m.scheduled_date else "—"
                cancha = m.venue or "—"
                resultado = f"⚠️ {m.status}"
            else:
                hora = m.scheduled_date.strftime(
                    "%H:%M") if m.scheduled_date else "—"
                cancha = m.venue or "—"
                resultado = "Programado"

            rows.append({
                "J":         m.jornada,
                "Local":     ht.name if ht else "—",
                "Visitante": at.name if at else "—",
                "Fecha":     fecha,
                "Hora":      hora,
                "Cancha":    cancha,
                "Resultado": resultado,
            })

    df_cal = pd.DataFrame(rows)
    jornadas = ["Todas"] + sorted(df_cal["J"].unique().tolist())
    sel_j = st.selectbox("Mostrar jornada", jornadas, key="pub_cal_j")
    if sel_j != "Todas":
        df_cal = df_cal[df_cal["J"] == sel_j]

    st.dataframe(df_cal, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════════════════
#  ADMIN: GESTIÓN CRUD
# ════════════════════════════════════════════════════════════════════════════

def page_management() -> None:
    st.title("🛠️ Gestión de Equipos y Jugadores")
    cat = st.selectbox("Categoría", CATEGORIES, key="mgmt_cat")

    tabs = st.tabs([
        "🏢 Equipos",
        "👥 Cédula de Jugadores",
        "🔄 Traspasos",
        "🏆 Elegibilidad Playoffs",
        "📋 Permisos & Defaults",
    ])

    # ── helpers locales ───────────────────────────────────────────────────
    def load_teams():
        with get_db() as db:
            s = active_season(db, cat)
            if not s:
                return [], None
            return (
                db.query(Team)
                .filter(Team.season_id == s.id)
                .order_by(Team.name)
                .all()
            ), s

    # ── TAB 0: Equipos ────────────────────────────────────────────────────
    with tabs[0]:
        teams, season = load_teams()
        if not season:
            st.warning("No hay temporada activa.")
            st.stop()

        if teams:
            with get_db() as db:
                df_teams = pd.DataFrame([
                    {
                        "Equipo":    t.name,
                        "Estado":    t.status,
                        "Jugadores": roster_count(db, t.id),
                        "Permisos":  t.permissions_used or 0,
                        "WOs":       t.defaults_count or 0,
                    }
                    for t in teams
                ])
            st.dataframe(df_teams, use_container_width=True, hide_index=True)
        else:
            st.info("Aún no hay equipos. Registra el primero.")

        # ── Eliminar / Dar de Baja equipo ─────────────────────────────────
        if teams:
            st.markdown("---")
            st.subheader("🗑️ Eliminar o dar de baja equipo")
            st.caption(
                "Si el equipo **no tiene partidos** se elimina completamente. "
                "Si ya jugó, se marca como **Dado de Baja** para preservar el historial."
            )
            del_team_name = st.selectbox(
                "Equipo a eliminar / dar de baja",
                [t.name for t in teams],
                key="del_team_sel",
            )
            if st.button("🗑️ Eliminar / Dar de Baja", key="btn_del_team",
                         type="secondary"):
                with get_db() as db:
                    s = active_season(db, cat)
                    team = db.query(Team).filter(
                        Team.name == del_team_name, Team.season_id == s.id
                    ).first()
                    if not team:
                        st.error("Equipo no encontrado.")
                    else:
                        # Contar partidos históricos (jugados o cualquier estado)
                        match_count = (
                            db.query(func.count(Match.id))
                            .filter(
                                (Match.home_team_id == team.id) |
                                (Match.away_team_id == team.id)
                            )
                            .scalar()
                        ) or 0

                        if match_count == 0:
                            # Sin historial → eliminar completamente
                            # 1. Dar de baja a todos sus jugadores
                            db.query(Player).filter(
                                Player.team_id == team.id
                            ).update({"team_id": None, "is_active": False})
                            # 2. Borrar el equipo
                            db.delete(team)
                            db.commit()
                            st.success(
                                f"✅ **{del_team_name}** eliminado permanentemente. "
                                "No tenía partidos registrados."
                            )
                        else:
                            # Con historial → solo marcar inactivo
                            team.status = "Dado de Baja"
                            db.commit()
                            st.warning(
                                f"⚠️ **{del_team_name}** marcado como **Dado de Baja**. "
                                f"Tenía {match_count} partido(s) en el historial — "
                                "no se puede eliminar sin romper el historial."
                            )
                        st.rerun()

        st.markdown("---")
        st.subheader("➕ Registrar nuevo equipo")
        with st.form("form_new_team", clear_on_submit=True):
            nombre_eq = st.text_input("Nombre del equipo")
            sub_eq = st.form_submit_button("Registrar Equipo", type="primary")
        if sub_eq:
            nombre_eq = nombre_eq.strip()
            if not nombre_eq:
                st.error("El nombre no puede estar vacío.")
            else:
                with get_db() as db:
                    s = active_season(db, cat)
                    dup = db.query(Team).filter(
                        Team.season_id == s.id,
                        Team.name == nombre_eq,
                    ).first()
                    if dup:
                        st.error(f"Ya existe un equipo llamado '{nombre_eq}'.")
                    else:
                        db.add(Team(
                            name=nombre_eq, category=cat,
                            season_id=s.id, status="Activo",
                        ))
                        db.commit()
                        st.success(f"✅ '{nombre_eq}' registrado.")
                        st.rerun()

    # ── TAB 1: Cédula ─────────────────────────────────────────────────────
    with tabs[1]:
        teams, season = load_teams()
        if not teams:
            st.warning("Primero registra equipos.")
        else:
            sel_tname = st.selectbox(
                "Equipo", [t.name for t in teams], key="ced_team_sel")
            sel_team = next(t for t in teams if t.name == sel_tname)

            with get_db() as db:
                players = (
                    db.query(Player)
                    .filter(Player.team_id == sel_team.id, Player.is_active == True)
                    .order_by(Player.number)
                    .all()
                )
                count = len(players)

            pct = count / MAX_ROSTER
            st.metric(
                "Estado de cédula",
                f"{count} / {MAX_ROSTER} Jugadores",
                delta=(
                    f"{MAX_ROSTER - count} lugares disponibles"
                    if count < MAX_ROSTER else "COMPLETA"
                ),
                delta_color="normal" if count < MAX_ROSTER else "off",
            )
            st.progress(min(pct, 1.0))

            if players:
                st.dataframe(
                    pd.DataFrame([
                        {"#": p.number, "Nombre": p.name,
                         "Alta": str(p.joined_team_date or "—")}
                        for p in players
                    ]),
                    use_container_width=True,
                    hide_index=True,
                )
                st.markdown("#### Dar de baja")
                baja_map = {f"#{p.number} — {p.name}": p.id for p in players}
                b_sel = st.selectbox("Jugador", list(
                    baja_map.keys()), key="baja_sel")
                b_mot = st.text_input("Motivo (opcional)", key="baja_mot")
                if st.button("Dar de Baja", key="btn_baja", type="secondary"):
                    pid = baja_map[b_sel]
                    with get_db() as db:
                        p = db.query(Player).get(pid)
                        db.add(TransferHistory(
                            player_id=p.id, from_team_id=p.team_id,
                            to_team_id=None,
                            reason=b_mot or "Baja administrativa",
                        ))
                        p.team_id = None
                        p.is_active = False
                        db.commit()
                    st.success(f"✅ {p.name} dado de baja.")
                    st.rerun()

            # Alta
            st.markdown("---")
            st.subheader("➕ Agregar jugador")
            if count >= MAX_ROSTER:
                st.warning(
                    f"⛔ La cédula de **{sel_tname}** está completa "
                    f"({MAX_ROSTER}/{MAX_ROSTER}). Da de baja un jugador primero."
                )
            else:
                with st.form("form_player", clear_on_submit=True):
                    c1, c2, c3 = st.columns([2, 2, 1])
                    with c1:
                        p_nom = st.text_input("Nombre(s)")
                    with c2:
                        p_ape = st.text_input("Apellido(s)")
                    with c3:
                        p_num = st.number_input("Dorsal", 0, 99, 0)
                    sub_p = st.form_submit_button(
                        "➕ Registrar", type="primary")
                if sub_p:
                    nombre = f"{p_nom.strip()} {p_ape.strip()}".strip()
                    if not nombre:
                        st.error("Nombre vacío.")
                    else:
                        with get_db() as db:
                            db.add(Player(
                                name=nombre, number=p_num, category=cat,
                                team_id=sel_team.id, is_active=True,
                                joined_team_date=date.today(),
                            ))
                            db.commit()
                        st.success(f"✅ {nombre} (#{p_num}) registrado.")
                        st.rerun()

    # ── TAB 2: Traspasos ──────────────────────────────────────────────────
    with tabs[2]:
        st.info(
            "Al traspasar, las estadísticas se **reinician a 0** con el nuevo equipo. "
            "El historial anterior queda en la BD con el equipo de origen."
        )
        teams, season = load_teams()
        if not season or len(teams) < 2:
            st.warning("Se necesitan al menos 2 equipos.")
        else:
            from_name = st.selectbox(
                "Equipo origen", [t.name for t in teams], key="tr_from")
            with get_db() as db:
                s = active_season(db, cat)
                ft = db.query(Team).filter(
                    Team.name == from_name, Team.season_id == s.id
                ).first()
                fp = (
                    db.query(Player)
                    .filter(Player.team_id == ft.id, Player.is_active == True)
                    .order_by(Player.number)
                    .all()
                )

            if not fp:
                st.warning("Sin jugadores activos en este equipo.")
            else:
                p_map = {f"#{p.number} — {p.name}": p.id for p in fp}
                p_label = st.selectbox("Jugador", list(
                    p_map.keys()), key="tr_player")
                p_id = p_map[p_label]

                dest_teams = [t.name for t in teams if t.name != from_name]
                to_name = st.selectbox(
                    "Equipo destino", dest_teams, key="tr_to")

                with get_db() as db:
                    s2 = active_season(db, cat)
                    to_team = db.query(Team).filter(
                        Team.name == to_name, Team.season_id == s2.id
                    ).first()
                    dest_cnt = roster_count(db, to_team.id)

                if dest_cnt >= MAX_ROSTER:
                    st.error(f"⛔ {to_name} ya tiene la cédula completa.")
                else:
                    motivo = st.text_input("Motivo", key="tr_mot")
                    if st.button("✅ Confirmar Traspaso", type="primary", key="btn_tr"):
                        with get_db() as db:
                            s3 = active_season(db, cat)
                            player = db.query(Player).get(p_id)
                            tt = db.query(Team).filter(
                                Team.name == to_name, Team.season_id == s3.id
                            ).first()
                            db.add(TransferHistory(
                                player_id=player.id,
                                from_team_id=player.team_id,
                                to_team_id=tt.id,
                                reason=motivo or "Traspaso",
                            ))
                            player.team_id = tt.id
                            player.joined_team_date = date.today()
                            db.commit()
                        st.success(
                            f"✅ {p_label} → **{to_name}**. "
                            "Estadísticas reiniciadas."
                        )
                        st.rerun()

    # ── TAB 3: Elegibilidad ───────────────────────────────────────────────
    with tabs[3]:
        teams, season = load_teams()
        if not teams:
            st.info("Sin equipos.")
        else:
            t_sel = st.selectbox(
                "Equipo", [t.name for t in teams], key="poff_team")
            with get_db() as db:
                s = active_season(db, cat)
                tobj = db.query(Team).filter(
                    Team.name == t_sel, Team.season_id == s.id
                ).first()
                eligs = playoff_eligible_players(db, tobj, s.id)
            if eligs:
                st.dataframe(
                    pd.DataFrame(eligs), use_container_width=True, hide_index=True
                )
                st.caption("Umbral: ≥ (partidos_equipo ÷ 2) + 1")
            else:
                st.info("Sin partidos jugados aún.")

    # ── TAB 4: Permisos & Defaults ────────────────────────────────────────
    with tabs[4]:
        teams, season = load_teams()
        if teams:
            with get_db() as db:
                df_disc = pd.DataFrame([
                    {
                        "Equipo":    t.name,
                        "Permisos":  f"{t.permissions_used or 0} / {MAX_PERMISSIONS}",
                        "WOs":       t.defaults_count or 0,
                        "Estado":    t.status,
                    }
                    for t in teams
                ])
            st.dataframe(df_disc, use_container_width=True, hide_index=True)

        st.markdown("---")
        col_p, col_d = st.columns(2)

        with col_p:
            st.subheader("📋 Permiso")
            teams, season = load_teams()
            if teams:
                perm_n = st.selectbox(
                    "Equipo", [t.name for t in teams], key="perm_n")
                if st.button("Registrar Permiso", key="btn_perm"):
                    with get_db() as db:
                        s = active_season(db, cat)
                        t = db.query(Team).filter(
                            Team.name == perm_n, Team.season_id == s.id
                        ).first()
                        if (t.permissions_used or 0) >= MAX_PERMISSIONS:
                            st.error(f"⛔ Ya agotó {MAX_PERMISSIONS} permisos.")
                        else:
                            t.permissions_used = (t.permissions_used or 0) + 1
                            db.commit()
                            st.success("✅ Permiso registrado.")
                            st.rerun()

        with col_d:
            st.subheader("⚠️ WO / Default")
            teams, season = load_teams()
            if teams:
                wo_n = st.selectbox(
                    "Equipo", [t.name for t in teams], key="wo_n")
                if st.button("Registrar WO", key="btn_wo", type="secondary"):
                    with get_db() as db:
                        s = active_season(db, cat)
                        t = db.query(Team).filter(
                            Team.name == wo_n, Team.season_id == s.id
                        ).first()
                        t.defaults_count = (t.defaults_count or 0) + 1
                        if t.defaults_count >= MAX_DEFAULTS_BAJA:
                            t.status = "Dado de Baja"
                            st.warning(
                                f"⛔ {wo_n} dado de baja automáticamente.")
                        db.commit()
                    st.success("✅ WO registrado.")
                    st.rerun()


# ════════════════════════════════════════════════════════════════════════════
#  ADMIN: CAPTURA DE PARTIDO (data_editor alta velocidad)
# ════════════════════════════════════════════════════════════════════════════

LINEUP_COLS = ["player_id", "Jugador",
               "Asistencia", "Faltas", "Triples", "Puntos"]


def _build_lineup_df(players: list) -> pd.DataFrame:
    """
    DataFrame inicial para st.data_editor.
    Siempre declara columnas explícitamente para evitar KeyError
    cuando la lista de jugadores está vacía.
    """
    rows = [
        {
            "player_id":  p.id,
            "Jugador":    f"#{p.number} {short_name(p.name)}",
            "Asistencia": False,
            "Faltas":     0,
            "Triples":    0,
            "Puntos":     0,
        }
        for p in players
    ]
    return pd.DataFrame(rows, columns=LINEUP_COLS)


def _col_config() -> dict:
    return {
        "player_id":  st.column_config.NumberColumn("ID",      disabled=True, width="small"),
        "Jugador":    st.column_config.TextColumn(disabled=True, width="medium"),
        "Asistencia": st.column_config.CheckboxColumn(width="small"),
        "Faltas":     st.column_config.NumberColumn(min_value=0, max_value=5,  step=1, width="small"),
        "Triples":    st.column_config.NumberColumn(min_value=0, max_value=30, step=1, width="small"),
        "Puntos":     st.column_config.NumberColumn(min_value=0, max_value=99, step=1, width="small"),
    }


def _validate_lineup(df: pd.DataFrame, team_name: str = "") -> list:
    """
    Validaciones estrictas fila por fila antes de cualquier commit.

    Regla 1 — Asistencia automática:
        Si Faltas > 0, Triples > 0 o Puntos > 0, la casilla Asistencia
        DEBE estar marcada. Si no lo está, se reporta como error.

    Regla 2 — Coherencia matemática:
        Puntos >= Triples * 3 para todo jugador con Asistencia = True.
        (Ejemplo: 4 triples = mínimo 12 puntos totales)

    Devuelve lista de strings de error (vacía = sin problemas).
    """
    prefix = f"**[{team_name}]** " if team_name else ""
    errors = []

    for _, row in df.iterrows():
        nombre = row["Jugador"]
        asistio = bool(row["Asistencia"])
        faltas = int(row["Faltas"])
        triples = int(row["Triples"])
        puntos = int(row["Puntos"])
        tiene_stats = faltas > 0 or triples > 0 or puntos > 0

        # Regla 1: Si tiene estadísticas, Asistencia debe ser True
        if tiene_stats and not asistio:
            campos = []
            if puntos > 0:
                campos.append(f"{puntos} pts")
            if triples > 0:
                campos.append(f"{triples} triples")
            if faltas > 0:
                campos.append(f"{faltas} faltas")
            errors.append(
                f"{prefix}**{nombre}** tiene estadísticas "
                f"({', '.join(campos)}) pero su casilla de "
                f"**Asistencia no está marcada**."
            )

        # Regla 2: Coherencia matemática de triples (solo si asistió)
        if asistio and triples > 0:
            min_pts = triples * 3
            if puntos < min_pts:
                errors.append(
                    f"{prefix}**{nombre}** — "
                    f"Puntos ({puntos}) < Triples × 3 "
                    f"({triples} × 3 = {min_pts}). "
                    f"Faltan al menos {min_pts - puntos} puntos."
                )

    return errors


def _save_lineup(db: Session, match_id: int, team_id: int, df: pd.DataFrame) -> None:
    """Graba stats de un lineup en la BD (sin borrar previamente)."""
    for _, row in df.iterrows():
        db.add(PlayerMatchStat(
            match_id=match_id,
            player_id=int(row["player_id"]),
            team_id=team_id,
            played=bool(row["Asistencia"]),
            points=int(row["Puntos"]),
            triples=int(row["Triples"]),
            fouls=int(row["Faltas"]),
        ))


def _show_scoreboard(home_name: str, away_name: str,
                     home_pts: int, away_pts: int) -> None:
    """
    Muestra el marcador autocalculado de forma visual entre las dos tablas.
    Usa HTML para lograr un display grande y llamativo.
    """
    winner_h = home_pts > away_pts
    winner_a = away_pts > home_pts
    style_h = "color:#27ae60;font-weight:900;" if winner_h else (
        "color:#e74c3c;" if winner_a else "")
    style_a = "color:#27ae60;font-weight:900;" if winner_a else (
        "color:#e74c3c;" if winner_h else "")

    st.markdown(
        f"""
        <div style="
            display:flex; align-items:center; justify-content:center;
            gap:1.5rem; padding:0.8rem 0.4rem;
            background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);
            border-radius:12px; margin:0.5rem 0 1rem 0;
        ">
            <div style="text-align:right; flex:1; min-width:0;">
                <div style="font-size:0.78rem; color:#aaa; text-transform:uppercase;
                            letter-spacing:1px; white-space:nowrap; overflow:hidden;
                            text-overflow:ellipsis;">{home_name}</div>
                <div style="font-size:3.2rem; line-height:1; {style_h}
                            color:{'#2ecc71' if winner_h else ('#e74c3c' if winner_a else '#fff')}">
                    {home_pts}
                </div>
            </div>
            <div style="font-size:1.6rem; color:#888; font-weight:700; flex:0 0 auto;">VS</div>
            <div style="text-align:left; flex:1; min-width:0;">
                <div style="font-size:0.78rem; color:#aaa; text-transform:uppercase;
                            letter-spacing:1px; white-space:nowrap; overflow:hidden;
                            text-overflow:ellipsis;">{away_name}</div>
                <div style="font-size:3.2rem; line-height:1;
                            color:{'#2ecc71' if winner_a else ('#e74c3c' if winner_h else '#fff')}">
                    {away_pts}
                </div>
            </div>
        </div>
        <p style="text-align:center; font-size:0.72rem; color:#666; margin-top:-0.6rem;">
            ✦ Marcador autocalculado de la tabla de jugadores ✦
        </p>
        """,
        unsafe_allow_html=True,
    )


def page_capture() -> None:
    st.title("⚡ Captura de Partido")
    st.caption("Transcripción rápida · data_editor")

    cat = st.selectbox("Categoría", CATEGORIES, key="cap_cat")

    cap_tabs = st.tabs(["📥 Nuevo Resultado", "✏️ Editar Partido Finalizado"])

    # ── TAB 0: Nuevo resultado ────────────────────────────────────────────
    with cap_tabs[0]:
        with get_db() as db:
            season = active_season(db, cat)
            if not season:
                st.warning("Sin temporada activa.")
                st.stop()
            pending = (
                db.query(Match)
                .filter(Match.season_id == season.id, Match.status == "Programado")
                .order_by(Match.jornada, Match.scheduled_date)
                .all()
            )
            match_opts = {}
            for m in pending:
                ht = db.query(Team).get(m.home_team_id)
                at = db.query(Team).get(m.away_team_id)
                fecha = m.scheduled_date.strftime(
                    "%d/%m/%y") if m.scheduled_date else "S/F"
                lbl = f"J{m.jornada} | {ht.name} vs {at.name} ({fecha})"
                match_opts[lbl] = m.id

        if not match_opts:
            st.success("✅ Todos los partidos están capturados.")
        else:
            sel_lbl = st.selectbox("Partido", list(
                match_opts.keys()), key="cap_match")
            match_id = match_opts[sel_lbl]

            with get_db() as db:
                match = db.query(Match).get(match_id)
                home_team = db.query(Team).get(match.home_team_id)
                away_team = db.query(Team).get(match.away_team_id)
                home_ps = (
                    db.query(Player)
                    .filter(Player.team_id == home_team.id, Player.is_active == True)
                    .order_by(Player.number).all()
                )
                away_ps = (
                    db.query(Player)
                    .filter(Player.team_id == away_team.id, Player.is_active == True)
                    .order_by(Player.number).all()
                )

            # data_editor local
            st.markdown(f"#### 🏠 {home_team.name}")
            home_edited = st.data_editor(
                _build_lineup_df(home_ps),
                column_config=_col_config(),
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="ed_home_new",
            )

            # data_editor visitante
            st.markdown(f"#### ✈️ {away_team.name}")
            away_edited = st.data_editor(
                _build_lineup_df(away_ps),
                column_config=_col_config(),
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="ed_away_new",
            )

            # ── Marcador autocalculado ────────────────────────────────────
            home_score = int(home_edited["Puntos"].sum())
            away_score = int(away_edited["Puntos"].sum())
            _show_scoreboard(home_team.name, away_team.name,
                             home_score, away_score)

            st.markdown("---")
            if st.button("💾 GUARDAR RESULTADOS", type="primary",
                         use_container_width=True, key="btn_save_new"):

                errs = (
                    _validate_lineup(home_edited, home_team.name) +
                    _validate_lineup(away_edited, away_team.name)
                )
                if errs:
                    st.error("❌ Corrige los errores antes de guardar:")
                    for e in errs:
                        st.markdown(f"  • {e}")
                else:
                    with get_db() as db:
                        m = db.query(Match).get(match_id)
                        ht = db.query(Team).get(m.home_team_id)
                        at = db.query(Team).get(m.away_team_id)
                        db.query(PlayerMatchStat).filter(
                            PlayerMatchStat.match_id == match_id
                        ).delete()
                        _save_lineup(db, match_id, ht.id, home_edited)
                        _save_lineup(db, match_id, at.id, away_edited)
                        m.status = "Jugado"
                        m.home_score = home_score
                        m.away_score = away_score
                        m.played_date = datetime.now()
                        db.commit()
                    st.success("✅ Partido guardado.")
                    st.balloons()
                    st.rerun()

            # WO rápido
            st.markdown("---")
            st.caption("Registrar resultado especial:")
            cwo1, cwo2 = st.columns(2)
            with cwo1:
                if st.button(f"⚠️ WO — {home_team.name}", key="wo_h"):
                    with get_db() as db:
                        m = db.query(Match).get(match_id)
                        ht = db.query(Team).get(m.home_team_id)
                        m.status = "WO Local"
                        m.home_score = 0
                        m.away_score = 20
                        m.played_date = datetime.now()
                        ht.defaults_count = (ht.defaults_count or 0) + 1
                        if ht.defaults_count >= MAX_DEFAULTS_BAJA:
                            ht.status = "Dado de Baja"
                        db.commit()
                    st.success("WO registrado.")
                    st.rerun()
            with cwo2:
                if st.button(f"⚠️ WO — {away_team.name}", key="wo_a"):
                    with get_db() as db:
                        m = db.query(Match).get(match_id)
                        at = db.query(Team).get(m.away_team_id)
                        m.status = "WO Visitante"
                        m.home_score = 20
                        m.away_score = 0
                        m.played_date = datetime.now()
                        at.defaults_count = (at.defaults_count or 0) + 1
                        if at.defaults_count >= MAX_DEFAULTS_BAJA:
                            at.status = "Dado de Baja"
                        db.commit()
                    st.success("WO registrado.")
                    st.rerun()

    # ── TAB 1: Editar partido finalizado ──────────────────────────────────
    with cap_tabs[1]:
        st.subheader("✏️ Corrección de Resultados")

        with get_db() as db:
            season = active_season(db, cat)
            if not season:
                st.info("Sin temporada activa.")
                st.stop()
            played = (
                db.query(Match)
                .filter(Match.season_id == season.id, Match.status == "Jugado")
                .order_by(Match.jornada.desc())
                .all()
            )
            played_opts = {}
            for m in played:
                ht = db.query(Team).get(m.home_team_id)
                at = db.query(Team).get(m.away_team_id)
                lbl = f"J{m.jornada} | {ht.name} {m.home_score}-{m.away_score} {at.name}"
                played_opts[lbl] = m.id

        if not played_opts:
            st.info("No hay partidos finalizados aún.")
        else:
            edit_lbl = st.selectbox("Partido", list(
                played_opts.keys()), key="edit_sel")
            edit_mid = played_opts[edit_lbl]

            with get_db() as db:
                match = db.query(Match).get(edit_mid)
                home_team = db.query(Team).get(match.home_team_id)
                away_team = db.query(Team).get(match.away_team_id)
                home_ps = (
                    db.query(Player)
                    .filter(Player.team_id == home_team.id, Player.is_active == True)
                    .order_by(Player.number).all()
                )
                away_ps = (
                    db.query(Player)
                    .filter(Player.team_id == away_team.id, Player.is_active == True)
                    .order_by(Player.number).all()
                )
                # Cargar stats existentes indexadas por player_id
                ex: dict = {}
                for st_row in (
                    db.query(PlayerMatchStat)
                    .filter(PlayerMatchStat.match_id == edit_mid).all()
                ):
                    ex[st_row.player_id] = {
                        "Asistencia": st_row.played,
                        "Faltas":     st_row.fouls,
                        "Triples":    st_row.triples,
                        "Puntos":     st_row.points,
                    }

            def build_edit_df(ps: list) -> pd.DataFrame:
                rows = []
                for p in ps:
                    s = ex.get(p.id, {})
                    rows.append({
                        "player_id":  p.id,
                        "Jugador":    f"#{p.number} {short_name(p.name)}",
                        "Asistencia": s.get("Asistencia", False),
                        "Faltas":     s.get("Faltas",     0),
                        "Triples":    s.get("Triples",    0),
                        "Puntos":     s.get("Puntos",     0),
                    })
                return pd.DataFrame(rows, columns=LINEUP_COLS)

            c_h, c_vs, c_a = st.columns([3, 1, 3])
            with c_h:
                st.markdown(f"**🏠 {home_team.name}**")
            with c_vs:
                st.markdown(
                    "<div style='text-align:center;padding-top:0.4rem'><b>VS</b></div>",
                    unsafe_allow_html=True,
                )
            with c_a:
                st.markdown(f"**✈️ {away_team.name}**")

            st.markdown(f"#### 🏠 {home_team.name}")
            h_edit = st.data_editor(
                build_edit_df(home_ps),
                column_config=_col_config(),
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="ed_home_edit",
            )

            st.markdown(f"#### ✈️ {away_team.name}")
            a_edit = st.data_editor(
                build_edit_df(away_ps),
                column_config=_col_config(),
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="ed_away_edit",
            )

            # ── Marcador autocalculado ────────────────────────────────────
            e_hs = int(h_edit["Puntos"].sum())
            e_as = int(a_edit["Puntos"].sum())
            _show_scoreboard(home_team.name, away_team.name, e_hs, e_as)

            st.markdown("---")
            if st.button("💾 ACTUALIZAR PARTIDO", type="primary",
                         use_container_width=True, key="btn_update"):

                errs = (
                    _validate_lineup(h_edit, home_team.name) +
                    _validate_lineup(a_edit, away_team.name)
                )
                if errs:
                    st.error("❌ Corrige los errores:")
                    for e in errs:
                        st.markdown(f"  • {e}")
                else:
                    with get_db() as db:
                        m = db.query(Match).get(edit_mid)
                        ht = db.query(Team).get(m.home_team_id)
                        at = db.query(Team).get(m.away_team_id)
                        db.query(PlayerMatchStat).filter(
                            PlayerMatchStat.match_id == edit_mid
                        ).delete()
                        _save_lineup(db, edit_mid, ht.id, h_edit)
                        _save_lineup(db, edit_mid, at.id, a_edit)
                        m.home_score = e_hs
                        m.away_score = e_as
                        m.played_date = datetime.now()
                        db.commit()
                    st.success("✅ Partido actualizado.")
                    st.rerun()


# ════════════════════════════════════════════════════════════════════════════
#  ADMIN: CALENDARIO
# ════════════════════════════════════════════════════════════════════════════

def page_calendar_admin() -> None:
    st.title("📅 Administración de Calendario")
    cat = st.selectbox("Categoría", CATEGORIES, key="adm_cal_cat")

    dia = WEEKDAY_NAMES[CATEGORY_WEEKDAY[cat]]
    st.info(f"📌 Categoría **{cat}** juega los **{dia}s**")

    with get_db() as db:
        season = active_season(db, cat)
        if not season:
            st.warning("Sin temporada activa.")
            return
        teams = active_teams(db, season.id)
        all_m = (
            db.query(Match)
            .filter(Match.season_id == season.id)
            .order_by(Match.jornada, Match.scheduled_date)
            .all()
        )
        # Snapshot en memoria — evita sesiones SQLAlchemy abiertas en widgets
        match_ids = [m.id for m in all_m]
        match_snap = {
            m.id: {
                "jornada": m.jornada,
                "vuelta":  m.vuelta,
                "status":  m.status,
                "venue":   m.venue,
                "sched":   m.scheduled_date,
                "home_id": m.home_team_id,
                "away_id": m.away_team_id,
            }
            for m in all_m
        }
        team_map = {t.id: t.name for t in db.query(Team).all()}

    # Estado de sesión para edición inline (toggle por match_id)
    if "cal_editing_mid" not in st.session_state:
        st.session_state["cal_editing_mid"] = None

    cal_tabs = st.tabs([
        "📋 Ver / Editar Partidos",
        "➕ Partido Manual",
        "🔧 Generar Calendario",
        "🏆 Generar Liguilla",
        "📱 Texto WhatsApp",
    ])

    # ══════════════════════════════════════════════════════════════════════
    #  TAB 0 — VER / EDITAR PARTIDOS
    # ══════════════════════════════════════════════════════════════════════
    with cal_tabs[0]:
        if not match_ids:
            st.info(
                "No hay partidos generados. "
                "Usa 'Generar Calendario' o 'Partido Manual'."
            )
        else:
            # Filtros de visualización
            col_f1, col_f2 = st.columns([2, 2])
            with col_f1:
                jornadas = sorted({mi["jornada"]
                                  for mi in match_snap.values()})
                j_sel = st.selectbox(
                    "Jornada", ["Todas"] + jornadas, key="cal_j_fil"
                )
            with col_f2:
                status_opts = [
                    "Todos", "Programado", "Pendiente", "Cancelado",
                    "Jugado", "WO Local", "WO Visitante", "WO Doble",
                ]
                st_fil = st.selectbox("Estado", status_opts, key="cal_st_fil")

            filtered = [
                mid for mid in match_ids
                if (j_sel == "Todas" or match_snap[mid]["jornada"] == j_sel)
                and (st_fil == "Todos" or match_snap[mid]["status"] == st_fil)
            ]

            if not filtered:
                st.info("Sin partidos con ese filtro.")
            else:
                BADGE = {
                    "Programado":   "🔵",
                    "Pendiente":    "⏳",
                    "Cancelado":    "🚫",
                    "Jugado":       "✅",
                    "WO Local":     "⚠️",
                    "WO Visitante": "⚠️",
                    "WO Doble":     "🚫",
                }
                for mid in filtered:
                    mi = match_snap[mid]
                    hn = team_map.get(mi["home_id"], "—")
                    an = team_map.get(mi["away_id"], "—")
                    sched_str = (
                        mi["sched"].strftime("%d/%m/%Y %H:%M")
                        if mi["sched"] else "Sin fecha"
                    )
                    venue_str = mi["venue"] or "Sin cancha"
                    badge = BADGE.get(mi["status"], "❓")

                    c_info, c_btn = st.columns([5, 1])
                    with c_info:
                        st.markdown(
                            f"{badge} **J{mi['jornada']}** · "
                            f"**{hn}** vs **{an}**  "
                            f"| {sched_str} | {venue_str} | _{mi['status']}_"
                        )
                    with c_btn:
                        if st.button("✏️ Editar", key=f"edit_btn_{mid}",
                                     use_container_width=True):
                            # Toggle: mismo ID cierra el formulario
                            st.session_state["cal_editing_mid"] = (
                                None
                                if st.session_state["cal_editing_mid"] == mid
                                else mid
                            )
                            st.rerun()

                    # Formulario inline — aparece bajo la fila seleccionada
                    if st.session_state["cal_editing_mid"] == mid:
                        with st.container(border=True):
                            st.markdown(
                                f"#### ✏️ Editando: J{mi['jornada']} · "
                                f"{hn} vs {an}"
                            )
                            default_d = (
                                mi["sched"].date() if mi["sched"]
                                else next_weekday_date(
                                    date.today(), CATEGORY_WEEKDAY[cat]
                                )
                            )
                            default_t = (
                                mi["sched"].time() if mi["sched"]
                                else datetime.strptime("19:00", "%H:%M").time()
                            )
                            ef1, ef2, ef3 = st.columns([2, 2, 1])
                            with ef1:
                                new_d = st.date_input(
                                    "Nueva fecha", value=default_d,
                                    key=f"efd_{mid}",
                                    format="DD/MM/YYYY"
                                )
                                if new_d.weekday() != CATEGORY_WEEKDAY[cat]:
                                    st.caption(
                                        f"⚠️ {WEEKDAY_NAMES[new_d.weekday()]} "
                                        f"— se recomienda {dia}."
                                    )
                            with ef2:
                                new_t = st.time_input(
                                    "Nueva hora", value=default_t,
                                    key=f"eft_{mid}",
                                )
                                v_idx = (
                                    VENUES.index(mi["venue"])
                                    if mi["venue"] in VENUES else 0
                                )
                                new_v = st.selectbox(
                                    "Cancha", VENUES,
                                    index=v_idx, key=f"efv_{mid}",
                                )
                                STATUS_EDIT_OPTS = [
                                    "Programado", "Pendiente", "Cancelado"
                                ]
                                cur_status = (
                                    mi["status"]
                                    if mi["status"] in STATUS_EDIT_OPTS
                                    else "Programado"
                                )
                                new_status = st.selectbox(
                                    "Estado del partido",
                                    STATUS_EDIT_OPTS,
                                    index=STATUS_EDIT_OPTS.index(cur_status),
                                    key=f"efst_{mid}",
                                )
                                if new_status == "Pendiente":
                                    st.caption(
                                        "⏳ Se mostrará como "
                                        "'Pendiente por reprogramar' en el calendario."
                                    )
                            with ef3:
                                st.write("")
                                st.write("")
                                st.write("")
                                if st.button(
                                    "💾 Guardar", key=f"efs_{mid}",
                                    type="primary", use_container_width=True,
                                ):
                                    with get_db() as db:
                                        mo = db.query(Match).get(mid)
                                        mo.scheduled_date = datetime.combine(
                                            new_d, new_t
                                        )
                                        mo.venue = new_v
                                        mo.status = new_status
                                        db.commit()
                                    st.session_state["cal_editing_mid"] = None
                                    st.success("✅ Partido actualizado.")
                                    st.rerun()
                                if st.button(
                                    "✖ Cancelar", key=f"efc_{mid}",
                                    use_container_width=True,
                                ):
                                    st.session_state["cal_editing_mid"] = None
                                    st.rerun()

                                # ── Eliminar partido ──────────────────────
                                st.write("")   # espaciado visual
                                can_delete = mi["status"] not in (
                                    "Jugado", "WO Local",
                                    "WO Visitante", "WO Doble",
                                )
                                if st.button(
                                    "🗑️ Eliminar",
                                    key=f"efdel_{mid}",
                                    type="secondary",
                                    use_container_width=True,
                                    disabled=not can_delete,
                                    help=(
                                        "Solo se pueden eliminar partidos "
                                        "Programados, Pendientes o Cancelados."
                                        if not can_delete else
                                        "Eliminar este partido de la base de datos."
                                    ),
                                ):
                                    with get_db() as db:
                                        mo = db.query(Match).get(mid)
                                        if mo and mo.status not in (
                                            "Jugado", "WO Local",
                                            "WO Visitante", "WO Doble",
                                        ):
                                            db.query(PlayerMatchStat).filter(
                                                PlayerMatchStat.match_id == mid
                                            ).delete()
                                            db.delete(mo)
                                            db.commit()
                                    st.session_state["cal_editing_mid"] = None
                                    st.success("🗑️ Partido eliminado.")
                                    st.rerun()

                    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    #  TAB 1 — CREAR PARTIDO MANUAL
    # ══════════════════════════════════════════════════════════════════════
    with cal_tabs[1]:
        st.subheader("➕ Crear Partido Manual")
        st.caption(
            "Agrega un partido fuera del calendario generado, o crea "
            "encuentros de liguilla / reclasificación."
        )
        if len(teams) < 2:
            st.warning("Necesitas al menos 2 equipos activos.")
        else:
            team_names = [t.name for t in teams]
            with st.form("manual_match_form", clear_on_submit=False):
                fc1, fc2 = st.columns(2)
                with fc1:
                    jornada_manual = st.number_input(
                        "Jornada / Ronda",
                        min_value=1, max_value=99,
                        value=max(
                            (mi["jornada"] for mi in match_snap.values()),
                            default=0,
                        ) + 1,
                    )
                    vuelta_manual = st.selectbox("Vuelta", [1, 2])
                    home_name = st.selectbox(
                        "Equipo Local 🏠", team_names, key="man_home"
                    )
                with fc2:
                    away_name = st.selectbox(
                        "Equipo Visitante ✈️",
                        team_names,          # lista completa — validación al guardar
                        key="man_away",
                    )
                    default_fecha = next_weekday_date(
                        date.today(), CATEGORY_WEEKDAY[cat]
                    )
                    fecha_manual = st.date_input(
                        "Fecha", value=default_fecha, format="DD/MM/YYYY")
                    hora_manual = st.time_input(
                        "Hora",
                        value=datetime.strptime("19:00", "%H:%M").time(),
                    )
                    cancha_manual = st.selectbox("Cancha", VENUES)

                if fecha_manual.weekday() != CATEGORY_WEEKDAY[cat]:
                    st.warning(
                        f"⚠️ {WEEKDAY_NAMES[fecha_manual.weekday()]} — "
                        f"se recomienda {dia}."
                    )
                sub_manual = st.form_submit_button(
                    "➕ Crear Partido", type="primary"
                )

            if sub_manual:
                # Validación: no puede ser el mismo equipo
                if home_name == away_name:
                    st.error(
                        "⛔ El equipo local y visitante no pueden ser el mismo.")
                else:
                    with get_db() as db:
                        s = active_season(db, cat)
                        ht = db.query(Team).filter(
                            Team.name == home_name, Team.season_id == s.id
                        ).first()
                        at = db.query(Team).filter(
                            Team.name == away_name, Team.season_id == s.id
                        ).first()
                        existing = db.query(Match).filter(
                            Match.season_id == s.id,
                            Match.jornada == jornada_manual,
                            Match.home_team_id == ht.id,
                            Match.away_team_id == at.id,
                        ).first()
                        if existing:
                            st.error(
                                f"⛔ Ya existe **{home_name} vs {away_name}** "
                                f"en la Jornada {jornada_manual}."
                            )
                        else:
                            db.add(Match(
                                season_id=s.id,
                                home_team_id=ht.id,
                                away_team_id=at.id,
                                venue=cancha_manual,
                                jornada=jornada_manual,
                                vuelta=vuelta_manual,
                                scheduled_date=datetime.combine(
                                    fecha_manual, hora_manual
                                ),
                                status="Programado",
                            ))
                            db.commit()
                            st.success(
                                f"✅ **{home_name}** vs **{away_name}** — "
                                f"J{jornada_manual} · "
                                f"{fecha_manual.strftime('%d/%m/%Y')} "
                                f"{hora_manual.strftime('%H:%M')} hrs · "
                                f"{cancha_manual}"
                            )
                            st.rerun()

    # ══════════════════════════════════════════════════════════════════════
    #  TAB 2 — GENERAR CALENDARIO ROUND-ROBIN
    # ══════════════════════════════════════════════════════════════════════
    with cal_tabs[2]:
        if len(teams) < 2:
            st.warning("Necesitas al menos 2 equipos activos.")
        else:
            st.warning(
                "⚠️ Se borrarán **solo** los partidos con estado **Programado**. "
                "Los Jugados/WO no se afectan."
            )
            programados = sum(
                1 for mi in match_snap.values() if mi["status"] == "Programado"
            )
            if programados:
                st.info(
                    f"Hay **{programados}** partido(s) Programado(s) "
                    "que serán eliminados."
                )

            default_start = next_weekday_date(
                date.today(), CATEGORY_WEEKDAY[cat])
            start_d = st.date_input(
                f"Fecha de inicio (sugerida: próximo {dia})",
                value=default_start,
                key="gen_sd",
                format="DD/MM/YYYY"
            )
            if start_d.weekday() != CATEGORY_WEEKDAY[cat]:
                st.warning(
                    f"Eligiste {WEEKDAY_NAMES[start_d.weekday()]}. "
                    f"Se recomienda {dia}."
                )
            day_gap = st.number_input(
                "Días entre jornadas", 7, 30, 7, key="gen_gap"
            )

            # Configuración de horarios automáticos
            st.markdown("##### ⏰ Horarios automáticos por jornada")
            gc1, gc2, gc3 = st.columns(3)
            with gc1:
                hora_inicio = st.time_input(
                    "Primer partido",
                    value=datetime.strptime("18:00", "%H:%M").time(),
                    key="gen_hora_ini",
                )
            with gc2:
                hora_limite = st.time_input(
                    "Último horario (Cancha 1)",
                    value=datetime.strptime("21:00", "%H:%M").time(),
                    key="gen_hora_lim",
                )
            with gc3:
                st.caption(
                    "Si se agota la Cancha Bicentenario, "
                    "los partidos extras van a Cancha Techada "
                    "reiniciando desde la hora de inicio."
                )

            fixtures_preview = generate_round_robin_schedule(teams)
            st.metric(
                "Partidos a generar",
                len(fixtures_preview),
                help=(
                    f"{len(teams)} equipos · "
                    f"{len(fixtures_preview)//2} por vuelta × 2 vueltas"
                ),
            )

            if st.button(
                "🔧 Generar Round-Robin Doble",
                type="primary",
                key="gen_btn",
            ):
                with get_db() as db:
                    s = active_season(db, cat)
                    teams_q = active_teams(db, s.id)

                    # Borrar solo programados
                    db.query(Match).filter(
                        Match.season_id == s.id,
                        Match.status == "Programado",
                    ).delete()

                    fixtures = generate_round_robin_schedule(teams_q)
                    total_rounds = len(fixtures) // 2
                    inserted = 0
                    skipped = 0

                    # Agrupar fixtures por jornada para asignar horarios
                    from collections import defaultdict
                    by_jornada: dict = defaultdict(list)
                    for home, away, jn in fixtures:
                        by_jornada[jn].append((home, away))

                    # Definir slots disponibles por cancha
                    hora_ini_h = hora_inicio.hour
                    hora_lim_h = hora_limite.hour
                    # Slots: 18, 19, 20, 21 hrs (inclusive) en Cancha 1
                    slots_c1 = list(range(hora_ini_h, hora_lim_h + 1))

                    for jn in sorted(by_jornada.keys()):
                        vuelta = 1 if jn <= total_rounds else 2
                        base_date = start_d + timedelta(
                            days=(jn - 1) * int(day_gap)
                        )
                        partidos_jornada = by_jornada[jn]

                        # Asignar slots: Bicentenario primero, luego Techada
                        slot_idx = 0
                        cancha_idx = 0  # 0 = Bicentenario, 1 = Techada
                        current_slots = slots_c1[:]  # copia para esta jornada

                        for home, away in partidos_jornada:
                            # Si se agotaron los slots de la cancha actual
                            if slot_idx >= len(current_slots):
                                cancha_idx += 1
                                slot_idx = 0
                                # Slots de la siguiente cancha: mismos horarios
                                current_slots = slots_c1[:]

                            hora_slot = current_slots[slot_idx]
                            slot_idx += 1

                            cancha = VENUES[min(cancha_idx, len(VENUES) - 1)]
                            match_dt = datetime.combine(
                                base_date,
                                datetime.strptime(
                                    f"{hora_slot:02d}:00", "%H:%M"
                                ).time(),
                            )

                            dup = db.query(Match).filter(
                                Match.season_id == s.id,
                                Match.jornada == jn,
                                Match.home_team_id == home.id,
                                Match.away_team_id == away.id,
                            ).first()
                            if dup:
                                skipped += 1
                                continue

                            db.add(Match(
                                season_id=s.id,
                                home_team_id=home.id,
                                away_team_id=away.id,
                                venue=cancha,
                                jornada=jn,
                                vuelta=vuelta,
                                scheduled_date=match_dt,
                                status="Programado",
                            ))
                            inserted += 1

                    db.commit()

                msg = f"✅ {inserted} partido(s) generados."
                if skipped:
                    msg += f" ({skipped} omitido(s) por duplicado)."
                st.success(msg)
                st.rerun()

    # ══════════════════════════════════════════════════════════════════════
    #  TAB 3 — GENERAR LIGUILLA
    # ══════════════════════════════════════════════════════════════════════
    with cal_tabs[3]:
        st.subheader("🏆 Asistente de Liguilla")
        st.caption(
            "Genera los cruces de liguilla basándote en la Tabla de Posiciones "
            "de Fase Regular. Las series se llevan en partidos con `phase='Liguilla'`."
        )

        with get_db() as db:
            s = active_season(db, cat)
            sid = s.id
            std_df = calculate_standings(db, sid)   # solo Fase Regular

        if std_df.empty or len(std_df) < 4:
            st.warning(
                "Necesitas al menos 4 equipos con partidos de Fase Regular "
                "para generar liguilla."
            )
        else:
            # Rankings: top 8 (o los que haya)
            top8 = std_df.head(8)["Equipo"].tolist()
            n_q = len(top8) // 2   # pares de cuartos posibles

            # Cruces cuartos: 1v8, 2v7, 3v6, 4v5 (por posición)
            cuartos_cruces = [
                (top8[i], top8[len(top8) - 1 - i])
                for i in range(n_q)
            ]

            # ── SECCIÓN CUARTOS ───────────────────────────────────────────
            st.markdown("### ⚔️ Cuartos de Final")
            st.info(
                f"Top {len(top8)} equipos: "
                + " · ".join(f"**{i+1}.** {t}" for i, t in enumerate(top8))
            )

            fmt_qf = st.selectbox(
                "Formato Cuartos de Final",
                ["A 1 partido", "Al mejor de 3"],
                key="qf_fmt",
            )
            next_jornada_qf = (
                max((mi["jornada"]
                    for mi in match_snap.values()), default=0) + 1
            )

            if st.button("⚔️ Generar Cuartos de Final", key="btn_qf",
                         type="primary"):
                with get_db() as db:
                    s2 = active_season(db, cat)
                    inserted = 0
                    for i, (h_name, a_name) in enumerate(cuartos_cruces):
                        ht = db.query(Team).filter(
                            Team.name == h_name, Team.season_id == s2.id
                        ).first()
                        at = db.query(Team).filter(
                            Team.name == a_name, Team.season_id == s2.id
                        ).first()
                        if not ht or not at:
                            continue
                        games = 1 if fmt_qf == "A 1 partido" else 2
                        for g in range(1, games + 1):
                            existing = db.query(Match).filter(
                                Match.season_id == s2.id,
                                Match.home_team_id == ht.id,
                                Match.away_team_id == at.id,
                                Match.playoff_round == "Cuartos",
                                Match.game_number == g,
                            ).first()
                            if not existing:
                                # J visitante en juego 2 (cancha del bajo sembrado)
                                db.add(Match(
                                    season_id=s2.id,
                                    home_team_id=ht.id if g != 2 else at.id,
                                    away_team_id=at.id if g != 2 else ht.id,
                                    jornada=next_jornada_qf +
                                    (g - 1),  # ✅ CORRECCIÓN
                                    vuelta=1,
                                    phase="Liguilla",
                                    playoff_round="Cuartos",
                                    game_number=g,
                                    status="Programado",
                                ))
                                inserted += 1
                    db.commit()
                st.success(f"✅ {inserted} partido(s) de Cuartos generados.")
                st.rerun()

            # ── Evaluador de Series (Cuartos) ─────────────────────────────
            with st.expander("📊 Revisar series de Cuartos / Generar Juego 3",
                             expanded=False):
                with get_db() as db:
                    s3 = active_season(db, cat)
                    qf_matches = (
                        db.query(Match)
                        .filter(
                            Match.season_id == s3.id,
                            Match.phase == "Liguilla",
                            Match.playoff_round == "Cuartos",
                        )
                        .order_by(Match.home_team_id)
                        .all()
                    )
                    team_map_all = {t.id: t.name for t in db.query(Team).all()}

                    # Agrupar por cruce (home_id, away_id normalizados)
                    from collections import defaultdict
                    series: dict = defaultdict(list)
                    for m in qf_matches:
                        key = tuple(sorted([m.home_team_id, m.away_team_id]))
                        series[key].append(m)

                    needs_g3 = []
                    for (tid1, tid2), games in series.items():
                        w1 = sum(
                            1 for g in games
                            if g.status == "Jugado" and (
                                (g.home_team_id == tid1 and (g.home_score or 0) > (g.away_score or 0)) or
                                (g.away_team_id == tid1 and (
                                    g.away_score or 0) > (g.home_score or 0))
                            )
                        )
                        w2 = len(
                            [g for g in games if g.status == "Jugado"]) - w1
                        played = sum(1 for g in games if g.status == "Jugado")
                        n1, n2 = team_map_all.get(
                            tid1, "?"), team_map_all.get(tid2, "?")
                        if w1 > w2:
                            st.success(
                                f"🏆 **{n1}** avanza ({w1}-{w2} vs {n2})")
                        elif w2 > w1:
                            st.success(
                                f"🏆 **{n2}** avanza ({w2}-{w1} vs {n1})")
                        elif played == 2:
                            st.warning(
                                f"⚖️ Serie empatada 1-1: **{n1}** vs **{n2}** — necesita Juego 3")
                            needs_g3.append((tid1, tid2, n1, n2))
                        else:
                            st.info(f"🔵 En curso: {n1} {w1}-{w2} {n2}")

                    if needs_g3:
                        st.markdown("---")
                        next_j_g3 = (
                            max((m.jornada for m in qf_matches),
                                default=next_jornada_qf) + 1
                        )
                        if st.button("⚡ Generar Juego 3 (Desempate)", key="btn_qf_g3"):
                            with get_db() as db2:
                                s4 = active_season(db2, cat)
                                for tid1, tid2, n1, n2 in needs_g3:
                                    ht = db2.query(Team).get(tid1)
                                    at = db2.query(Team).get(tid2)
                                    existing_g3 = db2.query(Match).filter(
                                        Match.season_id == s4.id,
                                        Match.home_team_id == ht.id,
                                        Match.away_team_id == at.id,
                                        Match.playoff_round == "Cuartos",
                                        Match.game_number == 3,
                                    ).first()
                                    if not existing_g3:
                                        db2.add(Match(
                                            season_id=s4.id,
                                            home_team_id=ht.id,
                                            away_team_id=at.id,
                                            jornada=next_j_g3,
                                            vuelta=1,
                                            phase="Liguilla",
                                            playoff_round="Cuartos",
                                            game_number=3,
                                            status="Programado",
                                        ))
                                db2.commit()
                            st.success("✅ Juego(s) 3 generados.")
                            st.rerun()

            st.markdown("---")

            # ── SECCIÓN SEMIFINALES ───────────────────────────────────────
            st.markdown("### 🥊 Semifinales")
            st.caption(
                "El re-seeding toma a los ganadores de Cuartos y los "
                "reordena por su posición original en la tabla: "
                "Mejor vs Peor, 2do vs 3ro."
            )

            fmt_sf = st.selectbox(
                "Formato Semifinales",
                ["A 1 partido", "Al mejor de 3"],
                key="sf_fmt",
            )

            if st.button("🥊 Generar Semifinales (con re-seeding)", key="btn_sf",
                         type="primary"):
                with get_db() as db:
                    s5 = active_season(db, cat)
                    # Determinar ganadores de Cuartos
                    qf_all = (
                        db.query(Match)
                        .filter(
                            Match.season_id == s5.id,
                            Match.phase == "Liguilla",
                            Match.playoff_round == "Cuartos",
                        ).all()
                    )
                    from collections import defaultdict
                    series_sf: dict = defaultdict(list)
                    for m in qf_all:
                        key = tuple(sorted([m.home_team_id, m.away_team_id]))
                        series_sf[key].append(m)

                    winners = []   # (seed_original, team_id)
                    for (tid1, tid2), games in series_sf.items():
                        w1 = sum(
                            1 for g in games
                            if g.status == "Jugado" and (
                                (g.home_team_id == tid1 and (g.home_score or 0) > (g.away_score or 0)) or
                                (g.away_team_id == tid1 and (
                                    g.away_score or 0) > (g.home_score or 0))
                            )
                        )
                        w2 = len(
                            [g for g in games if g.status == "Jugado"]) - w1
                        if w1 > w2:
                            winner_id = tid1
                        elif w2 > w1:
                            winner_id = tid2
                        else:
                            continue   # serie incompleta
                        # Seed = posición en la tabla regular
                        seed = next(
                            (i + 1 for i, row in std_df.iterrows()
                             if db.query(Team).filter(
                                 Team.name == row["Equipo"],
                                 Team.season_id == s5.id
                            ).first() and
                                db.query(Team).filter(
                                 Team.name == row["Equipo"],
                                 Team.season_id == s5.id
                            ).first().id == winner_id),
                            99
                        )
                        winners.append((seed, winner_id))

                    if len(winners) < 4:
                        st.error(
                            f"Solo hay {len(winners)} ganador(es) confirmado(s) "
                            "de Cuartos. Captura todos los resultados antes de "
                            "generar Semifinales."
                        )
                    else:
                        winners.sort(key=lambda x: x[0])
                        # Re-seeding: 1v4, 2v3
                        sf_cruces = [
                            (winners[0][1], winners[3][1]),
                            (winners[1][1], winners[2][1]),
                        ]
                        next_j_sf = max(
                            (m.jornada for m in qf_all), default=0
                        ) + 1
                        inserted_sf = 0
                        games_sf = 1 if fmt_sf == "A 1 partido" else 2
                        for ht_id, at_id in sf_cruces:
                            for g in range(1, games_sf + 1):
                                existing = db.query(Match).filter(
                                    Match.season_id == s5.id,
                                    Match.home_team_id == ht_id,
                                    Match.away_team_id == at_id,
                                    Match.playoff_round == "Semifinal",
                                    Match.game_number == g,
                                ).first()
                                if not existing:
                                    db.add(Match(
                                        season_id=s5.id,
                                        home_team_id=ht_id if g != 2 else at_id,
                                        away_team_id=at_id if g != 2 else ht_id,
                                        jornada=next_j_sf +
                                        (g - 1),  # ✅ CORRECCIÓN
                                        vuelta=1,
                                        phase="Liguilla",
                                        playoff_round="Semifinal",
                                        game_number=g,
                                        status="Programado",
                                    ))
                                    inserted_sf += 1
                        db.commit()
                        st.success(
                            f"✅ {inserted_sf} partido(s) de Semifinal generados "
                            "con re-seeding aplicado."
                        )
                        st.rerun()

            st.markdown("---")

            # ── SECCIÓN GRAN FINAL ────────────────────────────────────────
            st.markdown("### 🥇 Gran Final")
            st.caption(
                "La Final siempre es a 1 partido. Sin configuración adicional.")

            if st.button("🥇 Generar Final", key="btn_final", type="primary"):
                with get_db() as db:
                    s6 = active_season(db, cat)
                    sf_all = (
                        db.query(Match)
                        .filter(
                            Match.season_id == s6.id,
                            Match.phase == "Liguilla",
                            Match.playoff_round == "Semifinal",
                        ).all()
                    )
                    from collections import defaultdict
                    sf_series: dict = defaultdict(list)
                    for m in sf_all:
                        key = tuple(sorted([m.home_team_id, m.away_team_id]))
                        sf_series[key].append(m)

                    finalists = []
                    for (tid1, tid2), games in sf_series.items():
                        w1 = sum(
                            1 for g in games
                            if g.status == "Jugado" and (
                                (g.home_team_id == tid1 and (g.home_score or 0) > (g.away_score or 0)) or
                                (g.away_team_id == tid1 and (
                                    g.away_score or 0) > (g.home_score or 0))
                            )
                        )
                        w2 = len(
                            [g for g in games if g.status == "Jugado"]) - w1
                        if w1 > w2:
                            finalists.append((
                                next((i+1 for i, r in std_df.iterrows()
                                      if db.query(Team).filter(
                                          Team.name == r["Equipo"], Team.season_id == s6.id
                                ).first() and
                                    db.query(Team).filter(
                                          Team.name == r["Equipo"], Team.season_id == s6.id
                                ).first().id == tid1), 99),
                                tid1
                            ))
                        elif w2 > w1:
                            finalists.append((
                                next((i+1 for i, r in std_df.iterrows()
                                      if db.query(Team).filter(
                                          Team.name == r["Equipo"], Team.season_id == s6.id
                                ).first() and
                                    db.query(Team).filter(
                                          Team.name == r["Equipo"], Team.season_id == s6.id
                                ).first().id == tid2), 99),
                                tid2
                            ))

                    if len(finalists) < 2:
                        st.error(
                            f"Solo {len(finalists)} finalista(s) confirmado(s). "
                            "Captura los resultados de Semifinales primero."
                        )
                    else:
                        finalists.sort(key=lambda x: x[0])
                        ht_f, at_f = finalists[0][1], finalists[1][1]
                        next_j_f = max(
                            (m.jornada for m in sf_all), default=0
                        ) + 1
                        existing_f = db.query(Match).filter(
                            Match.season_id == s6.id,
                            Match.home_team_id == ht_f,
                            Match.away_team_id == at_f,
                            Match.playoff_round == "Final",
                            Match.game_number == 1,
                        ).first()
                        if existing_f:
                            st.warning(
                                "⚠️ La Final ya existe en el calendario.")
                        else:
                            db.add(Match(
                                season_id=s6.id,
                                home_team_id=ht_f,
                                away_team_id=at_f,
                                jornada=next_j_f,
                                vuelta=1,
                                phase="Liguilla",
                                playoff_round="Final",
                                game_number=1,
                                status="Programado",
                            ))
                            db.commit()
                            f1 = db.query(Team).get(ht_f)
                            f2 = db.query(Team).get(at_f)
                            f1n = f1.name if f1 else "?"
                            f2n = f2.name if f2 else "?"
                            st.success(
                                f"🥇 Gran Final generada: **{f1n}** vs **{f2n}**"
                            )
                            st.rerun()

    # ══════════════════════════════════════════════════════════════════════
    #  TAB 4 — TEXTO WHATSAPP
    # ══════════════════════════════════════════════════════════════════════
    with cal_tabs[4]:
        # Mostrar jornadas que tienen al menos 1 partido Programado o Pendiente
        wa_jornadas = sorted({
            mi["jornada"]
            for mi in match_snap.values()
            if mi["status"] in ("Programado", "Pendiente")
        })
        if not wa_jornadas:
            st.info("Sin jornadas con partidos programados o pendientes.")
        else:
            j_wa = st.selectbox("Jornada", wa_jornadas, key="wa_j")
            with get_db() as db:
                s = active_season(db, cat)

                # ── Partidos PROGRAMADOS de esta jornada ─────────────────
                prog_ms = (
                    db.query(Match)
                    .filter(
                        Match.season_id == s.id,
                        Match.jornada == j_wa,
                        Match.status == "Programado",
                    )
                    .order_by(Match.venue, Match.scheduled_date)
                    .all()
                )

                # ── Partidos PENDIENTES de esta jornada ──────────────────
                pend_ms = (
                    db.query(Match)
                    .filter(
                        Match.season_id == s.id,
                        Match.jornada == j_wa,
                        Match.status == "Pendiente",
                    )
                    .all()
                )

                # Calcular equipo con descanso (bye) si número impar
                all_team_ids_in_season = {t.id for t in teams}
                playing_ids = set()
                for m in prog_ms + pend_ms:
                    playing_ids.add(m.home_team_id)
                    playing_ids.add(m.away_team_id)
                bye_team_ids = all_team_ids_in_season - playing_ids
                bye_names = [
                    team_map.get(tid, "—") for tid in bye_team_ids
                ]

                # ── Agrupar programados por cancha ────────────────────────
                cancha_groups: dict[str, list] = {}
                for m in prog_ms:
                    cancha = m.venue or "Cancha por asignar"
                    if cancha not in cancha_groups:
                        cancha_groups[cancha] = []
                    ht = db.query(Team).get(m.home_team_id)
                    at = db.query(Team).get(m.away_team_id)
                    hora = (
                        m.scheduled_date.strftime("%H:%M")
                        if m.scheduled_date else "—"
                    )
                    cancha_groups[cancha].append((hora, ht.name, at.name))

                # Fecha de referencia: primer partido programado
                fecha_jornada = ""
                ref = prog_ms[0] if prog_ms else (
                    pend_ms[0] if pend_ms else None)
                if ref and ref.scheduled_date:
                    # Usamos nuestro propio diccionario en español
                    dia_semana = WEEKDAY_NAMES[ref.scheduled_date.weekday()]
                    fecha_jornada = f"{dia_semana} {ref.scheduled_date.strftime('%d/%m/%Y')}"

                # ── Construir texto ───────────────────────────────────────
                lines = [
                    "🏀 *Liga Municipal de Basquetbol Nochixtlán*",
                    f"📋 *{cat} — Jornada {j_wa}*",
                    f"📅 {fecha_jornada}" if fecha_jornada else "",
                    f"📌 Juegan los *{dia}s*",
                    "",
                ]

                # Sección de partidos programados por cancha
                if cancha_groups:
                    for cancha in VENUES:
                        if cancha not in cancha_groups:
                            continue
                        lines.append(f"*{cancha.upper()}*")
                        for hora, hn, an in sorted(cancha_groups[cancha]):
                            lines.append(f"  {hora} hrs - {hn} vs {an}")
                        lines.append("")
                    # Canchas imprevistas
                    for cancha, juegos in cancha_groups.items():
                        if cancha not in VENUES:
                            lines.append(f"*{cancha.upper()}*")
                            for hora, hn, an in sorted(juegos):
                                lines.append(f"  {hora} hrs - {hn} vs {an}")
                            lines.append("")

                # Sección de partidos PENDIENTES
                if pend_ms:
                    lines.append("*PARTIDOS PENDIENTES / POR REPROGRAMAR*")
                    for m in pend_ms:
                        ht = db.query(Team).get(m.home_team_id)
                        at = db.query(Team).get(m.away_team_id)
                        lines.append(
                            f"  ⏳ {ht.name if ht else '—'} "
                            f"vs {at.name if at else '—'}"
                        )
                    lines.append("")

                # Equipos con descanso (bye)
                for bn in sorted(bye_names):
                    lines.append(f"  🛋️ Descansa -> *{bn}*")
                if bye_names:
                    lines.append("")

                lines.append("¡Los esperamos! 🙌🏀")

            # Limpiar líneas vacías dobles
            cleaned: list[str] = []
            for line in lines:
                if line == "" and cleaned and cleaned[-1] == "":
                    continue
                cleaned.append(line)

            texto = "\n".join(cleaned)
            st.text_area("Copia este texto para WhatsApp:", texto, height=400)
            st.download_button(
                "📥 Descargar .txt",
                texto,
                file_name=f"J{j_wa}_{cat}.txt",
            )


# ════════════════════════════════════════════════════════════════════════════
#  VISTA STREAMER — comentada, lista para activar en producción
# ════════════════════════════════════════════════════════════════════════════

# def page_streamer() -> None:
#     """
#     Vista para capturar en OBS durante transmisiones de Facebook Live.
#     Modo oscuro, tipografía grande.  Descomentar + agregar en available_pages.
#     """
#     st.markdown("""
#     <style>
#     .stApp { background-color:#0a0a0a!important; color:#fff!important; }
#     .block-container { padding-top:0.5rem; }
#     h1 { font-size:3rem!important; color:#FFD700!important; text-align:center; }
#     h2,h3 { color:#FFD700!important; }
#     .stMetric [data-testid="metric-container"]>div:last-child {
#         font-size:2.8rem!important; color:#FFD700!important; }
#     </style>""", unsafe_allow_html=True)
#
#     st.title("🏀 LIGA MUNICIPAL NOCHIXTLÁN")
#     cat = st.selectbox("", CATEGORIES, key="str_cat", label_visibility="collapsed")
#
#     with get_db() as db:
#         season = active_season(db, cat)
#         if not season: return
#
#         last = (db.query(Match)
#                 .filter(Match.season_id==season.id, Match.status=="Jugado")
#                 .order_by(Match.played_date.desc()).first())
#         if last:
#             ht = db.query(Team).get(last.home_team_id)
#             at = db.query(Team).get(last.away_team_id)
#             st.markdown(f"### 🏆 Último resultado — J{last.jornada}")
#             c1,c2,c3 = st.columns([2,1,2])
#             with c1: st.metric(ht.name, last.home_score or 0)
#             with c2: st.markdown("<h3 style='text-align:center;padding-top:1.2rem'>VS</h3>",
#                                  unsafe_allow_html=True)
#             with c3: st.metric(at.name, last.away_score or 0)
#
#         st.markdown("---")
#         df = calculate_standings(db, season.id)
#         if not df.empty:
#             st.markdown(f"### 📊 POSICIONES — {cat.upper()}")
#             st.dataframe(df[["#","Equipo","PJ","PG","PP","DP","Pts"]],
#                          use_container_width=True, hide_index=True, height=260)
#
#         col_a,col_b = st.columns(2)
#         with col_a:
#             st.markdown("### 🥇 TOP ANOTADORES")
#             dfs = get_top_scorers(db, season.id, 5)
#             if not dfs.empty: st.dataframe(dfs, use_container_width=True, hide_index=True)
#         with col_b:
#             st.markdown("### 🎯 TOP TRIPLEROS")
#             dft = get_top_triples(db, season.id, 5)
#             if not dft.empty: st.dataframe(dft, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ════════════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════════════
#  PÁGINA PÚBLICA: ESTADÍSTICAS POR EQUIPO
# ════════════════════════════════════════════════════════════════════════════

def page_team_stats() -> None:
    st.title("📋 Estadísticas por Equipo")

    cat = st.selectbox("Categoría", CATEGORIES, key="ts_cat")

    with get_db() as db:
        season = season_selector(db, cat, key_prefix="ts")
        if not season:
            st.warning("No hay temporada activa para esta categoría.")
            return
        sname = f"{season.name}{' 🧪' if season.is_test else ''}"
        st.caption(
            f"📋 Temporada: **{sname}** — {season.category} {season.year}")

        # ✅ CORRECCIÓN: Guardamos el ID de la temporada elegida para usarlo en toda la página
        current_season_id = season.id
        current_season_year = season.year

        teams = (
            db.query(Team)
            .filter(
                Team.season_id == current_season_id,
                Team.status == "Activo",
            )
            .order_by(Team.name)
            .all()
        )

    if not teams:
        st.info("Aún no hay equipos registrados en esta categoría.")
        return

    team_names = [t.name for t in teams]
    sel_name = st.selectbox("Equipo", team_names, key="ts_team")
    sel_team = next(t for t in teams if t.name == sel_name)

    phase_label = st.radio(
        "📊 Mostrar estadísticas de:",
        ["Fase Regular", "Liguilla", "Ambas fases"],
        horizontal=True,
        key="ts_phase",
    )
    phase_filter: str | None = (
        None if phase_label == "Ambas fases" else phase_label
    )

    with get_db() as db:
        # ❌ ELIMINAMOS LA LÍNEA QUE CAUSABA EL BUG: season = active_season(db, cat)

        # ── Partidos del equipo (jugados) ─────────────────────────────────
        q_matches = db.query(Match).filter(
            Match.season_id == current_season_id,  # ✅ USAMOS EL ID GUARDADO
            Match.status == "Jugado",
            (Match.home_team_id == sel_team.id) |
            (Match.away_team_id == sel_team.id),
        )
        if phase_filter:
            q_matches = q_matches.filter(Match.phase == phase_filter)
        played_matches = q_matches.all()
        pj_total = len(played_matches)

        # Puntos a favor del equipo en partidos jugados
        pf_total = 0
        for m in played_matches:
            if m.home_team_id == sel_team.id:
                pf_total += m.home_score or 0
            else:
                pf_total += m.away_score or 0

        ppg = round(pf_total / pj_total, 1) if pj_total > 0 else 0.0

        # Triples totales del equipo
        q_trp = (
            db.query(func.sum(PlayerMatchStat.triples))
            .join(Match, Match.id == PlayerMatchStat.match_id)
            .filter(
                PlayerMatchStat.team_id == sel_team.id,
                Match.season_id == current_season_id,  # ✅ USAMOS EL ID GUARDADO
            )
        )
        if phase_filter:
            q_trp = q_trp.filter(Match.phase == phase_filter)
        triples_total = q_trp.scalar() or 0

        # ── Métricas globales ─────────────────────────────────────────────
        st.markdown(f"### 🏀 {sel_name}")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🗓️ PJ", pj_total,
                  help="Partidos Jugados en la temporada")
        m2.metric("🏀 PF", pf_total,      help="Puntos a Favor acumulados")
        m3.metric("📈 PPG", f"{ppg}",     help="Promedio de Puntos por Partido")
        m4.metric("🎯 3PT", triples_total, help="Triples totales del equipo")

        st.markdown("---")

        # ── Plantilla y estadísticas individuales ─────────────────────────
        st.subheader("👥 Plantilla")

        players = (
            db.query(Player)
            .filter(
                Player.team_id == sel_team.id,
                Player.is_active == True,
            )
            .order_by(Player.number)
            .all()
        )

        if not players:
            st.info("No hay jugadores registrados en este equipo.")
            return

        rows = []
        for p in players:
            q_agg = (
                db.query(
                    func.count(PlayerMatchStat.id).label("gp"),
                    func.sum(PlayerMatchStat.points).label("pts"),
                    func.sum(PlayerMatchStat.triples).label("trp"),
                    func.sum(PlayerMatchStat.fouls).label("fls"),
                )
                .join(Match, Match.id == PlayerMatchStat.match_id)
                .filter(
                    PlayerMatchStat.player_id == p.id,
                    PlayerMatchStat.team_id == sel_team.id,
                    PlayerMatchStat.played == True,
                    Match.season_id == current_season_id,  # ✅ USAMOS EL ID GUARDADO
                )
            )
            if phase_filter:
                q_agg = q_agg.filter(Match.phase == phase_filter)
            agg = q_agg.first()
            gp = agg.gp or 0
            pts = agg.pts or 0
            trp = agg.trp or 0
            fls = agg.fls or 0
            ppg_p = round(pts / gp, 1) if gp > 0 else 0.0

            rows.append({
                "#":      p.number,
                "Jugador": p.name,
                "PJ":     gp,
                "PTS":    pts,
                "PPG":    ppg_p,
                "3PT":    trp,
                "Faltas": fls,
            })

        df_players = (
            pd.DataFrame(rows)
            .sort_values("PTS", ascending=False)
            .reset_index(drop=True)
        )
        df_players.insert(0, "Pos", range(1, len(df_players) + 1))

        st.dataframe(
            df_players,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Pos":    st.column_config.NumberColumn("#",   width="small"),
                "#":      st.column_config.NumberColumn("Dorsal", width="small"),
                "Jugador": st.column_config.TextColumn(width="large"),
                "PJ":     st.column_config.NumberColumn("PJ",  width="small",
                                                        help="Partidos jugados (asistencias)"),
                "PTS":    st.column_config.NumberColumn("PTS", width="small",
                                                        help="Puntos totales en la temporada"),
                "PPG":    st.column_config.NumberColumn("PPG", width="small",
                                                        format="%.1f",
                                                        help="Promedio de puntos por partido"),
                "3PT":    st.column_config.NumberColumn("3PT", width="small",
                                                        help="Triples anotados"),
                "Faltas": st.column_config.NumberColumn("FC",  width="small",
                                                        help="Faltas cometidas"),
            },
        )
        st.caption(
            f"Temporada {current_season_year} · Categoría {cat} · {phase_label} · "
            f"Solo estadísticas con **{sel_name}** (anti-traspaso activo)"
        )


# ════════════════════════════════════════════════════════════════════════════
#  PÁGINA PÚBLICA: LIGUILLA / BRACKET
# ════════════════════════════════════════════════════════════════════════════

def _series_status(games: list, tid1: int, tid2: int,
                   name1: str, name2: str) -> tuple[str, int | None]:
    """
    Dada la lista de partidos de una serie, devuelve:
      (texto_resumen, winner_team_id | None)

    Regla: gana quien llega primero a 2 victorias (BO3) o quien gana el único
    juego (BO1). Si la serie no está terminada, winner=None.
    """
    w1 = w2 = 0
    for g in games:
        if g["status"] != "Jugado":
            continue
        hs, as_ = g["home_score"] or 0, g["away_score"] or 0
        if g["home_id"] == tid1:
            if hs > as_:
                w1 += 1
            elif as_ > hs:
                w2 += 1
        else:
            if as_ > hs:
                w1 += 1
            elif hs > as_:
                w2 += 1

    played = sum(1 for g in games if g["status"] == "Jugado")
    total = len(games)

    if w1 >= 2 or (total == 1 and w1 == 1):
        return f"🏆 **{name1}** avanza ({w1}-{w2})", tid1
    if w2 >= 2 or (total == 1 and w2 == 1):
        return f"🏆 **{name2}** avanza ({w2}-{w1})", tid2
    if played == 0:
        return f"🔵 {name1} vs {name2} — por jugar", None
    return f"⚡ {name1} **{w1}** — **{w2}** {name2}", None


def page_liguilla() -> None:
    st.title("🏆 Liguilla")
    cat = st.selectbox("Categoría", CATEGORIES, key="pub_lig_cat")

    with get_db() as db:
        season = season_selector(db, cat, key_prefix="pub_lig")
        if not season:
            st.warning("No hay temporada activa.")
            return
        sname = f"{season.name}{' 🧪' if season.is_test else ''}"
        st.caption(
            f"📋 Temporada: **{sname}** — {season.category} {season.year}")

        playoff_matches = (
            db.query(Match)
            .filter(
                Match.season_id == season.id,
                Match.phase == "Liguilla",
            )
            .order_by(Match.playoff_round, Match.game_number)
            .all()
        )
        team_map = {t.id: t.name for t in db.query(Team).all()}

        if not playoff_matches:
            st.info("Aún no hay partidos de Liguilla generados para esta temporada.")
            return

        # Snapshot seguro fuera de la sesión
        pm_snap = [
            {
                "round":       m.playoff_round,
                "game_number": m.game_number,
                "home_id":     m.home_team_id,
                "away_id":     m.away_team_id,
                "home_score":  m.home_score,
                "away_score":  m.away_score,
                "status":      m.status,
                "date":        m.scheduled_date,
            }
            for m in playoff_matches
        ]

    # Agrupar por ronda y luego por cruce
    from collections import defaultdict, OrderedDict
    ROUND_ORDER = {"Cuartos": 1, "Semifinal": 2, "Final": 3}
    ROUND_LABELS = {"Cuartos": "⚔️ Cuartos de Final",
                    "Semifinal": "🥊 Semifinales",
                    "Final": "🥇 Gran Final"}

    by_round: dict = defaultdict(lambda: defaultdict(list))
    for g in pm_snap:
        r = g["round"] or "—"
        key = tuple(sorted([g["home_id"], g["away_id"]]))
        by_round[r][key].append(g)

    for rnd in sorted(by_round.keys(), key=lambda r: ROUND_ORDER.get(r, 9)):
        st.markdown(f"## {ROUND_LABELS.get(rnd, rnd)}")

        for (tid1, tid2), games in by_round[rnd].items():
            n1 = team_map.get(tid1, "?")
            n2 = team_map.get(tid2, "?")
            summary, winner_id = _series_status(games, tid1, tid2, n1, n2)

            with st.container(border=True):
                col_sum, col_games = st.columns([2, 3])
                with col_sum:
                    st.markdown(f"**{n1}** vs **{n2}**")
                    st.markdown(summary)

                with col_games:
                    for g in sorted(games, key=lambda x: x["game_number"]):
                        gn = g["game_number"]
                        hn = team_map.get(g["home_id"], "?")
                        an = team_map.get(g["away_id"], "?")
                        gdate = (
                            g["date"].strftime("%d/%m %H:%M")
                            if g["date"] else "—"
                        )
                        if g["status"] == "Jugado":
                            res = f"{g['home_score']} — {g['away_score']}"
                            label = f"**J{gn}** · {hn} {res} {an} · {gdate}"
                        else:
                            label = f"**J{gn}** · {hn} vs {an} · {gdate} _(por jugar)_"
                        st.markdown(label)

        st.markdown("---")


# ════════════════════════════════════════════════════════════════════════════
#  ADMIN: GESTOR DE TEMPORADAS
# ════════════════════════════════════════════════════════════════════════════

def _delete_season(season_id: int) -> tuple[int, int, int]:
    """
    Elimina una temporada y todos sus datos en cascada.

    Orden seguro (respeta FKs con PRAGMA foreign_keys=ON):
      1. Borrar PlayerMatchStat de los partidos de la temporada.
      2. Borrar Matches de la temporada.
      3. Desacoplar Players de los Teams de esta temporada
         (player.team_id → None) SIN borrar el registro del jugador.
      4. Borrar Teams de la temporada.
      5. Borrar la Season.

    Retorna (partidos_borrados, equipos_borrados, jugadores_desacoplados).
    """
    with get_db() as db:
        season = db.query(Season).get(season_id)
        if not season or season.is_active:
            return 0, 0, 0

        # IDs de partidos y equipos de esta temporada
        match_ids = [
            m.id for m in db.query(Match.id)
            .filter(Match.season_id == season_id).all()
        ]
        team_ids = [
            t.id for t in db.query(Team.id)
            .filter(Team.season_id == season_id).all()
        ]

        # 1. Borrar stats de jugadores vinculadas a esos partidos
        if match_ids:
            db.query(PlayerMatchStat).filter(
                PlayerMatchStat.match_id.in_(match_ids)
            ).delete(synchronize_session=False)

        # 2. Borrar partidos
        n_matches = db.query(Match).filter(
            Match.season_id == season_id
        ).delete(synchronize_session=False)

        # 3. Desacoplar jugadores SIN borrarlos del catálogo
        n_players = 0
        if team_ids:
            n_players = db.query(Player).filter(
                Player.team_id.in_(team_ids)
            ).update({"team_id": None, "is_active": False},
                     synchronize_session=False)

        # 4. Borrar equipos de la temporada
        n_teams = db.query(Team).filter(
            Team.season_id == season_id
        ).delete(synchronize_session=False)

        # 5. Borrar la temporada
        db.delete(season)
        db.commit()

    return n_matches, n_teams, n_players


def page_season_manager() -> None:
    st.title("🏆 Gestor de Temporadas")
    st.caption(
        "Crea nuevas temporadas, clona cédulas y consulta el historial "
        "sin borrar ni tocar el archivo .db."
    )

    # ── Historial con filas interactivas ─────────────────────────────────
    with get_db() as db:
        all_seasons = (
            db.query(Season)
            .order_by(Season.category, Season.is_active.desc(),
                      Season.created_at.desc())
            .all()
        )
        season_data = []
        for s in all_seasons:
            n_teams = db.query(func.count(Team.id)).filter(
                Team.season_id == s.id
            ).scalar() or 0
            n_matches = db.query(func.count(Match.id)).filter(
                Match.season_id == s.id,
                Match.status == "Jugado",
            ).scalar() or 0
            season_data.append({
                "id":       s.id,
                "name":     s.name,
                "category": s.category,
                "year":     s.year,
                "is_active": s.is_active,
                "is_test":  s.is_test,
                "n_teams":  n_teams,
                "n_matches": n_matches,
                "created":  s.created_at.strftime("%d/%m/%Y") if s.created_at else "—",
            })

    if not season_data:
        st.info("No hay temporadas registradas.")
    else:
        st.subheader("📋 Historial de temporadas")

        # Cabecera
        hdr = st.columns([3, 1.5, 1, 1.2, 1, 1, 1.3])
        for col, label in zip(hdr, [
            "**Nombre**", "**Categoría**", "**Año**",
            "**Estado**", "**Equipos**", "**Jugados**", "**Acción**"
        ]):
            col.markdown(label)
        st.markdown("<hr style='margin:0.25rem 0 0.5rem'>",
                    unsafe_allow_html=True)

        for row in season_data:
            c1, c2, c3, c4, c5, c6, c7 = st.columns(
                [3, 1.5, 1, 1.2, 1, 1, 1.3])
            tipo_icon = "🧪 " if row["is_test"] else "🏆 "
            estado_lbl = "🟢 Activa" if row["is_active"] else "📦 Cerrada"
            c1.write(f"{tipo_icon}{row['name']}")
            c2.write(row["category"])
            c3.write(str(row["year"]))
            c4.write(estado_lbl)
            c5.write(str(row["n_teams"]))
            c6.write(str(row["n_matches"]))

            with c7:
                if row["is_active"]:
                    # No se puede eliminar la temporada activa
                    st.button(
                        "🗑️", key=f"del_{row['id']}",
                        disabled=True,
                        help="Activa una otra temporada antes de eliminar esta.",
                    )
                else:
                    if st.button(
                        "🗑️", key=f"del_{row['id']}",
                        help=f"Eliminar «{row['name']}» y todos sus datos",
                        type="secondary",
                    ):
                        # Guardar en session_state cuál temporada se quiere borrar
                        st.session_state["del_confirm_id"] = row["id"]
                        st.session_state["del_confirm_name"] = row["name"]
                        st.session_state["del_confirm_cat"] = row["category"]

        # ── Confirmación de borrado ───────────────────────────────────────
        del_id = st.session_state.get("del_confirm_id")
        if del_id:
            del_name = st.session_state.get("del_confirm_name", "")
            del_cat = st.session_state.get("del_confirm_cat", "")
            st.markdown("---")
            st.error(
                f"### 🗑️ Confirmar eliminación: «{del_name}» ({del_cat})\n\n"
                "Esto borrará **permanentemente** todos los partidos y "
                "estadísticas de esta temporada. "
                "Los jugadores y equipos del catálogo **no se borran** — "
                "solo se desacoplan de la temporada."
            )
            confirmed = st.checkbox(
                "✅ Entiendo que esta acción es **irreversible**. "
                "Quiero eliminar esta temporada.",
                key="del_check",
            )
            dc1, dc2 = st.columns(2)
            with dc1:
                if st.button(
                    "🗑️ Eliminar definitivamente",
                    type="primary",
                    disabled=not confirmed,
                    use_container_width=True,
                    key="del_confirm_btn",
                ):
                    n_m, n_t, n_p = _delete_season(del_id)
                    st.session_state.pop("del_confirm_id",   None)
                    st.session_state.pop("del_confirm_name", None)
                    st.session_state.pop("del_confirm_cat",  None)
                    st.session_state.pop("del_check",        None)
                    st.success(
                        f"✅ Temporada eliminada. "
                        f"Se borraron **{n_m}** partido(s), **{n_t}** equipo(s). "
                        f"**{n_p}** jugador(es) desacoplados (sus registros se conservan)."
                    )
                    st.rerun()
            with dc2:
                if st.button(
                    "✖ Cancelar",
                    use_container_width=True,
                    key="del_cancel_btn",
                ):
                    st.session_state.pop("del_confirm_id",   None)
                    st.session_state.pop("del_confirm_name", None)
                    st.session_state.pop("del_confirm_cat",  None)
                    st.rerun()

    st.markdown("---")

    # ── CREAR NUEVA TEMPORADA ─────────────────────────────────────────────
    st.subheader("➕ Crear nueva temporada")
    st.info(
        "Al crear y activar una nueva temporada, la anterior queda "
        "**cerrada (archivada)**. Todos sus datos históricos se conservan "
        "y siguen disponibles en el historial."
    )

    with st.form("form_new_season", clear_on_submit=True):
        sc1, sc2 = st.columns(2)
        with sc1:
            new_cat = st.selectbox("Categoría", CATEGORIES, key="ns_cat")
            new_name = st.text_input(
                "Nombre de la temporada",
                placeholder="Torneo Apertura 2026",
            )
            new_year = st.number_input(
                "Año", min_value=2020, max_value=2100,
                value=date.today().year,
            )
        with sc2:
            is_test_new = st.checkbox(
                "🧪 Es temporada de prueba",
                help="Marca esto para torneos de práctica o configuración. "
                     "No afecta estadísticas reales.",
            )
            clone_cedulas = st.checkbox(
                "📋 Clonar equipos y cédulas de la temporada activa",
                value=True,
                help="Copia los equipos y jugadores activos a la nueva temporada. "
                     "Partidos y estadísticas siempre empiezan en cero.",
            )
            st.caption(
                "✅ Clonando: los equipos y jugadores se copian, "
                "pero sin partidos ni estadísticas.\n\n"
                "❌ Sin clonar: empiezas con la temporada completamente en blanco."
            )

        submitted = st.form_submit_button("🚀 Crear y Activar Temporada",
                                          type="primary")

    if submitted:
        new_name = new_name.strip()
        if not new_name:
            st.error("El nombre de la temporada no puede estar vacío.")
            st.stop()

        # Variables que se llenarán DENTRO de la sesión para evitar
        # DetachedInstanceError al usarlas en st.info/st.success fuera del with.
        old_season_name: str | None = None
        cloned_teams = 0
        cloned_players = 0

        with get_db() as db:
            # 1. Buscar la temporada activa actual para esta categoría
            old_season = active_season(db, new_cat)

            # Extraer el nombre mientras la sesión está viva
            if old_season:
                old_season_name = old_season.name

            # 2. Crear nueva temporada
            new_season = Season(
                name=new_name,
                category=new_cat,
                year=int(new_year),
                is_active=True,
                is_test=is_test_new,
            )
            db.add(new_season)
            db.flush()   # obtener new_season.id antes de commit

            # 3. Desactivar la temporada anterior
            if old_season:
                old_season.is_active = False

            # 4. Clonar equipos y jugadores si se solicitó
            if clone_cedulas and old_season:
                old_teams = (
                    db.query(Team)
                    .filter(Team.season_id == old_season.id)
                    .all()
                )
                for ot in old_teams:
                    nt = Team(
                        name=ot.name,
                        category=ot.category,
                        season_id=new_season.id,
                        status="Activo",
                        permissions_used=0,
                        defaults_count=0,
                    )
                    db.add(nt)
                    db.flush()   # obtener nt.id

                    old_players = (
                        db.query(Player)
                        .filter(
                            Player.team_id == ot.id,
                            Player.is_active == True,
                        )
                        .all()
                    )
                    for op in old_players:
                        np_ = Player(
                            name=op.name,
                            number=op.number,
                            category=op.category,
                            team_id=nt.id,
                            is_active=True,
                            joined_team_date=date.today(),
                        )
                        db.add(np_)
                        cloned_players += 1

                    cloned_teams += 1

            db.commit()
        # ← sesión cerrada aquí; usamos solo strings/ints a partir de este punto

        # ── Resumen post-creación ─────────────────────────────────────────
        tipo_str = "🧪 PRUEBA" if is_test_new else "🏆 OFICIAL"
        st.success(
            f"✅ Temporada **{new_name}** ({tipo_str}) creada y activada "
            f"para la categoría **{new_cat}**."
        )
        if old_season_name:
            st.info(f"📦 Temporada anterior **'{old_season_name}'** archivada.")
        if clone_cedulas and old_season_name:
            st.success(
                f"📋 Clonados: **{cloned_teams}** equipo(s) y "
                f"**{cloned_players}** jugador(es). "
                "Partidos y estadísticas empiezan en cero."
            )
        elif not clone_cedulas:
            st.warning(
                "Sin clonar: la nueva temporada no tiene equipos. "
                "Regístralos en **Admin: Gestión**."
            )
        st.rerun()

    st.markdown("---")

    # ── REACTIVAR UNA TEMPORADA CERRADA ──────────────────────────────────
    with st.expander("♻️ Reactivar una temporada cerrada", expanded=False):
        st.warning(
            "⚠️ Reactivar una temporada anterior desactivará la que "
            "está activa actualmente para esa categoría. "
            "Todos los datos de ambas temporadas se conservan."
        )

        with get_db() as db:
            closed_seasons = (
                db.query(Season)
                .filter(Season.is_active == False)
                .order_by(Season.category, Season.created_at.desc())
                .all()
            )
            closed_opts = {
                f"[{s.category}] {s.name} ({s.year})"
                f"{'  🧪' if s.is_test else ''}": s.id
                for s in closed_seasons
            }

        if not closed_opts:
            st.info("No hay temporadas cerradas disponibles para reactivar.")
        else:
            react_lbl = st.selectbox(
                "Temporada a reactivar", list(closed_opts.keys()),
                key="react_sel",
            )
            if st.button("♻️ Reactivar esta temporada", key="btn_react",
                         type="secondary"):
                with get_db() as db:
                    target = db.query(Season).get(closed_opts[react_lbl])
                    target_name = target.name
                    target_cat = target.category
                    current = active_season(db, target_cat)
                    if current:
                        current.is_active = False
                    target.is_active = True
                    db.commit()
                st.success(
                    f"✅ **{target_name}** reactivada para {target_cat}."
                )
                st.rerun()


def main() -> None:
    st.set_page_config(
        page_title="Liga Nochixtlán",
        page_icon="🏀",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Inicializar DB si no existen tablas
    init_db()

    PUBLIC_PAGES = [
        "📊 Tabla de Posiciones",
        "🥇 Líderes Estadísticos",
        "📅 Calendario",
        "📋 Estadísticas por Equipo",
        "🏆 Liguilla",
    ]
    ADMIN_PAGES = [
        "🏆 Admin: Temporadas",
        "🛠️ Admin: Gestión",
        "📅 Admin: Calendario",
        "⚡ Admin: Captura Partido",
        # "📡 Vista Streamer",  # descomentar para activar
    ]

    available = PUBLIC_PAGES + (ADMIN_PAGES if is_admin() else [])

    # ── Sidebar ────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(
            "<h2 style='text-align:center;margin-bottom:0'>🏀</h2>"
            "<p style='text-align:center;font-weight:700;font-size:1rem;"
            "margin-top:0;line-height:1.3'>Liga Municipal<br>Nochixtlán</p>",
            unsafe_allow_html=True,
        )
        st.markdown("---")

        if "page" not in st.session_state:
            st.session_state["page"] = PUBLIC_PAGES[0]
        if st.session_state["page"] not in available:
            st.session_state["page"] = PUBLIC_PAGES[0]

        selected = st.radio(
            "nav",
            available,
            index=available.index(st.session_state["page"]),
            key="nav_radio",
            label_visibility="collapsed",
        )
        st.session_state["page"] = selected

        login_widget()
        st.markdown("---")
        st.caption("Liga Municipal de Basquetbol\nNochixtlán © 2025")

    # ── Routing ────────────────────────────────────────────────────────────
    p = st.session_state["page"]

    if p == "📊 Tabla de Posiciones":
        page_standings()
    elif p == "🥇 Líderes Estadísticos":
        page_leaders()
    elif p == "📅 Calendario":
        page_calendar_public()
    elif p == "📋 Estadísticas por Equipo":
        page_team_stats()
    elif p == "🏆 Liguilla":
        page_liguilla()
    elif p == "⚡ Admin: Captura Partido":
        page_capture()
    elif p == "🛠️ Admin: Gestión":
        page_management()
    elif p == "📅 Admin: Calendario":
        page_calendar_admin()
    elif p == "🏆 Admin: Temporadas":
        page_season_manager()
    # elif p == "📡 Vista Streamer":
    #     page_streamer()


if __name__ == "__main__":
    main()
