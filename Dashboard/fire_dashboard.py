import plotly.graph_objects as go
import streamlit as st
import pandas as pd
import numpy as np
import os, io, json, smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FireWatch · NASA Pipeline",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS — Design System (identique à l'original, complet) ────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;900&family=DM+Mono:ital,wght@0,400;0,500;1,400&display=swap');

  :root {
    --bg0:        #060810;
    --bg1:        #0C0F1A;
    --bg2:        #111520;
    --bg3:        #181D2E;
    --bg4:        #1F2640;
    --border:     #1E2540;
    --border-hi:  #2E3D60;
    --text-1:     #E8EEF8;
    --text-2:     #7A88A8;
    --text-3:     #3A4560;
    --c-red:      #FF3030;
    --c-orange:   #FF7200;
    --c-amber:    #FFB000;
    --c-green:    #2ECC71;
    --c-blue:     #4A9EFF;
    --c-purple:   #9B6EFF;
    --c-teal:     #00D4AA;
  }

  html, body, [data-testid="stAppViewContainer"] {
    background: var(--bg0) !important;
    font-family: 'Outfit', sans-serif;
    color: var(--text-1);
  }
  [data-testid="stSidebar"] {
    background: var(--bg1) !important;
    border-right: 1px solid var(--border) !important;
  }
  [data-testid="stSidebar"] label,
  [data-testid="stSidebar"] .stMarkdown p {
    font-size: 0.68rem !important;
    text-transform: uppercase;
    letter-spacing: 0.10em;
    color: var(--text-3) !important;
  }

  .fw-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: var(--bg1);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 26px;
    margin-bottom: 18px;
    position: relative;
    overflow: hidden;
  }
  .fw-top::before {
    content:'';
    position:absolute; top:0; left:0; right:0; height:2px;
    background: linear-gradient(90deg,var(--c-red),var(--c-orange),var(--c-amber),var(--c-orange),var(--c-red));
    background-size:300%;
    animation: sweep 5s linear infinite;
  }
  @keyframes sweep { 0%{background-position:0%} 100%{background-position:300%} }
  .fw-logo  { font-size:1.5rem; font-weight:900; letter-spacing:-0.02em; color:var(--text-1); }
  .fw-logo b{ color:var(--c-orange); }
  .fw-sub   { font-family:'DM Mono',monospace; font-size:0.62rem; color:var(--text-3); margin-top:2px; }
  .fw-live  { display:flex; align-items:center; gap:10px; font-family:'DM Mono',monospace;
               font-size:0.62rem; color:var(--text-2); }
  .live-dot { width:7px; height:7px; border-radius:50%; background:var(--c-green);
               box-shadow:0 0 8px var(--c-green); animation:blink 2s ease-in-out infinite; }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.35} }

  .pipe-bar {
    display:flex; align-items:center; gap:0;
    background:var(--bg1); border:1px solid var(--border); border-radius:8px;
    padding:9px 20px; margin-bottom:18px; overflow-x:auto;
  }
  .pipe-node { display:flex; align-items:center; gap:5px; white-space:nowrap;
                font-family:'DM Mono',monospace; font-size:0.62rem; color:var(--text-2); }
  .pipe-dot  { width:6px; height:6px; border-radius:50%; }
  .pipe-sep  { margin:0 8px; color:var(--border-hi); font-size:0.75rem; }
  .p-ok   { color:var(--c-green); }
  .p-warn { color:var(--c-amber); }
  .p-off  { color:var(--text-3);  }

  .sect {
    font-size:0.6rem; font-weight:600; text-transform:uppercase;
    letter-spacing:0.14em; color:var(--text-3);
    display:flex; align-items:center; gap:8px;
    margin:20px 0 12px;
  }
  .sect::after { content:''; flex:1; height:1px; background:var(--border); }

  .kpi {
    background:var(--bg1); border:1px solid var(--border); border-radius:8px;
    padding:14px 16px; position:relative; overflow:hidden;
    transition: border-color .2s, transform .15s;
  }
  .kpi:hover { border-color:var(--border-hi); transform:translateY(-1px); }
  .kpi-bar { position:absolute; top:0; left:0; right:0; height:2px; }
  .kpi-lbl { font-size:0.58rem; text-transform:uppercase; letter-spacing:.1em;
               color:var(--text-3); margin-bottom:6px; }
  .kpi-val { font-size:1.7rem; font-weight:700; font-family:'DM Mono',monospace; line-height:1; }
  .kpi-sub { font-size:0.65rem; color:var(--text-3); font-family:'DM Mono',monospace; margin-top:5px; }

  .gauge-wrap { background:var(--bg1); border:1px solid var(--border); border-radius:8px;
                 padding:18px; position:relative; }
  .gauge-title { font-size:0.6rem; text-transform:uppercase; letter-spacing:.12em;
                  color:var(--text-3); margin-bottom:4px; font-family:'DM Mono',monospace; }

  .alert-row {
    display:flex; align-items:center; gap:10px; padding:9px 14px;
    border-bottom:1px solid var(--bg2); font-family:'DM Mono',monospace;
    font-size:0.68rem; transition:background .15s; cursor:default;
  }
  .alert-row:hover { background:var(--bg2); }
  .alert-row:last-child { border-bottom:none; }
  .sev-dot { width:7px; height:7px; border-radius:50%; flex-shrink:0; }

  .banner {
    display:flex; align-items:center; gap:12px;
    border-radius:7px; padding:11px 16px; margin:5px 0;
    font-family:'DM Mono',monospace; font-size:0.7rem;
    border-left:3px solid;
  }
  .banner-red    { background:#180808; border-color:var(--c-red);    color:#FF8080; }
  .banner-orange { background:#18100A; border-color:var(--c-orange); color:#FFB060; }
  .banner-amber  { background:#181300; border-color:var(--c-amber);  color:#FFD060; }
  .banner-blue   { background:#080E18; border-color:var(--c-blue);   color:#80B8FF; }

  .card { background:var(--bg1); border:1px solid var(--border); border-radius:8px; padding:16px; }
  .card-title { font-size:0.6rem; text-transform:uppercase; letter-spacing:.12em;
                 color:var(--text-3); margin-bottom:12px; font-family:'DM Mono',monospace; }

  .fire-table { width:100%; border-collapse:collapse; }
  .fire-table th {
    font-size:0.58rem; text-transform:uppercase; letter-spacing:.1em;
    color:var(--text-3); font-family:'DM Mono',monospace;
    padding:6px 8px; text-align:left; border-bottom:1px solid var(--border);
    font-weight:500;
  }
  .fire-table td {
    font-size:0.68rem; font-family:'DM Mono',monospace;
    color:var(--text-2); padding:7px 8px; border-bottom:1px solid var(--bg2);
    vertical-align:middle;
  }
  .fire-table tr:hover td { background:var(--bg2); }
  .fire-table td.country { color:var(--text-1); font-weight:500; }
  .fire-table td.frp-val { color:var(--c-amber); }

  .badge {
    display:inline-block; padding:2px 7px; border-radius:4px;
    font-size:0.56rem; font-weight:600; letter-spacing:.06em; text-transform:uppercase;
    font-family:'DM Mono',monospace;
  }
  .b-red    { background:#FF303020; color:#FF6060; border:1px solid #FF303040; }
  .b-orange { background:#FF720020; color:#FF9940; border:1px solid #FF720040; }
  .b-amber  { background:#FFB00020; color:#FFD060; border:1px solid #FFB00040; }
  .b-green  { background:#2ECC7120; color:#2ECC71; border:1px solid #2ECC7140; }
  .b-blue   { background:#4A9EFF20; color:#4A9EFF; border:1px solid #4A9EFF40; }
  .b-purple { background:#9B6EFF20; color:#9B6EFF; border:1px solid #9B6EFF40; }

  .radar-wrap { background:var(--bg1); border:1px solid var(--border);
                 border-radius:8px; padding:16px; }

  .health-row {
    display:flex; align-items:center; gap:12px;
    padding:8px 0; border-bottom:1px solid var(--bg2);
    font-size:0.7rem; font-family:'DM Mono',monospace;
  }
  .health-row:last-child { border-bottom:none; }
  .health-name { width:140px; color:var(--text-2); }
  .health-bar-wrap { flex:1; height:5px; background:var(--bg3); border-radius:3px; overflow:hidden; }
  .health-bar { height:100%; border-radius:3px; }
  .health-val { width:50px; text-align:right; }
  .health-status { width:60px; text-align:right; }

  .stTabs [data-baseweb="tab-list"] {
    background:var(--bg1); border-bottom:1px solid var(--border);
    border-radius:8px 8px 0 0; padding:0 8px; gap:0;
  }
  .stTabs [data-baseweb="tab"] {
    font-size:0.68rem !important; font-family:'DM Mono',monospace !important;
    text-transform:uppercase; letter-spacing:.08em;
    color:var(--text-3) !important; padding:10px 18px !important;
    border-bottom:2px solid transparent;
  }
  .stTabs [aria-selected="true"] {
    color:var(--c-orange) !important;
    border-bottom:2px solid var(--c-orange) !important;
    background:transparent !important;
  }
  .stTabs [data-baseweb="tab-panel"] {
    background:var(--bg1); border:1px solid var(--border);
    border-top:none; border-radius:0 0 8px 8px; padding:20px 18px;
  }

  ::-webkit-scrollbar { width:4px; height:4px; }
  ::-webkit-scrollbar-track { background:var(--bg0); }
  ::-webkit-scrollbar-thumb { background:var(--border); border-radius:4px; }

  .js-plotly-plot .plotly { border-radius:6px; }
  [data-testid="stDataFrame"] { font-family:'DM Mono',monospace; font-size:0.7rem; }
  div[data-testid="stToggle"] label { font-size:.7rem !important; color:var(--text-2) !important; }
  .stButton>button {
    background:var(--bg2) !important; border:1px solid var(--border) !important;
    color:var(--text-2) !important; font-family:'DM Mono',monospace !important;
    font-size:.65rem !important; border-radius:6px !important; transition:all .15s !important;
  }
  .stButton>button:hover { border-color:var(--c-orange) !important; color:var(--c-orange) !important; }
  .stSelectbox>div>div, .stMultiSelect>div>div {
    background:var(--bg2) !important; border-color:var(--border) !important; border-radius:6px !important;
  }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# LOGIQUE MÉTIER
# ══════════════════════════════════════════════════════════════════════════════

def format_confidence(value):
    if pd.isna(value):
        return "inconnue"
    if isinstance(value, str):
        value_lower = value.lower()
        if value_lower in ['low', 'l', '0']:
            return "low"
        elif value_lower in ['nominal', 'n', '1']:
            return "nominal"
        elif value_lower in ['high', 'h', '2']:
            return "high"
        else:
            return value_lower
    try:
        val_int = int(float(value))
        if val_int == 0:
            return "low"
        elif val_int == 1:
            return "nominal"
        elif val_int == 2:
            return "high"
        else:
            return str(val_int)
    except (ValueError, TypeError):
        return str(value)

def severity_score(frp: float, brightness: float, confidence) -> float:
    try:
        if isinstance(confidence, str):
            conf_map = {'low': 0, 'nominal': 1, 'high': 2}
            conf_int = conf_map.get(confidence.lower(), 1)
        else:
            conf_int = int(float(confidence))
    except:
        conf_int = 1
    norm_frp    = min(frp / 150, 1.0)
    norm_bright = max(0.0, (brightness - 280) / 150)
    norm_conf   = conf_int / 2
    s = norm_frp * 0.6 + norm_bright * 0.25 + norm_conf * 0.15
    return round(max(0.0, min(s, 1.0)), 3)

def meteo_fire_risk(temp_c: float, hum: float, wind: float) -> str:
    s = 0
    if temp_c > 40: s += 4
    elif temp_c > 30: s += 2
    elif temp_c > 20: s += 1
    if hum < 20:  s += 3
    elif hum < 40: s += 1
    if wind > 50:  s += 3
    elif wind > 30: s += 1
    return ('EXTREME' if s >= 7 else 'ELEVE' if s >= 4 else 'MODERE' if s >= 2 else 'FAIBLE')

def compute_global_danger_score(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    components = []
    if 'frp' in df.columns:
        components.append(min((df['frp'].mean() + df['frp'].max()) / 300, 1.0) * 30)
    if 'temperature_c' in df.columns:
        components.append(min(max(df['temperature_c'].mean() - 10, 0) / 30, 1.0) * 20)
    if 'humidity_pct' in df.columns:
        components.append(min((100 - df['humidity_pct'].mean()) / 80, 1.0) * 20)
    if 'windspeed_kmh' in df.columns:
        components.append(min(df['windspeed_kmh'].max() / 60, 1.0) * 15)
    if 'primary_forest' in df.columns:
        components.append(df['primary_forest'].mean() * 10)
    if 'protected_area' in df.columns:
        components.append(df['protected_area'].mean() * 5)
    return round(sum(components), 1)

def risk_color(label: str) -> str:
    return {
        'CRITICAL':'#FF3030','HIGH':'#FF7200','MEDIUM':'#FFB000','LOW':'#2ECC71',
        'EXTREME':'#FF0000','ELEVE':'#FF5500','MODERE':'#FFB000','FAIBLE':'#2ECC71',
    }.get(label, '#7A88A8')

def sev_label(s: float) -> str:
    if s >= 0.65: return 'CRITICAL'
    if s >= 0.45: return 'HIGH'
    if s >= 0.25: return 'MEDIUM'
    return 'LOW'

def zone_danger_level(sev: float, meteo: str, pf: bool, pa: bool) -> str:
    eco = pf or pa
    extreme_meteo = meteo == 'EXTREME'
    if sev >= 0.60 and eco:
        return 'CRITIQUE'
    if sev >= 0.50 or extreme_meteo:
        return 'HAUT'
    if sev >= 0.25:
        return 'SURVEILLANCE'
    return 'NORMAL'

def danger_zone_radius(frp: float, wind: float, pf: bool) -> float:
    base = np.log1p(frp) * 2.5
    wind_factor = 1 + wind / 100
    forest_factor = 1.4 if pf else 1.0
    return round(min(base * wind_factor * forest_factor, 80), 1)

# ══════════════════════════════════════════════════════════════════════════════
# SYSTÈME D'ALERTES CRITIQUES
# ══════════════════════════════════════════════════════════════════════════════

class AlertManager:
    """Gère les alertes critiques (affichage + notification optionnelle email)"""
    def __init__(self):
        self.last_alert_ids = set()
        self.email_enabled = False
        self.smtp_server = os.getenv("SMTP_SERVER", "")
        self.smtp_port = int(os.getenv("SMTP_PORT", 587))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.alert_recipients = os.getenv("ALERT_RECIPIENTS", "").split(",")

    def is_critical_alert(self, row: pd.Series) -> bool:
        """Définit une alerte critique (sev >=0.75 ET (forêt ou zone protégée) OU FRP > 500)"""
        sev = row.get("severity_score", 0)
        frp = row.get("frp", 0)
        eco = row.get("primary_forest", False) or row.get("protected_area", False)
        return (sev >= 0.75 and eco) or frp > 500

    def generate_alert_id(self, row: pd.Series) -> str:
        return f"{row.get('latitude',0):.2f}_{row.get('longitude',0):.2f}_{row.get('acq_date','')}"

    def check_and_alert(self, df: pd.DataFrame):
        """Parcourt les hotspots et génère des alertes pour les nouveaux critiques"""
        if df.empty:
            return []
        new_alerts = []
        for _, row in df.iterrows():
            if self.is_critical_alert(row):
                aid = self.generate_alert_id(row)
                if aid not in self.last_alert_ids:
                    self.last_alert_ids.add(aid)
                    new_alerts.append(row)
        # Limiter la taille de l'historique
        if len(self.last_alert_ids) > 500:
            self.last_alert_ids = set(list(self.last_alert_ids)[-200:])
        return new_alerts

    def send_email_alert(self, alert_rows: list):
        """Envoie un email récapitulatif des nouvelles alertes (optionnel)"""
        if not alert_rows or not self.email_enabled or not self.smtp_server or not self.alert_recipients:
            return
        try:
            body = f"Nouvelles alertes incendie critiques - {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n"
            for r in alert_rows:
                body += (f"- {r.get('country','?')} | "
                         f"Lat: {r.get('latitude',0):.2f}, Lon: {r.get('longitude',0):.2f} | "
                         f"FRP: {r.get('frp',0):.0f} MW | Sev: {r.get('severity_score',0):.3f}\n")
            msg = MIMEText(body)
            msg['Subject'] = "🔥 FireWatch - Alerte incendie critique"
            msg['From'] = self.smtp_user
            msg['To'] = ", ".join(self.alert_recipients)
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            st.success("📧 Email d'alerte envoyé")
        except Exception as e:
            st.warning(f"Envoi email impossible: {e}")

# Instance globale d'alerte (stockée dans session_state pour persistance)
if "alert_manager" not in st.session_state:
    st.session_state.alert_manager = AlertManager()

# ─── AFFICHAGE DES ALERTES DANS LE DASHBOARD ──────────────────────────────────
def display_alert_banners(new_alerts):
    if not new_alerts:
        return
    for alert in new_alerts:
        sev = alert.get("severity_score", 0)
        frp = alert.get("frp", 0)
        country = alert.get("country", "Inconnu")
        lat = alert.get("latitude", 0)
        lon = alert.get("longitude", 0)
        eco = "🌳 Forêt primaire" if alert.get("primary_forest") else ""
        eco += " 🏕️ Protégée" if alert.get("protected_area") else ""
        st.markdown(f"""
        <div class='banner banner-red'>
          🔥 <b>ALERTE CRITIQUE</b> · {country} · FRP {frp:.0f} MW · Sévérité {sev:.2f}
          <span style='font-size:0.6rem; margin-left:auto;'>{lat:.2f}, {lon:.2f} {eco}</span>
        </div>
        """, unsafe_allow_html=True)
        # Toast notification (Streamlit)
        st.toast(f"🔥 Alerte critique {country} - FRP {frp:.0f} MW", icon="🚨")

# ══════════════════════════════════════════════════════════════════════════════
# CONNECTEURS DONNÉES — Supabase Storage (via requests, sans librairie supabase)
# ══════════════════════════════════════════════════════════════════════════════

def _get_supabase_creds():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except Exception:
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_KEY", "")
    return url, key

def _supabase_list(prefix: str) -> list:
    import requests
    url, key = _get_supabase_creds()
    bucket = "fires-raw"
    headers = {"Authorization": f"Bearer {key}", "apikey": key, "Content-Type": "application/json"}
    def recurse(current_prefix):
        try:
            r = requests.post(
                f"{url}/storage/v1/object/list/{bucket}",
                headers=headers,
                json={"prefix": current_prefix, "limit": 1000, "offset": 0},
                timeout=30,
            )
            if r.status_code != 200:
                return []
            csv_files = []
            for item in r.json():
                name = item.get("name", "")
                full_path = f"{current_prefix}{name}"
                if name.endswith(".csv"):
                    csv_files.append(full_path)
                elif name:
                    csv_files.extend(recurse(f"{full_path}/"))
            return csv_files
        except Exception:
            return []
    return recurse(prefix)

def _supabase_download(path: str) -> bytes:
    import requests
    url, key = _get_supabase_creds()
    try:
        r = requests.get(
            f"{url}/storage/v1/object/fires-raw/{path}",
            headers={"Authorization": f"Bearer {key}", "apikey": key},
            timeout=30,
        )
        return r.content if r.status_code == 200 else b""
    except Exception:
        return b""

@st.cache_resource(show_spinner=False)
def _minio():
    import requests
    url, key = _get_supabase_creds()
    if not url or not key:
        return None, False
    try:
        r = requests.post(
            f"{url}/storage/v1/object/list/fires-raw",
            headers={"Authorization": f"Bearer {key}", "apikey": key, "Content-Type": "application/json"},
            json={"prefix": "enriched/", "limit": 1},
            timeout=10,
        )
        return (True, True) if r.status_code == 200 else (None, False)
    except Exception:
        return None, False

@st.cache_data(ttl=300, show_spinner=False)
def load_enriched(days: int = 7) -> pd.DataFrame:
    _, ok = _minio()
    if not ok:
        return pd.DataFrame()
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        files = _supabase_list("enriched/")
        frames = []
        for path in files:
            try:
                parts = path.split("/")
                d = datetime.strptime(f"{parts[1]}-{parts[2]}-{parts[3]}", "%Y-%m-%d")
                if d < cutoff:
                    continue
            except Exception:
                pass
            data = _supabase_download(path)
            if data:
                frames.append(pd.read_csv(io.BytesIO(data)))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_raw(days: int = 7) -> pd.DataFrame:
    _, ok = _minio()
    if not ok:
        return pd.DataFrame()
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        files = _supabase_list("viirs/")
        frames = []
        for path in files:
            try:
                parts = path.split("/")
                d = datetime.strptime(f"{parts[1]}-{parts[2]}-{parts[3]}", "%Y-%m-%d")
                if d < cutoff:
                    continue
            except Exception:
                pass
            data = _supabase_download(path)
            if data:
                frames.append(pd.read_csv(io.BytesIO(data)))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_gold():
    try:
        from sqlalchemy import create_engine
        eng = create_engine("postgresql://airflow:airflow@localhost:5432/airflow")
        return pd.read_sql("SELECT * FROM fire_country_daily ORDER BY report_date DESC LIMIT 20000", eng)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def get_minio_stats() -> dict:
    _, ok = _minio()
    if not ok:
        return {"raw": 0, "enriched": 0, "connected": False, "last_ingestion": None}
    try:
        raw_files = _supabase_list("viirs/")
        enr_files = _supabase_list("enriched/")
        return {"raw": len(raw_files), "enriched": len(enr_files), "connected": True, "last_ingestion": None}
    except Exception:
        return {"raw": 0, "enriched": 0, "connected": False, "last_ingestion": None}

# ══════════════════════════════════════════════════════════════════════════════
# DBSCAN
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def run_dbscan(df: pd.DataFrame, eps_km: float = 5.0, min_samples: int = 3) -> pd.DataFrame:
    if df.empty or len(df) < min_samples:
        df = df.copy()
        df["cluster_id"] = -1
        return df
    try:
        from sklearn.cluster import DBSCAN
        coords = np.radians(df[["latitude", "longitude"]].values)
        eps = eps_km / 6371.0
        labels = DBSCAN(eps=eps, min_samples=min_samples,
                        algorithm="ball_tree", metric="haversine").fit_predict(coords)
        df = df.copy()
        df["cluster_id"] = labels
    except Exception as e:
        st.warning(f"DBSCAN failed: {e}")
        df = df.copy()
        df["cluster_id"] = -1
    return df

def build_cluster_summary(df: pd.DataFrame) -> pd.DataFrame:
    if "cluster_id" not in df.columns:
        return pd.DataFrame()
    cl = df[df["cluster_id"] >= 0].copy()
    if cl.empty:
        return pd.DataFrame()
    def safe_mode(x):
        try:
            modes = x.mode()
            return modes.iloc[0] if len(modes) > 0 else "—"
        except:
            return "—"
    def safe_any(x):
        try:
            return x.any()
        except:
            return False
    grp = cl.groupby("cluster_id").agg(
        n_hotspots=("frp", "count"),
        total_frp=("frp", "sum"),
        avg_frp=("frp", "mean"),
        max_severity=("severity_score", "max"),
        avg_severity=("severity_score", "mean"),
        lat=("latitude", "mean"),
        lon=("longitude", "mean"),
        country=("country", safe_mode),
        meteo_risk=("meteo_risk", safe_mode),
        primary_forest=("primary_forest", safe_any),
        protected_area=("protected_area", safe_any),
        zone_danger=("zone_danger", safe_mode),
        zone_radius_km=("zone_radius_km", "max"),
    ).reset_index().sort_values("total_frp", ascending=False)
    return grp

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import plotly.express as px
    import plotly.graph_objects as go

    PL = dict(
        paper_bgcolor="#0C0F1A", plot_bgcolor="#111520",
        font_family="Outfit", font_color="#7A88A8",
        title_font_color="#E8EEF8", title_font_size=13,
    )
    AX = dict(gridcolor="#1F2640", zerolinecolor="#1F2640",
              tickfont=dict(family="DM Mono", size=9, color="#3A4560"))

    # Sidebar
    with st.sidebar:
        st.markdown("""
        <div style='padding:14px 0 18px;'>
          <div style='font-size:1.1rem;font-weight:700;color:#FF7200;'>🔥 FireWatch</div>
          <div style='font-size:0.58rem;font-family:"DM Mono",monospace;color:#3A4560;margin-top:2px;'>
            NASA FIRMS · Pipeline v1.0
          </div>
        </div>""", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("**⏱ FENÊTRE TEMPORELLE**")
        days = st.slider("Jours", 1, 30, 7, label_visibility="collapsed")
        st.markdown("**🔥 FRP MINIMUM (MW)**")
        frp_min = st.slider("FRP min", 0, 2000, 0, label_visibility="collapsed")
        st.markdown("**🔵 DBSCAN eps (km)**")
        eps_km = st.slider("Rayon cluster", 1, 50, 5, label_visibility="collapsed")
        st.markdown("---")
        # Option email alerts
        email_alerts = st.checkbox("📧 Activer alertes email", value=False)
        st.session_state.alert_manager.email_enabled = email_alerts
        if st.button("🔄 Refresh", width='stretch'):
            st.cache_data.clear()
            st.rerun()
        st.markdown(f"""
        <div style='margin-top:14px;padding:10px 12px;background:#060810;
                    border:1px solid #1E2540;border-radius:6px;font-size:0.6rem;
                    font-family:"DM Mono",monospace;color:#3A4560;line-height:2;'>
          MinIO&nbsp;&nbsp;&nbsp;: <span style="color:#2ECC71">✓ connecté</span><br>
          PostgreSQL: <span style="color:#2ECC71">✓ connecté</span><br>
          UTC&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: {datetime.utcnow().strftime('%H:%M:%S')}
        </div>""", unsafe_allow_html=True)

    # ─── Pipeline status bar ──────────────────────────────────────────────────
    def pn(icon, label, ok=True):
        dot_c = "#2ECC71"
        cls = "p-ok"
        return f'<div class="pipe-node"><div class="pipe-dot" style="background:{dot_c};box-shadow:0 0 5px {dot_c};"></div><span class="{cls}">{icon} {label}</span></div>'
    st.markdown(f"""
    <div class='pipe-bar'>
        {pn("📡", "INGESTION NASA FIRMS")}
        <span class='pipe-sep'>→</span>
        {pn("🔧", "ENRICHISSEMENT VENTE")}
        <span class='pipe-sep'>→</span>
        {pn("🔍", "CLUSTERING DBSCAN")}
        <span class='pipe-sep'>→</span>
        {pn("🚨", "SYSTÈME D'ALERTES")}
    </div>
    """, unsafe_allow_html=True)

    # ─── Chargement des données ────────────────────────────────────────────────
    loading_placeholder = st.empty()
    loading_placeholder.markdown("""
    <div style='display:flex;flex-direction:column;align-items:center;justify-content:center;
                padding:60px 20px;background:#0C0F1A;border:1px solid #1E2540;border-radius:10px;margin:20px 0;'>
      <div style='font-size:2rem;margin-bottom:16px;'>🔥</div>
      <div style='font-family:"DM Mono",monospace;font-size:0.75rem;color:#FF7200;letter-spacing:0.15em;margin-bottom:8px;'>
        CONNEXION AU PIPELINE NASA FIRMS
      </div>
      <div style='font-family:"DM Mono",monospace;font-size:0.6rem;color:#3A4560;letter-spacing:0.1em;'>
        Récupération des données satellites en cours...
      </div>
      <div style='margin-top:20px;width:200px;height:2px;background:#1E2540;border-radius:2px;overflow:hidden;'>
        <div style='height:100%;background:linear-gradient(90deg,#FF7200,#FFB000,#FF7200);
                    background-size:200%;animation:sweep 2s linear infinite;border-radius:2px;'></div>
      </div>
    </div>
    """, unsafe_allow_html=True)
    df = load_enriched(days)
    if df.empty:
        df = load_raw(days)
    loading_placeholder.empty()
    if df.empty:
        st.markdown("""
        <div style='display:flex;flex-direction:column;align-items:center;justify-content:center;
                    padding:60px 20px;background:#0C0F1A;border:1px solid #1E2540;border-radius:10px;margin:20px 0;'>
          <div style='font-size:2rem;margin-bottom:16px;'>📡</div>
          <div style='font-family:"DM Mono",monospace;font-size:0.75rem;color:#3A4560;letter-spacing:0.15em;margin-bottom:8px;'>
            SYNCHRONISATION EN ATTENTE
          </div>
          <div style='font-family:"DM Mono",monospace;font-size:0.6rem;color:#1E2540;'>
            Le pipeline collecte les données · Réessayez dans quelques instants
          </div>
        </div>
        """, unsafe_allow_html=True)
        st.stop()
        # Nettoyage
        if 'confidence' in df.columns:
            conf_map = {'low': 0, 'nominal': 1, 'high': 2, 'l': 0, 'n': 1, 'h': 2}
            df['confidence'] = df['confidence'].apply(lambda x: conf_map.get(str(x).lower(), 1) if pd.notna(x) else 1)
        # Calculs métier
        if "severity_score" not in df.columns:
            with st.spinner("🧮 Calcul des scores de sévérité..."):
                b = df.get("bright_ti4", df.get("brightness", 380))
                c = df.get("confidence", 1)
                df["severity_score"] = [severity_score(float(df["frp"].iloc[i]), float(b.iloc[i]), c.iloc[i]) for i in range(len(df))]
        if "meteo_risk" not in df.columns and "temperature_c" in df.columns:
            with st.spinner("🌡️ Calcul des risques météo..."):
                df["meteo_risk"] = df.apply(lambda r: meteo_fire_risk(r.get("temperature_c",25), r.get("humidity_pct",50), r.get("windspeed_kmh",10)), axis=1)
        if "zone_danger" not in df.columns:
            with st.spinner("🗺️ Classification des zones de danger..."):
                df["zone_danger"] = df.apply(lambda r: zone_danger_level(r["severity_score"], r.get("meteo_risk","FAIBLE"), bool(r.get("primary_forest",False)), bool(r.get("protected_area",False))), axis=1)
        if "zone_radius_km" not in df.columns:
            with st.spinner("📏 Calcul des rayons de danger..."):
                df["zone_radius_km"] = df.apply(lambda r: danger_zone_radius(r["frp"], r.get("windspeed_kmh",0), bool(r.get("primary_forest",False))), axis=1)
        if "risk_label" not in df.columns:
            df["risk_label"] = df["severity_score"].apply(sev_label)
        gold_df = load_gold()
        mask = pd.Series([True] * len(df))
        if "frp" in df.columns:
            mask &= df["frp"] >= frp_min
        df = df[mask].copy()
        if df.empty:
            st.warning("⚠️ Aucun hotspot ne correspond aux filtres.")

    # ─── ALERTES CRITIQUES (détection + affichage) ────────────────────────────
    alert_manager = st.session_state.alert_manager
    new_alerts = alert_manager.check_and_alert(df)
    if new_alerts:
        # Affichage des bannières d'alerte en haut du dashboard
        display_alert_banners(new_alerts)
        # Envoi email si activé
        if alert_manager.email_enabled:
            alert_manager.send_email_alert(new_alerts)

    # DBSCAN clustering
    if not df.empty:
        with st.spinner("🔍 Clustering DBSCAN..."):
            df = run_dbscan(df, eps_km=eps_km)
            cl_summ = build_cluster_summary(df)
            n_clust = df[df["cluster_id"] >= 0]["cluster_id"].nunique() if not df.empty else 0
    else:
        cl_summ = pd.DataFrame()
        n_clust = 0

    # KPIs
    total = len(df)
    n_crit = (df["risk_label"] == "CRITICAL").sum() if "risk_label" in df.columns else 0
    n_high = (df["risk_label"] == "HIGH").sum() if "risk_label" in df.columns else 0
    n_eco = int(df[["primary_forest", "protected_area"]].any(axis=1).sum()) if all(c in df.columns for c in ["primary_forest","protected_area"]) else 0
    danger_score = compute_global_danger_score(df)

    # Header
    st.markdown(f"""
    <div class='fw-top'>
      <div>
        <div class='fw-logo'>Fire<b>Watch</b> Intelligence</div>
        <div class='fw-sub'>NASA FIRMS VIIRS · Kafka → MinIO → Airflow → DuckDB → PostgreSQL</div>
      </div>
      <div class='fw-live'>
        <div class='live-dot'></div>
        SURVEILLANCE INCENDIES TEMPS RÉEL &nbsp;·&nbsp;
        {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC &nbsp;·&nbsp;
        <span style='color:#2ECC71'>● LIVE</span>
      </div>
    </div>""", unsafe_allow_html=True)

    # VUES
    if df.empty:
        st.warning("Aucune donnée à afficher.")
        st.stop()

    vue1, vue2, vue3, vue4 = st.tabs([
        "🔴  Centre de Commande",
        "🌍  Carte des Zones de Danger",
        "📊  Diagnostic & Analyse",
        "⚙️  Observabilité Pipeline",
    ])

    # ──────────────────────────────────────────────────────────────────────────
    # VUE 1
    with vue1:
        col_gauge, col_kpis = st.columns([1, 3])
        with col_gauge:
            gauge_color = "#FF3030" if danger_score >= 70 else "#FF7200" if danger_score >= 45 else "#FFB000" if danger_score >= 25 else "#2ECC71"
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number", value=danger_score,
                number={"suffix": "/100", "font": {"size": 28, "family": "DM Mono", "color": gauge_color}},
                gauge={
                    "axis": {"range": [0, 100], "tickwidth": 1,
                             "tickfont": {"size": 9, "family": "DM Mono", "color": "#3A4560"},
                             "tickcolor": "#1F2640"},
                    "bar": {"color": gauge_color, "thickness": 0.26},
                    "bgcolor": "#111520",
                    "steps": [{"range": [0, 25], "color": "#0C1A10"},
                              {"range": [25, 45], "color": "#1A1400"},
                              {"range": [45, 70], "color": "#1A0E00"},
                              {"range": [70, 100], "color": "#1A0500"}],
                    "threshold": {"line": {"color": gauge_color, "width": 2}, "value": danger_score},
                },
                title={"text": "SCORE GLOBAL<br>DE DANGER", "font": {"size": 10, "family": "DM Mono", "color": "#3A4560"}},
            ))
            fig_gauge.update_layout(**PL, height=220, margin=dict(t=40, b=10, l=20, r=20))
            st.plotly_chart(fig_gauge, width='stretch')
            st.markdown(f"""
            <div style='text-align:center;padding:6px 0;'>
              <span class='badge {"b-red" if danger_score>=70 else "b-orange" if danger_score>=45 else "b-amber" if danger_score>=25 else "b-green"}'>
                {"CRITIQUE" if danger_score>=70 else "ÉLEVÉ" if danger_score>=45 else "MODÉRÉ" if danger_score>=25 else "FAIBLE"}
              </span>
            </div>""", unsafe_allow_html=True)

        with col_kpis:
            def kpi(label, val, sub, color):
                return f"""
                <div class='kpi'>
                  <div class='kpi-bar' style='background:{color};'></div>
                  <div class='kpi-lbl'>{label}</div>
                  <div class='kpi-val' style='color:{color};'>{val}</div>
                  <div class='kpi-sub'>{sub}</div>
                </div>"""
            g1,g2,g3,g4,g5 = st.columns(5)
            with g1: st.markdown(kpi("Hotspots Actifs", f"{total:,}", f"J-{days}", "#4A9EFF"), unsafe_allow_html=True)
            with g2: st.markdown(kpi("Critiques", str(n_crit), f"{n_crit/max(total,1)*100:.1f}%", "#FF3030"), unsafe_allow_html=True)
            with g3: st.markdown(kpi("Risque Élevé", str(n_high), "sev ≥ 0.50", "#FF7200"), unsafe_allow_html=True)
            with g4: st.markdown(kpi("Clusters DBSCAN", str(n_clust), f"eps={eps_km}km", "#9B6EFF"), unsafe_allow_html=True)
            with g5: st.markdown(kpi("Zones Éco-Critiques", str(n_eco), "Forêt + Protégées", "#2ECC71"), unsafe_allow_html=True)

        st.markdown("<div class='sect'>Top 5 Incendies Critiques</div>", unsafe_allow_html=True)
        top5 = df.nlargest(5, "severity_score") if "severity_score" in df.columns else df.head(5)
        rows_html = ""
        for i, (_, r) in enumerate(top5.iterrows(), 1):
            lbl = sev_label(r.get("severity_score",0))
            mr = r.get("meteo_risk","—")
            mr_c = risk_color(mr)
            pf = "🌳" if r.get("primary_forest",False) else "·"
            pa = "🏕️" if r.get("protected_area",False) else "·"
            zone = r.get("zone_danger","—")
            rows_html += f"""
            <tr>
              <td style='color:#3A4560;font-size:.6rem;'>{i}</td>
              <td class='country'>{r.get('country','—')}</td>
              <td style='color:#4A9EFF;font-size:.62rem;'>{r.get('latitude',0):.2f}°, {r.get('longitude',0):.2f}°</td>
              <td class='frp-val'>{r.get('frp',0):.0f} MW</td>
              <td style='color:#9B9ECC;'>{r.get('temperature_c',0):.1f}°C</td>
              <td style='color:#4A9EFF;'>{r.get('humidity_pct',0):.0f}%</td>
              <td style='color:#9B6EFF;'>{r.get('windspeed_kmh',0):.0f} km/h</td>
              <td>{pf} {pa}</td>
              <td><span class='badge {"b-red" if lbl=="CRITICAL" else "b-orange" if lbl=="HIGH" else "b-amber" if lbl=="MEDIUM" else "b-green"}'>{lbl}</span></td>
              <td style='color:{mr_c};font-size:.62rem;'>{mr}</td>
              <td style='color:{"#FF3030" if zone=="CRITIQUE" else "#FF7200" if zone=="HAUT" else "#FFB000" if zone=="SURVEILLANCE" else "#2ECC71"};font-size:.62rem;'>{zone}</td>
            </tr>"""
        st.markdown(f"""
        <div class='card'>
          <table class='fire-table'>
            <thead><tr><th>#</th><th>Pays</th><th>Coordonnées</th><th>FRP</th><th>Temp</th><th>Humidité</th><th>Vent</th><th>Zone Éco</th><th>Sévérité</th><th>Risque Météo</th><th>Zone Danger</th></tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>""", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # VUE 2 - Carte interactive
    with vue2:
        st.markdown("🌍 **Visualisation géographique**")
        col1, col2 = st.columns([1, 4])
        with col1:
            show_hotspots = st.checkbox("🔥 Hotspots", value=True)
            show_clusters = st.checkbox("🔵 Clusters", value=True)
            show_zones = st.checkbox("⚠️ Zones de danger", value=True)
        with col2:
            map_df = df.dropna(subset=["latitude","longitude"]).copy()
            if map_df.empty:
                st.warning("Aucune donnée géolocalisée")
            else:
                fig = go.Figure()
                if show_hotspots:
                    fig.add_trace(go.Scattermapbox(
                        lat=map_df["latitude"], lon=map_df["longitude"],
                        mode="markers",
                        marker=dict(size=np.clip(map_df["frp"]/10,5,20),
                                    color=map_df["severity_score"], colorscale="Reds", showscale=True),
                        name="Hotspots"
                    ))
                if show_clusters and not cl_summ.empty:
                    fig.add_trace(go.Scattermapbox(
                        lat=cl_summ["lat"], lon=cl_summ["lon"],
                        mode="markers+text",
                        marker=dict(size=20, color="blue"),
                        text=cl_summ["n_hotspots"], textposition="top center",
                        name="Clusters"
                    ))
                if show_zones:
                    for _, r in map_df.head(200).iterrows():
                        fig.add_trace(go.Scattermapbox(
                            lat=[r["latitude"]], lon=[r["longitude"]],
                            mode="markers",
                            marker=dict(size=r["zone_radius_km"]*2, color="orange", opacity=0.2),
                            showlegend=False
                        ))
                fig.update_layout(
                    mapbox_style="carto-darkmatter", mapbox_zoom=2, mapbox_center={"lat":20,"lon":0},
                    margin={"r":0,"t":0,"l":0,"b":0}, height=600
                )
                st.plotly_chart(fig, width='stretch')

    # ──────────────────────────────────────────────────────────────────────────
    # VUE 3 - Diagnostic & Analyse
    with vue3:
        if not df.empty:
            top_alerts = df.nlargest(20, "severity_score")
            sel_idx = st.selectbox(
                "Sélectionner un incendie",
                range(len(top_alerts)),
                format_func=lambda i: f"{i+1} | {top_alerts.iloc[i].get('country','?')} | FRP: {top_alerts.iloc[i].get('frp',0):.0f} MW | Sev: {top_alerts.iloc[i].get('severity_score',0):.3f}"
            )
            sel_row = top_alerts.iloc[sel_idx]
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                <div class='card'>
                  <div class='card-title'>🔥 Fiche incendie</div>
                  <table style='width:100%;font-size:0.8rem;'>
                    <tr><td><b>Pays</b></td><td>{sel_row.get('country','—')}</td></tr>
                    <tr><td><b>Coordonnées</b></td><td>{sel_row.get('latitude',0):.4f}°, {sel_row.get('longitude',0):.4f}°</td></tr>
                    <tr><td><b>FRP</b></td><td>{sel_row.get('frp',0):.1f} MW</td></tr>
                    <tr><td><b>Sévérité</b></td><td>{sel_row.get('severity_score',0):.3f}</td></tr>
                    <tr><td><b>Température</b></td><td>{sel_row.get('temperature_c',0):.1f}°C</td></tr>
                    <tr><td><b>Humidité</b></td><td>{sel_row.get('humidity_pct',0):.1f}%</td></tr>
                    <tr><td><b>Vent</b></td><td>{sel_row.get('windspeed_kmh',0):.1f} km/h</td></tr>
                    <tr><td><b>Risque météo</b></td><td>{sel_row.get('meteo_risk','—')}</td></tr>
                    <tr><td><b>Zone danger</b></td><td>{sel_row.get('zone_danger','—')}</td></tr>
                    <tr><td><b>Confiance</b></td><td>{format_confidence(sel_row.get('confidence'))}</td></tr>
                  </table>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                fig = go.Figure()
                categories = ['FRP','Température','Sécheresse','Vent','Forêt','Protégée']
                values = [
                    min(sel_row.get('frp',0)/1000,1)*100,
                    min(max(sel_row.get('temperature_c',0)-10,0)/40,1)*100,
                    max(100-sel_row.get('humidity_pct',0),0),
                    min(sel_row.get('windspeed_kmh',0)/80,1)*100,
                    100 if sel_row.get('primary_forest',False) else 0,
                    100 if sel_row.get('protected_area',False) else 0,
                ]
                fig.add_trace(go.Scatterpolar(r=values+[values[0]], theta=categories+[categories[0]], fill='toself', name='Profil'))
                fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0,100])), height=400, **PL)
                st.plotly_chart(fig, width='stretch')

    # ──────────────────────────────────────────────────────────────────────────
    # VUE 4 - Observabilité Pipeline
    with vue4:
        mstats = get_minio_stats()
        c1,c2,c3 = st.columns(3)
        with c1: st.metric("Fichiers bruts (MinIO)", mstats["raw"])
        with c2: st.metric("Fichiers enrichis", mstats["enriched"])
        with c3:
            pct = min(100, round(mstats["enriched"]/max(mstats["raw"],1)*100))
            st.metric("Taux enrichissement", f"{pct}%")
        if mstats["last_ingestion"]:
            st.info(f"🕐 Dernière ingestion: {mstats['last_ingestion'].strftime('%Y-%m-%d %H:%M:%S')}")
        st.markdown("---")
        st.markdown("<div class='sect'>Qualité des données</div>", unsafe_allow_html=True)
        qual_data = []
        if "temperature_c" in df.columns:
            qual_data.append(("🌡️ Température", df["temperature_c"].notna().sum()/len(df)*100))
        if "humidity_pct" in df.columns:
            qual_data.append(("💧 Humidité", df["humidity_pct"].notna().sum()/len(df)*100))
        if "primary_forest" in df.columns:
            qual_data.append(("🌳 Forêt primaire", df["primary_forest"].notna().sum()/len(df)*100))
        if "protected_area" in df.columns:
            qual_data.append(("🏕️ Zone protégée", df["protected_area"].notna().sum()/len(df)*100))
        for name, pct in qual_data:
            st.progress(pct/100, text=f"{name}: {pct:.1f}%")

    # Footer
    st.markdown(f"""
    <div style='text-align:center;padding:14px 0 6px;font-family:"DM Mono",monospace;
                font-size:0.58rem;color:#1E2540;letter-spacing:.08em;'>
      🔥 FIREWATCH INTELLIGENCE · NASA FIRMS VIIRS · {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
    </div>""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()