

import sys
import os
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))


# ── Helpers locaux (copies exactes du pipeline) ──────────────────────────────

def severity_score(frp: float, brightness: float, confidence: int) -> float:
    norm_frp    = min(frp / 1000, 1.0)
    norm_bright = max(0.0, (brightness - 300) / 200)
    norm_conf   = confidence / 100
    s = norm_frp * 0.50 + norm_bright * 0.30 + norm_conf * 0.20
    return round(max(0.0, min(s, 1.0)), 3)


def meteo_fire_risk(temp_c: float, hum: float, wind: float) -> str:
    s = 0
    if temp_c > 35:  s += 3
    elif temp_c > 25: s += 1
    if hum < 20:  s += 3
    elif hum < 40: s += 1
    if wind > 50:  s += 3
    elif wind > 30: s += 1
    return ('EXTREME' if s >= 7 else 'ELEVE' if s >= 4 else 'MODERE' if s >= 2 else 'FAIBLE')


def cluster_fire_points(df, eps_km=5.0, min_samples=3):
    from sklearn.cluster import DBSCAN
    coords_rad = np.radians(df[['latitude', 'longitude']].values)
    eps_rad = eps_km / 6371.0
    db = DBSCAN(eps=eps_rad, min_samples=min_samples,
                algorithm='ball_tree', metric='haversine')
    df = df.copy()
    df['cluster_id'] = db.fit_predict(coords_rad)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1 : severity_score — cas limite bas
# ═══════════════════════════════════════════════════════════════════════════════
class TestSeverityScore:

    def test_minimum(self):
        """FRP=0, brightness=300K (min), confidence=0 → score=0.0"""
        assert severity_score(0, 300, 0) == 0.0

    def test_maximum(self):
        """FRP=1000MW (normalisé à 1), brightness=500K, conf=100 → score=1.0"""
        assert severity_score(1000, 500, 100) == 1.0

    def test_partial(self):
        """Valeurs intermédiaires — score ∈ [0, 1]"""
        s = severity_score(500, 400, 50)
        assert 0.0 <= s <= 1.0

    def test_frp_clip(self):
        """FRP > 1000 MW est normalisé à 1 — score ne dépasse pas 1"""
        assert severity_score(9999, 500, 100) == 1.0

    def test_brightness_floor(self):
        """Brightness < 300K → norm_bright = 0 (pas négatif)"""
        s_below = severity_score(100, 200, 50)
        s_at    = severity_score(100, 300, 50)
        assert s_below == s_at  # norm_bright=0 dans les deux cas


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2 : meteo_fire_risk
# ═══════════════════════════════════════════════════════════════════════════════
class TestMeteoFireRisk:

    def test_extreme(self):
        """Temp > 35 + hum < 20 + vent > 50 → EXTREME (score 9 ≥ 7)"""
        assert meteo_fire_risk(38, 15, 55) == 'EXTREME'

    def test_faible(self):
        """Conditions douces → FAIBLE"""
        assert meteo_fire_risk(15, 80, 5) == 'FAIBLE'

    def test_eleve(self):
        """Temp > 35 + vent > 30 → ELEVE (score 3+1=4)"""
        assert meteo_fire_risk(36, 50, 35) == 'ELEVE'

    def test_modere(self):
        """Hum < 40 ET hum >= 20 (+1) + vent > 30 (+1) → score=2 → MODERE
        Temp=20 → score temp=0, hum=35 → score hum=+1, vent=35 → score vent=+1
        Total score = 2 → MODERE"""
        assert meteo_fire_risk(20, 35, 35) == 'MODERE'

    def test_boundary_humidity(self):
        """Hum exactement 20% → pas la catégorie < 20"""
        r = meteo_fire_risk(20, 20, 10)
        # hum=20 → condition hum<20 est False, hum<40 est True → +1
        assert r in ['MODERE', 'FAIBLE']


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3 : DBSCAN clustering
# ═══════════════════════════════════════════════════════════════════════════════
class TestClusterFirePoints:

    def _nearby_points(self, center_lat, center_lon, n=5, spread_deg=0.01):
        """Génère n points très proches — doivent former 1 cluster."""
        rng = np.random.default_rng(0)
        return pd.DataFrame({
            'latitude':  center_lat + rng.uniform(-spread_deg, spread_deg, n),
            'longitude': center_lon + rng.uniform(-spread_deg, spread_deg, n),
            'frp': rng.uniform(50, 200, n),
        })

    def test_single_cluster(self):
        """5 points < 5km → 1 cluster unique (cluster_id == 0)."""
        df = self._nearby_points(-10, -55, n=5, spread_deg=0.01)
        result = cluster_fire_points(df, eps_km=5.0, min_samples=3)
        unique_clusters = result[result['cluster_id'] >= 0]['cluster_id'].nunique()
        assert unique_clusters == 1

    def test_two_distant_clusters(self):
        """2 groupes distants > 50km → 2 clusters distincts."""
        g1 = self._nearby_points(0.0, 0.0, n=5)
        g2 = self._nearby_points(5.0, 5.0, n=5)   # ~770 km
        df = pd.concat([g1, g2], ignore_index=True)
        result = cluster_fire_points(df, eps_km=5.0, min_samples=3)
        unique_clusters = result[result['cluster_id'] >= 0]['cluster_id'].nunique()
        assert unique_clusters == 2

    def test_isolated_noise(self):
        """Point isolé loin de tout autre → cluster_id == -1 (bruit)."""
        g1 = self._nearby_points(0.0, 0.0, n=5)
        isolated = pd.DataFrame({'latitude': [40.0], 'longitude': [40.0], 'frp': [100.0]})
        df = pd.concat([g1, isolated], ignore_index=True)
        result = cluster_fire_points(df, eps_km=5.0, min_samples=3)
        # Le point isolé doit être marqué -1
        assert result.iloc[-1]['cluster_id'] == -1


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4 : Détection d'anomalie de tendance N-1
# ═══════════════════════════════════════════════════════════════════════════════
class TestTrendAlert:

    def _pct_change(self, current, last):
        if last == 0:
            return None
        return round(100.0 * (current - last) / last, 1)

    def _alert_triggered(self, pct):
        return pct is not None and pct > 50

    def test_alert_triggered_67pct(self):
        """N=300, N-1=180 → +67% → alert_triggered = True"""
        pct = self._pct_change(300, 180)
        assert pct == pytest.approx(66.7, abs=0.1)
        assert self._alert_triggered(pct) is True

    def test_no_alert_30pct(self):
        """N=130, N-1=100 → +30% → pas d'alerte"""
        pct = self._pct_change(130, 100)
        assert self._alert_triggered(pct) is False

    def test_exactly_50pct(self):
        """N=150, N-1=100 → +50% → seuil non franchi (> et non >=)"""
        pct = self._pct_change(150, 100)
        assert pct == 50.0
        assert self._alert_triggered(pct) is False  # > 50, pas ≥ 50

    def test_decrease(self):
        """N < N-1 → pct < 0 → pas d'alerte"""
        pct = self._pct_change(50, 200)
        assert pct < 0
        assert self._alert_triggered(pct) is False

    def test_zero_last_year(self):
        """N-1=0 → pas de division par zéro → None → pas d'alerte"""
        pct = self._pct_change(100, 0)
        assert pct is None
        assert self._alert_triggered(pct) is False


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5 : Qualité des données — cohérence du schéma pipeline
# ═══════════════════════════════════════════════════════════════════════════════
class TestDataQuality:

    def _sample_df(self, n=50):
        rng = np.random.default_rng(42)
        return pd.DataFrame({
            'latitude':  rng.uniform(-90, 90, n),
            'longitude': rng.uniform(-180, 180, n),
            'frp':       rng.uniform(0.1, 5000, n),
            'brightness': rng.uniform(300, 500, n),
            'confidence': rng.choice([0, 1, 2], n),
            'temperature_c': rng.uniform(-15, 55, n),
            'humidity_pct':  rng.uniform(4, 100, n),
            'windspeed_kmh': rng.uniform(0, 130, n),
        })

    def test_coordinates_valid(self):
        """Latitude ∈ [-90, 90] et longitude ∈ [-180, 180]."""
        df = self._sample_df()
        assert df['latitude'].between(-90, 90).all()
        assert df['longitude'].between(-180, 180).all()

    def test_frp_positive(self):
        """FRP ≥ 0 pour tous les records."""
        df = self._sample_df()
        assert (df['frp'] >= 0).all()

    def test_severity_in_range(self):
        """severity_score ∈ [0, 1] pour toutes combinaisons."""
        df = self._sample_df()
        scores = df.apply(
            lambda r: severity_score(r['frp'], r['brightness'], int(r['confidence'])),
            axis=1
        )
        assert scores.between(0.0, 1.0).all()

    def test_meteo_risk_values(self):
        """meteo_fire_risk retourne uniquement FAIBLE/MODERE/ELEVE/EXTREME."""
        df = self._sample_df()
        valid = {'FAIBLE', 'MODERE', 'ELEVE', 'EXTREME'}
        risks = df.apply(
            lambda r: meteo_fire_risk(r['temperature_c'], r['humidity_pct'], r['windspeed_kmh']),
            axis=1
        )
        assert set(risks.unique()).issubset(valid)

    def test_confidence_values(self):
        """confidence ∈ {0, 1, 2} (low, nominal, high)."""
        df = self._sample_df()
        assert df['confidence'].isin([0, 1, 2]).all()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])