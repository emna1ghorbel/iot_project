"""
train_model.py
==============
Entraîne un modèle Isolation Forest pour détecter les anomalies
de consommation électrique dans le système Smart Home.

Pourquoi Isolation Forest ?
- Algorithme non supervisé → pas besoin de données étiquetées
- Très efficace pour détecter les anomalies rares (3-5%)
- Léger et rapide → compatible avec AWS Lambda
- Fonctionne bien avec des séries temporelles électriques

Auteurs : Ranim Bouguila, Emna Ghorbel
"""

import numpy as np
import pandas as pd
import joblib
import boto3
import os
import json
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
REGION        = "us-east-1"
MODEL_DIR     = "ai_model"          # dossier local pour sauvegarder les modèles
TAUX_ANOMALIE = 0.05                # 5% de contamination pour Isolation Forest
RANDOM_STATE  = 42

# ─────────────────────────────────────────────
# ÉTAPE 1 : GÉNÉRATION DU DATASET D'ENTRAÎNEMENT
# ─────────────────────────────────────────────

def generer_dataset(nb_points=5000):
    """
    Génère un dataset réaliste de consommation électrique.
    
    Pourquoi générer des données ?
    On n'a pas encore de données réelles accumulées depuis Wokwi.
    On génère des données qui respectent les mêmes distributions
    que celles publiées par l'ESP32 simulé.
    
    Retourne un DataFrame avec : voltage, current, power, energy, is_anomaly
    """
    print(f"📊 Génération de {nb_points} points de données...")
    np.random.seed(RANDOM_STATE)
    
    records = []
    energie_kwh = 0.0
    debut = datetime(2025, 1, 1)
    
    for i in range(nb_points):
        ts = debut + timedelta(seconds=i * 5)
        heure = ts.hour + ts.minute / 60.0
        is_anomaly = 0
        
        # Profil de puissance selon l'heure (identique au simulateur Wokwi)
        if 0 <= heure < 6:
            power = np.random.uniform(50, 150)
        elif 6 <= heure < 9:
            power = np.random.uniform(1200, 2000)
        elif 9 <= heure < 17:
            power = np.random.uniform(400, 900)
        elif 17 <= heure < 22:
            power = np.random.uniform(1500, 2500)
        else:
            power = np.random.uniform(200, 600)
        
        voltage = np.random.normal(230, 2)
        current = power / voltage
        
        # Injection d'anomalies (5%)
        if np.random.random() < TAUX_ANOMALIE:
            type_anomalie = np.random.choice([
                "surconsommation", "surtension", "coupure"
            ])
            if type_anomalie == "surconsommation":
                power   *= np.random.uniform(2.5, 4.0)
                current  = power / voltage
            elif type_anomalie == "surtension":
                voltage *= np.random.uniform(1.15, 1.3)
                current  = power / voltage
            elif type_anomalie == "coupure":
                power   = 0.0
                current = 0.0
            is_anomaly = 1
        
        energie_kwh += (power * 5) / 3_600_000
        
        records.append({
            "timestamp" : ts.isoformat(),
            "voltage"   : round(voltage, 2),
            "current"   : round(abs(current), 3),
            "power"     : round(power, 2),
            "energy"    : round(energie_kwh, 6),
            "is_anomaly": is_anomaly
        })
    
    df = pd.DataFrame(records)
    print(f"✅ Dataset généré : {len(df)} points")
    print(f"   Anomalies     : {df['is_anomaly'].sum()} ({df['is_anomaly'].mean()*100:.1f}%)")
    print(f"   Puissance moy : {df['power'].mean():.1f} W")
    print(f"   Tension moy   : {df['voltage'].mean():.1f} V")
    return df


# ─────────────────────────────────────────────
# ÉTAPE 2 : ENTRAÎNEMENT DU MODÈLE
# ─────────────────────────────────────────────

def entrainer_modele(df):
    """
    Entraîne le modèle Isolation Forest.
    
    Pourquoi StandardScaler ?
    Les features ont des échelles très différentes :
    - voltage ~230V, current ~5A, power ~1000W, energy ~0.001kWh
    Le scaler normalise tout entre 0 et 1 pour que le modèle
    traite chaque feature avec la même importance.
    
    Paramètres Isolation Forest :
    - n_estimators=100  : 100 arbres → bon compromis précision/vitesse
    - contamination=0.05: on suppose 5% d'anomalies dans le dataset
    - random_state=42   : reproductibilité des résultats
    """
    print("\n🤖 Entraînement du modèle Isolation Forest...")
    
    # Features utilisées pour la détection
    features = ["voltage", "current", "power", "energy"]
    X = df[features].values
    y = df["is_anomaly"].values  # pour évaluation uniquement
    
    # Normalisation des données
    # Pourquoi ? L'Isolation Forest est sensible aux échelles
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Entraînement Isolation Forest
    # On entraîne UNIQUEMENT sur les données normales
    # pour que le modèle apprenne ce qu'est "normal"
    X_normal = X_scaled[y == 0]
    
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,    # 5% d'anomalies attendues
        random_state=RANDOM_STATE,
        n_jobs=-1              # utiliser tous les CPUs disponibles
    )
    model.fit(X_normal)
    
    print("✅ Modèle entraîné !")
    return model, scaler, features


# ─────────────────────────────────────────────
# ÉTAPE 3 : ÉVALUATION DU MODÈLE
# ─────────────────────────────────────────────

def evaluer_modele(model, scaler, df, features):
    """
    Évalue les performances du modèle sur le dataset complet.
    
    Isolation Forest retourne :
    -  1 → données normales
    - -1 → anomalie détectée
    
    On convertit -1 → 1 et 1 → 0 pour comparer avec is_anomaly.
    """
    print("\n📈 Évaluation du modèle...")
    
    X = df[features].values
    y_true = df["is_anomaly"].values
    
    X_scaled = scaler.transform(X)
    y_pred_raw = model.predict(X_scaled)
    
    # Conversion : -1 → anomalie (1), 1 → normal (0)
    y_pred = (y_pred_raw == -1).astype(int)
    
    # Scores d'anomalie (plus négatif = plus anormal)
    scores = model.score_samples(X_scaled)
    
    print("\n  Rapport de classification :")
    print(classification_report(
        y_true, y_pred,
        target_names=["Normal", "Anomalie"],
        zero_division=0
    ))
    
    # Statistiques des scores
    print(f"  Score moyen (normal)  : {scores[y_true==0].mean():.4f}")
    print(f"  Score moyen (anomalie): {scores[y_true==1].mean():.4f}")
    
    return y_pred, scores


# ─────────────────────────────────────────────
# ÉTAPE 4 : SAUVEGARDE LOCALE
# ─────────────────────────────────────────────

def sauvegarder_local(model, scaler, features):
    """
    Sauvegarde le modèle et le scaler en local.
    
    Pourquoi joblib ?
    joblib est optimisé pour sauvegarder les objets scikit-learn.
    Plus rapide que pickle pour les tableaux numpy (utilisés par sklearn).
    """
    print("\n💾 Sauvegarde locale des modèles...")
    
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    model_path  = os.path.join(MODEL_DIR, "model.pkl")
    scaler_path = os.path.join(MODEL_DIR, "scaler.pkl")
    config_path = os.path.join(MODEL_DIR, "config.json")
    
    joblib.dump(model,  model_path)
    joblib.dump(scaler, scaler_path)
    
    # Sauvegarder la configuration
    config = {
        "features"     : features,
        "contamination": TAUX_ANOMALIE,
        "n_estimators" : 100,
        "trained_at"   : datetime.now().isoformat(),
        "model_file"   : "model.pkl",
        "scaler_file"  : "scaler.pkl"
    }
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"  ✅ model.pkl  → {model_path}")
    print(f"  ✅ scaler.pkl → {scaler_path}")
    print(f"  ✅ config.json→ {config_path}")
    
    return model_path, scaler_path


# ─────────────────────────────────────────────
# ÉTAPE 5 : UPLOAD VERS S3 (pour Lambda)
# ─────────────────────────────────────────────

def upload_vers_s3(model_path, scaler_path):
    """
    Upload les modèles vers S3 pour que la Lambda puisse les charger.
    
    Pourquoi S3 ?
    AWS Lambda a une taille limite de déploiement (250MB).
    Les modèles sklearn peuvent être lourds avec leurs dépendances.
    S3 permet de charger le modèle dynamiquement au runtime.
    """
    print("\n☁️  Upload vers S3...")
    
    try:
        sts    = boto3.client("sts", region_name=REGION)
        s3     = boto3.client("s3", region_name=REGION)
        
        account_id  = sts.get_caller_identity()["Account"]
        bucket_name = f"smarthome-models-{account_id}"
        
        # Créer le bucket si nécessaire
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"  ✅ Bucket créé : {bucket_name}")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"  ℹ️  Bucket existant : {bucket_name}")
        except Exception as e:
            if "BucketAlreadyExists" not in str(e):
                raise
        
        # Upload model.pkl
        s3.upload_file(model_path,  bucket_name, "model.pkl")
        s3.upload_file(scaler_path, bucket_name, "scaler.pkl")
        
        print(f"  ✅ model.pkl  → s3://{bucket_name}/model.pkl")
        print(f"  ✅ scaler.pkl → s3://{bucket_name}/scaler.pkl")
        print(f"\n  📌 Nom du bucket (à noter) : {bucket_name}")
        
        return bucket_name
        
    except Exception as e:
        print(f"  ⚠️  Upload S3 échoué : {e}")
        print("  → Les modèles sont sauvegardés localement dans ai_model/")
        print("  → Tu pourras uploader manuellement plus tard.")
        return None


# ─────────────────────────────────────────────
# PROGRAMME PRINCIPAL
# ─────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  🤖 Smart Home — Entraînement Modèle IA")
    print("  Algorithme : Isolation Forest (scikit-learn)")
    print("=" * 55)
    
    # 1. Générer le dataset
    df = generer_dataset(nb_points=5000)
    
    # 2. Entraîner le modèle
    model, scaler, features = entrainer_modele(df)
    
    # 3. Évaluer le modèle
    evaluer_modele(model, scaler, df, features)
    
    # 4. Sauvegarder en local
    model_path, scaler_path = sauvegarder_local(model, scaler, features)
    
    # 5. Upload vers S3
    bucket = upload_vers_s3(model_path, scaler_path)
    
    print("\n" + "=" * 55)
    print("  ✅ Entraînement terminé !")
    print("  Fichiers créés dans ai_model/ :")
    print("    - model.pkl   (Isolation Forest)")
    print("    - scaler.pkl  (StandardScaler)")
    print("    - config.json (configuration)")
    if bucket:
        print(f"  Modèles uploadés sur S3 : {bucket}")
    print("=" * 55)


if __name__ == "__main__":
    main()