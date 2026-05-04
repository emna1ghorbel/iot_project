# 🏠 Smart Home IoT & AI Monitoring System

Ce projet est une solution complète de surveillance intelligente pour la maison (Smart Home), combinant l'IoT, le Cloud (AWS) et l'Intelligence Artificielle pour monitorer la consommation électrique et détecter automatiquement les anomalies.

## 🚀 Vue d'ensemble du fonctionnement

Le système simule plusieurs appareils domestiques qui envoient leurs données de consommation vers le cloud. Un moteur d'IA analyse ces données en temps réel pour détecter des comportements suspects (surconsommation, surtension, etc.) et peut couper automatiquement les appareils pour protéger l'installation. Un tableau de bord professionnel permet de visualiser l'état de la maison et de contrôler les appareils à distance.

---

## 🏗️ Architecture du Projet

```text
iot_project/
├── ai_model/                # Modèles d'IA et script d'entraînement
│   ├── config.json          # Configuration du modèle
│   ├── model.pkl            # Modèle Isolation Forest entraîné
│   ├── scaler.pkl           # Scaler pour la normalisation des données
│   └── train_model.py       # Script d'entraînement du modèle
├── backend/                 # Serveur Flask (API REST)
│   └── app.py               # Point d'entrée de l'API
├── certs/                   # Certificats de sécurité AWS IoT Core
│   ├── AmazonRootCA1.pem
│   ├── certificate.pem.crt
│   └── private.pem.key
├── frontend/                # Interface Utilisateur (Dashboard)
│   └── index.html           # Dashboard interactif
├── simulator/               # Simulation et Monitoring
│   └── src/
│       ├── detect_anomalies.py  # Moteur de détection d'anomalies IA
│       └── sim.py               # Simulateur d'appareils IoT
├── Policy.json              # Définition des permissions IoT
├── rule-dynamo.json         # Règle d'ingestion vers DynamoDB
├── rule-timestream.json     # Règle d'ingestion vers Timestream
├── rule.json                # Configuration des règles IoT
├── trust.json               # Relation de confiance IAM
├── test_event.json          # Événement de test pour les fonctions
├── response.json            # Exemple de réponse API
├── requirement.txt          # Liste des dépendances Python
└── README.md                # Documentation du projet
```

Le projet est divisé en quatre piliers principaux :

1.  **Simulateur IoT (`simulator/`)** : Simule des appareils connectés (Climatisation, Chauffe-eau, Éclairage, Circuit Principal).
2.  **Intelligence Artificielle (`ai_model/` & `detect_anomalies.py`)** : Détecte les anomalies de consommation à l'aide d'un modèle *Isolation Forest*.
3.  **Backend API (`backend/`)** : Serveur Flask faisant le pont entre AWS et l'interface utilisateur.
4.  **Frontend Dashboard (`frontend/`)** : Interface web moderne et interactive pour le suivi en temps réel.

### 🔄 Flux de données
`Simulateur` ➔ `AWS IoT Core (MQTT)` ➔ `AWS DynamoDB` ➔ `AI Monitor` ➔ `Backend Flask` ➔ `Dashboard Web`

---

## 🛠️ Détails des Composants

### 1. Simulateur IoT (`sim.py`)
*   **Rôle** : Agit comme une passerelle ESP32.
*   **Fonctionnement** :
    *   Génère des mesures de tension (V), courant (A), puissance (W) et énergie cumulée (kWh).
    *   Publie les données toutes les 5 secondes sur le topic MQTT `smarthome/energy`.
    *   Écoute les commandes sur `smarthome/relay` pour allumer/éteindre les appareils.
    *   Sauvegarde également les données directement dans la table DynamoDB `smarthome_data`.

### 2. Monitoring & IA (`detect_anomalies.py`)
*   **Modèle** : Utilise **Isolation Forest** (Scikit-Learn), un algorithme non supervisé idéal pour détecter les valeurs aberrantes.
*   **Fonctionnement** :
    *   Scanne les dernières données dans DynamoDB toutes les 20 secondes.
    *   Évalue chaque mesure via le modèle IA (`model.pkl`).
    *   **Sécurité Hybrid** : Combine l'IA avec des seuils de sécurité hardcodés (`DEVICE_NORMAL_RANGES`) pour une fiabilité maximale.
    *   **Action Corrective** : Si une anomalie est détectée, il envoie immédiatement une commande `OFF` via MQTT et enregistre une alerte dans `SmartHomeAlerts`.

### 3. Backend Flask (`app.py`)
*   **Rôle** : API REST pour l'interface utilisateur.
*   **Endpoints principaux** :
    *   `/api/data/latest` : Dernières mesures de chaque appareil.
    *   `/api/data/history` : Historique de consommation (1h, 6h, 24h).
    *   `/api/alerts` : Liste des dernières anomalies détectées.
    *   `/api/relay` : Envoi de commandes ON/OFF aux appareils.

### 4. Dashboard UI (`index.html`)
*   **Design** : Interface premium en mode sombre (Dark Mode) avec Glassmorphism.
*   **Fonctionnalités** :
    *   Cartes d'état en temps réel pour chaque appareil.
    *   Graphiques dynamiques de consommation via **Chart.js**.
    *   Système de notifications (Toast) pour les actions de l'utilisateur.
    *   Indicateur de connectivité en direct.

---

## ⚙️ Configuration AWS

Le projet repose sur les services AWS suivants :
*   **IoT Core** : Gestion des messages MQTT et certificats de sécurité.
*   **DynamoDB** :
    *   `smarthome_data` : Partition Key `device_id` (String), Sort Key `timestamp` (String).
    *   `SmartHomeAlerts` : Stockage des incidents détectés par l'IA.
*   **IAM** : Politiques d'accès pour permettre la lecture/écriture sur les ressources.

---

## 📦 Installation et Lancement

### Prérequis
*   Python 3.9+
*   Compte AWS avec les tables DynamoDB créées.
*   Certificats AWS IoT placés dans un dossier `certs/`.

### Étapes
1.  **Installer les dépendances** :
    ```bash
    pip install -r requirement.txt
    pip install scikit-learn joblib pandas numpy
    ```

2.  **Lancer le simulateur** :
    ```bash
    python simulator/src/sim.py
    ```

3.  **Lancer le moniteur d'IA** :
    ```bash
    python simulator/src/detect_anomalies.py
    ```

4.  **Lancer le backend** :
    ```bash
    python backend/app.py
    ```

5.  **Ouvrir le dashboard** :
    Ouvrez simplement `frontend/index.html` dans votre navigateur.

---

## 🧪 Scénarios de Test
*   **Fonctionnement Normal** : Les appareils consomment selon leurs profils de base.
*   **Détection d'Anomalie** : Si un appareil dépasse son seuil critique (ex: Climatisation > 1800W), l'IA détecte l'anomalie, coupe l'appareil, et une alerte rouge apparaît instantanément sur le dashboard.
*   **Contrôle Manuel** : Vous pouvez forcer l'extinction ou l'allumage d'un appareil directement depuis les cartes du dashboard.

---
*Projet développé par l'équipe IoT Intelligence.*
