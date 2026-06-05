# Lancer l'interface Airflow

## Prérequis
- Docker Desktop ouvert et en cours d'exécution

---

## 1. Démarrer les conteneurs

Dans un terminal, depuis le dossier `airflow/` :

```bash
docker compose up -d
```

Attendre ~30 secondes le temps que les services démarrent.

---

## 2. Ouvrir l'interface web

Aller sur : **http://localhost:8080**

| Champ | Valeur |
|-------|--------|
| Login | `admin` |
| Mot de passe | `admin` |

---

## 3. Lancer le DAG manuellement

1. Chercher **`fraud_detection_pipeline`** dans la liste
2. S'assurer que le toggle est **activé** (bleu)
3. Cliquer sur le bouton ▶ (Trigger DAG) à droite
4. Aller dans **Graph** ou **Grid** pour suivre l'avancement

Le pipeline complet prend ~25 secondes :
`fetch_data` → `validate_schema` → `infer_fraud` → `store_predictions`

---

## 4. Arrêter les conteneurs

```bash
docker compose down
```

---

## Notes

- Le DAG tourne automatiquement **toutes les heures** (`@hourly`) quand les conteneurs sont actifs
- Les prédictions sont stockées dans **NeonDB** → table `fraud_predictions`
- En cas de problème de port 8080, relancer en admin : `net stop winnat` puis `net start winnat`
