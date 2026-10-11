# Lab 05 — Structured Streaming : les modifications de Wikipédia en direct

**Cours :** ECE Big Data Processing (Adaltas, automne 2026), séance 05 *Structured Streaming*
**Énoncé :** [`05.structured-streaming/lab_docker_wikistreams.md`](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/05.structured-streaming/lab_docker_wikistreams.md) (copie dans `lab_structured_streaming_original.md`)
**Fait le :** 8 octobre 2026

Le principe : un producteur Python lit le flux public des modifications de Wikimedia, garde celles qui
l'intéressent et les envoie dans le topic Kafka `wikistreams`. Dans le conteneur JupyterLab, Spark
Structured Streaming lit ce topic en continu et agrège les modifications par fenêtres de temps.

---

## Contenu du dossier

| Fichier | Rôle |
|---|---|
| `notebooks/wikistream_pyspark.ipynb` | Le notebook du cours (la démo) suivi des explorations des consignes 10 à 12, **exécuté** |
| `wikistream_producer.py` | Le producteur du cours, avec un bloc de requête modifié (consigne 11 : les filtres) |
| `admin.py` | Celui du lab 04 avec le topic `wikistreams`, comme l'énoncé le propose |
| `compose.yaml` | Celui du cours, inchangé : Kafka + JupyterLab/Spark sur un réseau commun |
| `data/`, `jobs/` | Dossiers demandés par l'énoncé, montés dans le conteneur (vides) |
| `demo/` | Les fichiers du cours, tels quels |
| `lab_structured_streaming_original.md` | L'énoncé |

Aucun commentaire n'a été ajouté dans le code : ceux qui restent sont ceux du cours, mot pour mot.

## Lancer (Git Bash)

Python : le venv du lab 04, `C:\Users\marti\projects\courses\dataprocessing\venv-kafka`, auquel
`pywikibot[eventstreams]` 11.8.0 a été ajouté.

```bash
cd ~/OneDrive/Documents/IA/Inge5/lab05
docker compose up -d
PY=/c/Users/marti/projects/courses/dataprocessing/venv-kafka/Scripts/python
$PY admin.py
$PY wikistream_producer.py
```

Le producteur tourne 10 minutes (plus 1 minute d'attente avant de fermer). Pendant ce temps :

1. `docker logs pyspark_notebook 2>&1 | grep "token="` donne le lien JupyterLab ;
2. ouvrir `work/wikistream_pyspark.ipynb` et lancer *Run All* ;
3. `docker logs -f pyspark_notebook` montre la sortie de la requête « console » de la démo (étape 9).

Tout arrêter : `docker compose down`.

Exécution sans navigateur, telle qu'utilisée pour le rendu (la redirection vers `/proc/1/fd/1` envoie
la sortie dans `docker logs`, comme quand le notebook tourne dans JupyterLab) :

```bash
MSYS_NO_PATHCONV=1 docker exec -w /home/jovyan/work pyspark_notebook bash -c 'export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"; jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=900 wikistream_pyspark.ipynb > /proc/1/fd/1 2>&1'
```

## Ce qu'il a fallu corriger dans les fichiers du cours

1. **`admin.py` ne créait pas le topic.** Il lance la création, liste les topics sans attendre et se
   termine : la demande ne part pas toujours. Au lab 04 le topic avait été créé quand même ; ici,
   deux secondes après, la liste des topics était vide. Le `admin.py` du lab 04 attend la fin de la
   création (`.result()`).
2. **Le producteur du cours reste muet.** Il demande les événements depuis le 9 février 2026
   (`since='20260209'`), alors que Wikimedia ne garde que 8 jours d'historique : le flux repart donc
   de 8 jours en arrière, sur une portion qui ne contient que des journaux et des événements
   `revision-create`. Mesuré : 6 302 événements reçus en 60 s, aucun `edit` de fr.wikipedia. Le
   producteur attend son premier événement sans rien afficher. Remplacé par un `since` relatif
   (les 2 dernières heures).
3. **Le connecteur Kafka 4.1.0 ne fonctionne pas avec Spark 4.2.0**, la version de l'image actuelle :
   la requête s'arrête sur `NoSuchMethodError: HDFSMetadataLog.<init>`, une méthode interne de
   Spark qui a changé entre les deux versions. Le notebook demande maintenant le connecteur de la
   même version que PySpark (`pyspark.__version__`), ce qui ne cassera plus à la prochaine mise à
   jour de l'image.

## Consigne 11 — filtrer côté producteur

Le bloc de requête du producteur suit maintenant :

- **4 Wikipédia** : fr, en, de et es (`server_name`) ;
- **modifications et créations de pages** (`type` dans `edit`, `new`) ;
- **les articles seulement** (`namespace=0` : ni pages de discussion, ni pages utilisateur) ;
- le flux `recentchange` seul : les événements `revision-create` n'ont ni `server_name` ni `type`,
  le filtre les écartait de toute façon.

Avec `register_filter`, chaque critère doit être vrai, et une liste veut dire « l'une de ces
valeurs ». D'autres filtres possibles sur le même modèle : `register_filter(title='Paris')` pour
suivre une page précise, `register_filter(ftype='none', bot=True)` pour exclure les bots.

Le filtrage se fait **chez le client** : Wikimedia envoie tous les événements de tous les wikis, et
pywikibot écarte ceux qui ne correspondent pas avant l'envoi à Kafka. Kafka ne reçoit donc que les
événements utiles.

## Consignes 10 et 12 — les explorations

Toutes les requêtes partent du même DataFrame de flux, enrichi de deux colonnes : `event_time`
(le `timestamp` Unix converti en vrai horodatage) et `size_delta` (taille de la page après moins
avant, en octets).

| Exploration | Fenêtres | Ce qui est calculé |
|---|---|---|
| Fenêtres **fixes** (*tumbling*) | 10 min, sans chevauchement | Modifications par wiki, dont celles des bots, et octets nets ajoutés |
| Fenêtres **glissantes** (*sliding*) | 30 min, avancent de 10 min | Modifications, contributeurs et pages distincts, part des bots |
| Taille des modifications | aucune | Par wiki : octets ajoutés et retirés, médiane, plus gros ajout et plus grosse suppression, créations de pages |
| Pages les plus modifiées | aucune | Top 10 des pages, puis suivi en continu des 3 premières (filtre côté consommateur) |

La requête « console » de la démo écrit dans `docker logs`. Pour que les résultats apparaissent dans
le notebook, les explorations écrivent dans une table en mémoire (`format("memory")`), et
`processAllAvailable()` attend que tout ce qui est déjà arrivé dans Kafka soit traité avant de relire
la table.

Une cellule vérifie la différence entre les deux types de fenêtres : dans les fenêtres glissantes de
30 minutes qui avancent de 10, chaque modification est comptée **3 fois**, contre une seule fois dans
les fenêtres fixes.

## Résultats (exécution du 8 octobre 2026)

Le producteur a tourné 10 minutes à partir de 16:10. Wikimedia relisant l'historique à environ 6
événements par seconde, il a couvert les modifications de **12:10 à 12:51 UTC** : **3 602
événements** dans Kafka, tous retrouvés par Spark (3 602 dans les fenêtres fixes).

**Démo du cours** (requête console, `docker logs`) : 3 381 modifications humaines et 221 de bots
dans la fenêtre d'une heure 12:00–13:00. Lors d'un essai pendant que le producteur tournait, la même
requête a produit 85 lots successifs (`Batch: 0` à `Batch: 84`) au fil de l'arrivée des données.

**Fenêtres fixes de 10 minutes** — modifications par wiki :

| Fenêtre (UTC) | de | en | es | fr | Total |
|---|---|---|---|---|---|
| 12:10–12:20 | 116 | 502 | 39 | 116 | 773 |
| 12:20–12:30 | 110 | 518 | 64 | 122 | 814 |
| 12:30–12:40 | 99 | 640 | 60 | 121 | 920 |
| 12:40–12:50 | 118 | 663 | 64 | 133 | 978 |
| 12:50–13:00 | 12 | 82 | 5 | 18 | 117 (fenêtre entamée) |

**Fenêtres glissantes de 30 minutes** (pas de 10 minutes) : 7 fenêtres, de 11:50–12:20 à
12:50–13:20 ; la plus chargée, 12:20–12:50, compte 2 712 modifications par environ 1 057
contributeurs sur 2 215 pages, dont 6,5 % par des bots. Les fenêtres des bords ne sont que
partiellement remplies. Total des fenêtres glissantes : 10 806 = **3,00 × 3 602**.

**Taille des modifications** :

| Wiki | Modifs | Créations | Octets ajoutés | Octets retirés | Médiane | Plus gros ajout / retrait |
|---|---|---|---|---|---|---|
| en | 2 405 | 49 | 739 954 | 207 102 | +11 | +101 552 / −99 166 |
| fr | 510 | 10 | 134 320 | 105 917 | +23 | +16 363 / −96 836 |
| de | 455 | 14 | 112 707 | 29 951 | +14 | +17 647 / −8 252 |
| es | 232 | 4 | 61 294 | 29 021 | +12 | +11 126 / −15 282 |

La médiane reste à quelques dizaines d'octets : la plupart des modifications sont des retouches,
et les moyennes seraient tirées par quelques ajouts ou suppressions de plusieurs dizaines de Ko.

**Pages les plus modifiées** : *Linguistic history of India* (17 modifications), *Shinji Takashima
(basketball)* (16) et *Atsuya Ogawa* (15), toutes sur en.wikipedia et chacune par un seul
contributeur — des séries de retouches successives plutôt que des débats. Les trois sont ensuite
suivies par une requête filtrée qui ne garde que leurs modifications.

## Pièges rencontrés

- **Disque plein en cours de TP** (5,4 Go libres) : le disque virtuel de Docker occupait 110,6 Go pour
  38 Go de données. Compacté (`fstrim`, `docker desktop stop`, `diskpart`) : 42,4 Go, 81 Go libres.
- **Wikimedia ralentit la relecture de l'historique** après plusieurs demandes rapprochées : 164
  événements filtrés par seconde au premier essai, environ 5 par seconde ensuite.
- **pywikibot écrit un fichier `throttle.ctrl`** dans le dossier d'où on le lance : fichier de
  travail, à ne pas rendre.
- `MSYS_NO_PATHCONV=1` reste nécessaire pour `docker exec -w /home/jovyan/work` sous Git Bash.
