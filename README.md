# Projet Big Data : Topologie Maître-Esclaves avec Jupyter Notebook, PySpark, Spark et PostgreSQL

Ce projet met en œuvre une architecture Big Data basée sur une topologie maître-esclaves (1 maître, 2 esclaves). Le traitement des données est effectué via Jupyter Notebook, PySpark et Spark, tandis que PostgreSQL est utilisé comme base de données pour le stockage.

## Objectifs du projet

1. Configurer un cluster Spark avec une topologie maître-esclaves.
2. Utiliser une image Docker personnalisée contenant Jupyter Notebook, PySpark, Spark et un connecteur Java pour PostgreSQL.
3. Connecter PostgreSQL au niveau de Jupyter Notebook du cluster maître .
4. Réaliser un traitement de données avec PySpark.
5. Stocker les résultats dans deux nouvelles tables PostgreSQL.

---

## Structure du projet

Le projet est organisé en plusieurs étapes décrites dans les fichiers suivants :

### 1. [Conteneur_perso.md](./1-Conteneur_perso.md)

- **Objectif** : Télécharger et personnaliser une image Docker.
- **Contenu** :
  - Explications pour télécharger une image de base contenant Jupyter Notebook, PySpark et Spark.
  - Instructions pour personnaliser l'image en ajoutant un connecteur Java pour PostgreSQL et d'autres outils utiles.

### 2. [Config_cluster.md](./2-Config_cluster.md)

- **Objectif** : Configurer un cluster Spark avec une topologie maître-esclaves.
- **Contenu** :
  - Instructions pour créer et lancer des conteneurs Docker à partir de l'image personnalisée.
  - Configuration des conteneurs pour établir une topologie de cluster avec un maître et deux esclaves.

### 3. [Projet_pratique.md](./3-Projet_pratique.md)

- **Objectif** : Mettre en pratique le traitement des données.
- **Contenu** :
  - Exemple de script PySpark exécuté dans Jupyter Notebook pour traiter des données.
  - Explications sur l'utilisation de PostgreSQL pour stocker les résultats dans deux nouvelles tables.

---

## Prérequis

1. **Docker** : Assurez-vous que Docker est installé et opérationnel.
2. **PostgreSQL** : Une instance PostgreSQL doit être configurée pour recevoir les résultats.
3. **Images Docker nécessaires** : Une image de base contenant Jupyter Notebook, PySpark et Spark est requise (voir [Conteneur_perso.md](./1-Conteneur_perso.md)).

---

## Instructions de déploiement

### Étape 1 : Créer l'image Docker personnalisée

Suivez les instructions dans [Conteneur_perso.md](./1-Conteneur_perso.md) pour créer une image Docker personnalisée.

### Étape 2 : Configurer le cluster

Configurez et démarrez votre cluster Spark avec les conteneurs Docker personnalisés en suivant les instructions dans [Config_cluster.md](./2-Config_cluster.md).

### Étape 3 : Effectuer le traitement des données

Utilisez Jupyter Notebook pour exécuter le script PySpark décrit dans [Projet_pratique.md](./3-Projet_pratique.md). Les résultats seront stockés dans PostgreSQL.

---

## Technologies utilisées

- **Jupyter Notebook** : Interface interactive pour écrire et exécuter des scripts.
- **PySpark et Spark** : Pour le traitement distribué des données.
- **PostgreSQL** : Base de données relationnelle pour le stockage.
- **Docker** : Conteneurisation des outils pour une gestion simplifiée.

---

## Auteur

**Moctarr Basiru King Rahman**  
Étudiant en Mastère Data Engineering à l'ECE Paris  
[LinkedIn](https://www.linkedin.com/in/moctarr-basiru-king-rahman-7337a5214) | [Email](mailto:moctarrbasiru.kingrahman@edu.ece.fr)

---

## Licence

Ce projet est distribué sous la licence [MIT](./LICENSE). Vous êtes libre de l'utiliser, de le modifier et de le distribuer.

---

## Remarque

Si vous rencontrez des problèmes ou avez des questions, n'hésitez pas à ouvrir une *issue* dans ce dépôt ou à me contacter directement.
