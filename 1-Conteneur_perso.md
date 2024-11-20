### Étape 1 : Télécharger l'image Jupyter avec Spark
Les images **Jupyter Docker Stacks** incluent différentes configurations. L'image `jupyter/pyspark-notebook` est celle qui contient Jupyter et Spark.

1. **Télécharger l’image Docker :**
   Ouvrez un terminal et exécutez la commande suivante pour télécharger l’image `jupyter/pyspark-notebook` :
   ```bash
   docker pull jupyter/pyspark-notebook
   ```

2. **Vérifiez que l’image est bien téléchargée :**
   Vous pouvez vérifier avec :
   ```bash
   docker images
   ```
   Vous devriez voir `jupyter/pyspark-notebook` dans la liste.

### Étape 2 : Ajouter PostgreSQL à l'image
Puisque `jupyter/pyspark-notebook` ne contient pas PostgreSQL, nous allons créer un Dockerfile personnalisé pour ajouter PostgreSQL à cette image.

1. **Créer un Dockerfile personnalisé :**
   Dans le même répertoire, créez un fichier appelé `Dockerfile` (sans extension) et ajoutez les lignes suivantes :
   ```dockerfile
   # Utiliser l'image de base Jupyter avec Spark
   FROM jupyter/pyspark-notebook

   USER root

   # Installer les outils
   RUN apt-get update && apt-get install -y openjdk-11-jdk net-tools curl

   # Créer le répertoire de données PostgreSQL
   # RUN mkdir -p /var/lib/postgresql/data && chown -R postgres:postgres /var/lib/postgresql

   # Télécharger le fichier JDBC PostgreSQL
   RUN curl -o /usr/local/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

   # Configurer PostgreSQL pour qu'il démarre sans mot de passe
   #USER postgres
   #RUN /usr/lib/postgresql/14/bin/initdb -D /var/lib/postgresql/data

   # Changer l’utilisateur par défaut pour Jupyter Notebook et démarrer PostgreSQL
   USER $NB_USER
   CMD ["start-notebook.sh"]
   ```

2. **Construire votre nouvelle image Docker :**
   Dans le terminal, exécutez cette commande pour construire l’image Docker personnalisée (assurez-vous d’être dans le même dossier que le `Dockerfile`) :
   ```bash
   docker build -t my-jupyter-pyspark .
   ```
   Cette commande crée une nouvelle image Docker appelée `my-jupyter-pyspark`

### Étape 3 : Exécuter votre nouveau conteneur (optionel juste pour test)
Vous pouvez maintenant exécuter votre propre conteneur Docker, qui inclut Jupyter Notebook et Spark
1. **Démarrer le conteneur :**
   Lancez votre conteneur avec cette commande :
   ```bash
   docker run -p 8888:8888 -p 5432:5432 my-jupyter-pyspark-postgres
   ```
   - **Port 8888** est pour accéder à Jupyter Notebook dans le navigateur.
   - **Port 5432** est pour accéder à PostgreSQL.
   
   **NB :** il est préférable de l'éxécuter avec un paramètre --name
   ```bash
   docker run --name nom-au-choix -p 8888:8888 -p 5432:5432 my-jupyter-pyspark-postgres
   ```
   Ensuite, au lieu de recréer un nouveau conteneur, redémarrez-le simplement avec :
   ```bash
   docker start -ai nom-au-choix
   ```

2. **Ajouter un port pour Spark (si nécessaire)**

   Si vous voulez utiliser également **Spark** dans votre configuration et que vous souhaitez activer son interface web ou ses services, il peut être utile d'exposer certains ports supplémentaires, par exemple :

   - **`-p 4040:4040`**  
  Ce port est utilisé par **Spark** pour son **UI (User Interface)** des tâches. Il permet de surveiller les jobs Spark en cours d'exécution, les stages, les tasks, etc.

      ```bash
      docker run --name nom-au-choix -p 8888:8888 -p 5432:5432 -p 4040:4040 my-jupyter-pyspark-postgres
      ```
  

3. **Accéder à Jupyter Notebook :**
   - Dans le terminal, vous verrez un lien ressemblant à `http://127.0.0.1:8888/?token=...`
   - Ouvrez ce lien dans votre navigateur pour accéder à Jupyter Notebook.

