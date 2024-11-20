Configuration de Jupyter et de la base de donnée

---

### **1. Configuration de la SparkSession dans Jupyter**
C'est la première étape pour connecter Jupyter à votre cluster Spark. Cette configuration permet à Spark de gérer le traitement distribué.

#### Code :
```python
from pyspark.sql import SparkSession

# Configuration de la SparkSession pour se connecter au cluster
spark = SparkSession.builder \
    .master("spark://pyspark-master:7077") \
    .appName("MySparkApp") \
    .config("spark.executor.memory", "1g") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()
```

---

### **2. Acquisition des données**

Exécutez la commande suivante pour télécharger et lancer la base de données PostgreSQL Dellstore :

```bash
docker run -itd --net=pyspark-cluster -p 5432:5432 --name pg-ds-dellstore aa8y/postgres-dataset:dellstore
```

**NB :**si le port est déjà utilisé, cf : [PB_port_utilise.md](./PB_port_utilise.md)

#### A. Importer les données dans PostgreSQL** (via `psql`) :

    Pour accéder à la base de données, exécutez :

    ```bash
    docker exec -it pg-ds-dellstore psql -d dellstore
    ```

    ```sql
    CREATE TABLE customer_orders AS
    SELECT 
        c.customerid,
        c.firstname,
        c.lastname,
        c.username,
        c.city,
        c.state,
        c.zip,
        c.country,
        c.age,
        c.gender,
        c.income,
        o.orderid,
        o.orderdate,
        o.netamount,
        o.tax,
        o.totalamount
    FROM 
        customers c
    JOIN 
        orders o ON c.customerid = o.customerid;
    ```
    
    ```sql
    -- Vérifier le contenu
    SELECT * FROM customer_orders LIMIT 5;
    ```
    
---

 
#### B. Lire les données depuis PostgreSQL dans PySpark :
Connectez Spark à PostgreSQL pour lire les données dans un DataFrame Spark.

#### Code :
```python
# Charger la table PostgreSQL dans PySpark
df_spark = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://pg-ds-dellstore:5432/dellstore") \
    .option("dbtable", "public.customer_orders") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()

df_spark.show()  # Vérifier que les données sont bien chargées
```
---

### **3. Effectuer le traitement dans Spark**
Effectuez votre traitement (par exemple : compter le nombre total de lignes dans la table).

#### Code :
```python
#df_spark.show()
# Montrer un échantillon des données
df_spark.show(5)

# Exemple 1 : Filtrer les clients ayant un revenu supérieur à 50 000
df_high_income = df_spark.filter(df_spark["income"] > 50000)

# Montrer les résultats filtrés
df_high_income.show()

# Exemple 2 : Calculer la moyenne des montants totaux par pays
df_avg_total_by_country = df_spark.groupBy("country").avg("totalamount")

# Renommer les colonnes pour plus de clarté
df_avg_total_by_country = df_avg_total_by_country.withColumnRenamed("avg(totalamount)", "avg_totalamount")

# Montrer les moyennes par pays
df_avg_total_by_country.show()
```

---

### **4. Stocker les résultats dans PostgreSQL**
#### A. Sauvegarder le résultat dans une table PostgreSQL :
Utilisez Spark pour écrire le DataFrame dans PostgreSQL.

#### Code :
```python
# Créer une nouvelle table avec les clients ayant un revenu > 50 000
df_high_income.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://pg-ds-dellstore:5432/dellstore") \
    .option("dbtable", "public.high_income_customers") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \  # Remplace la table si elle existe déjà
    .save()
```

```python
# Créer une nouvelle table avec la moyenne des montants totaux par pays
df_avg_total_by_country.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://pg-ds-dellstore:5432/dellstore") \
    .option("dbtable", "public.avg_total_by_country") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \  # Remplace la table si elle existe déjà
    .save()
```

---

### **5. Vérification**
Accédez à PostgreSQL pour vérifier les données enregistrées.

#### Commande depuis le conteneur :
```bash
docker exec -it pg-ds-dellstore psql -d dellstore
```

```sql
SELECT * FROM high_income_customers LIMIT 5;
SELECT * FROM avg_total_by_country LIMIT 5;
```

### **6. Connexion de Power BI à PostgreSQL**

1. Lancez Power BI Desktop
2. Cliquez sur "Obtenir des données" et sélectionnez "Base de données PostgreSQL"
3. Configurez la connexion :
   - Serveur : localhost
   - Base de données : dellstore
   - Mode de connectivité : Importer
4. Sélectionnez les tables `high_income_customers` `avg_total_by_country`