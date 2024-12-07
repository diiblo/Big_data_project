# Traitement

---

#### **1. Configuration dans Jupyter et connexion à la la base de donnée**

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

### **2. Effectuer le traitement dans Spark**
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

### **3. Stocker les résultats dans PostgreSQL**

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

### **4. Vérification**
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