# Configuration  de la base de donnée...

---

Exécutez la commande suivante pour télécharger et lancer la base de données PostgreSQL dellstore :

```bash
docker run -itd --net=pyspark-cluster -p 5432:5432 --name pg-ds-dellstore aa8y/postgres-dataset:dellstore
```

**NB :** si le port est déjà utilisé, cf : [PB_port_utilise.md](./PB_port_utilise.md)

#### Importer les données dans PostgreSQL (via `psql`) :

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