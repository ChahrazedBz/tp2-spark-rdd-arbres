from pyspark import SparkContext, SparkConf

# ----------------------------
# 1. Spark configuration
# ----------------------------
conf = SparkConf().setAppName("TP2-ArbresRDD")
sc = SparkContext(conf=conf)

# ----------------------------
# 2. Load the CSV file from HDFS
# ----------------------------
input_path = "hdfs://hadoop-master:9000/user/root/arbres/arbres.csv"
raw_rdd = sc.textFile(input_path)

# ----------------------------
# 3. Separate header from data
# ----------------------------
header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda row: row != header)

DELIMITER = ";"   # change to "," if your file uses commas

columns = header.split(DELIMITER)
columns_lower = [c.strip().lower() for c in columns]

# Find column indexes dynamically by name
def find_index(possible_names):
    for name in possible_names:
        for i, col in enumerate(columns_lower):
            if name in col:
                return i

    raise ValueError(
        "Column not found among {}. Header was: {}".format(
            possible_names, columns
        )
    )

idx_hauteur = find_index(["hauteur"])
idx_genre = find_index(["genre"])

print("DEBUG - Header columns:", columns)
print(
    "DEBUG - Using hauteur index = {}, genre index = {}".format(
        idx_hauteur, idx_genre
    )
)

# ----------------------------
# (1) Create RDD + count lines
# ----------------------------
nb_lignes = data_rdd.count()

print("===========================================")
print("(1) Nombre de lignes dans le RDD :", nb_lignes)
print("===========================================")

# Split each line into fields
fields_rdd = data_rdd.map(lambda row: row.split(DELIMITER))

# Helper to safely parse height as float
def safe_float(value):
    try:
        return float(value.replace(",", "."))
    except (ValueError, AttributeError):
        return None

# ----------------------------
# (2) Average height
# ----------------------------
hauteurs_rdd = fields_rdd.map(
    lambda f: safe_float(f[idx_hauteur])
).filter(
    lambda h: h is not None and h > 0
)

hauteur_moyenne = hauteurs_rdd.mean()

print("===========================================")
print("(2) Hauteur moyenne des arbres :", hauteur_moyenne)
print("===========================================")

# ----------------------------
# (3) Genre of the tallest tree
# ----------------------------
pairs_rdd = fields_rdd.map(
    lambda f: (safe_float(f[idx_hauteur]), f[idx_genre])
).filter(
    lambda pair: pair[0] is not None
)

sorted_rdd = pairs_rdd.sortByKey(ascending=False)
plus_grand = sorted_rdd.first()

print("===========================================")
print(
    "(3) Genre du plus grand arbre :",
    plus_grand[1],
    " (hauteur =",
    plus_grand[0],
    ")"
)
print("===========================================")

# ----------------------------
# (4) Number of trees per genre
# ----------------------------
genre_pairs_rdd = fields_rdd.map(
    lambda f: (f[idx_genre], 1)
)

genre_counts_rdd = genre_pairs_rdd.reduceByKey(
    lambda a, b: a + b
)

print("===========================================")
print("(4) Nombre d'arbres par genre :")

for genre, count in genre_counts_rdd.collect():
    print("   {} : {}".format(genre, count))

print("===========================================")

# ----------------------------
# Stop Spark
# ----------------------------
sc.stop()