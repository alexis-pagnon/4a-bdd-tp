# Etape 1 : importer les modules

import json
import matplotlib;
import numpy;
import pymongo;
import pandas;

# Étape 2: Connexion à la base de données MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')


db = client['superheroes_db'] # Nom de la base de données
# collection = db['heroes'] # Nom de la collection

# Pour les étapes 17 et 18
# collection = db['view_heroes_intelligence']
collection = db['view_heroes_force']

# Étape 3: Importer les données des super-héros dans MongoDB
"""
# 1. Préparer les données JSON
with open('SuperHerosComplet.json', 'r') as file:
 data = json.load(file)
# 2. Insérer les données dans MongoDB
result = collection.insert_many(data)
print('Inserted {} documents'.format(len(result.inserted_ids)))
# 3. Vérifier les insertions
print(collection.find_one())
"""
# Étape 4 : créer des indexes
"""
index_text = pymongo.IndexModel(
    [("name", pymongo.TEXT), ("biography.publisher", pymongo.TEXT)],
    name="index_text",
)

index_intelligence = pymongo.IndexModel(
    [("powerstats.intelligence", pymongo.DESCENDING)],
    name="index_intelligence"
)

collection.create_indexes([index_text, index_intelligence])
"""

# Etape 5 : Extraire les données de MongoDB vers un DataFrame pandas

superheros = collection.find({},{}).to_list()

dataframe_heros = pandas.DataFrame(superheros)

# Etape 6 :

# Aplatir les colonnes
champs_powerstats = [
    "intelligence", "strength", "speed",
    "durability", "power", "combat"
]
champs_appearance = [
    "gender", "race", "height",
    "weight", "eyeColor", "hairColor"
]
champs_biography = [
    "fullName", "alterEgos", "aliases",
    "placeOfBirth", "firstAppearance",
    "publisher", "alignment"
]
champs_work = [
    "occupation", "base"
]
champs_connections = [
    "groupAffiliation", "relatives"
]
champs_images = [
    "xs", "sm", "md", "lg"
]


for i in champs_powerstats:
    dataframe_heros["powerstats."+i] = dataframe_heros["powerstats"].apply(lambda x: x.get(i, None))
for i in champs_appearance:
    dataframe_heros["appearance."+i] = dataframe_heros["appearance"].apply(lambda x: x.get(i, None))
for i in champs_biography:
    dataframe_heros["biography."+i] = dataframe_heros["biography"].apply(lambda x: x.get(i, None))
for i in champs_work:
    dataframe_heros["work."+i] = dataframe_heros["work"].apply(lambda x: x.get(i, None))
for i in champs_connections:
    dataframe_heros["connections."+i] = dataframe_heros["connections"].apply(lambda x: x.get(i, None))
for i in champs_images:
    dataframe_heros["images."+i] = dataframe_heros["images"].apply(lambda x: x.get(i, None))

# Supprime les colonnes qui ont été applaties
dataframe_heros = dataframe_heros.drop(['powerstats', 'appearance', 'biography', 'work', 'connections', 'images'], axis=1)

# Normalisation des formats

def height_normalisation(listeTaille):
    # On ne veut garder que les tailles à partir du système métrique et on retourne en cm
    if(len(listeTaille)==2):
        height:str = listeTaille[1]
        if " cm" in height:
            height = height.replace(" cm","")
            return float(height)
        elif " meters" in height:
            height = height.replace(" meters","")
            return float(height)*100
        else: return None
    else: return None


def weight_normalisation(listePoids):
    # On ne veut garder que les tailles à partir du système des grammes et on retourne en kg
    if(len(listePoids)==2):
        weight:str = listePoids[1]
        if " kg" in weight:
            weight = weight.replace(" kg","")
            return float(weight)
        elif " g" in weight:
            weight = weight.replace(" g","")
            return float(weight)/1000
        else: return None
    else: return None

# On ne garde que le système métrique
dataframe_heros["appearance.height"] = dataframe_heros["appearance.height"].apply(lambda x: height_normalisation(x))
# On ne garde que le poids en kg
dataframe_heros["appearance.weight"] = dataframe_heros["appearance.weight"].apply(lambda x: weight_normalisation(x))


# Traitement des données textuelles
dataframe_heros.replace("-", None, inplace=True)

# Suppression des colonnes inutiles
inutiles = input("Saisissez toutes les colonnes inutiles : ").split(" ")
for i in inutiles :
    if i in dataframe_heros.columns:
        dataframe_heros = dataframe_heros.drop([i], axis=1)

# Supprime la ligne si les valeurs manquantes
dataframe_heros.dropna(inplace=True)



# Menu
while True:
    menu = input("Choisir une action :"+
    "\n\t1 : Calculer une moyenne"+
    "\n\t2 : Calculer une médiane"+
    "\n\t3 : Calculer une variance"+
    "\n\t4 : Visualiser une statistique"+
    "\n\t5 : Quitter\n")
    if menu == "1":
        statistique = input("Choisir une statistique :"+
                            "\n\t1 : Vitesse"+
                            "\n\t2 : Intelligence"+
                            "\n\t3 : Force"+
                            "\n\t4 : Durabilité"+
                            "\n\t5 : Puissance"+
                            "\n\t6 : Combat\n")
        if statistique == "1":
            average = numpy.average(dataframe_heros["powerstats.speed"].to_numpy())
            print(f"La moyenne de la vitesse des super-héros est : {average}")
        elif statistique == "2":
            average = numpy.average(dataframe_heros["powerstats.intelligence"].to_numpy())
            print(f"La moyenne de l'intelligence des super-héros est : {average}")
        elif statistique == "3":
            average = numpy.average(dataframe_heros["powerstats.strength"].to_numpy())
            print(f"La moyenne de la force des super-héros est : {average}")
        elif statistique == "4":
            average = numpy.average(dataframe_heros["powerstats.durability"].to_numpy())
            print(f"La moyenne de la durabilité des super-héros est : {average}")
        elif statistique == "5":
            average = numpy.average(dataframe_heros["powerstats.power"].to_numpy())
            print(f"La moyenne de la puissance des super-héros est : {average}")
        elif statistique == "6":
            average = numpy.average(dataframe_heros["powerstats.combat"].to_numpy())
            print(f"La moyenne du combat des super-héros est : {average}")
        else: 
            print("Mauvaise saisie !")
        
    elif menu == "2":
        statistique = input("Choisir une statistique :"+
                            "\n\t1 : Vitesse"+
                            "\n\t2 : Intelligence"+
                            "\n\t3 : Force"+
                            "\n\t4 : Durabilité"+
                            "\n\t5 : Puissance"+
                            "\n\t6 : Combat\n")
        if statistique == "1":
            average = numpy.median(dataframe_heros["powerstats.speed"].to_numpy())
            print(f"La médiane de la vitesse des super-héros est : {average}")
        elif statistique == "2":
            average = numpy.median(dataframe_heros["powerstats.intelligence"].to_numpy())
            print(f"La médiane de l'intelligence des super-héros est : {average}")
        elif statistique == "3":
            average = numpy.median(dataframe_heros["powerstats.strength"].to_numpy())
            print(f"La momédianeyenne de la force des super-héros est : {average}")
        elif statistique == "4":
            average = numpy.median(dataframe_heros["powerstats.durability"].to_numpy())
            print(f"La médiane de la durabilité des super-héros est : {average}")
        elif statistique == "5":
            average = numpy.median(dataframe_heros["powerstats.power"].to_numpy())
            print(f"La médiane de la puissance des super-héros est : {average}")
        elif statistique == "6":
            average = numpy.median(dataframe_heros["powerstats.combat"].to_numpy())
            print(f"La médiane du combat des super-héros est : {average}")
        else: 
            print("Mauvaise saisie !")
    
    elif menu == "3":
        statistique = input("Choisir une statistique :"+
                            "\n\t1 : Vitesse"+
                            "\n\t2 : Intelligence"+
                            "\n\t3 : Force"+
                            "\n\t4 : Durabilité"+
                            "\n\t5 : Puissance"+
                            "\n\t6 : Combat\n")
        if statistique == "1":
            average = numpy.var(dataframe_heros["powerstats.speed"].to_numpy())
            print(f"La variance de la vitesse des super-héros est : {average}")
        elif statistique == "2":
            average = numpy.var(dataframe_heros["powerstats.intelligence"].to_numpy())
            print(f"La variance de l'intelligence des super-héros est : {average}")
        elif statistique == "3":
            average = numpy.var(dataframe_heros["powerstats.strength"].to_numpy())
            print(f"La variance de la force des super-héros est : {average}")
        elif statistique == "4":
            average = numpy.var(dataframe_heros["powerstats.durability"].to_numpy())
            print(f"La variance de la durabilité des super-héros est : {average}")
        elif statistique == "5":
            average = numpy.var(dataframe_heros["powerstats.power"].to_numpy())
            print(f"La variance de la puissance des super-héros est : {average}")
        elif statistique == "6":
            average = numpy.var(dataframe_heros["powerstats.combat"].to_numpy())
            print(f"La variance du combat des super-héros est : {average}")
        else: 
            print("Mauvaise saisie !")
    
    elif menu == "4":
        visu = input("Choisir une option :"+
                     "\n\t1 : Distribution de la force et de l'intelligence"+
                     "\n\t2 : Nombre de super héros par éditeur\n")
        if visu == "1":
            dataframe_heros["powerstats.strength"].hist(bins=20, alpha=0.7, label='Force')
            dataframe_heros["powerstats.intelligence"].hist(bins=20, alpha=0.7, label='Intelligence')
            matplotlib.pyplot.legend(loc='upper right')
            matplotlib.pyplot.xlabel('Valeur')
            matplotlib.pyplot.ylabel('Fréquence')
            matplotlib.pyplot.title('Distribution de la force et de l\'intelligence des super-héros')
            matplotlib.pyplot.show()
        
        elif visu == "2":
            publisher_counts = dataframe_heros["biography.publisher"].value_counts()
            publisher_counts.plot(kind='bar')
            matplotlib.pyplot.xlabel('Éditeur')
            matplotlib.pyplot.ylabel('Nombre de super-héros')
            matplotlib.pyplot.title('Nombre de super-héros par éditeur')
            matplotlib.pyplot.show()
        
        else:
            print("Mauvaise saisie !")


    elif menu == "5":
        break
    else:
        print("Option non valide. Veuillez réessayer.")
    


moyenne_intelligence = numpy.average(dataframe_heros["powerstats.intelligence"].to_numpy())

# Vue MongoDB avec les héros ayant une intelligence supérieure à moyenne_intelligence
"""
db.create_collection(
    'view_heroes_intelligence',
    viewOn='heroes',
    pipeline=[
    {"$match": {
            "powerstats.intelligence": {"$gt": moyenne_intelligence}
        }}])
"""



# Vue MongoDB qui organise les super-héros en fonction de leur force, du plus fort au moins fort
"""
db.create_collection(
    'view_heroes_force',
    viewOn='heroes',
    pipeline=[
    {"$sort": {"powerstats.strength": -1}}
    ])
"""

# Etape 18
# Visualiser l'évolution des valeurs de la force des super-héros

dataframe_heros["powerstats.strength"].plot(kind='line')
matplotlib.pyplot.ylabel('Force')
matplotlib.pyplot.title('Évolution des valeurs de la force des super-héros')
matplotlib.pyplot.show()
