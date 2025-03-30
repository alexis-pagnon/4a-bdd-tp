import boto3
from botocore.client import Config
from botocore.exceptions import *
import time
import datetime

# Configure AWS credentials (dummy values in this case)
boto3.setup_default_session(
 aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
 aws_secret_access_key=' wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKE',
 region_name='us-west-2'
)


def create_dynamodb_resource():
    """Crée une ressource DynamoDB connectée à l'instance locale."""
    return boto3.resource('dynamodb', endpoint_url='http://localhost:8000')


def check_table_exists(dynamodb, table_name):
    # Utiliser le client DynamoDB pour lister les tables
    client = dynamodb.meta.client
    # Initialisation pour la pagination
    paginator = client.get_paginator('list_tables')
    page_iterator = paginator.paginate()
    # Parcourir toutes les tables pour voir si table_name existe
    for page in page_iterator:
        if table_name in page['TableNames']:
            return True
    return False


def create_table_livres(dynamodb):
    try:
        table = dynamodb.create_table(
            TableName='Livres',
            KeySchema=[
                {'AttributeName': 'ISBN', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'ISBN', 'AttributeType': 'S'},
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
        print(f"Table Livres created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table Livres already exists.")
        else: raise


def create_table_emprunts(dynamodb):
    try:
        table = dynamodb.create_table(
            TableName='Emprunts',
            KeySchema=[
                {
                    'AttributeName': 'emprunt_id', 'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'emprunt_id', 'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
        print(f"Table Emprunts created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table Emprunts already exists.")
        else: raise


def insert_item(dynamodb, table_name, item):
    """Insère un élément dans la table DynamoDB."""
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    print(f"Item inserted: {item}")


# CRUD Livres
def create_livre(dynamodb, isbn:str, titre:str, auteur:str, annee_publication:int, disponible:bool):
    item = {
        'ISBN':isbn,
        'titre':titre,
        'auteur':auteur,
        'annee_publication':annee_publication,
        'disponible':disponible
    }
    insert_item(dynamodb,'Livres',item)


def retrieve_livre(dynamodb, isbn:str=None):
    table = dynamodb.Table('Livres')
    # Cherche un livre à partir de son isbn
    if(isbn):
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('ISBN').eq(isbn)
        )
    # Récupère tous les livres
    else:
        response = table.scan()
    items = response['Items']
    return items


def update_livre(dynamodb,isbn:str,titre:str=None, auteur:str=None, annee_publication:int=None, disponible:bool=None):
    table = dynamodb.Table('Livres')

    expressions = []
    expression_attribute_values = {}

    if titre or auteur or annee_publication or (disponible is not None):
        if titre:
            expressions.append("titre = :titre")
            expression_attribute_values[":titre"] = titre

        if auteur:
            expressions.append("auteur = :auteur")
            expression_attribute_values[":auteur"] = auteur

        if annee_publication:
            expressions.append("annee_publication = :annee_publication")
            expression_attribute_values[":annee_publication"] = annee_publication

        if disponible is not None:
            expressions.append("disponible = :disponible")
            expression_attribute_values[":disponible"] = disponible
        
        update_expression = "SET " + ', '.join(expressions)
        
        response = table.update_item(
            Key={'ISBN': isbn},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )
        if response:
            print(f"Livre avec ISBN {isbn} mis à jour avec succès.")
        else:
            print(f"Livre avec ISBN {isbn} n'a pas été trouvé.")
    
    else:
        print(f"Il faut indiquer des valeurs à modifier.")


def delete_livre(dynamodb, isbn: str):
    table = dynamodb.Table('Livres')
    try:
        response = table.delete_item(
            Key={'ISBN': isbn}
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Livre avec ISBN {isbn} supprimé avec succès.")
        else:
            print(f"Échec de la suppression du livre avec ISBN {isbn}.")
    except Exception as e:
        print(f"Erreur lors de la suppression du livre avec ISBN {isbn} : {e}")




# CRUD Emprunts
def create_emprunt(dynamodb, emprunt_id:str, isbn:str, utilisateur:str, date_emprunt:str, date_retour:str=None):
    item = {
        'emprunt_id':emprunt_id,
        'ISBN':isbn,
        'utilisateur':utilisateur,
        'date_emprunt':date_emprunt,
        'date_retour':date_retour
    }
    insert_item(dynamodb,'Emprunts',item)


def retrieve_emprunt(dynamodb, emprunt_id:str=None):
    table = dynamodb.Table('Emprunts')
    if(emprunt_id):
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('emprunt_id').eq(emprunt_id)
        )
    else:
        response = table.scan()
    items = response['Items']
    return items


def update_emprunt(dynamodb,emprunt_id:str, isbn:str=None, utilisateur:str=None, date_emprunt:str=None, date_retour:str=None):
    table = dynamodb.Table('Emprunts')

    expressions = []
    expression_attribute_values = {}

    if isbn or utilisateur or date_emprunt or date_retour:
        if isbn:
            expressions.append("isbn = :isbn")
            expression_attribute_values[":isbn"] = isbn

        if utilisateur:
            expressions.append("utilisateur = :utilisateur")
            expression_attribute_values[":utilisateur"] = utilisateur

        if date_emprunt:
            expressions.append("date_emprunt = :date_emprunt")
            expression_attribute_values[":date_emprunt"] = date_emprunt

        if date_retour:
            expressions.append("date_retour = :date_retour")
            expression_attribute_values[":date_retour"] = date_retour
        
        update_expression = "SET " + ', '.join(expressions)
        
        response = table.update_item(
            Key={'emprunt_id': emprunt_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )
        if response:
            print(f"Emprunt {emprunt_id} mis à jour avec succès.")
        else:
            print(f"Emprunt {emprunt_id} n'a pas été trouvé.")
    
    else:
        print(f"Il faut indiquer des valeurs à modifier.")


def delete_emprunt(dynamodb, emprunt_id: str):
    table = dynamodb.Table('Emprunts')
    try:
        response = table.delete_item(
            Key={'emprunt_id': emprunt_id}
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Emprunt {emprunt_id} supprimé avec succès.")
        else:
            print(f"Échec de la suppression de l'emprunt {emprunt_id}.")
    except Exception as e:
        print(f"Erreur lors de la suppression de l'emprunt {emprunt_id} : {e}")


def retrieve_emprunt_specific(dynamodb, attribut:str, valeur):
    table = dynamodb.Table('Emprunts')
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr(attribut).eq(valeur)
    )
    items = response['Items']
    return items


def retrieve_livre_specific(dynamodb, attribut:str, valeur):
    table = dynamodb.Table('Livres')
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr(attribut).eq(valeur)
    )
    items = response['Items']
    return items


def count_emprunts_par_livre(dynamodb):
    table = dynamodb.Table('Emprunts')
    response = table.scan()
    items = response['Items']
    # Dictionnaire pour stocker le nombre d'emprunts par ISBN
    emprunts_par_isbn = {}

    for item in items:
        isbn = item['ISBN']
        if isbn in emprunts_par_isbn:
            emprunts_par_isbn[isbn] += 1
        else:
            emprunts_par_isbn[isbn] = 1

    return emprunts_par_isbn


def main():
    dynamodb = create_dynamodb_resource()
    # Création des tables
    if(not(check_table_exists(dynamodb, 'Livres'))):
        create_table_livres(dynamodb)
    if(not(check_table_exists(dynamodb, 'Emprunts'))):
        create_table_emprunts(dynamodb)
    

    while True:
        option = input(
            "Choissisez un menu :\n"
            + "1 - Créer un livre\n"
            + "2 - Afficher les livres\n"
            + "3 - Modifier un livre\n"
            + "4 - Supprimer un livre\n"
            + "5 - Emprunter un livre\n"
            + "6 - Voir les livres empruntés par un utilisateur\n"
            + "7 - Retouner un livre\n"
            + "8 - Trouver tous les livres d'un auteur\n"
            + "9 - Lister tous les emprunts qui dépassent un certaine durée\n"
            + "10 - Trouver les livres les plus empruntés\n"
            + "Autre - Quitter\n"
        )
        # Ajouter un livre
        if option == '1':
            saisie_isbn = input("Saisissez l'isbn du livre :\n")
            saisie_titre = input("Saisissez le titre du livre :\n")
            saisie_auteur = input("Saisissez l'auteur du livre :\n")
            saisie_annee_publication = int(input("Saisissez l'année de publication du livre :\n"))
            saisie_disponible = input("Saisissez la disponibilité du livre (True ou False) :\n") == "True"
    
            create_livre(dynamodb,saisie_isbn,saisie_titre,saisie_auteur,saisie_annee_publication,saisie_disponible)
        
        # Afficher les livres
        elif option == '2':
            saisie = input("Saisissez l'ISBN du livre à afficher (ou entrée vide pour tout afficher) :\n")
            if saisie == "":
                print(retrieve_livre(dynamodb))
            else:
                print(retrieve_livre(dynamodb, saisie))

        # Modifier un livre
        elif option == '3':

            print("Saisissez les informations du livre modifié (ou directement entrée pour ne rien changer) :")
            saisie_isbn = input("Saisissez l'isbn du livre :\n")
            saisie_titre = input("Saisissez le titre du livre :\n")
            saisie_auteur = input("Saisissez l'auteur du livre :\n")
            saisie_annee_publication = input("Saisissez l'année de publication du livre :\n")
            saisie_disponible = input("Saisissez la disponibilité du livre (True ou False) :\n")

            if saisie_isbn == "":
                saisie_isbn = None
            if saisie_titre == "":
                saisie_titre = None
            if saisie_auteur == "":
                saisie_auteur = None
            if saisie_annee_publication == "":
                saisie_annee_publication = None
            else:
                saisie_annee_publication = int(saisie_annee_publication)
            if saisie_disponible == "":
                saisie_disponible = None
            else:
                saisie_disponible = (saisie_disponible == "True")

            update_livre(dynamodb,saisie_isbn,saisie_titre,saisie_auteur,saisie_annee_publication,saisie_disponible)

        # Supprimer un livre
        elif option == '4':
            saisie = input("Saisissez l'ISBN du livre à supprimer :\n")
            delete_livre(dynamodb,saisie)

        # Emprunter un livre
        elif option == '5':
            saisie_isbn = input("Saisissez l'ISBN du livre à emprunter :\n")

            livre = retrieve_livre(dynamodb,saisie_isbn)[0]

            if livre['disponible']:
                saisie_utilisateur = input("Le livre est disponible. Saisissez votre nom :\n")
                current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Nos id d'emprunts seront "ISBN-Date d'emprunt en epoch"
                emprunt_id = f"{saisie_isbn}-{time.time()}"
                create_emprunt(dynamodb,emprunt_id=emprunt_id,isbn=saisie_isbn,utilisateur=saisie_utilisateur, date_emprunt=str(current_date))
                # Le livre n'est plus disponible
                update_livre(dynamodb,saisie_isbn,disponible=False) 
            else:
                print("Livre indisponible.")


        # Voir les livres empruntés par un utilisateur
        elif option == '6':
            saisie_utilisateur = input("Saisissez le nom de l'utilisateur :\n")
            items = retrieve_emprunt_specific(dynamodb,'utilisateur',saisie_utilisateur)
            for i in items:
                print(i)

        # Retouner un livre
        elif option == '7':
            saisie_isbn = input("Saisissez l'ISBN du livre à retourner :\n")
            livre = retrieve_livre(dynamodb,saisie_isbn)[0]

            if not(livre['disponible']):

                emprunts = retrieve_emprunt_specific(dynamodb,'ISBN',saisie_isbn)
                for item in emprunts:
                    if not(item["date_retour"]):
                        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        update_emprunt(dynamodb,emprunt_id=item["emprunt_id"],date_retour=str(date))
                        update_livre(dynamodb,saisie_isbn,disponible=True)
                        break
                
            else:
                print("Livre déjà disponible.")
        
        # Trouver tous les livres d'un auteur
        elif option == '8':
            saisie_auteur = input("Saisissez le nom de l'auteur :\n")
            print(retrieve_livre_specific(dynamodb,'auteur',saisie_auteur))
        
        # Lister tous les emprunts qui dépassent un certaine durée
        elif option == '9':
            try:
                saisie_duree = float(input("Saisissez la durée de dépassement (en secondes) :\n"))
            except Exception:
                print("Mauvaise durée saisie")

            # Récupère la liste des livres empruntés
            liste_emprunts = retrieve_emprunt(dynamodb)
            for emprunt in liste_emprunts:
                
                date_emprunt = datetime.datetime.strptime(emprunt['date_emprunt'], '%Y-%m-%d %H:%M:%S').timestamp()
                # Si pas de date de retour, on prend la date actuelle
                if(emprunt['date_retour'] is None):
                    date_retour = time.time()
                else:
                    date_retour = datetime.datetime.strptime(emprunt['date_retour'], '%Y-%m-%d %H:%M:%S').timestamp()


                if (date_retour - date_emprunt) > saisie_duree:
                    print(emprunt)

        # Trouver les livres les plus empruntés
        elif option == '10':

            frequences = count_emprunts_par_livre(dynamodb)
            for isbn in frequences:
                print(f"{isbn} : {frequences[isbn]}")

        else: 
            print("Sortie du menu.")
            break



main()