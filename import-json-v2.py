from faker import Faker
from pymongo import MongoClient
import random
import datetime
import uuid
import copy
import json

fake = Faker('es_ES')

# Conexion a MongoDB
client = MongoClient("mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

# Valores variables
cadenas_tv = ["PRIMERA CADENA", "SEGUNDA CADENA", "CANAL 24H", "TELEDEPORTE", "CLAN"]
centros = ["TVE INFORMATIVOS", "RNE INFORMATIVOS", "TVE CATALUÑA", "RTVE ANDALUCÍA"]
formas_posibles = ["PROGRAMA ESPECIAL", "DEBATE", "ENTREVISTA", "REPORTAJE"]
temas_posibles = ["POLÍTICA", "ECONOMÍA", "CULTURA", "DEPORTES", "SOCIEDAD"]
personas_posibles = ["GOBIERNO", "ONU", "UNIÓN EUROPEA", "OMS", "UNESCO"]
nombres_responsables = ["Juan Pérez", "María García", "Luis López", "Carmen Sánchez", "Pedro Rivera", "Lucía Díaz", "Carlos Gómez", "Ana Fernández"]
funciones_responsables = ["PRESENTADOR", "REPORTERO", "CORRESPONSAL", "COLABORADOR", "DIRECTOR"]

# Documento base
with open("salida.json", "r", encoding="utf-8") as f:
    base_doc = json.load(f)

# Funciones de ayuda

def generar_responsables():
    responsables = []
    for _ in range(random.randint(1, 3)):
        nombre = random.choice(nombres_responsables)
        funcion = random.choice(funciones_responsables)
        responsables.append({
            "NOMRES": {
                "id_termino": str(random.randint(100000, 999999)),
                "id_tesauro": "3",
                "tipo": "tesauro",
                "valor": nombre.upper()
            },
            "FUNRES": {
                "id_termino": str(random.randint(1000, 9999)),
                "id_tesauro": "1043",
                "tipo": "lexico",
                "valor": funcion.upper()
            },
            "PAISRES": {
                "id_termino": "0",
                "id_tesauro": "5",
                "tipo": "tesauro",
                "valor": None
            }
        })
    return responsables

def generar_variaciones(base):
    doc = copy.deepcopy(base)
    fecha = datetime.datetime.now().strftime("%Y-%m-%dZ")
    random_id = random.randint(1000, 9999)

    # Titulo y sumario
    doc["TITULOS"]["TITULO"] = fake.sentence(nb_words=6).upper()
    doc["TITULOS"]["SUMARIO"] = fake.text(max_nb_chars=300).upper()

    # Referencias de control
    doc["CONTROL"]["REF"] = f"I{random.randint(10000000, 99999999)}"
    doc["CONTROL"]["USUARIO_MODIFICACION"] = f"I{random.randint(10000, 99999)}"
    doc["CONTROL"]["FECHA_MODIFICACION"]["valor"] = fecha

    # Fechas principales (mantienen estructura, solo si quisieras variar)

    # DATDESFIS: IDs y CLIPID
    for i, item in enumerate(doc["DESCRIPCION_FISICA"]["DATDESFIS"]):
        item["CLIPID"] = f"INF_{fecha.replace('-', '')}_{random_id+i}"
        item["IDASSET"] = f"PRD.{uuid.uuid4().int >> 64}"

    # Cadenas
    doc["EMISION"]["CADENA"]["valor"] = random.choice(cadenas_tv)

    # Centro
    doc["CONTROL"]["CENTRO"]["valor"] = random.choice(centros)

    # Formas
    doc["FORMAS"]["FORMA"] = [
        {
            "id_termino": str(random.randint(100000, 999999)),
            "id_tesauro": "199",
            "tipo": "tesauro",
            "valor": random.choice(formas_posibles)
        }
    ]

    # Temas
    doc["MATERIAS"]["TEMAS"] = [
        {
            "id_termino": str(random.randint(40000000, 49999999)),
            "id_tesauro": "4",
            "tipo": "tesauro",
            "valor": random.choice(temas_posibles)
        }
    ]

    # Personas
    doc["PERSONAS"]["PER"] = [
        {
            "PERS": {
                "id_termino": str(random.randint(30000000, 39999999)),
                "id_tesauro": "3",
                "tipo": "tesauro",
                "valor": random.choice(personas_posibles)
            }
        }
    ]

    # Responsables
    doc["MRESP"] = generar_responsables()

    return doc

# Insercion
batch_size = 10000
num_docs = 20000000

for i in range(0, num_docs, batch_size):
    batch = [generar_variaciones(base_doc) for _ in range(batch_size)]
    collection.insert_many(batch)
    print(f"Insertados {i + batch_size} documentos")
