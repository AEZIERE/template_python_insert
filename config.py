from configparser import ConfigParser
import psycopg2
import os
from sqlalchemy import create_engine



config = {
            "host": os.getenv("HOST"),
            "port": os.getenv("PORT"),
            "database": os.getenv("DATABASE"),
            "user": os.getenv("USER"),
            "password": os.getenv("PASSWORD")
        }

db_uri = "postgresql://postgres:changeme@localhost:5432/eurostats"

class bdd_connection(object):
    """
    Context manager pour garantir la fermeture de la connexion à la base de données
    """
    def __init__(self):
        self.conn = None
    def __enter__(self):


        self.conn = psycopg2.connect(**config)
        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.close()

class engine_conn(object):
    """
    Context manager pour garantir la fermeture de la connexion à la base de données
    """
    def __init__(self):
        self.conn = None
    def __enter__(self):
        # Specify the connection details in a SQLAlchemy URL format
        db_uri = "postgresql://postgres:changeme@localhost:5432/eurostats"

        # Create a SQLAlchemy engine to connect to the PostgreSQL database
        engine = create_engine(db_uri)
        self.conn = engine.connect()
        return self.conn


    def __exit__(self, type, value, traceback):
        self.conn.close()