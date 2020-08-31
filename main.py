from sqlalchemy import *
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey, exc, desc
from sqlalchemy.orm import relationship

engine_name = 'sqlite:///telecharge.db'
engine = create_engine(engine_name, echo=True)
metadata = MetaData(bind=None)
metadata.create_all(engine)

