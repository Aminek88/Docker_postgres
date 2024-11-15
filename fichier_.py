import sqlalchemy
import pandas as pd 
import os 
from sqlalchemy import create_engine
from sqlalchemy import Column , Integer , String , Float , ForeignKey , Table , DateTime , Interval , MetaData
from sqlalchemy.orm import declarative_base , relationship
from sqlalchemy.orm import sessionmaker




base_url = os.getenv("DATABASE_URL","postgresql://postgres:1212@localhost:5432/postgres")
engine = create_engine(base_url)


# Define the Base
Base = declarative_base()

# Table intermédiaire pour la relation plusieurs-à-plusieurs
Annonce_equipement = Table(
    'annonce_equipement', Base.metadata,
    Column('annonce_id', Integer, ForeignKey('annonce.annonce_id'), primary_key=True),
    Column('equipement_id', Integer, ForeignKey('equipement.equi_id'), primary_key=True)
)

# Table des villes
class Cities(Base): 
    __tablename__ = 'cities'
    city_id = Column(Integer, primary_key=True)
    name_ville = Column(String, unique=True)

    # Relation avec les annonces
    annonces = relationship('Annonce', order_by='Annonce.annonce_id', back_populates='city')

# Table des annonces
class Annonce(Base): 
    __tablename__ = 'annonce'
    annonce_id = Column(Integer, primary_key=True)
    title = Column(String)
    price = Column(Float)
    date = Column(DateTime)
    nb_rooms = Column(Integer)
    nb_baths = Column(Integer)
    nb_salons = Column(Integer)
    etage = Column(Integer)
    surface_area = Column(Integer)
    property = Column(String)  # Format textuel pour l'âge du bien
    link = Column(String)
    city_id = Column(Integer, ForeignKey('cities.city_id'))
    city = relationship('Cities', back_populates='annonces')

    # Relation avec les équipements via la table intermédiaire
    equipements = relationship('Equipement', secondary=Annonce_equipement, back_populates='annonces')

# Table des équipements
class Equipement(Base):
    __tablename__ = 'equipement'
    equi_id = Column(Integer, primary_key=True)
    name_equi = Column(String)

    # Relation avec les annonces via la table intermédiaire
    annonces = relationship('Annonce', secondary=Annonce_equipement, back_populates='equipements')


# Créer toutes les tables
Base.metadata.create_all(engine)

print("Tables créées avec succès !")



Session = sessionmaker(bind=engine)
session = Session()


file_path = 'data_final.csv'
data=pd.read_csv(file_path)

for _, row in data.iterrows():
    # Handle city
    city = session.query(Cities).filter_by(name_ville=row['Localisation']).first()
    if not city:
        city = Cities(name_ville=row['Localisation'])
        session.add(city)
        session.commit()
        

    annonce = Annonce(
        title=row['Title'] , 
        price = row['Price'] , 
        date = row['Date'] , 
        nb_rooms = row['Chambre'] ,
        nb_baths=row['Salle de bain'] , 
        nb_salons = row['Salons'] , 
        etage = row['Etage'] ,
        surface_area = row['Surface habitable'] , 
        property = row['Age de bien'] , 
        link = row['EquipementURL'] , 
        city_id = city.city_id
    )
    session.add(annonce)

    equipement_col = ['Ascenseur', 'Balcon', 'Chauffage', 'Climatisation',
       'Concierge', 'Cuisine equipee', 'Duplex', 'Meuble', 'Parking',
       'Securite', 'Terrasse']
    
    for eq_name in equipement_col :
            equipement =session.query(Equipement).filter_by(name_equi = eq_name).first()
            if not equipement : 
                equipement=Equipement(name_equi = eq_name)
                session.add(equipement)
                session.commit()
            annonce.equipements.append(equipement)
    session.commit()

print('Terminer')
