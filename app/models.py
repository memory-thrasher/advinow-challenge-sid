from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
import datetime
from sqlalchemy.engine import Row

Base = declarative_base()

#ideally, LZ would be in a different server entirely
class LandingZone(Base):
    __tablename__ = "BSD_raw"

    #first some tracking metadata
    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    landed_at = Column(DateTime(timezone=False), nullable=False, default=datetime.datetime.utcnow)
    ingested_at = Column(DateTime(timezone=False), nullable=True)
    ingest_last_error = Column(String(1000), nullable=True)

    #now the actual data
    #note: String(None) = VARCHAR(MAX); it's not sql's job to reject landing data if it's too long. business logic will perform the appropriate fix at ingest time.
    business_id = Column(String(None), nullable=True)
    business_name = Column(String(None), nullable=True)
    symptom_code = Column(String(None), nullable=True)
    symptom_name = Column(String(None), nullable=True)
    symptom_diagnostic = Column(String(None), nullable=True)


class Business(Base):
    __tablename__ = "business"
    #note: no autoincrement because business ids are in landing data
    id = Column(Integer, primary_key=True, unique=True)
    name = Column(String(1000), nullable=False)
    created_at = Column(DateTime(timezone=False), nullable=True, default=datetime.datetime.utcnow)
    bsd_raw_id = Column(Integer, nullable=False)#non-enforced FK to landing zone for future diagnostics

class Symptom(Base):
    __tablename__ = "symptom"
    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    created_at = Column(DateTime(timezone=False), nullable=True, default=datetime.datetime.utcnow)
    bsd_raw_id = Column(Integer, nullable=False)#non-enforced FK to landing zone for future diagnostics
    code = Column(String(100), nullable=True)
    name = Column(String(100), nullable=True)
    diagnostic = Column(Boolean, nullable=True)
    code_name_idx = Index("code_name_idx", code, name)

#I'm not sure why businesses and symptoms would be related in this way, but that's how the sample data reads
class BusinessSymptomCrosswalk(Base):
    __tablename__ = "business_symptom_crosswalk" #many-to-many relationship. nomenclature varies
    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    created_at = Column(DateTime(timezone=False), nullable=True, default=datetime.datetime.utcnow)
    bsd_raw_id = Column(Integer, nullable=False)#non-enforced FK to landing zone for future diagnostics
    business_id = Column(Integer, nullable=False)
    symptom_id = Column(Integer, nullable=False)
    bid_sid_idx = Index("bid_sid_idx", business_id, symptom_id)


