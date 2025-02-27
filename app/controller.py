import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session
import settings
from models import * #an ORM model is on the short list of places I think import * is appropriate

engine = create_async_engine(settings.DB_URL)

def tryParseInt(s):
    try:
        return int(s)
    except ValueError as e:
        return None

#might as well complete the MVC pattern. The purpose of this controller is to have one db connection per api call, no matter how many different db calls the api call needs to make
class Controller(object):
    async def __aenter__(self):
        self.sm = async_sessionmaker(engine, expire_on_commit=False)
        self.sesh = self.sm()#default session for small stuff, while larger actions will make parallel sessions
        # self.tx = self.sesh.begin()
        return self

    async def __aexit__(self, *args):
        # await self.tx.commit()
        await self.sesh.commit()
        await self.sesh.close()

    async def push(self, lz: LandingZone):
        self.sesh.add(lz)

    async def handle_ingest_error(self, lz, ctx):
        lz.ingest_last_error = ctx
        self.sesh.add(lz)

    async def get_or_create_business(self, lz):
        async with self.sm() as session:
            async with session.begin():#sql transaction prevents race condition, even between multiple nodes
                bid = tryParseInt(lz.business_id)
                if bid == None:
                    raise Exception("business id is not an int")
                if lz.business_name == None:
                    raise Exception("business name missing")
                bn = lz.business_name if len(lz.business_name) < 997 else lz.business_name[0:997] + "..."
                row: Row = (await session.execute(select(Business)
                                                  .where(Business.id == bid))).first()
                if row == None:
                    business = Business()
                    business.id = bid
                    business.name = bn
                    business.bsd_raw_id = lz.id
                    session.add(business)
                else:
                    business = row.Business
                return business.id

    async def get_or_create_symptom(self, lz):
        async with self.sm() as session:
            async with session.begin():
                scode = lz.symptom_code if len(lz.symptom_code) < 97 else lz.symptom_code[0:97] + "..."
                sn = lz.symptom_name if len(lz.symptom_name) < 97 else lz.symptom_name[0:97] + "..."
                diag: bool = lz.symptom_diagnostic.lower() in ["yes", "true"]
                row: Row = (await session.execute(select(Symptom)
                                                  .where(Symptom.code == scode)
                                                  .where(Symptom.name == sn))).first()
                if row == None:
                    symptom = Symptom()
                    symptom.bsd_raw_id = lz.id
                    symptom.name = sn
                    symptom.code = scode
                    symptom.diagnostic = diag
                    session.add(symptom)
                    symptom = (await session.execute(select(Symptom)
                                                     .where(Symptom.code == scode)
                                                     .where(Symptom.name == sn))).first().Symptom
                else:
                    symptom = row.Symptom
                if symptom.diagnostic != diag:
                    symptom.diagnostic = diag
                    session.add(symptom)
                return symptom.id

    async def do_ingest(self):
        for row in await self.sesh.execute(select(LandingZone).where(LandingZone.ingested_at == None)):
            #spawning each lz record in parallel would not help much since they'd all spend most of their time waiting on the mutex that would have to be added to the get_or_create_* funcs.
            lz = row.LandingZone
            try:
                business_task = self.get_or_create_business(lz)
                symptom_task = self.get_or_create_symptom(lz)
                bid = await business_task
                sid = await symptom_task
                row = (await self.sesh.execute(select(BusinessSymptomCrosswalk)
                                               .where(BusinessSymptomCrosswalk.business_id == bid)
                                               .where(BusinessSymptomCrosswalk.symptom_id == sid)
                                               )).first()
                if row == None:
                    cx = BusinessSymptomCrosswalk()
                    cx.bsd_raw_id = lz.id
                    cx.business_id = bid
                    cx.symptom_id = sid
                    self.sesh.add(cx)
                else:
                    cx = row.BusinessSymptomCrosswalk
                lz.ingest_last_error = ""
                lz.ingested_at = datetime.datetime.utcnow()
                self.sesh.add(lz)
            except Exception as e:
                print("exception: ", e)
                try:
                    await self.handle_ingest_error(lz, str(e) + "\nunhandled error")
                except Exception as e2:
                    print(e2)

    async def fetch(self, bid: int, diag: bool):
        #this could also be a view rather than an adhoc cluster of joins but I wanted to do it in python because that's new to me
        stmt = (select(Business, Symptom, BusinessSymptomCrosswalk)
                .join(BusinessSymptomCrosswalk, BusinessSymptomCrosswalk.business_id == Business.id)
                .join(Symptom, BusinessSymptomCrosswalk.symptom_id == Symptom.id))
        if bid != None:
            stmt = stmt.where(Business.id == bid)
        if diag != None:
            stmt = stmt.where(Symptom.diagnostic == diag)
        return await self.sesh.execute(stmt)
