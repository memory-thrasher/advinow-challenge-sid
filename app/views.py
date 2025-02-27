from fastapi import APIRouter, UploadFile, HTTPException, Query
from fastapi.responses import StreamingResponse
from models import LandingZone
from controller import Controller
import asyncio
import json

router = APIRouter()

#note, invoke as:
#curl localhost:8013/upload_business_symptom -F 'file=@app/data/business_symptom_data.csv'
@router.post('/upload_business_symptom', responses={422: {"description": "invalid file"}})
async def upload_business_symptom(file: UploadFile):
    """
    Accepts a CSV file, performing minimal parsing into a staging area for future processing
    """
    try:
        #we could implement column reordering logic. out of scope for this exercise, but having at least a check is important.
        headerRow = file.file.readline().strip()
        if headerRow != b'Business ID,Business Name,Symptom Code,Symptom Name,Symptom Diagnostic':
            print("Header row mismatch: got ", headerRow) #print to simulate logging, IRL this'd be to a db
            raise HTTPException(status_code=422, detail="header mismatch, please reorder columns in this order: Business ID,Business Name,Symptom Code,Symptom Name,Symptom Diagnostic")
        async with Controller() as cntrlr:
            tasks = []
            for row_raw in file.file:
                row = row_raw.strip().decode("utf-8")
                lz = LandingZone()
                (lz.business_id, lz.business_name, lz.symptom_code, lz.symptom_name, lz.symptom_diagnostic) = row.split(",")
                await cntrlr.push(lz)
        return ""
    except Exception as e:
        return {'Error: ' + str(e)}

@router.post('/do_ingest')
async def do_ingest():
    """
    performs initial processing of all staged but yet-unprocessed data
    """
    try:
        async with Controller() as cntrlr:
            await cntrlr.do_ingest()
    except Exception as e:
        return {'Error: ' + str(e)}

@router.get('/status')
async def get_status():
    """
    Returns a fixed string, to verify the server is online
    """
    try:
        return {"Health OK"}
    except Exception as e:
        return {'Error: ' + str(e)}

@router.get('/fetch')
async def fetch(bid: int = Query(None, description="Business ID (int) to filter, optional"),
                diag: bool = Query(None, description="Diagnostic boolean to filter, optional")):
    """
    Fetches all business and symptom data based on optional filters, as json
    """
    try:
        async with Controller() as cntrlr:
            genA = await cntrlr.fetch(bid, diag)
            def genB():
                yield "["
                first = True
                for row in genA:
                    if not first:
                        yield ","
                    else:
                        first = False
                    yield json.dumps({"Business ID": row.Business.id, "Business Name": row.Business.name,
                                      "Symptom Code": row.Symptom.code, "Symptom Name": row.Symptom.name,
                                      "Symptom Diagnostic": str(row.Symptom.diagnostic).lower()})
                yield "]"
            return StreamingResponse(genB(), media_type="application/json")
        # return StreamingResponse(genB(), media_type="application/json")
        #StreamingResponse = forward the data from the db to the caller row by row, without ever buffering it all in middleware ram
    except Exception as e:
        return {'Error: ' + str(e)}
