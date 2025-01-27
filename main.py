from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from datetime import datetime
import asyncpg
import asyncio

app = FastAPI()

class DeviceData(BaseModel):
	device_id: str
	recorded_at: datetime
	carbon_monoxide_ppm: float
	temperature_celcius: float
	pm1_ug_m3: float
	pm2_5_ug_m3: float
	pm4_ug_m3: float
	pm10_ug_m3: float


async def get_db_connection():
	try:
		return await asyncpg.connect(
			user='postgres',
			password='spark',
			database='postgres',
			host='localhost',
			port=5432
		)
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.post("/data")
async def post_data(data: DeviceData, db=Depends(get_db_connection)):
	print(data)

	query = """
		INSERT INTO device_data (device_id, recorded_at, carbon_monoxide_ppm, temperature_celcius, pm1_ug_m3, pm2_5_ug_m3, pm4_ug_m3, pm10_ug_m3)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	"""

	try:
		await db.execute(query, data.device_id, data.recorded_at, data.carbon_monoxide_ppm, data.temperature_celcius, 
						data.pm1_ug_m3, data.pm2_5_ug_m3, data.pm4_ug_m3, data.pm10_ug_m3)
		await db.close()
		return {"status": "success", "message": "Data inserted into the database"}
	except Exception as e:
		await db.close()
		raise HTTPException(status_code=500, detail=f"Error inserting data into the database: {str(e)}")

@app.get("/data")
async def get_data(db=Depends(get_db_connection)):
	try:
		query = "SELECT * FROM device_data ORDER BY recorded_at DESC LIMIT 100;"
		rows = await db.fetch(query)
		await db.close()

		result = [dict(row) for row in rows]
		return {"data": result}
	except Exception as e:
		return {"status": "failed", "error": str(e)}
