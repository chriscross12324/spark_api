from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, WebSocketException, Request
from contextlib import asynccontextmanager
from pydantic import BaseModel
from datetime import datetime
import asyncpg
import asyncio
import logging

# Store active WebSocket connections for each device
subscriptions = {}

logging.basicConfig(level=logging.INFO)

async def get_db_connection(retry_interval=5):
	"""Establish a connection to the PostgreSQL database."""
	while True:
		try:
			return await asyncpg.connect(
				user='postgres',
				password='spark',
				database='spark',
				host='localhost',
				port=5432
			)
		except Exception as e:
			print(f"Database connection failed: {str(e)}")
			print("Retrying in 5 seconds...")
			await asyncio.sleep(retry_interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
	"""Lifespan context manager for FastAPI to handle background tasks."""
	db = None
	task = None

	try:
		db = await get_db_connection()
		task = asyncio.create_task(db.add_listener('device_data_update', handle_device_data_update))
		yield
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")
	finally:
		# Cleanly cancel the task when the app shuts down
		if task:
			task.cancel()
			try:
				await task
			except asyncio.CancelledError:
				pass
		if db:
			await db.close()


# Create FastAPI app with lifespan context
app = FastAPI(lifespan=lifespan, root_path="/spark")


@app.websocket("/ws/{device_id}")
async def websocket_endpoint(websocket: WebSocket, device_id: str):
	"""WebSocket endpoint for subscribing to updates on a specific device."""
	await websocket.accept()

	# Add new WebSocket connection to subscriptions
	if device_id not in subscriptions:
		subscriptions[device_id] = []
	
	subscriptions[device_id].append(websocket)

	try:
		# Send all the data for the device when the client connects
		db = await get_db_connection()
		all_device_data = await fetch_device_data(device_id, db)

		# Convert datetime objects to ISO 8601 strings
		for entry in all_device_data:
			for key, value in entry.items():
				if isinstance(value, datetime):
					entry[key] = value.isoformat()

		# Send all data to the subscribed WebSocket clients
		await websocket.send_json({"device_id": device_id, "data": all_device_data})

		# Keep connection alive and await incoming messages
		while True:
			await websocket.receive_text()
			
	except WebSocketDisconnect:
		# Remove disconnected WebSocket and clean up if necessary
		subscriptions[device_id].remove(websocket)
		if not subscriptions[device_id]:
			del subscriptions[device_id]
	except WebSocketException as e:
		print(f"WebSocket error for device {device_id}: {e}")
		await websocket.close()
	


async def handle_device_data_update(db, pid, channel, payload):
	"""Handle database notifications and send updates to subscribed WebSocket clients."""
	device_id = payload
	if device_id in subscriptions:
		data = await fetch_latest_device_data(device_id, db)

		# Convert datetime objects to ISO 8601 strings
		for entry in data:
			for key, value in entry.items():
				if isinstance(value, datetime):
					entry[key] = value.isoformat()

		# Send updated data to all subscribed WebSocket clients
		for websocket in subscriptions[device_id]:
			await websocket.send_json({"device_id": device_id, "data": data})



async def fetch_device_data(device_id: str, db):
	"""Fetch all data for a specific device from the database."""
	query = "SELECT * FROM device_data WHERE device_id = $1 ORDER BY recorded_at DESC LIMIT 800"
	rows = await db.fetch(query, device_id)

	result = [dict(row) for row in rows]
	return result

async def fetch_latest_device_data(device_id: str, db):
	"""Fetch the latest data for a specific device from the database."""
	query = "SELECT * FROM device_data WHERE device_id = $1 ORDER BY recorded_at DESC LIMIT 1"
	row = await db.fetchrow(query, device_id)

	if row:
		return [dict(row)]
	return []


class DeviceData(BaseModel):
	device_id: str
	recorded_at: datetime
	carbon_monoxide_ppm: float
	temperature_celcius: float
	pm1_ug_m3: float
	pm2_5_ug_m3: float
	pm4_ug_m3: float
	pm10_ug_m3: float

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


@app.middleware("http")
async def log_requests(request: Request, call_next):
	print("Incoming request detected!")
	try:
		body = await request.body()
		logging.info(f"Received request: {request.method} {request.url}\nHeaders: {request.headers}\nBody: {body.decode() if body else 'No Body'}")
	except Exception as e:
		logging.error(f"Error logging request: {e}")

	response = await call_next(request)
	return response
