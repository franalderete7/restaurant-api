import statistics
import math
import csv
import sqlite3
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from fastapi import FastAPI, HTTPException

app = FastAPI()

DB_NAME = "restaurants.db"


class Restaurant(BaseModel):
    id: str
    rating: int
    name: str
    site: str
    email: str
    phone: str
    street: str
    city: str
    state: str
    lat: float
    lng: float


def create_table_and_import_data():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurants (
        id TEXT PRIMARY KEY,
        rating INTEGER,
        name TEXT,
        site TEXT,
        email TEXT,
        phone TEXT,
        street TEXT,
        city TEXT,
        state TEXT,
        lat FLOAT,
        lng FLOAT
    )
    """)

    with open('restaurantes.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            print(f"Inserting row: {row}")  # Debug print statement
            cursor.execute("""
            INSERT OR IGNORE INTO restaurants (
                id, rating, name, site, email, phone, street, city, state, lat, lng)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['id'], row['rating'], row['name'], row['site'], row['email'],
                row['phone'], row['street'], row['city'], row['state'],
                row['lat'], row['lng']
            ))

    conn.commit()
    conn.close()


@app.on_event("startup")
async def startup_event():
    create_table_and_import_data()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)


@app.get("/restaurants", response_model=List[Restaurant])
async def get_all_restaurants():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM restaurants")
        rows = cursor.fetchall()

        restaurants = [Restaurant(
            id=row[0], rating=row[1], name=row[2], site=row[3], email=row[4],
            phone=row[5], street=row[6], city=row[7], state=row[8], lat=row[9], lng=row[10]
        ) for row in rows]

        return restaurants

    except sqlite3.Error as e:
        print("Error accessing the restaurants table:", e)
        raise HTTPException(status_code=500, detail="Database error occurred")

    finally:
        if conn:
            conn.close()


@app.get("/restaurants/{restaurant_id}", response_model=Restaurant)
async def get_restaurant_by_id(restaurant_id: str):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM restaurants WHERE id=?",
                       (restaurant_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Restaurant not found")

        restaurant = Restaurant(
            id=row[0], rating=row[1], name=row[2], site=row[3], email=row[4],
            phone=row[5], street=row[6], city=row[7], state=row[8], lat=row[9], lng=row[10]
        )

        return restaurant

    except sqlite3.Error as e:
        print("Error accessing the restaurants table:", e)
        raise HTTPException(status_code=500, detail="Database error occurred")

    finally:
        if conn:
            conn.close()


@app.post("/restaurants", response_model=Restaurant)
async def create_restaurant(restaurant: Restaurant):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO restaurants (id, rating, name, site, email, phone, street, city, state, lat, lng)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            restaurant.id, restaurant.rating, restaurant.name, restaurant.site, restaurant.email,
            restaurant.phone, restaurant.street, restaurant.city, restaurant.state, restaurant.lat, restaurant.lng
        ))
        conn.commit()

        return restaurant

    except sqlite3.Error as e:
        print("Error creating a new restaurant:", e)
        raise HTTPException(status_code=500, detail="Database error occurred")

    finally:
        if conn:
            conn.close()


@app.put("/restaurants/{restaurant_id}", response_model=Restaurant)
async def update_restaurant(restaurant_id: str, restaurant: Restaurant):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE restaurants SET rating=?, name=?, site=?, email=?, phone=?, street=?, city=?, state=?, lat=?, lng=?
            WHERE id=?
        """, (
            restaurant.rating, restaurant.name, restaurant.site, restaurant.email, restaurant.phone,
            restaurant.street, restaurant.city, restaurant.state, restaurant.lat, restaurant.lng, restaurant_id
        ))
        conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Restaurant not found")

        updated_restaurant = Restaurant(
            id=restaurant_id, rating=restaurant.rating, name=restaurant.name, site=restaurant.site,
            email=restaurant.email, phone=restaurant.phone, street=restaurant.street, city=restaurant.city,
            state=restaurant.state, lat=restaurant.lat, lng=restaurant.lng
        )

        return updated_restaurant

    except sqlite3.Error as e:
        print("Error updating the restaurant:", e)
        raise HTTPException(status_code=500, detail="Database error occurred")

    finally:
        if conn:
            conn.close()


@app.delete("/restaurants/{restaurant_id}")
async def delete_restaurant(restaurant_id: str):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM restaurants WHERE id=?", (restaurant_id,))
        conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Restaurant not found")

        return {"message": "Restaurant deleted successfully"}

    except sqlite3.Error as e:
        print("Error deleting the restaurant:", e)
        raise HTTPException(status_code=500, detail="Database error occurred")

    finally:
        if conn:
            conn.close()


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)

    a = (math.sin(d_lat / 2) * math.sin(d_lat / 2) +
         math.sin(d_lon / 2) * math.sin(d_lon / 2) * math.cos(lat1) * math.cos(lat2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c * 1000  # Distance in meters


@app.get("/restaurants/statistics")
async def get_restaurants_statistics(latitude: Optional[float] = None, longitude: Optional[float] = None, radius: Optional[int] = None):
    if latitude is None or longitude is None or radius is None:
        raise HTTPException(
            status_code=400, detail="Latitude, longitude, and radius are required query parameters.")

    if not (-90 <= latitude <= 90):
        raise HTTPException(
            status_code=400, detail="Invalid latitude value. Must be between -90 and 90.")

    if not (-180 <= longitude <= 180):
        raise HTTPException(
            status_code=400, detail="Invalid longitude value. Must be between -180 and 180.")

    if radius <= 0:
        raise HTTPException(
            status_code=400, detail="Invalid radius value. Must be greater than 0.")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT rating, lat, lng FROM restaurants")
    rows = cursor.fetchall()
    print("All restaurants:", rows)
    conn.close()

    # Filter restaurants within the circle
    inside_circle = [
        row for row in rows
        if haversine_distance(latitude, longitude, row[1], row[2]) <= radius
    ]

    print("Restaurants inside the circle:", inside_circle)

    if not inside_circle:
        raise HTTPException(status_code=404, detail="Restaurants not found")

    count = len(inside_circle)
    avg = sum(row[0] for row in inside_circle) / count
    std = statistics.stdev(row[0] for row in inside_circle)

    return {"count": count, "avg": avg, "std": std}
