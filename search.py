import json
import requests
import googlemaps
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


API_KEY = os.getenv("API_KEY")
GOOGLE_KEY = os.getenv("GOOGLE_KEY")

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'

TERM = 'Vegan+Cafe'
LOCATION = 'San+Francisco%2C+CA'
SEARCH_LIMIT = 50

tags_set = set()


# Send a GET request to the API.
def get_response():
    url = API_HOST + SEARCH_PATH
    params = {
        "term": TERM,
        "location": LOCATION,
        "limit": SEARCH_LIMIT
    }
    headers = {
        'Authorization': 'Bearer %s' % API_KEY,
    }
    
    return requests.request('GET', url, headers=headers, params=params)


# Get data from google.com.
def google_search_data(name, lat, lng):
    gmaps = googlemaps.Client(key=GOOGLE_KEY)
    # Find id in google by place's name and coordinates.
    find_place_id = gmaps.find_place(name,
                                     "textquery",
                                     fields=["place_id"],
                                     location_bias=f"point:{lat},{lng}")

    if not find_place_id['candidates']:
        
        return {'rating': None, 'website': None}
   
    else:
        # Find website and rating in google by place's google id.
        place_data = gmaps.place(find_place_id['candidates'][0]['place_id'],
                                 fields=['website', 'rating'])
        if not 'rating' in place_data['result']:
            place_data['result']['rating'] = None
        if not 'website' in place_data['result']:
            place_data['result']['website'] = None
        
        return {'rating': place_data['result']['rating'],
                'website': place_data['result']['website']}


# Organize data of one business as a dictionary.
def group_business_data(data):
    latitude = data['coordinates']['latitude']
    longitude = data['coordinates']['longitude']
    google_data = google_search_data(data['name'], latitude, longitude)

    tags = []
    for tag in data['categories']:
        tags.append(tag['alias'])
        tags_set.add(tag['alias'])

    data_dict = {
        "Name": data['name'],
        "Phone": data['phone'], 
        "Website": google_data['website'],
        "Tags": tags,
        "Address": data['location']['address1'],
        "City": data['location']['city'],
        "Zip_code": data['location']['zip_code'],
        "Latitude": latitude, 
        "Longitude": longitude, 
        "Rating": data['rating'], 
        "Google_rating": google_data['rating']
    }

    return data_dict


# Create tables in the database.
def create_tables(db, name):    
    if name == "tags":
        db.execute("""
            CREATE TABLE tags (
                id_tag int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name_tag varchar(50) NOT NULL
            ) ENGINE='InnoDB' AUTO_INCREMENT=1
        """)
    elif name == "business":
        db.execute("""
            CREATE TABLE business (
                id_b int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name_b varchar(60) NOT NULL,
                phone char(12) NULL,
                website varchar(125) NULL,
                address varchar(80) NULL,
                city varchar(60) NULL,
                zip_code char(9) NULL,
                lat float(10,6) NOT NULL,
                lng float(10,6) NOT NULL,
                rating float(2,1) NOT NULL,
                g_rating float(2,1) NOT NULL
            ) ENGINE='InnoDB' AUTO_INCREMENT=1
        """)
    elif name == "business_tag":
        db.execute("""
            CREATE TABLE business_tag (
                id_tag int unsigned NOT NULL,
                id_b int unsigned NOT NULL,
                FOREIGN KEY (id_tag) REFERENCES tags (id_tag),
                FOREIGN KEY (id_b) REFERENCES business (id_b)
            ) ENGINE='InnoDB';
        """)


def write_data_into_db(db, data):
    tags_list = list(tags_set)
    for tag in tags_list:
        db.execute("INSERT INTO tags (name_tag) VALUES (:tag)",
                    {"tag": tag})
    db.commit()
    for business in data:
        db.execute("""
            INSERT INTO business (name_b, phone, website, address, city,
            zip_code, lat, lng, rating, g_rating) VALUES (:name, :phone,
            :website, :address, :city, :zip_code, :lat, :lng, :rating,
            :g_rating)""",
            {"name": business["Name"],
             "phone": business["Phone"],
             "website": business["Website"],
             "address": business["Address"],
             "city": business["City"],
             "zip_code": business["Zip_code"],
             "lat": business["Latitude"],
             "lng": business["Longitude"],
             "rating": business["Rating"],
             "g_rating": business["Google_rating"]})
        id_b = db.execute("SELECT id_b FROM business WHERE name_b = :name",
                          {"name": business["Name"]}).fetchone()
        for tag in business['Tags']:
            id_tag = db.execute("SELECT id_tag FROM tags WHERE name_tag = :tag",
                                {"tag": tag}).fetchone()
            db.execute("""
                INSERT INTO business_tag (id_tag, id_b) VALUES
                (:id_tag, :id_b)""",
                {"id_tag": id_tag[0], "id_b": id_b[0]})
    db.commit()


def main():
    # Send a request and get response from yelp.com.
    response = get_response()
    try:
        yelp_data = response.json()
    except JSONDecodeError:
        print("Error")
        exit(1)

    # Organize all received data as list of dictionaries.
    businesses_data = []
    for business in yelp_data['businesses']:
        data = group_business_data(business)
        businesses_data.append(data)

    # Connect to the database.
    engine = create_engine(os.getenv("DATABASE_URL"))
    db = scoped_session(sessionmaker(bind=engine))

    # If the database has not tables, create them.
    if not engine.dialect.has_table(engine, 'business'):
        create_tables(db, "business")
    if not engine.dialect.has_table(engine, 'tags'):
        create_tables(db, "tags")
    if not engine.dialect.has_table(engine, 'business_tag'):
        create_tables(db, "business_tag")

    # Write data into the database.
    write_data_into_db(db, businesses_data)


if name == 'main':
    main()
