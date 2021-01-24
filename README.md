# Vegan Cafe in San Francisco

## Description

This script gets data of 50 restaurants from [`yelp.com`](www.yelp.com) by request "Vegan Cafe" and saves it in the database.
In order to use it, you should define an environment variable `DATABASE_URL` that is necessary for database connecting and an API key `API_KEY` for Yelp Fusion API and `GOOGLE_KEY` for Google Maps Places API.

Example of database URL:  
> 'mysql://user:pass@host/db'

A list of requirements is in *requirements.txt*.