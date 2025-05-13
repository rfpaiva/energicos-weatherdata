import streamlit as st
import pandas as pd
import boto3

def getWeatherData(station, year):
    session = boto3.Session(
        aws_access_key_id=st.secrets["ACCESS_KEY"],
        aws_secret_access_key=st.secrets["ACCESS_SECRET"],
        region_name="eu-west-1"
    )
    
    table = session.resource("dynamodb").Table("weather_dwd_actual")
    pk = STATIONS[station]['pk']
    sk = f"timestamp#{year}"
    response = table.query(
        KeyConditionExpression="#pk = :pk AND begins_with(#sk, :sk)",
        ExpressionAttributeNames={"#pk": "pk", "#sk": "sk"},
        ExpressionAttributeValues={":pk": pk, ":sk": sk},
        ProjectionExpression = "pk, sk, temperature, humidity, clouds, precipitation, pressure, windSpeed, lat, lon"
    )

    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression="#pk = :pk AND begins_with(#sk, :sk)",
            ExpressionAttributeNames={"#pk": "pk", "#sk": "sk"},
            ExpressionAttributeValues={":pk": pk, ":sk": sk},
            ProjectionExpression = "pk, sk, temperature, humidity, clouds, precipitation, pressure, windSpeed, lat, lon",
            ExclusiveStartKey=response['LastEvaluatedKey']
        )

        data.extend(response['Items'])
        print(len(data))
        
    df = pd.json_normalize(data)
    fieldOrder = ["pk", "sk", "temperature", "humidity", "clouds", "precipitation", "pressure", "windSpeed", "lat", "lon"]
    df = df[fieldOrder]
    return df.to_csv(index=False)

STATIONS = {
    "Berlin Buch - 00400": {"station_id": "00400", "name": "Berlin Buch", "latitude": 52.630956, "longitude": 13.502135, "height": "60 m", "plz": "13125", "pk": "wea#berlin-buch"},
    "Berlin Dahlem - 00403": {"station_id": "00403", "name": "Berlin Dahlem", "latitude": 52.453711, "longitude": 13.301731, "height": "51 m", "plz": "12203", "pk": "wea#berlin-dahlem"},
    "Berlin Marzahn - 00420": {"station_id": "00420", "name": "Berlin Marzahn", "latitude": 52.544744, "longitude": 13.559789, "height": "61 m", "plz": "12679", "pk": "wea#berlin-marzahn"},
    "Berlin Schoenefeld - 00427": {"station_id": "00427", "name": "Berlin Schoenefeld", "latitude": 52.380702, "longitude": 13.530576, "height": "46 m", "plz": "12529", "pk": "wea#berlin-schoenefeld"},
    "Berlin Tempelhof - 00433": {"station_id": "00433", "name": "Berlin Tempelhof", "latitude": 52.467551, "longitude": 13.401981, "height": "48 m", "plz": "10965", "pk": "wea#berlin-tempelhof"},
    "Cottbus - 00880": {"station_id": "00880", "name": "Cottbus", "latitude": 51.775938, "longitude": 14.316799, "height": "69 m", "plz": "03052", "pk": "wea#cottbus"},
    "Grannsee Zehdenick - 05745": {"station_id": "05745", "name": "Grannsee Zehdenick", "latitude": 52.96635, "longitude": 13.326752, "height": "51 m", "plz": "16792", "pk": "wea#grannsee-zehdenick"},
    "Hamburg Fuhlsbüttel - 01975": {"station_id": "01975", "name": "Hamburg Fuhlsbüttel", "latitude": 53.633172, "longitude": 9.988102, "height": "11 m", "plz": "22335", "pk": "wea#hamburg-fuhlsbuettel"},
    "Kyritz - 02794": {"station_id": "02794", "name": "Kyritz", "latitude": 52.936248, "longitude": 12.409335, "height": "40 m", "plz": "16866", "pk": "wea#kyritz"},
    "Mainz-Lerchenberg - 03137": {"station_id": "03137", "name": "Mainz-Lerchenberg", "latitude": 49.965815, "longitude": 8.204851, "height": "199 m", "plz": "55127", "pk": "wea#mainz-lerchenberg"},
    "Malchin Teterow - 05009": {"station_id": "05009", "name": "Malchin Teterow", "latitude": 53.761035, "longitude": 12.557375, "height": "37 m", "plz": "17166", "pk": "wea#teterow-malchin"},
    "Potsdam - 03987": {"station_id": "03987", "name": "Potsdam", "latitude": 52.381247, "longitude": 13.06223, "height": "81 m", "plz": "14469", "pk": "wea#potsdam"},
    "Schwarzenbeck Grambek - 01736": {"station_id": "01736", "name": "Schwarzenbeck Grambek", "latitude": 53.573115, "longitude": 10.679686, "height": "26 m", "plz": "23883", "pk": "wea#schwarzenbeck-grambek"},
    "Stralsund Steinhagen Negast - 06199": {"station_id": "06199", "name": "Stralsund Steinhagen Negast", "latitude": 54.248381, "longitude": 13.041864, "height": "16 m", "plz": "18442", "pk": "wea#stralsund-steinhagen-negast"},
    "Wittstock Rote Mühle - 05643": {"station_id": "05643", "name": "Wittstock Rote Mühle", "latitude": 53.186436, "longitude": 12.494939, "height": "68 m", "plz": "16909", "pk": "wea#wittstock-rote-muehle"},
}

years = [
    2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025
]

# st.sidebar.success(f"Welcome *{st.session_state['name']}*!")
# authenticator.logout(location='sidebar')

st.title("Weather Data CSV Builder")

# First dropdown (e.g., country)
station = st.selectbox("Select a station:", STATIONS.keys(), None, placeholder="Select")

# Dependent second dropdown (e.g., city)    
year = st.selectbox("Select a year:", years, None, placeholder="Select")

if st.button("Prepare file", disabled=not station or not year):
    st.download_button(
        "Download Selection as CSV",
        data=getWeatherData(station, year),
        file_name=f"{station}-{year}.csv",
        mime="text/csv",on_click="ignore",
        type="primary",
        icon=":material/download:"
    )