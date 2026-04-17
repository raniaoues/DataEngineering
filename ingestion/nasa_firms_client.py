import requests # type: ignore
import pandas as pd 
from  io import StringIO  #NASA returns text, not a file. StringIO tricks pandas into reading text as if it were a file
from dotenv import load_dotenv # type: ignore
import os # hedhi w ili kabalha :to safely read your API key from .env

load_dotenv() #reads your .env file and loads the variables into memory.
API_KEY=os.getenv("FIRMS_API_KEY") #retrieves your key by name. Your key never appears in the code itself.
BBOX="-180,-90,180,90" #BBOX = Bounding Box — it defines the geographic zone to query. Format is: min_longitude, min_latitude, max_longitude, max_latitude-180,-90,180,90 = the entire planet 🌍 Later you can narrow it to a specific country, e.g. Tunisia: 7.5,30.2,11.6,37.5
FIRMS_URL = (
    f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
    f"{API_KEY}/VIIRS_SNPP_NRT/{BBOX}/5"
) # hedha nbuildiw bih l api 

def fetch_fire_data():
    response = requests.get(FIRMS_URL) #hedha bsh yabaath http get request l nasa 3bara tekteb fi url fil browser mtek ama bel python 
    df=pd.read_csv(StringIO(response.text))#response.text is a big string of CSV text. StringIO() wraps it so pandas can read it like a file
    print(f"{len(df)} fire hotspots fetched")
    print(df.head())
    
    return df 

if __name__ == "__main__":
    fetch_fire_data()