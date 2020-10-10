import sql as bsql
import pdpipe as pdp
import numpy as np
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def region_transformation(region):
    if region.lower().__contains__('kungsholm'):
        return 'kungsholmen'
    if region.lower().__contains__('vasastan'):
        return 'vasastan'
    if region.lower().__contains__('östermalm'):
        return 'östermalm'
    if region.lower().__contains__('norrmalm'):
        return 'norrmalm'
    if region.lower().__contains__('södermalm'):
        return 'södermalm'
    if region.lower().__contains__('gärdet'):
        return 'gärdet'
    if region.lower().__contains__('vasastan'):
        return 'vasastan'
    if region.lower().__contains__('norrmalm'):
        return 'norrmalm'
    if region.lower().__contains__('gamla stan'):
        return 'gamla stan'

def builtYear_transformation(builtYear):
    if (builtYear <= 1919):
        return '<=1919'
    elif (builtYear >= 1920 and builtYear <= 1949):
        return '1920-1949'
    elif (builtYear >= 1950 and builtYear <= 1999):
        return '1950-1999'
    elif (builtYear >= 2000):
        return '>=2000'

def Address_transformation(address):
    return geolocator.geocode(address).address.split(',')[4].lstrip(' ')

sql_file_name = 'RegressionData.sql'
sql_file_path = 'C:\\Users\\chris\\OneDrive\\GIT\\Booli\\'
sql = bsql.SQL(sql_file_name, sql_file_path)
cnxn, cursor = sql.connect()

df = sql.executeQueryFromFile(cnxn)
geolocator = Nominatim(user_agent = "ChristopherFuru")
# df = df[:5]

pipeline = pdp.ApplyByCols('Region', region_transformation, 'Region')
pipeline += pdp.ApplyByCols('BuiltYear', builtYear_transformation, 'BuiltYear')
# pipeline += pdp.ApplyByCols('Address', Address_transformation, 'Region_New')
pipeline += pdp.RowDrop({'Region': lambda x: x == None})
pipeline += pdp.RowDrop({'OperatingCostInSek': lambda x: pd.isnull(x) == True})
pipeline += pdp.RowDrop({'NumberOfRooms': lambda x: x == 0})
pipeline += pdp.RowDrop({'FloorNumber': lambda x: pd.isnull(x) == True})
pipeline += pdp.RowDrop({'BuiltYear': lambda x: pd.isnull(x) == True})
pipeline += pdp.OneHotEncode('Region')
pipeline += pdp.OneHotEncode('BuiltYear')

df2 = pipeline(df)
df2
# df.a
# # geolocator = Nominatim(user_agent="test")
# location = geolocator.geocode("odenplan metro station")
# location.address.split(',')[4].lstrip(' ')

# df_test = pd.DataFrame([location.address.split(',')[4]], columns = ['Region'])
# df_test['Latitude'] = location.latitude
# df_test['Longitude'] = location.longitude

# def get_geocode(x_df):
#     geocode = RateLimiter(geolocator.geocode)
#     x_df['Location'] = x_df.Address.apply(geocode)
#     return x_df

# def get_point_from_location(x_df):
#     x_df['point'] = x_df['Location'].apply(lambda loc: tuple(loc.point) if loc else None)
#     return x_df

# def get_coordinates_from_point(x_df):
#     x_df[['latitude', 'longitude', 'altitude']] = pd.DataFrame(x_df['point'].tolist(), index = x_df.index)
#     return x_df

# def get_data():
#     return sql.executeQueryFromFile(cnxn)

# res = (
#     get_data()
#     .pipe(get_geocode)
#     .pipe(get_point_from_location)
#     .pipe(get_coordinates_from_point)
# )
# res.to_excel("SoldApartments.xlsx")

# geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
# df['Location'] = df.Address.apply(geocode)
# df['point'] = df['Location'].apply(lambda loc: tuple(loc.point) if loc else None)
# df[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df['point'].tolist(), index=df.index)
# df