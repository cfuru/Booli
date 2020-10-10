import sql as bsql
import pdpipe as pdp
import numpy as np
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import statsmodels.formula.api as smf
import patsy
from geopy.distance import geodesic

def District_transformation(District):
    if District.lower().__contains__('kungsholm'):
        return 'kungsholmen'
    if District.lower().__contains__('vasastan'):
        return 'vasastan'
    if District.lower().__contains__('östermalm'):
        return 'östermalm'
    if District.lower().__contains__('norrmalm'):
        return 'norrmalm'
    if District.lower().__contains__('södermalm'):
        return 'södermalm'
    if District.lower().__contains__('gärdet'):
        return 'gärdet'
    if District.lower().__contains__('vasastan'):
        return 'vasastan'
    if District.lower().__contains__('norrmalm'):
        return 'norrmalm'
    if District.lower().__contains__('gamla stan'):
        return 'gamla stan'

def builtYear_transformation(builtYear):
    if (builtYear <= 1919):
        return '1919'
    elif (builtYear >= 1920 and builtYear <= 1949):
        return '1920_1949'
    elif (builtYear >= 1950 and builtYear <= 1999):
        return '1950_1999'
    elif (builtYear >= 2000):
        return '2000'

def GoMining(df):
    variables = []
    for name in df.columns:
        try:
            formula = 'SoldPricePerSquaredMeterInSek ~ MonthlyChargeInSek + ' + name
            model = smf.ols(formula, data = df)

            if model.nobs < len(df)/2:
                continue

            results = model.fit()
        except (ValueError, TypeError, patsy.PatsyError):
            continue
        
        variables.append((results.rsquared, name))

    return variables

def MiningReport(variables, n=30):
    variables.sort(reverse=True)
    for r2, name in variables[:n]:
        print(name, r2)

sql_file_path = 'C:\\Users\\chris\\OneDrive\\GIT\\Booli\\'
sql_file_name_data = 'RegressionData.sql'
sql_file_name_coordinates_subwaystations = 'CoordinatesSubwayStations.sql'
sql_file_name_coordinates_addresses = 'CoordinatesAddress.sql'

sql_data = bsql.SQL(sql_file_name_data, sql_file_path)
cnxn, cursor = sql_data.connect()
df = sql_data.executeQueryFromFile(cnxn)

sql_subwaystations = bsql.SQL(sql_file_name_coordinates_subwaystations, sql_file_path)
cnxn, cursor = sql_subwaystations.connect()
df_subwaystations = sql_subwaystations.executeQueryFromFile(cnxn)

sql_addresses = bsql.SQL(sql_file_name_coordinates_addresses, sql_file_path)
cnxn, cursor = sql_addresses.connect()
df_addresses = sql_addresses.executeQueryFromFile(cnxn)

geodesic(df_subwaystations[['Latitude', 'Longitude']], df_addresses[['Latitude', 'Longitude']])

pipeline = pdp.ApplyByCols('District', District_transformation, 'District')
pipeline += pdp.ApplyByCols('BuiltYear', builtYear_transformation, 'BuiltYear')
pipeline += pdp.RowDrop({'District': lambda x: x == None})
pipeline += pdp.RowDrop({'OperatingCostInSek': lambda x: pd.isnull(x) == True})
pipeline += pdp.RowDrop({'NumberOfRooms': lambda x: x == 0})
pipeline += pdp.RowDrop({'FloorNumber': lambda x: pd.isnull(x) == True})
pipeline += pdp.RowDrop({'BuiltYear': lambda x: pd.isnull(x) == True})
pipeline += pdp.OneHotEncode('District')
pipeline += pdp.OneHotEncode('BuiltYear')
pipeline += pdp.ColDrop(['Address'])

df_pipeline = pipeline(df)
variables = GoMining(df_pipeline)
MiningReport(variables)

formula = 'SoldPricePerSquaredMeterInSek ~ MonthlyChargeInSek + \
            PricePerSquaredMeterInSek + \
            District_östermalm + \
            BuiltYear_1950_1999 + \
            District_kungsholmen + \
            BuiltYear_2000 + \
            District_södermalm + \
            District_vasastan + \
            FloorNumber'

model = smf.ols(formula, data = df_pipeline)
results = model.fit()
results.summary()
