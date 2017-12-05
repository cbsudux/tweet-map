import geonamescache
import pandas as pd
from geotext import GeoText
import numpy as np
import plotly
import plotly.plotly as py
from pycountrylist import *
import plotly
import plotly.plotly as py
import pycountry


# Reading dataframe

df = pd.read_csv('temporary.csv')

df.head()

df['freq'] = df.groupby('location')['location'].transform('count')


df.isnull().values.any()
df = df.dropna()   

# Re indexing the lazy way

filename = 'barca_loc_freq.csv'
df.to_csv(filename, encoding='utf-8')


df_2 = pd.read_csv('barca_loc_freq.csv')

x = df.groupby('Location')

df = df_2.drop('Unnamed: 0',axis = 1)

#Capitlising
df['location'] = df['location'].apply(lambda x: x.title())
df = df.drop('Unnamed: 0',axis =1)
df = df.drop('freq',axis =1)

# Removing noisy locations -> List of cities and countries
def pre_process(x):
    places = GeoText(x)
    if places.cities:
        return places.cities
    
    elif places.countries:
        return places.countries
    
    else:
        return np.NaN

df_2 = df.applymap(pre_process)
df_2.isnull().values.any()
df_final = df_2.dropna()

# Extracting City/Country , Barcelona, Spain --> Spain

def pre_process_again(x):
    
    return x[len(x)-1]

df_final_new = df_final.applymap(pre_process_again)
df = df_final_new
df.head()


country_list = []
dict_new = {}
for i in countries:
    x = i['name']
    y = i['code']
    country_list.append({'country':x,'code':y})


# list of all cities with country code information

gc = geonamescache.GeonamesCache()
cities = gc.get_cities()


# Map city/country to country code

def map_to_country(x):
    
    for i in country_list:
        y = i['country']  
        if x in y:
            return i['code']
        
        else:
            city = gc.get_cities_by_name(x)
            
            try:
                
                for j in city[0].values():
                    return j['countrycode']
            except IndexError:
                pass

df_2 = df.applymap(map_to_country)


df_2.isnull().values.any()
df_2 = df_2.dropna()

# Add country name

def add_country(x):
    
     for i in country_list:
            if x == i['code']:
                return x+','+i['country']


df_2 = df_2.applymap(add_country)


df_2.columns = ['code,country']


df_2 = df_2.dropna()


df_3 = df_2['code,country'].apply(lambda x: pd.Series(x.split(','))) # WHICH ONE?? THIS OR ABOVE


df_3.columns = ['code','country']


df = df_3


# Dataframe with original index number, code, country

df


# Read Dataframe with frequency for individual cities
# Merge, df and dframe with common id number and only retain frequency 
# Then, groupby country code and sum frequecny

dframe = pd.read_csv('barca_loc_freq_reindexed.csv')
dframe
df_test = df.join(dframe)
df_FINAL = df_test.drop(['Unnamed: 0','location'],axis = 1)

# Groupby frequency

df_FINAL = df_FINAL.groupby(['code','country'], as_index = False)[['freq']].sum()


# Plotly needs 3 letter country code. So converting 2 letter to 3 letter. Planning on removing this soon
pycountry.countries

l_2 = []
l_3 = []
for country in pycountry.countries:
    l_2.append(country.alpha_2)
    l_3.append(country.alpha_3)

df_x = pd.DataFrame({'code':l_2,'code_3':l_3})
df = pd.merge(df_FINAL,df_x, on='code', how='outer')


data = [ dict(
        type = 'choropleth',
        locations = df['code_3'],
        z = df['freq'],
        text = df['country'],
        colorscale = [[0,"rgb(5, 10, 172)"],[0.35,"rgb(40, 60, 190)"],[0.5,"rgb(70, 100, 245)"],\
            [0.6,"rgb(90, 120, 245)"],[0.7,"rgb(106, 137, 247)"],[1,"rgb(220, 220, 220)"]],
        autocolorscale = False,
        reversescale = True,
        marker = dict(
            line = dict (
                color = 'rgb(180,180,180)',
                width = 0.5
            ) ),
        colorbar = dict(
            autotick = False,
            tickprefix = '',
            title = 'Fans'),
      ) ]

layout = dict(
    title = 'Barcelona <3',
    geo = dict(
        showframe = False,
        showcoastlines = False,
        projection = dict(
            type = 'Mercator'
        )
    )
)

fig = dict( data=data, layout=layout )
plotly.offline.plot( fig, validate=False, filename='d3-world-map' )

