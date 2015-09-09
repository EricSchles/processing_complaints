import pandas as pd
import shapefile
# http://www.digital-geography.com/importing-shapefiles-in-python/#.VfBKBXUViko
from glob import glob
from shapely.geometry import Point, MultiPoint 
#check out http://streamhacker.com/2010/03/23/python-point-in-polygon-shapely/, http://toblerity.org/shapely/manual.html#points
from geopy.geocoders import Nominatim
# https://pypi.python.org/pypi/geopy

#where I got the data: http://www.zillow.com/howto/api/neighborhood-boundaries.htm

#This method transforms the shape file from zillow and finds all the points for the 
#different neighborhoods around the city
#This can easily be extended to find any neighborhood in the zillow dataset
#This method returns a list of geo objects which come from the shapefile object, check out pyshp for more info!
def grab_nyc_neighborhoods():
    filename = glob("*.shp")
    ctr = shapefile.Reader(filename[0])
    geomet = ctr.shapeRecords()
    nyc = []
    for geo in geomet:
        if any([place for place in geo.record if type(place) == type(str()) and "New York City" in place]):
           nyc.append(geo)
    return nyc


#This method determines whether or not a give point is inside a neighborhood by looping through all the neighborhoods
#and then just stating whether or not it's in the neighborhood or not
#We assume something went wrong if more than one neighborhood is returned which is handled in create_neighborhood_grouping
#neighborhoods is a list of geo objects which come from the shapefile object - check out pyshp for more info
def determine_neighborhood(nyc,location):
    point = Point((location["lat"],location["long"]))
    neighborhoods = []
    for geo in nyc:
        points = [tuple(ind_point) for ind_point in geo.shape.points]
        poly = MultiPoint(points).convex_hull
        if point.within(poly):
            neighborhoods.append(geo)
    return neighborhoods


#This method is the main workhorse of the program
#It figures out what neighborhood each point is in
#It then passes back a dataframe which will be saved when all pieces of information are processed
def create_neighborhood_grouping(nyc,locations):
    df = pd.DataFrame()
    for location in locations:
        tmp_dict = {}
        neighborhood = determine_neighborhood(nyc,location)
        if len(neighborhood) == 1:
            neighborhood = neighborhood[0]
            tmp_dict["neighborhood"] = neighborhood.record
            tmp_dict["location"] = location
            df = df.append(tmp_dict,ignore_index=True)
        else:
            print "something went wrong with finding a single neighborhood"
            for i in [hood.record for hood in nieghborhood]:
                print i
    return df


#This 'helper' method appends th,st,rd,nd to the end of a number
#this is because the address resolution api which translates an address into it's lat/long representation
#doesn't like just flat numbers, so this is my work around until I can find a better api
def append_ending(number):
    if number[-1] == "1":
        if len(number) >= 2 and number[-2] == "1":
            number += "th"
        else:
            number += "st"
    elif number[-1] == "2":
        if len(number) >= 2 and number[-2] == "1":
            number += "th"
        else:
            number += "nd"
    elif number[-1] == "3":
        if len(number) >= 2 and number[-2] == "1":
            number += "th"
        else:
            number += "rd"
        if number[-2] == "1":
            number += "th"
    else:
        number += "th"
    return number



#This method processes the address
#it acts on the digits of the street name and the digits of the house number
#there were a number of small bugs in the addresses I got from a few of these spreadsheets
#so a lot of this stuff probably won't carry over but I don't want to create yet another data cleaning method
#so I just stuck it in here
def process_address(addr):
    
    house_number = addr["house number"].split(" ")[0].split("/")[0].replace("'","")
    if house_number.isalpha():
        return "two cross streets"
    street_name = addr["street name"]
    
    street = ""
    for name in street_name.split(" "):
        if name == "nd" or name == "st" or name == "th":
            continue
        if name.isdigit():
            number = append_ending(name)
            street += number+ " "
        else:
            street += name.title() + " "
    return house_number + " " + street + "NYC"


#This method processes complaints, resolves their addresses into lat/long and then saves the location
def process_complaints(doc):
    geolocator = Nominatim()
    df = pd.read_excel(doc, "Facade, Brickwork, Exterior Wal", index_col=None, na_values=["NA"])
    addresses = []
    locations = []
    for i in df.index:
        addr = process_address({"house number":str(df.ix[i]["House Number"]), "street name":str(df.ix[i]["Street Name"])})
        if addr == "two cross streets":
            #I don't yet know how to handle the case with two cross streets yet, but i should fix this once I'm out of prototype stage
            continue
        addresses.append(addr)
    for addr in addresses:
        print addr
        loc = geolocator.geocode(addr)
        tmp = {}
        print loc
        tmp["lat"] = loc.latitude
        tmp["long"] = loc.longitude
        locations.append(tmp)
    return locations

#This method processes pluto data, resolves their addresses into lat/long and then saves the location
def process_pluto(doc):
    geolocator = Nominatim()
    df = pd.read_excel(doc, "Combined F&S", index_col=None, na_values=["NA"])
    addresses = []
    locations = []
    for i in df.index:
        addr = process_address({"house number":str(df.ix[i]["Address Number"]), "street name":str(df.ix[i]["Street"])})
        addresses.append(addr) #figure out how to get the full address
    for addr in addresses:
        print addr
        loc = geolocator.geocode(addr)
        tmp = {}
        print loc
        tmp["lat"] = loc.latitude
        tmp["long"] = loc.longitude
        locations.append(tmp)
    return locations

#As you can see here we have a main -> 
#The main gets the locations, saves them to a dataframe and then saves that dataframe to a csv
if __name__ == '__main__':
    nyc = grab_nyc_neighborhoods()
    locations = []
    df = pd.DataFrame()
    locations.append({"complaint_locations":process_complaints("FacadeWallComplaints_one.xlsx")})
    locations.append({"pluto_locations":process_pluto("plutoCleaned.xlsx")})
    for loc in locations:
        key = loc.keys()[0]
        df = df.append(create_neighborhood_grouping(nyc,loc[key]),ignore_index=True)
    df.to_csv("grouping.csv")
