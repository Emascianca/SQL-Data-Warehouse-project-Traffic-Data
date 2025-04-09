import csv
from collections import Counter
from rapidfuzz import process, fuzz
import os
import numpy as np
import random
import pickle

# Setting the right directory
os.chdir(r"C:\Users\crist\OneDrive\Documenti\Desktop\UNI CRISTIAN\LM 2 S1\LDS\lds_project")
print("Directory corrente:", os.getcwd())


#The list correct_cities contains the top 25 cities by frequency (in people.csv, column:CITY)
correct_cities = {
    "chicago", "unknown", "cicero", "skokie", "evanston", "berwyn",
    "calumet city", "oak lawn", "oak park", "des plaines", "dolton",
    "elmwood park", "burbank", "south holland", "maywood", 
    "evergreen park", "aurora", "blue island", "naperville", 
    "lansing", "bellwood", "hammond", "niles", "bolingbrook", 
    "glenview", "riverdale"
}

#Loading list_cities that contains a list of real cities in USA
with open("list_cities", "rb") as file:
    real_cities = pickle.load(file)

#Loading vehicles.csv in order to match those vehicle_id created during the vehicle data cleaning phase
    #RECALL: in vehicle data cleaning, we filled the missing values in vehicle_id only when the person was a driver or a non-contact-vehicle
    #HOW? giving them an incremental id starting from the highest value of vehicle_id
with open("Vehicles-Copy1.csv", mode='r', encoding='utf-8-sig') as vhc:
    df_vhc = list(csv.DictReader(vhc, delimiter=","))
    vhc_index = {row['CRASH_UNIT_ID']: row for row in df_vhc} 

#Loading people.csv 
with open("People.csv", mode='r', encoding='utf-8-sig') as ppl:
    df_ppl = list(csv.DictReader(ppl, delimiter=","))

    i = 0
    remove_dup = []        # Indices of duplicates to remove
    unique_id_set = set()  # Set to track unique PERSON_IDs
    set_cities = set()     # Set to collect city names
    correct_ages = list()  # List to store valid ages

    for row in df_ppl:
        # Replacing missing values with 0's
        if row['AGE'] == '': row['AGE'] = 0

        # Using PERSON_ID as a key; keeping only the first duplicated record and drop the others
        if row['PERSON_ID'] not in unique_id_set:
            unique_id_set.add(row['PERSON_ID'])
        else: 
            remove_dup.append(i)
        i+=1
        
        # Filtering valid ages: greater than 15 or between 1 and 15 if the person type is not DRIVER
        if float(row['AGE']) > 15 or (float(row['AGE']) in range(1,16) and row['PERSON_TYPE'] != 'DRIVER'):
            correct_ages.append(float(row['AGE']))

    # Calculating the age distribution (only for the valid ages)    
    ages_np = np.array(correct_ages)
    unique_ages, counts = np.unique(ages_np, return_counts=True)
    proportion = counts / len(ages_np)

    # Removing duplicate records 
    df_ppl = np.delete(df_ppl, remove_dup)
            
    #Filling in missing values and fixing errors
    for row in df_ppl:
        
        #Incremental IDs taken from vehicles data cleaning phase 
        if row['VEHICLE_ID'] == '' and row['PERSON_TYPE'] in ['DRIVER', 'NON-CONTACT VEHICLE']:
            current_id = row['PERSON_ID'][1:]
            vehicle_row = vhc_index.get(current_id) 
            row['VEHICLE_ID'] = vehicle_row['VEHICLE_ID']
        
        # Dropping CRASH DATE column
        del row['CRASH_DATE']
        
        # Filling in missing or invalid values in various columns
        if row['SEX'] == '' or row['SEX'] == 'U': row['SEX'] = 'X'
        if row['CITY'] == '' or row['CITY'].isdigit() or row['CITY'] == 'UNK' or not row['CITY'].strip(): row['CITY'] = 'UNKNOWN' 
        if row['CITY'] == 'CHGO': row['CITY'] = 'CHICAGO'
        if row['AGE'] == '' or float(row['AGE']) == 0 or (float(row['AGE'])<15 and row['PERSON_TYPE']=='DRIVER'):
            row['AGE'] = np.random.choice(unique_ages, p=proportion)
        if row['STATE'] == '': row['STATE'] = 'XX'
        if row['EJECTION'] == '': row['EJECTION'] = 'UNKNOWN'
        if row['AIRBAG_DEPLOYED'] == '': row['AIRBAG_DEPLOYED'] = 'DEPLOYMENT UNKNOWN'
        if row['SAFETY_EQUIPMENT'] == '': row['SAFETY_EQUIPMENT'] = 'USAGE UNKNOWN'
        if row['INJURY_CLASSIFICATION'] == '': row['INJURY_CLASSIFICATION'] = 'NO INDICATION OF INJURY'
        if row['DRIVER_ACTION'] == '': row['DRIVER_ACTION'] = 'UNKNOWN'
        if row['DRIVER_VISION'] == '': row['DRIVER_VISION'] = 'UNKNOWN'
        if row['PHYSICAL_CONDITION'] == '': row['PHYSICAL_CONDITION'] = 'UNKNOWN'
        if row['BAC_RESULT'] == '': row['BAC_RESULT'] = 'UNKNOWN'
        if row['DAMAGE'] == '': row['DAMAGE'] = random.uniform(0, 500)
        row['DAMAGE'] = round(float(row['DAMAGE']), 2)
        
        if row['VEHICLE_ID'] == '': row['VEHICLE_ID'] = row['PERSON_TYPE']

        
        # Fixing spelling errors in the CITY column
        # HOW? if a city exists (because it is in real_cities list) then it's fine.
            #If not, then compare each wrong city with each element in the list of the top 25 cities by frequency.
            #If the similarity score is greater than 80 then replace with the correct name.
            
        if row['CITY'].strip() in real_cities: 
            continue
        else:
            match = process.extractOne(row['CITY'].lower().strip(), correct_cities, scorer=fuzz.token_set_ratio)
            if match and match[1] >= 80:  
                corrected_city = match[0].upper()  
                #print(f"Correcting city {row['CITY']} to {corrected_city}")
                row['CITY'] = corrected_city  # Updating the name
   


# Writing the cleaned dataset
with open("People-Copy-Final.csv", mode='w', encoding='utf-8-sig', newline='') as updated_file:
    
    writer = csv.DictWriter(updated_file, fieldnames=df_ppl[0].keys())
    writer.writeheader()
    writer.writerows(df_ppl)





