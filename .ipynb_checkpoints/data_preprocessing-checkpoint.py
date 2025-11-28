#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Bodhi Global Analysis (Jungyeon Lee)

These data preprocessing scripts show how the data have been processed.
Note that some dataset paths were changed during preprocessing, so running the code all at once may cause errors. 
If you want to run the code, please update the dataset paths accordingly before executing.
"""

import pandas as pd
import numpy as np
import os

def country_only(df_sub, df_filtered, variable):
    df_filtered = df_filtered[["Country_code", "Year", variable]].copy()

    df_filtered = df_filtered.sort_values(["Country_code", "Year"])
    df_filtered[variable] = df_filtered.groupby("Country_code")[variable].ffill()
    df_merged = pd.merge(
        df_sub,
        df_filtered,
        on=["Country_code", "Year"],
        how="left"
    )
    
    df_merged[variable] = df_merged.groupby("Country_code")[variable].ffill()
    df_merged[variable] = df_merged.groupby("Country_code")[variable].bfill()
    print(len(df_merged))
    return df_merged

def adm1_only(df_sub, df_filtered, variables):
    cols = ["ADM1_code", "Year"] + variables
    df_filtered = df_filtered[cols].copy()
    df_filtered = df_filtered.sort_values(["ADM1_code", "Year"])

    df_filtered[variables] = df_filtered.groupby("ADM1_code")[variables].ffill()

    df_merged = pd.merge(
        df_sub,
        df_filtered,
        on=["ADM1_code", "Year"],
        how="left"
    )

    df_merged[variables] = df_merged.groupby("ADM1_code")[variables].ffill()
    df_merged[variables] = df_merged.groupby("ADM1_code")[variables].bfill()

    print(f"Merged rows: {len(df_merged)}")
    return df_merged

def process_population_data(df, year):
    result = df[['ADM1_PCODE', 'F_TL', 'M_TL']].copy()
    result['Year'] = year
    result.rename(columns={
        'ADM1_PCODE': 'ADM1_code',
        'F_TL': 'female_pop',
        'M_TL': 'male_pop'
    }, inplace=True)
    return result

year = [2020, 2021, 2022, 2023, 2024]

country_list = ["Burkina Faso", "Cameroon", "Chad", "Djibouti", "Eritrea", "Ethiopia", "Gambia", 
                "Guinea", "Kenya", "Mali", "Mauritania", "Niger", "Nigeria", "Senegal", "Somalia", 
                "South Sudan", "Sudan", "Uganda"]

country_code_mapping = {"Burkina Faso": "BF", "Cameroon": "CM", "Chad": "TD", "Djibouti": "DJ",
                        "Eritrea": "ER", "Ethiopia": "ET", "Gambia": "GM", "Guinea": "GN", "Kenya": "KE",
                        "Mali": "ML", "Mauritania": "MR", "Niger": "NE", "Nigeria": "NG", "Senegal": "SN",
                        "Somalia": "SO", "South Sudan": "SS", "Sudan": "SD", "Uganda": "UG"}

country_code_mapping_alpha3 = {"Burkina Faso": "BFA","Cameroon": "CMR","Chad": "TCD","Djibouti": "DJI",
    "Eritrea": "ERI","Ethiopia": "ETH","Gambia": "GMB","Guinea": "GIN","Kenya": "KEN","Mali": "MLI",
    "Mauritania": "MRT","Niger": "NER","Nigeria": "NGA","Senegal": "SEN","Somalia": "SOM","South Sudan": "SSD",
    "Sudan": "SDN","Uganda": "UGA"}

admin1_code_mapping = {"BF46": "Boucle du Mouhoun","BF47": "Cascades","BF13": "Centre","BF48": "Centre-Est",
    "BF49": "Centre-Nord","BF50": "Centre-Ouest","BF51": "Centre-Sud","BF52": "Est", "BF53": "Hauts-Bassins", "BF54": "Nord",
    "BF55": "Plateau-Central","BF56": "Sahel","BF57": "Sud-Ouest","CM001": "Adamawa", "CM002": "Centre","CM003": "East",
    "CM004": "Far-North","CM005": "Littoral","CM006": "North","CM007": "North-West","CM009": "South","CM010": "South-West",
    "CM008": "West","TD01": "Batha","TD02": "Borkou","TD03": "Chari-Baguirmi","TD04": "Guéra","TD05": "Hadjer-Lamis",
    "TD06": "Kanem","TD07": "Lac","TD08": "Logone Occidental","TD09": "Logone Oriental","TD10": "Mandoul","TD11": "Mayo-Kebbi Est",
    "TD12": "Mayo-Kebbi Ouest","TD13": "Moyen-Chari","TD14": "Ouaddaï","TD15": "Salamat","TD16": "Tandjilé","TD17": "Wadi Fira",
    "TD18": "N'Djamena","TD19": "Barh-El-Gazel","TD20": "Ennedi Est","TD21": "Sila","TD22": "Tibesti","TD23": "Ennedi Ouest",
    "DJ01": "Obock","DJ02": "Tadjoura","DJ03": "Dikhil","DJ04": "Djiboutii","DJ05": "Ali Sabieh","DJ06": "Arta",
    "ER4": "Anseba","ER6": "Debub","ER1": "Debubawi Keih Bahri","ER5": "Gash Barka","ER2": "Maekel","ER3": "Semienawi Keih Bahri",
    "ET14": "Addis Ababa","ET02": "Afar","ET03": "Amhara", "ET06": "Benishangul Gumz","ET15": "Dire Dawa","ET12": "Gambela",
    "ET13": "Harari","ET04": "Oromia","ET16": "Sidama","ET07": "SNNP","ET05": "Somali","ET11": "South West Ethiopia","ET01": "Tigray",
    "GM01": "Banjul City Council","GM07": "Central River North","GM04": "Central River South","GM05": "Kanifing Municipal Council",
    "GM08": "Lower River","GM06": "North Bank","GM02": "Upper River","GM03": "West Coast","GN001": "Boke","GN002": "Conakry",
    "GN003": "Faranah", "GN004": "Kankan","GN005": "Kindia","GN006": "Labe", "GN007": "Mamou","GN008": "Nzerekore",
    "KE030": "Baringo","KE036": "Bomet","KE039": "Bungoma","KE040": "Busia","KE028": "Elgeyo-Marakwet","KE014": "Embu",
    "KE007": "Garissa","KE043": "Homa Bay","KE011": "Isiolo","KE034": "Kajiado","KE037": "Kakamega","KE035": "Kericho",
    "KE022": "Kiambu","KE003": "Kilifi","KE020": "Kirinyaga","KE045": "Kisii","KE042": "Kisumu","KE015": "Kitui","KE002": "Kwale",
    "KE031": "Laikipia","KE005": "Lamu","KE016": "Machakos","KE017": "Makueni","KE009": "Mandera","KE010": "Marsabit",
    "KE012": "Meru","KE044": "Migori","KE001": "Mombasa","KE021": "Murang'a","KE047": "Nairobi","KE029": "Nandi",
    "KE033": "Narok","KE046": "Nyamira","KE018": "Nyandarua","KE019": "Nyeri","KE025": "Samburu","KE041": "Siaya",
    "KE006": "Taita Taveta","KE004": "Tana River","KE013": "Tharaka-Nithi","KE026": "Trans Nzoia","KE023": "Turkana",
    "KE027": "Uasin Gishu","KE038": "Vihiga","KE008": "Wajir", "KE024": "West Pokot", "KE032": "Nakuru","ML01": "Kayes",
    "ML02": "Koulikoro","ML03": "Sikasso","ML04": "Ségou","ML05": "Mopti","ML06": "Tombouctou",
    "ML07": "Gao","ML08": "Kidal","ML09": "Bamako","ML10": "Menaka","MR01": "Adrar","MR02": "Assaba","MR03": "Brakna",
    "MR04": "Dakhlet-Nouadhibou","MR05": "Gorgol","MR06": "Guidimakha","MR07": "Hodh El Chargi", "MR08": "Hodh El Gharbi",
    "MR09": "Inchiri","MR10": "Nouakchott","MR11": "Tagant", "MR12": "Trarza", "MR13": "Tris-Zemmour","NE007": "Zinder",
    "NE003": "Dosso","NE006": "Tillabéri","NE005": "Tahoua","NE001": "Agadez","NE008": "Niamey","NE002": "Diffa",
    "NE004": "Maradi","NG001": "Abia","NG002": "Adamawa","NG003": "Akwa Ibom","NG004": "Anambra","NG005": "Bauchi",
    "NG006": "Bayelsa","NG007": "Benue","NG008": "Borno","NG009": "Cross River","NG010": "Delta", "NG011": "Ebonyi","NG012": "Edo",
    "NG013": "Ekiti","NG014": "Enugu","NG015": "Federal Capital Territory","NG016": "Gombe","NG017": "Imo","NG018": "Jigawa",
    "NG019": "Kaduna","NG020": "Kano","NG021": "Katsina","NG022": "Kebbi","NG023": "Kogi","NG024": "Kwara","NG025": "Lagos",
    "NG026": "Nasarawa","NG027": "Niger","NG028": "Ogun","NG029": "Ondo", "NG030": "Osun","NG031": "Oyo","NG032": "Plateau",
    "NG033": "Rivers","NG034": "Sokoto","NG035": "Taraba","NG036": "Yobe","NG037": "Zamfara","SN01": "Dakar","SN02": "Diourbel",
    "SN03": "Fatick","SN04": "Kaffrine","SN05": "Kaolack","SN06": "Kedougou","SN07": "Kolda","SN08": "Louga","SN09": "Matam","SN10": "Saint Louis", "SN11": "Sedhiou", "SN12": "Tambacounda",
    "SN13": "Thies","SN14": "Ziguinchor","SO11": "Awdal","SO25": "Bakool","SO22": "Banadir","SO16": "Bari","SO24": "Bay",
    "SO19": "Galgaduud","SO26": "Gedo", "SO20": "Hiraan", "SO28": "Lower Juba", "SO23": "Lower Shabelle", "SO27": "Middle Juba", "SO21": "Middle Shabelle",
    "SO18": "Mudug","SO17": "Nugaal","SO15": "Sanaag","SO14": "Sool","SO13": "Togdheer","SO12": "Woqooyi Galbeed","SS01": "Central Equatoria",
    "SS02": "Eastern Equatoria", "SS03": "Jonglei","SS04": "Lakes","SS05": "Northern Bahr el Ghazal", "SS06": "Unity", "SS00" : "Abyei Region", 
    "SS07": "Upper Nile","SS08": "Warrap", "SS09": "Western Bahr el Ghazal","SS10": "Western Equatoria","SD19": "Abyei PCA",
    "SD15": "Aj Jazirah","SD08": "Blue Nile","SD06": "Central Darfur","SD05": "East Darfur","SD12": "Gedaref",
    "SD11": "Kassala","SD01": "Khartoum","SD02": "North Darfur","SD13": "North Kordofan","SD17": "Northern","SD10": "Red Sea",
    "SD16": "River Nile","SD14": "Sennar","SD03": "South Darfur","SD07": "South Kordofan","SD04": "West Darfur","SD18": "West Kordofan",
    "SD09": "White Nile","UG1": "Central","UG2": "Eastern","UG3": "Northern","UG4": "Western"}


data = [(country, country_code_mapping[country], y) for country in country_list for y in year]
df_country = pd.DataFrame(data, columns=["Country", "Country_code", "Year"])

adm1_df = pd.DataFrame([(code, name) for code, name in admin1_code_mapping.items()],columns=["ADM1_code", "ADM1"])
adm1_df["Country_code"] = adm1_df["ADM1_code"].str.extract(r"^([A-Z]{2})")
adm1_df["Country"] = adm1_df["Country_code"].map({v: k for k, v in country_code_mapping.items()})
df_sub = adm1_df.assign(key=1).merge(pd.DataFrame({"Year": year, "key": 1}), on="key").drop("key", axis=1)
df_sub = df_sub[["Country", "Country_code", "ADM1", "ADM1_code", "Year"]]

# ODA and ODF
file_path = "data/OECD/ODA_Total_Net_from_Official_Donors.csv"
df = pd.read_csv(file_path)
df_filtered = df[df['Year'].isin(year)]
df_filtered = df_filtered[df_filtered['Recipient'].isin(country_list)]
df_filtered = df_filtered[(df_filtered['Aid type'] == 'ODA: Total Net') & (df_filtered['Amount type'] == 'Current Prices (USD millions)') & (df_filtered['Donor'] == 'Official Donors, Total')]
df_filtered = df_filtered[["Recipient", "Year", "Value"]].copy()
df_filtered.rename(columns={"Recipient": "Country","Year": "Year","Value": "oda_total_net"}, inplace=True)
df_filtered["Country_code"] = df_filtered["Country"].map(country_code_mapping)
df_sub = country_only(df_sub, df_filtered, 'oda_total_net')

file_path2 = "data/OECD/ODF_Total_Net.csv"
df2 = pd.read_csv(file_path2)
df_filtered2 = df2[df2['Year'].isin(year)]
df_filtered2 = df_filtered2[df_filtered2['Recipient'].isin(country_list)]
df_filtered2 = df_filtered2[df_filtered2['Amount type'] == 'Current Prices (USD millions)']
df_filtered2 = df_filtered2[["Recipient", "Year", "Value"]].copy()
df_filtered2.rename(columns={"Recipient": "Country","Year": "Year","Value": "odf_total_net"}, inplace=True)
df_filtered2["Country_code"] = df_filtered2["Country"].map(country_code_mapping)
df_sub = country_only(df_sub, df_filtered2, 'odf_total_net')

# Population Data
countries_data = { "Burkina Faso": [2023, 2024], "Cameroon": [2023, 2024],
    "Chad": [2021, 2023], "Djibouti": [2009], "Eritrea": [2001], "Ethiopia": [2022, 2023],
    "Gambia": [2021], "Guinea": [2024], "Kenya": [2019], "Mali": [2022],
    "Mauritania": [2022], "Niger": [2022, 2023, 2024], "Nigeria": [2021, 2022],
    "Senegal": [2021, 2022], "Somalia": [2005], "South Sudan": [2022], "Sudan": [2022, 2024], "Uganda": [2023]}

special_year_map = {"Djibouti": 2020, "Eritrea": 2020, "Kenya": 2020, "Somalia": 2020,}
processed_data = []
for country, years in countries_data.items():
    for year_ in years:
        file_path = f"data/Subnational Population Statistics/{country}/{country}_{year_}.xlsx"
        df = pd.read_excel(file_path)

        process_year = special_year_map.get(country, year_)
        df_processed = process_population_data(df, process_year)

        df_processed["Country"] = country
        df_processed["Year"] = process_year

        processed_data.append(df_processed)
        
df_filtered3 = pd.concat(processed_data, ignore_index=True)
df_sub = adm1_only(df_sub, df_filtered3, ['female_pop','male_pop'])

# Humanitarian Needs
file_path4 = "data/UN OCHA - Humanitarian Needs.xlsx"
df4 = pd.read_excel(file_path4, sheet_name="Raw Data")
df_filtered4 = df4[df4['Year'].isin(year)]
df_filtered4 = df_filtered4[df_filtered4['Crisis Country'].isin(country_list)]
df_filtered4 = df_filtered4[df_filtered4['OUSG Metric'] == 'People in need']
df_filtered4 = df_filtered4.loc[df_filtered4.groupby(['Year', 'Country Code'])['Value'].idxmax()]
df_filtered4 = df_filtered4.reset_index(drop=True)
df_filtered4 = df_filtered4[["Crisis Country", "Year", "Value"]].copy()
df_filtered4.rename(columns={"Crisis Country": "Country","Year": "Year","Value": "people_in_need"}, inplace=True)
df_filtered4["Country_code"] = df_filtered4["Country"].map(country_code_mapping)

file_path4_e1 = "data/Eritrea_people_in_need.xlsx"
df4_e1 = pd.read_excel(file_path4_e1)
df4_e1['Year'] = df4_e1['Year'].astype(int)
df4_e1 = df4_e1[df4_e1['Year'].isin(year)]
cols_to_sum = ['Refugees','Asylum seekers','Other people in need of international protection',
    'Internally displaced persons','Stateless Persons','Others of concern to UNHCR','Host community']
df_people_in_need_e1 = (df4_e1.groupby('Year')[cols_to_sum].sum().sum(axis=1).reset_index(name='people_in_need'))
df_people_in_need_e1['Country'] = 'Eritrea'
df_people_in_need_e1["Country_code"] = df_people_in_need_e1["Country"].map(country_code_mapping)

file_path4_e2 = "data/Guinea_people_in_need.xlsx"
df4_e2 = pd.read_excel(file_path4_e2)
df4_e2['Year'] = df4_e2['Year'].astype(int)
df4_e2 = df4_e2[df4_e2['Year'].isin(year)]
df_people_in_need_e2 = (df4_e2.groupby('Year')[cols_to_sum].sum().sum(axis=1).reset_index(name='people_in_need'))
df_people_in_need_e2['Country'] = 'Guinea'
df_people_in_need_e2["Country_code"] = df_people_in_need_e2["Country"].map(country_code_mapping)
df_filtered4 = pd.concat([df_filtered4, df_people_in_need_e1, df_people_in_need_e2], ignore_index=True)
df_sub = country_only(df_sub, df_filtered4, 'people_in_need')

# INFORM Risk Index
file_path5 = "data/INFORM Risk Index.xlsx"
df5 = pd.read_excel(file_path5)

reverse_mapping = {v: k for k, v in country_code_mapping_alpha3.items()}
df5['Country'] = df5['Iso3'].map(reverse_mapping)
df_filtered5 = df5[df5['INFORMYear'].isin(year)]
df_filtered5 = df_filtered5[df_filtered5['Country'].isin(country_list)]
df_filtered5 = df_filtered5[df_filtered5['IndicatorName'] == 'INFORM Risk Index']
df_filtered5 = df_filtered5[["Country", "INFORMYear", "IndicatorScore"]].copy()
df_filtered5.rename(columns={"INFORMYear": "Year","IndicatorScore": "INFORM_risk"}, inplace=True)
df_filtered5["Country_code"] = df_filtered5["Country"].map(country_code_mapping)
df_sub = country_only(df_sub, df_filtered5, 'INFORM_risk')

# Conflict Data
conflict_2020 = "data/conflict/conflict_2020.xlsx"
conflict_2021 = "data/conflict/conflict_2021.xlsx"
conflict_2022 = "data/conflict/conflict_2022.xlsx"
conflict_2023 = "data/conflict/conflict_2023.xlsx"
conflict_2024 = "data/conflict/conflict_2024.xlsx"

df_c_2020 = pd.read_excel(conflict_2020)
df_c_2021 = pd.read_excel(conflict_2021)
df_c_2022 = pd.read_excel(conflict_2022)
df_c_2023 = pd.read_excel(conflict_2023)
df_c_2024 = pd.read_excel(conflict_2024)

df_conflicts = pd.concat([df_c_2020, df_c_2021, df_c_2022, df_c_2023, df_c_2024], ignore_index=True)

df_sub['conflict_count'] = 0

df_merged = df_sub.merge(
    df_conflicts[['Year', 'ADM1_PCODE', 'conflict_counts']],
    how='left',
    left_on=['ADM1_code', 'Year'],
    right_on=['ADM1_PCODE', 'Year']
)

df_merged['conflict_count'] = df_merged['conflict_counts'].fillna(0)

df_sub = df_merged.drop(columns=['ADM1_PCODE', 'conflict_counts'])

# SIPRI - Military Expenditure Database
file_path6 = "data/SIPRI - Military Expenditure Database.xlsx"
df6 = pd.read_excel(file_path6)
df_filtered6 = pd.melt(df6,id_vars=['Country'],var_name='Year',value_name='military_spending')
df_filtered6['Year'] = df_filtered6['Year'].astype(int)
df_filtered6 = df_filtered6[df_filtered6['Country'].isin(country_list)]
df_filtered6["Country_code"] = df_filtered6["Country"].map(country_code_mapping)
df_sub = country_only(df_sub, df_filtered6, 'military_spending')

# IPC - Acute Food Insecurity Country Data
file_path7 = "data/IPC - Acute Food Insecurity Country Data.xlsx"
df7 = pd.read_excel(file_path7)
df7['ipc_food_insecurity'] = df7['Phase 4'] + df7['Phase 5']
df_sub = adm1_only(df_sub, df7, ['ipc_food_insecurity'])
df_sub['ipc_food_insecurity'] = df_sub['ipc_food_insecurity'].fillna(0)

# Ibrahim Index of African Governance (IIAG)
file_path8 = "data/Ibrahim Index of African Governance (IIAG).xlsx"
df8 = pd.read_excel(file_path8)
df_filtered8 = df8[df8['Location'].isin(country_list)]
df_filtered8 = pd.melt(df_filtered8 ,id_vars=['Location'],var_name='Year',value_name='rule_of_law')
df_filtered8["Country_code"] = df_filtered8["Location"].map(country_code_mapping)
df_sub = country_only(df_sub, df_filtered8, 'rule_of_law')

# Subnational Gender Development Index
file_path9 = "data/Global Data Lab.xlsx"
df9 = pd.read_excel(file_path9)
df_filtered9 = df9[df9['Country'].isin(country_list)]
df_filtered9.to_excel('Global Data Lab_preprocessing.xlsx')
df_filtered9 = pd.read_excel("data/Global Data Lab_v2.xlsx")
df_filtered9 = df_filtered9[['GDLCODE', 2020, 2021, 2022]]
df_filtered9 = df_filtered9.rename(columns={'GDLCODE': 'ADM1_code'})
df_filtered9 = pd.melt(df_filtered9 ,id_vars=['ADM1_code'],var_name='Year',value_name='gender_devel')
df_sub = adm1_only(df_sub, df_filtered9, ['gender_devel'])
country_year_avg = df_sub.groupby(['Country', 'Year'])['gender_devel'].transform('mean')
df_sub['gender_devel'] = df_sub['gender_devel'].fillna(country_year_avg)
target_countries = ['Eritrea', 'Kenya', 'South Sudan']
yearly_avg = df_sub.groupby('Year')['gender_devel'].mean()
def fill_gender_devel(row):
    if row['Country'] in target_countries and pd.isna(row['gender_devel']):
        return yearly_avg.loc[row['Year']]
    else:
        return row['gender_devel']
df_sub['gender_devel'] = df_sub.apply(fill_gender_devel, axis=1)

# Subnational Multidimensional Poverty Index
file_path10_20 = "data/OPHI - Global MPI/Subnational Results MPI 2020.xlsx"
df10_20 = pd.read_excel(file_path10_20)
df10_20 = df10_20[df10_20['Country'].isin(country_list)]
df10_20.to_excel('SGDI_2020.xlsx')

file_path10_21 = "data/OPHI - Global MPI/Subnational Results MPI 2021.xlsx"
df10_21= pd.read_excel(file_path10_21)
df10_21 = df10_20[df10_21['Country'].isin(country_list)]
df10_21.to_excel('SGDI_2021.xlsx')

file_path10_22 = "data/OPHI - Global MPI/Subnational Results MPI 2022.xlsx"
df10_22= pd.read_excel(file_path10_22)
df10_22 = df10_22[df10_22['Country'].isin(country_list)]
df10_22.to_excel('SGDI_2022.xlsx')

file_path10_24 = "data/OPHI - Global MPI/Subnational Results MPI 2024.xlsx"
df10_24= pd.read_excel(file_path10_24)
df10_24 = df10_24[df10_24['Country'].isin(country_list)]
df10_24.to_excel('SGDI_2024.xlsx')

file_path10_20 = "data/OPHI - Global MPI/SGDI_2020.xlsx"
df10_20 = pd.read_excel(file_path10_20)

file_path10_21 = "data/OPHI - Global MPI/SGDI_2021.xlsx"
df10_21= pd.read_excel(file_path10_21)

file_path10_22 = "data/OPHI - Global MPI/SGDI_2022.xlsx"
df10_22= pd.read_excel(file_path10_22)

file_path10_24 = "data/OPHI - Global MPI/SGDI_2024.xlsx"
df10_24= pd.read_excel(file_path10_24)

cols = ['Year', 'ADM1_code', 'Multidimensional Poverty Index']

df10_20_sel = df10_20[cols]
df10_21_sel = df10_21[cols]
df10_22_sel = df10_22[cols]
df10_24_sel = df10_24[cols]

df_merged = pd.concat([df10_20_sel, df10_21_sel, df10_22_sel, df10_24_sel], ignore_index=True)
df_merged = df_merged.dropna()
df_merged = df_merged.rename(columns={'Multidimensional Poverty Index': 'mpi'})

df_sub = adm1_only(df_sub, df_merged, ['mpi'])
df_sub['mpi'] = df_sub.groupby('Year')['mpi'].transform(lambda x: x.fillna(x.mean()))

# ND-GAIN Country Index
file_path11_ready = "data/ND-GAIN Country Index_2025/resources/readiness/readiness.csv"
file_path11_vul = "data/ND-GAIN Country Index_2025/resources/vulnerability/vulnerability.csv"
df11_ready = pd.read_csv(file_path11_ready)
df11_vul = pd.read_csv(file_path11_vul)
df_filtered11_r = df11_ready[['Name', '2020', '2021', '2022', '2023']]
df_filtered11_r = df_filtered11_r[df_filtered11_r['Name'].isin(country_list)]
df_filtered11_r = pd.melt(df_filtered11_r ,id_vars=['Name'],var_name='Year',value_name='nd_readiness')
df_filtered11_r['Year'] = df_filtered11_r['Year'].astype(int)
df_filtered11_r["Country_code"] = df_filtered11_r["Name"].map(country_code_mapping)
df_sub = country_only(df_sub, df_filtered11_r, 'nd_readiness')
df_filtered11_v = df11_vul[['Name', '2020', '2021', '2022', '2023']]
df_filtered11_v = df_filtered11_v[df_filtered11_v['Name'].isin(country_list)]
df_filtered11_v = pd.melt(df_filtered11_v ,id_vars=['Name'],var_name='Year',value_name='nd_vulnerability')
df_filtered11_v['Year'] = df_filtered11_v['Year'].astype(int)
df_filtered11_v["Country_code"] = df_filtered11_v["Name"].map(country_code_mapping)
df_sub = country_only(df_sub, df_filtered11_v, 'nd_vulnerability')
df_sub['nd_readiness'] = df_sub.groupby('Year')['nd_readiness'].transform(lambda x: x.fillna(x.mean()))
df_sub['nd_vulnerability'] = df_sub.groupby('Year')['nd_vulnerability'].transform(lambda x: x.fillna(x.mean()))

# World Bank - Climate Change Indicators: Foreign direct investment, net inflows (% of GDP)
year = [2020, 2021, 2022, 2023, 2024]

country_list = ["Burkina Faso", "Cameroon", "Chad", "Djibouti", "Eritrea", "Ethiopia", "Gambia", 
                "Guinea", "Kenya", "Mali", "Mauritania", "Niger", "Nigeria", "Senegal", "Somalia", 
                "South Sudan", "Sudan", "Uganda"]

base_path = "data/World Bank/Climate Change Indicators"

all_dfs = []

for country in country_list:
    file_path = os.path.join(base_path, f"{country}.csv")

    try:
        df = pd.read_csv(file_path).iloc[1:].reset_index(drop=True)
        df['Year'] = df['Year'].astype(int)
        df = df[df['Year'].isin(year)]
        df = df[df['Indicator Name'] == 'Foreign direct investment, net inflows (% of GDP)']
        df.rename(columns={"Value": "foreign_invest"}, inplace=True)
        df = df[['Country Name', 'Year', 'foreign_invest']]

        all_dfs.append(df)

    except Exception as e:
        print("error")

df12_all = pd.concat(all_dfs, ignore_index=True)
df12_all["Country_code"] = df12_all["Country Name"].map(country_code_mapping)
df_sub = country_only(df_sub, df12_all, 'foreign_invest')

# World Bank - Agriculture and Rural Development
year = [2020, 2021, 2022, 2023, 2024]

country_list = ["Burkina Faso", "Cameroon", "Chad", "Djibouti", "Eritrea", "Ethiopia", "Gambia", 
                "Guinea", "Kenya", "Mali", "Mauritania", "Niger", "Nigeria", "Senegal", "Somalia", 
                "South Sudan", "Sudan", "Uganda"]

base_path = "data/World Bank/Agriculture and Rural Development"

all_dfs = []

for country in country_list:
    file_path = os.path.join(base_path, f"{country}.csv")

    try:
        df = pd.read_csv(file_path).iloc[1:].reset_index(drop=True)
        df['Year'] = df['Year'].astype(int)
        df = df[df['Year'].isin(year)]
        df = df[df['Indicator Name'] == 'Rural population (% of total population)']
        df.rename(columns={"Value": "rural_population"}, inplace=True)
        df = df[['Country Name', 'Year', 'rural_population']]

        all_dfs.append(df)

    except Exception as e:
        print("error")
        
df13_a = pd.concat(all_dfs, ignore_index=True)

all_dfs = []

for country in country_list:
    file_path = os.path.join(base_path, f"{country}.csv")

    try:
        df = pd.read_csv(file_path).iloc[1:].reset_index(drop=True)
        df['Year'] = df['Year'].astype(int)
        df = df[df['Year'].isin(year)]
        df = df[df['Indicator Name'] == 'Food production index (2014-2016 = 100)']
        df.rename(columns={"Value": "fpi"}, inplace=True)
        df = df[['Country Name', 'Year', 'fpi']]

        all_dfs.append(df)

    except Exception as e:
        print("error")
        
df13_b = pd.concat(all_dfs, ignore_index=True)

all_dfs = []

for country in country_list:
    file_path = os.path.join(base_path, f"{country}.csv")

    try:
        df = pd.read_csv(file_path).iloc[1:].reset_index(drop=True)
        df['Year'] = df['Year'].astype(int)
        df = df[df['Year'].isin(year)]
        df = df[df['Indicator Name'] == 'Livestock production index (2014-2016 = 100)']
        df.rename(columns={"Value": "lpi"}, inplace=True)
        df = df[['Country Name', 'Year', 'lpi']]

        all_dfs.append(df)

    except Exception as e:
        print("error")
        
df13_c = pd.concat(all_dfs, ignore_index=True)
df13_a["Country_code"] = df13_a["Country Name"].map(country_code_mapping)
df13_b["Country_code"] = df13_b["Country Name"].map(country_code_mapping)
df13_c["Country_code"] = df13_c["Country Name"].map(country_code_mapping)

df_sub = country_only(df_sub, df13_a, 'rural_population')
df_sub = country_only(df_sub, df13_b, 'fpi')
df_sub = country_only(df_sub, df13_c, 'lpi')

# WFP - Rainfall Indicators at Subnational Level

base_path = "data/WFP - Rainfall Indicators at Subnational Level"
all_dfs = []
for country in country_list:
    file_path = os.path.join(base_path, f"{country}.csv")
    try:
        df = pd.read_csv(file_path)
        df['Year'] = df['date'].astype(str).str[:4].astype(int)
        df = df[df['Year'].isin(year)]
        df = df.rename(columns={'PCODE': 'ADM1_code'})
        df = df[df['ADM1_code'].isin(admin1_code_mapping.keys())].reset_index(drop=True)
        df_grouped = df.groupby(['ADM1_code', 'Year'], as_index=False)['rfh_avg'].sum()
        df_grouped['Country'] = country
        all_dfs.append(df_grouped)
        
    except Exception as e:
        print("error")

df14_all = pd.concat(all_dfs, ignore_index=True)
df_sub = adm1_only(df_sub, df14_all, ['rfh_avg'])
df_sub['rfh_avg'] = df_sub.groupby(['Country', 'Year'])['rfh_avg'].transform(lambda x: x.fillna(x.mean()))
df_sub['rfh_avg'] = df_sub.groupby('Country')['rfh_avg'].transform(lambda x: x.fillna(x.mean()))
overall_mean = df_sub['rfh_avg'].mean()
df_sub['rfh_avg'] = df_sub['rfh_avg'].fillna(overall_mean)

# EM-DAT -  Emergency Events Database
file_path15 = "data/EM-DAT -  Emergency Events Database.xlsx"
df15 = pd.read_excel(file_path15)
df15['Start Year'] = df15['Start Year'].astype(int)
df15 = df15[df15['Start Year'].isin(year)]
disaster_types = ['Flood', 'Storm', 'Earthquake', 'Drought']
df15 = df15[df15['Disaster Type'].isin(disaster_types)].reset_index(drop=True)
df15 = df15[df15['Country'].isin(country_list)]

file_path15_2 = "data/EM-DAT -  Emergency Events Database_processed.xlsx"
df15_p = pd.read_excel(file_path15_2, index_col=None)
df_counts = (df15_p.groupby(['ADM1_code', 'Start Year', 'Disaster Type']).size().reset_index(name='counts'))
df_15_final = df_counts.pivot_table( index=['ADM1_code', 'Start Year'],columns='Disaster Type',  values='counts',
    fill_value=0).reset_index()
df_15_final.columns.name = None
df_15_final.columns = ['ADM1_code', 'Year', 'c_drought', 'c_flood', 'c_storm']
df_sub = adm1_only(df_sub, df_15_final, ['c_drought', 'c_flood', 'c_storm'])
df_sub[['c_drought', 'c_flood', 'c_storm']] = df_sub[['c_drought', 'c_flood', 'c_storm']].fillna(0)

# Coordinate
file_path17 = "data/coordinate.xlsx"
df17 = pd.read_excel(file_path17)
df17 = df17.drop_duplicates(subset=['ADM1_PCODE'], keep='first')
df17.rename(columns={"ADM1_PCODE": "ADM1_code"}, inplace=True)
df_sub = adm1_only(df_sub, df17, ['longitude', 'latitude'])

df_sub.to_excel("data/Adenium_Dataset.xlsx")

# Conflict (ACLED)
file_path = "data/Adenium_Dataset.xlsx"
df = pd.read_excel(file_path)

conflict_2020 = "data/conflict/conflict_2020_acled.xlsx"
conflict_2021 = "data/conflict/conflict_2021_acled.xlsx"
conflict_2022 = "data/conflict/conflict_2022_acled.xlsx"
conflict_2023 = "data/conflict/conflict_2023_acled.xlsx"
conflict_2024 = "data/conflict/conflict_2024_acled.xlsx"

df_c_2020 = pd.read_excel(conflict_2020)
df_c_2021 = pd.read_excel(conflict_2021)
df_c_2022 = pd.read_excel(conflict_2022)
df_c_2023 = pd.read_excel(conflict_2023)
df_c_2024 = pd.read_excel(conflict_2024)

df_conflicts = pd.concat([df_c_2020, df_c_2021, df_c_2022, df_c_2023, df_c_2024], ignore_index=True)

df_sub['conflict_count_acled'] = 0

df_merged = df_sub.merge(
    df_conflicts[['Year', 'ADM1_PCODE', 'conflict_counts']],
    how='left',
    left_on=['ADM1_code', 'Year'],
    right_on=['ADM1_PCODE', 'Year']
)

df_merged['conflict_count_acled'] = df_merged['conflict_counts'].fillna(0)
df_sub = adm1_only(df, df_merged, ['conflict_count_acled'])
df_sub.drop(columns =  'conflict_count', inplace = True)

df_sub.to_excel("data/Adenium_Dataset2.xlsx")