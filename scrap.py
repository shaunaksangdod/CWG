import pandas,os,errno
import timeit as t
import global_data
import numpy as np
import glob
import collections
import csv
import json
import csv_to_json as cj
import math


p_data = pandas.read_csv('data/p_dataframe.csv')
h_data = pandas.read_csv('data/h_dataframe.csv')

region_1 = [2001,2002,2100,1900,1801,1802,1700,300]
region_2 = [704, 1500, 701, 702, 703, 600]
region_3 = [1000,1300,901,902,903,904,905,906,1400,800]
region_4 = [3201,3202,3203,3204,3205,3206,3207,3208,3209,3210,3211,3212,3301,3302,3303,3304,3305,3306,3307,3308,3309,3310,3311,3312,3313]
region_5 = [2801,2802,2901,2902,2903,3101,3001,3002,3003,2701,2702,3102,3103,3104,3105,3106,3107]
region_6 = [1600,401,402,403]
region_7 = [3701,3702,3703,3704,3705,3706,3707,3708,3709,3710,3801,3802,3803,3804,3805,3806,3807,3808,3809,3810,3901,3902,3903,4001,4002,4003,4004,4005,4006,4007,4008,4009,4010,4011,4012,4013,4014,4015,4016,4017,4018,4101,4102,4103,4104,4105,4106,4107,4108,4109,4110,4111,4112,4113,4114]
region_8 = [200,500,100]
region_9 = [2201,2202,2401,2203,2402,2300]
region_10= [2500,2600,1201,1202,1203,1204,1205,1206,1207,1101,1102]


#The counties for the 10 PUMA Regions
puma_maps_dict={100:"St. Lawrence County PUMA",200:"Clinton, Franklin, Essex & Hamilton Counties PUMA",300:"Warren & Washington Counties PUMA",
                    401:"Herkimer (North & Central) & Oneida (Outer) Counties PUMA",402:"Oneida County (Central)--Greater Utica & Rome Cities PUMA",
                    403:"Otsego, Schoharie, Oneida (South) & Herkimer (South) Counties PUMA",500:"Jefferson & Lewis Counties PUMA",600:"Oswego County PUMA",
                    701:"Onondaga County (Central)--Syracuse City PUMA",702:"Onondaga County (North) PUMA",703:"Onondaga County (Central)--Syracuse City (Outer) PUMA",
                    704:"Cayuga & Onondaga (South) Counties PUMA",800:"Wayne & Seneca Counties PUMA",901:"Monroe County (East) PUMA",902:"Monroe County (Central)--Rochester City (East) PUMA",
                    903:"Monroe County (Central)--Rochester City (West) PUMA",904:"Monroe County (Central)--Greece & Gates Towns PUMA",905:"Monroe County (North & West) PUMA",906:"Monroe County (South) PUMA",
                    1000:"Genesee & Orleans Counties PUMA",1101:"Niagara County (Southwest)--Greater Niagara Falls & North Tonawanda Area PUMA",1102:"Niagara County (North & East) PUMA",1201:"Erie County (Northwest) PUMA",
                    1202:"Erie County (North Central) PUMA",1203:"Erie County (Northeast) PUMA",1204:"Erie County (Central) PUMA",1205:"Erie County (West Central)--Buffalo City (East) PUMA",1206:"Erie County (West Central)--Buffalo City (West) PUMA",
                    1207:"Erie County (South) PUMA",1300:"Livingston & Wyoming Counties PUMA",1400:"Ontario & Yates Counties PUMA",1500:"Madison & Cortland Counties PUMA",
                    1600:"Fulton & Montgomery Counties PUMA",1700:"Schenectady County--Schenectady City PUMA",1801:"Saratoga County (South & Central) PUMA",1802:"Saratoga County (Outer) PUMA",1900:"Rensselaer County--Troy City PUMA",
                    2001:"Albany County (East Central)--Albany City PUMA",2002:"Albany County (Outside Albany City) PUMA",2100:"Columbia & Greene Counties PUMA",2201:"Broome County (West Central)--Greater Binghamton City & Greater Johnson City Village PUMA",
                    2202:"Broome (Outer West) & Tioga Counties PUMA",2203:"Chenango, Delaware & Broome (East) Counties PUMA",2300:"Tompkins County PUMA",2401:"Chemung (South) & Steuben (East) Counties--Greater Elmira & Greater Corning Cities PUMA",
                    2402:"Steuben (North & West), Schuyler & Chemung (North) Counties PUMA",2500:"Cattaraugus & Allegany Counties PUMA",2600:"Chautauqua County PUMA",2701:"Sullivan & Ulster (West) Counties PUMA",2702:"Ulster County (East) PUMA",
                    2801:"Dutchess County (North & East) PUMA",2802:"Dutchess County (Southwest) PUMA",2901:"Orange County (Northeast)--Greater Newburgh City PUMA",2902:"Orange County (Northwest) PUMA",2903:"Orange County (Southeast) PUMA",
                    3001:"Rockland County (North)--New City & Congers PUMA",3002:"Rockland County (South)--Orangetown, Clarkstown (South) & Ramapo (Southeast) Towns PUMA",3003:"Rockland County (West)--Spring Valley, Suffern Villages & Monsey PUMA",
                    3101:"Putnam County PUMA",3102:"Westchester County (Northwest) PUMA",3103:"Westchester County (Northeast) PUMA",3104:"Westchester County (Southeast) PUMA",3105:"Westchester County (Central)--White Plains City PUMA",3106:"Westchester County (Southwest)--Yonkers City PUMA",
                    3107:"Westchester County (South Central)--New Rochelle & Mount Vernon Cities PUMA",3201:"Nassau County (Northwest)--North Hempstead Town (North) PUMA",3202:"Nassau County (Northeast)--Oyster Bay Town (North) & Glen Cove City PUMA",
                    3203:"Nassau County (East Central)--Oyster Bay Town (Central) PUMA",3204:"Nassau County (West Central)--North Hempstead Town (South) PUMA",3205:"Nassau County (West Central)--Hempstead Town (Northwest) PUMA",3206:"Nassau County (Central)--Hempstead Town (North Central)--Meadowbrook Corridor PUMA",
                    3207:"Nassau County (Central)--Hempstead Town (Northeast) PUMA",3208:"Nassau County (Southeast)--Oyster Bay Town (South) PUMA",3209:"Nassau County (Central)--Hempstead Town (East Central) PUMA",3210:"Nassau County (South Central)--Hempstead Town (Southeast) PUMA",3211:"Nassau County (West Central)--Hempstead Town (West Central) PUMA",
                    3212:"Nassau County (Southwest)--Hempstead Town (Southwest) & Long Beach City PUMA",3301:"Suffolk County (Northwest)--Huntington Town (North) PUMA",3302:"Suffolk County (Northwest)--Huntington Town (South) PUMA",3303:"Suffolk County (Northwest)--Smithtown Town PUMA",3304:"Suffolk County (North Central)--Brookhaven Town (North) PUMA",
                    3305:"Suffolk County (East) PUMA",3306:"Suffolk County (South Central)--Brookhaven Town (South) PUMA",3307:"Suffolk County (Central)--Brookhaven Town (Central) PUMA",3308:"Suffolk County (Central)--Brookhaven Town (West Central) PUMA",3309:"Suffolk County (Central)--Islip Town (East) PUMA",3310:"Suffolk County (Central)--Islip Town (Northwest) PUMA",
                    3311:"Suffolk County (Southwest)--Islip Town (South) PUMA",3312:"Suffolk County (Southwest)--Babylon Town (Southeast) PUMA",3313:"Suffolk County (West Central)--Babylon Town (Northwest) PUMA",3701:"NYC-Bronx Community District 8--Riverdale, Fieldston & Kingsbridge PUMA",3702:"NYC-Bronx Community District 12--Wakefield, Williamsbridge & Woodlawn PUMA",
                    3703:"NYC-Bronx Community District 10--Co-op City, Pelham Bay & Schuylerville PUMA",3704:"NYC-Bronx Community District 11--Pelham Parkway, Morris Park & Laconia PUMA",3705:"NYC-Bronx Community District 3 & 6--Belmont, Crotona Park East & East Tremont PUMA",3706:"NYC-Bronx Community District 7--Bedford Park, Fordham North & Norwood PUMA",3707:"NYC-Bronx Community District 5--Morris Heights, Fordham South & Mount Hope PUMA",
                    3708:"NYC-Bronx Community District 4--Concourse, Highbridge & Mount Eden PUMA",3709:"NYC-Bronx Community District 9--Castle Hill, Clason Point & Parkchester PUMA",3710:"NYC-Bronx Community District 1 & 2--Hunts Point, Longwood & Melrose PUMA",3801:"NYC-Manhattan Community District 12--Washington Heights, Inwood & Marble Hill PUMA",
                    3802:"NYC-Manhattan Community District 9--Hamilton Heights, Manhattanville & West Harlem PUMA",3803:"NYC-Manhattan Community District 10--Central Harlem PUMA",3804:"NYC-Manhattan Community District 11--East Harlem PUMA",3805:"NYC-Manhattan Community District 8--Upper East Side PUMA",3806:"NYC-Manhattan Community District 7--Upper West Side & West Side PUMA",3807:"NYC-Manhattan Community District 4 & 5--Chelsea, Clinton & Midtown Business District PUMA",
                    3808:"NYC-Manhattan Community District 6--Murray Hill, Gramercy & Stuyvesant Town PUMA",3809:"NYC-Manhattan Community District 3--Chinatown & Lower East Side PUMA",3810:"NYC-Manhattan Community District 1 & 2--Battery Park City, Greenwich Village & Soho PUMA",3901:"NYC-Staten Island Community District 3--Tottenville, Great Kills & Annadale PUMA",3902:"NYC-Staten Island Community District 2--New Springville & South Beach PUMA",3903:"NYC-Staten Island Community District 1--Port Richmond, Stapleton & Mariner's Harbor PUMA",
                    4001:"NYC-Brooklyn Community District 1--Greenpoint & Williamsburg PUMA",4002:"NYC-Brooklyn Community District 4--Bushwick PUMA",4003:"NYC-Brooklyn Community District 3--Bedford-Stuyvesant PUMA",4004:"NYC-Brooklyn Community District 2--Brooklyn Heights & Fort Greene PUMA",4005:"NYC-Brooklyn Community District 6--Park Slope, Carroll Gardens & Red Hook PUMA",4006:"NYC-Brooklyn Community District 8--Crown Heights North & Prospect Heights PUMA",
                    4007:"NYC-Brooklyn Community District 16--Brownsville & Ocean Hill PUMA",4008:"NYC-Brooklyn Community District 5--East New York & Starrett City PUMA",4009:"NYC-Brooklyn Community District 18--Canarsie & Flatlands PUMA",4010:"NYC-Brooklyn Community District 17--East Flatbush, Farragut & Rugby PUMA",4011:"NYC-Brooklyn Community District 9--Crown Heights South, Prospect Lefferts & Wingate PUMA",
                    4012:"NYC-Brooklyn Community District 7--Sunset Park & Windsor Terrace PUMA",4013:"NYC-Brooklyn Community District 10--Bay Ridge & Dyker Heights PUMA",4014:"NYC-Brooklyn Community District 12--Borough Park, Kensington & Ocean Parkway PUMA",4015:"NYC-Brooklyn Community District 14--Flatbush & Midwood PUMA",
                    4016:"NYC-Brooklyn Community District 15--Sheepshead Bay, Gerritsen Beach & Homecrest PUMA",4017:"NYC-Brooklyn Community District 11--Bensonhurst & Bath Beach PUMA",4018:"NYC-Brooklyn Community District 13--Brighton Beach & Coney Island PUMA",4101:"NYC-Queens Community District 1--Astoria & Long Island City PUMA",4102:"NYC-Queens Community District 3--Jackson Heights & North Corona PUMA",4103:"NYC-Queens Community District 7--Flushing, Murray Hill & Whitestone PUMA",4104:"NYC-Queens Community District 11--Bayside, Douglaston & Little Neck PUMA",
                    4105:"NYC-Queens Community District 13--Queens Village, Cambria Heights & Rosedale PUMA",4106:"NYC-Queens Community District 8--Briarwood, Fresh Meadows & Hillcrest PUMA",4107:"NYC-Queens Community District 4--Elmhurst & South Corona PUMA",4108:"NYC-Queens Community District 6--Forest Hills & Rego Park PUMA",4109:"NYC-Queens Community District 2--Sunnyside & Woodside PUMA",4110:"NYC-Queens Community District 5--Ridgewood, Glendale & Middle Village PUMA",4111:"NYC-Queens Community District 9--Richmond Hill & Woodhaven PUMA",
                    4112:"NYC-Queens Community District 12--Jamaica, Hollis & St. Albans PUMA",4113:"NYC-Queens Community District 10--Howard Beach & Ozone Park PUMA",4114:"NYC-Queens Community District 14--Far Rockaway, Breezy Point & Broad Channel PUMA"}

# For mapping the counties to the excel
def puma_county(county_num):

    if county_num in puma_maps_dict:
        return puma_maps_dict.get(county_num)
    else:
        return None

# for mapping the counties defined above to 10 regions
def puma_region(rcounty_num):
    # check_region = [puma_maps_dict[k] for k in region_1 if k in puma_maps_dict]
    if rcounty_num in region_1 and  rcounty_num in puma_maps_dict:
        return "Capital Region"
    elif rcounty_num in region_2 and  rcounty_num in puma_maps_dict:
        return "Central NY"
    elif rcounty_num in region_3 and  rcounty_num in puma_maps_dict:
        return "Finger Lakes"
    elif rcounty_num in region_4 and  rcounty_num in puma_maps_dict:
        return "Long Island"
    elif rcounty_num in region_5 and  rcounty_num in puma_maps_dict:
        return "Mid-Hudson"
    elif rcounty_num in region_6 and  rcounty_num in puma_maps_dict:
        return "Mohawk"
    elif rcounty_num in region_7 and  rcounty_num in puma_maps_dict:
        return "New York"
    elif rcounty_num in region_8 and  rcounty_num in puma_maps_dict:
        return "North Country"
    elif rcounty_num in region_9 and  rcounty_num in puma_maps_dict:
        return "Southern Tier"
    elif rcounty_num in region_10 and  rcounty_num in puma_maps_dict:
        return "Western NY"
    else:
        return None

def get_PUMA_from_CSV():
    PUMA = list()
    for p in p_data['PUMA']:
        if p not in PUMA:
            PUMA.append(p)
    return PUMA
def create_row_dataframe(PUMA=None,NATIVITY=None,AGEP=None,SCHL=None,SEX=None,WKHP=None,
ESR=None,PINCP=None,POVPIP=None,GRPIP=None,TEN=None,RAC1P=None,HISP=None,GRP11=None,FB_WNH=None):
    if PUMA is not None:
        if not isinstance(PUMA, list):
            PUMA = [PUMA]

    #   GRPIP and TEN are from housing data set.
    arguments = locals()
    #print 'args: ',arguments
    count = 0
    for a in arguments.keys():
        if a is not 'PUMA' \
                and a is not 'NATIVITY' \
                and a is not 'RAC1P' \
                and a is not 'HISP'\
                and a is not 'FB_WNH'\
                and a is not 'GRP11':
            if arguments[a] is not None:
                count += 1

    #print count,' args passed apart from PUMA* and NATIVITY'
    #   Temp doing this for NB_all only

    #   Each Variable following is a column in the data frame
    HSINC_UnEmp_m,HSINC_UnEmp_f,HSINC_emp_m,HSINC_emp_f = 0, 0, 0, 0 #   from df
    HSINC_UnEmp_mf_t,HSINC_Emp_mf_t = 0, 0 #   derived from sum
    Total_HSINC,Total_HSINC_m,Total_HSINC_f = 0, 0, 0 #   derived from sum

    HS_UnEmp_m, HS_UnEmp_f, HS_emp_m, HS_emp_f = 0, 0, 0, 0 #   from df
    HS_UnEmp_mf_t, HS_Emp_mf_t = 0, 0 #   derived from sum
    Total_HS,Total_HS_m,Total_HS_f = 0, 0, 0 #   derived from sum

    BABS_UnEmp_m, BABS_UnEmp_f, BABS_emp_m, BABS_emp_f = 0, 0, 0, 0 #   from df
    BABS_UnEmp_mf_t, BABS_Emp_mf_t = 0, 0 #   derived from sum
    Total_BABS,Total_BABS_m,Total_BABS_f = 0, 0, 0 #   derived from sum

    Total_UnEmp_m, Total_UnEmp_f, Total_emp_m, Total_emp_f = 0, 0, 0, 0 #   derived from sum
    Total_UnEmp_mf_t, Total_Emp_mf_t = 0, 0 #   derived from sum
    Total_geo,Total_geo_m,Total_geo_f = 0, 0, 0 #   derived from sum


    # Filter out Nativity and Age
    p_data_tmp = p_data[p_data['NATIVITY'] == NATIVITY]
    p_data1 = p_data_tmp[p_data_tmp['AGEP'].isin(range(25, 64 + 1))]
    # Filter out PUMA
    if PUMA is not None:
        p_data1 = p_data1[p_data1['PUMA'].isin(PUMA)] ##todo PUMA should always be a list now

    # English Ability
    # Filter out for Group 11
    if GRP11 is 1 and NATIVITY is 2:
        p_data1=p_data1[p_data1['ENG'].isin(range(3,4+1))]
    elif NATIVITY is 2:
        p_data1 = p_data1[p_data1['ENG'].isin(range(1, 2 + 1))]

    # Filter out Race for NB_WNH
    if RAC1P is 1 and NATIVITY is 1:
        p_data1 = p_data1[p_data1['RAC1P'] == RAC1P]
    if HISP is 1 and NATIVITY is 1:
        p_data1 = p_data1[p_data1['HISP'] == HISP]
    
    # Filter out Race for FB_WNH
    if RAC1P is 1 and NATIVITY is 2 and FB_WNH is 1:
        p_data1 = p_data1[p_data1['RAC1P'] == RAC1P]
    if HISP is 1 and NATIVITY is 2 and FB_WNH is 1:
        p_data1 = p_data1[p_data1['HISP'] == HISP]

    
    # Filter out Race for FB_POC
    if HISP is 1 and RAC1P is 1 and NATIVITY is 2 and FB_WNH is None:
        #FB_NW : nothing to do
        p_data1_FB_NW = p_data1[p_data1['RAC1P'].isin(range(2, 9 + 1))]  # FB_NW
        p_data1_FB_WH = p_data1[p_data1['RAC1P'] == 1]  # FB_WH
        p_data_FB_WH = p_data1_FB_WH[p_data1_FB_WH['HISP'].isin(range(2, 24 + 1))]  # FB_WH
        #p_data1 = p_data_FB_WH + p_data1_FB_NW -- concate them.
        ''' # Check for intersection. None means no overlapping
        s1 = pandas.merge(p_data1_FB_WH, p_data1_FB_NW, how='right', on=['SERIALNO'])
        s1 = s1.dropna(inplace=True)
        print s1
        '''
        #p_data1 = pandas.concat([p_data1_FB_NW,p_data1_FB_WH],ignore_index=True)
        p_data1 = p_data1_FB_NW.append(p_data_FB_WH, ignore_index=True)


    #for Income level_FT workers
    if WKHP is 1 and PINCP is 1:
        # For Income level_FT workers
        p_data1['WKHP'] = pandas.to_numeric(p_data1['WKHP'], errors='coerce')
        p_data1['PINCP'] = pandas.to_numeric(p_data1['PINCP'],errors='coerce')
        p_data_result = p_data1[p_data1['WKHP'].isin(range(35, 99 + 1))]
        p_data_result = p_data_result[p_data_result['PINCP'] >  -20000]
        
        HSINC_PINCP_m, HSINC_PINCP_f, HSINC_Weigthed_Total_m, HSINC_Weighted_Total_f = 0, 0, 0, 0  # from df
        HSINC_Avg_PINCP_m, HSINC_Avg_PINCP_f=0,0
        HSINC_Avg_PINCP_mf_t, HSINC_Weigthed_Total_mf_t = 0, 0  # derived from sum
        Total_HSINC, Total_HSINC_m, Total_HSINC_f = 0, 0, 0  # derived from sum

        HS_PINCP_m, HS_PINCP_f, HS_Weigthed_Total_m, HS_Weighted_Total_f = 0, 0, 0, 0  # from df
        HS_Avg_PINCP_m, HS_Avg_PINCP_f = 0, 0
        HS_Avg_PINCP_mf_t, HS_Weigthed_Total_mf_t = 0, 0  # derived from sum
        Total_HS, Total_HS_m, Total_HS_f = 0, 0, 0  # derived from sum

        BABS_PINCP_m, BABS_PINCP_f, BABS_Weigthed_Total_m, BABS_Weigthed_Total_f = 0, 0, 0, 0  # from df
        BABS_Avg_PINCP_m, BABS_Avg_PINCP_f = 0, 0
        BABS_Avg_PINCP_mf_t, BABS_Weigthed_Total_mf_t = 0, 0  # derived from sum
        Total_BABS, Total_BABS_m, Total_BABS_f = 0, 0, 0  # derived from sum

        Total_PINCP_m, Total_PINCP_f, Total_Weigthed_Total_m, Total_Weigthed_Total_f = 0, 0, 0, 0  # derived from sum
        Total_Avg_Male,Total_Avg_female = 0,0
        Total_Avg_PINCP_m, Total_Avg_PINCP_f = 0,0
        Total_Avg_PINCP_mf_t, Total_Weigthed_Total_mf_t = 0, 0  # derived from sum
        Total_geo, Total_geo_m, Total_geo_f = 0, 0, 0  # derived from sum

        # HSINC Avg_PINCP Male
        for i in p_data_result.index:
            if p_data_result.at[i, 'SCHL'] in range(0, 15 + 1) \
                    and p_data_result.at[i, 'SEX'] == 1:
                HSINC_PINCP_m += p_data_result.at[i,'PINCP'] * p_data_result.at[i,'PWGTP']
                Total_HSINC_m += p_data_result.at[i,'PWGTP']

                HSINC_Avg_PINCP_m = HSINC_PINCP_m/ Total_HSINC_m

        #HSINC Avg_PINCP Female
        for i in p_data_result.index:
            if p_data_result.at[i, 'SCHL'] in range(0, 15 + 1) \
                    and p_data_result.at[i, 'SEX'] == 2:
                HSINC_PINCP_f += p_data_result.at[i,'PINCP'] * p_data_result.at[i,'PWGTP']
                Total_HSINC_f += p_data_result.at[i,'PWGTP']

                HSINC_Avg_PINCP_f = HSINC_PINCP_f/Total_HSINC_f


        # HS Avg_PINCP Male
        for i in p_data_result.index:
            if p_data_result.at[i, 'SCHL'] in range(16, 20 + 1) \
                    and p_data_result.at[i, 'SEX'] == 1:
                HS_PINCP_m += p_data_result.at[i, 'PINCP'] * p_data_result.at[i, 'PWGTP']
                Total_HS_m += p_data_result.at[i, 'PWGTP']

                HS_Avg_PINCP_m = HS_PINCP_m/Total_HS_m

        # HS Avg_PINCP Female
        for i in p_data_result.index:
            if p_data_result.at[i, 'SCHL'] in range(16, 20 + 1) \
                    and p_data_result.at[i, 'SEX'] == 2:
                HS_PINCP_f += p_data_result.at[i, 'PINCP'] * p_data_result.at[i, 'PWGTP']
                Total_HS_f += p_data_result.at[i, 'PWGTP']

                HS_Avg_PINCP_f = HS_PINCP_f/Total_HS_f

        # BABS Employed Male
        for i in p_data_result.index:
            if p_data_result.at[i, 'SCHL'] in range(21, 24 + 1) \
                    and p_data_result.at[i, 'SEX'] == 1:
                BABS_PINCP_m += p_data_result.at[i, 'PINCP'] * p_data_result.at[i, 'PWGTP']
                Total_BABS_m += p_data_result.at[i, 'PWGTP']

                BABS_Avg_PINCP_m = BABS_PINCP_m/Total_BABS_m

        # BABS Employed Female
        for i in p_data_result.index:
            if p_data_result.at[i, 'SCHL'] in range(21, 24 + 1) \
                    and p_data_result.at[i, 'SEX'] == 2:
                BABS_PINCP_f += p_data_result.at[i, 'PINCP'] * p_data_result.at[i, 'PWGTP']
                Total_BABS_f += p_data_result.at[i, 'PWGTP']

                BABS_Avg_PINCP_f = BABS_PINCP_f/Total_BABS_f
           
        #Avg PINCP Male 
        for i in p_data_result.index:
            if p_data_result.at[i,'SEX'] == 1:
                Total_PINCP_m += p_data_result.at[i,'PINCP'] * p_data_result.at[i, 'PWGTP']
                Total_Avg_Male += p_data_result.at[i, 'PWGTP']
            
                Total_Avg_PINCP_m = Total_PINCP_m/Total_Avg_Male

        for i in p_data_result.index:
            if p_data_result.at[i, 'SEX'] == 2:
                Total_PINCP_f += p_data_result.at[i,'PINCP'] * p_data_result.at[i, 'PWGTP']
                Total_Avg_female += p_data_result.at[i,'PWGTP']
            
                Total_Avg_PINCP_f = Total_PINCP_f/Total_Avg_female
        if (Total_HSINC_m + Total_HSINC_f) != 0:
            HSINC_Avg_PINCP_mf_t = (HSINC_PINCP_m + HSINC_PINCP_f) * 1.0 / (Total_HSINC_m + Total_HSINC_f)
        else:
            HSINC_Avg_PINCP_mf_t=np.nan

        if (Total_HS_m + Total_HS_f) != 0:
            HS_Avg_PINCP_mf_t = (HS_PINCP_m + HS_PINCP_f) * 1.0 / (Total_HS_m + Total_HS_f)
        else:
            HS_Avg_PINCP_mf_t = np.nan

        if (Total_BABS_m + Total_BABS_f) != 0:
            BABS_Avg_PINCP_mf_t = (BABS_PINCP_m + BABS_PINCP_f) * 1.0 / (Total_BABS_m + Total_BABS_f)
        else:
            BABS_Avg_PINCP_mf_t=np.nan
        Total_HSINC = Total_HSINC_m + Total_HSINC_f    
        
        Total_HS = Total_HS_m + Total_HS_f

        Total_BABS = Total_BABS_m + Total_BABS_f
        if (Total_Avg_Male + Total_Avg_female) !=0:
            Total_Avg_PINCP_mf_t=(Total_PINCP_m + Total_PINCP_f) * 1.0 / (Total_Avg_Male + Total_Avg_female)
        else:
            Total_Avg_PINCP_mf_t=np.nan
        Total_geo_f = Total_HSINC_f + Total_HS_f + Total_BABS_f
        
        Total_geo_m = Total_HSINC_m + Total_HS_m + Total_BABS_m
        
        Total_geo =  Total_geo_m + Total_geo_f


        list_to_return_Income_level = [
                                        Total_geo,Total_geo_m,Total_geo_f,
                                        Total_Avg_PINCP_mf_t,Total_Avg_PINCP_m,Total_Avg_PINCP_f,
                                        #Weighted Total PINCP
            
                                        Total_BABS,Total_BABS_m,Total_BABS_f,
                                        BABS_Avg_PINCP_mf_t, BABS_Avg_PINCP_m,BABS_Avg_PINCP_f,
                                        #Weighted Total PINCP BABS 

                                        Total_HS, Total_HS_m, Total_HS_f,
                                        HS_Avg_PINCP_mf_t, HS_Avg_PINCP_m, HS_Avg_PINCP_f,
                                        # Weighted Total PINCP HS 

                                        Total_HSINC, Total_HSINC_m, Total_HSINC_f,
                                        HSINC_Avg_PINCP_mf_t, HSINC_Avg_PINCP_m, HSINC_Avg_PINCP_f,
                                        # Weighted Total PINCP HSINC 
                                        ]
        if GRP11 is None:
            return list_to_return_Income_level
        else:
            return list_to_return_Income_level[18:]
    if GRPIP is 1 or TEN is 1:
        hp_data = p_data1.join(h_data.set_index('SERIALNO'), on='SERIALNO', how='right', lsuffix='_population',
                               rsuffix='_housing').reset_index(drop=True)
        hp_data['GRPIP'] = pandas.to_numeric(hp_data['GRPIP'], errors='coerce')

    p_data_positive, p_data_negative = [], []
    # --------------------------------------------------positive part-----------------------------------------------
    if count is 1:
        if ESR is 1:
            p_data_positive = p_data1[(p_data1['ESR'] == 1) | (p_data1['ESR'] == 2) | (p_data1['ESR'] == 4) | (p_data1['ESR'] == 5) ]
        elif WKHP is 1:
            p_data1['WKHP'] = pandas.to_numeric(p_data1['WKHP'], errors='coerce')
            #p_data_positive = p_data1[p_data1['WKHP'].convert_objects(convert_numeric=True).isin(range(35, 99 + 1))]
            p_data_positive = p_data1[p_data1['WKHP'].isin(range(35, 99 + 1))]
        elif POVPIP is 1:
            p_data1['POVPIP'] = pandas.to_numeric(p_data1['POVPIP'], errors='coerce')
            p_data_positive = p_data1[p_data1['POVPIP'] > 150]
        elif GRPIP is 1:
            #hp_data = p_data1.join(h_data.set_index('SERIALNO'),on='SERIALNO',how='right',lsuffix='_population',rsuffix='_housing')
            #l = pandas.merge(p_data1,h_data,how='right',on='SERIALNO')
            p_data_positive = hp_data[hp_data['GRPIP'] <= 50]
        elif TEN is 1:
            p_data_positive = hp_data[hp_data['TEN'].isin(range(1, 2 + 1))]

    elif count is 2:
        if WKHP is 1 and POVPIP is 1:
            p_data1['WKHP'] = pandas.to_numeric(p_data1['WKHP'], errors='coerce')
            p_data1['POVPIP'] = pandas.to_numeric(p_data1['POVPIP'], errors='coerce')
            p_data_positive = p_data1[p_data1['WKHP'].isin(range(35, 99 + 1))]
            p_data_positive = p_data_positive[p_data_positive['POVPIP'] > 150]

    # HSINC Employed Male
    for i in p_data_positive.index:
        if p_data_positive.at[i, 'SCHL'] in range(0, 15 + 1) \
                and p_data_positive.at[i, 'SEX'] == 1:
            HSINC_emp_m += p_data_positive.at[i, 'PWGTP']
    # print 'HSINC Emp Male: ', HSINC_emp_m

    # HSINC Employed Female
    for i in p_data_positive.index:
        if p_data_positive.at[i, 'SCHL'] in range(0, 15 + 1) \
                and p_data_positive.at[i, 'SEX'] == 2:
            HSINC_emp_f += p_data_positive.at[i, 'PWGTP']
            # print 'HSINC Emp Female: ', HSINC_emp_f

    # HS Employed Male
    for i in p_data_positive.index:
        if p_data_positive.at[i, 'SCHL'] in range(16, 20 + 1) \
                and p_data_positive.at[i, 'SEX'] == 1:
            HS_emp_m += p_data_positive.at[i, 'PWGTP']
    # print 'HS Emp Male: ', HS_emp_m

    # HS Employed Female
    for i in p_data_positive.index:
        if p_data_positive.at[i, 'SCHL'] in range(16, 20 + 1) \
                and p_data_positive.at[i, 'SEX'] == 2:
            HS_emp_f += p_data_positive.at[i, 'PWGTP']
    # print 'HS Emp Female: ', HS_emp_f

    # BABS Employed Male
    for i in p_data_positive.index:
        if p_data_positive.at[i, 'SCHL'] in range(21, 24 + 1) \
                and p_data_positive.at[i, 'SEX'] == 1:
            BABS_emp_m += p_data_positive.at[i, 'PWGTP']
    # print 'BABS Emp Male: ', BABS_emp_m

    # BABS Employed Female
    for i in p_data_positive.index:
        if p_data_positive.at[i, 'SCHL'] in range(21, 24 + 1) \
                and p_data_positive.at[i, 'SEX'] == 2:
            BABS_emp_f += p_data_positive.at[i, 'PWGTP']
    # print 'BABS Emp Female: ', BABS_emp_f

    # ------------------------------------------------------negative part-------------------------------------------
    if count is 1:
        if ESR is 1:
            p_data_negative = p_data1[p_data1['ESR'] == 3]
        elif WKHP is 1:
            p_data_negative = p_data1[p_data1['WKHP'].isin(range(1, 34 + 1))]
        elif POVPIP is 1:
            p_data_negative = p_data1[p_data1['POVPIP'].isin(range(0,150 + 1))]
        elif GRPIP is 1:
            p_data_negative = hp_data[hp_data['GRPIP'] > 50]
        elif TEN is 1:
            p_data_negative = hp_data[hp_data['TEN'].isin(range(3, 4 + 1))]

    elif count is 2:
        if WKHP is 1 and POVPIP is 1:
            p_data_negative = p_data1[p_data1['WKHP'].isin(range(35, 99 + 1)) & p_data1['POVPIP'].isin(range(0,150 + 1))]

    # HSINC Unemployed Male
    for i in p_data_negative.index:
        if p_data_negative.at[i,'SCHL'] in range(0, 15 + 1) \
                and p_data_negative.at[i,'SEX'] == 1:
            HSINC_UnEmp_m += p_data_negative.at[i,'PWGTP']
    #print 'HSINC UnEmp Male: ', HSINC_UnEmp_m

    # HSINC Unemployed Female
    for i in p_data_negative.index:
        if p_data_negative.at[i,'SCHL'] in range(0, 15 + 1) \
                and p_data_negative.at[i,'SEX'] == 2:
            HSINC_UnEmp_f += p_data_negative.at[i,'PWGTP']
    #print 'HSINC UnEmp Female: ', HSINC_UnEmp_f

    # HS Unemployed Male
    for i in p_data_negative.index:
        if p_data_negative.at[i, 'SCHL'] in range(16, 20 + 1) \
                and p_data_negative.at[i, 'SEX'] == 1:
            HS_UnEmp_m += p_data_negative.at[i, 'PWGTP']
    # print 'HS UnEmp Male: ', HS_UnEmp_m

    # HS Unemployed Female
    for i in p_data_negative.index:
        if p_data_negative.at[i, 'SCHL'] in range(16, 20 + 1) \
                and p_data_negative.at[i, 'SEX'] == 2:
            HS_UnEmp_f += p_data_negative.at[i, 'PWGTP']
            # print 'HS UnEmp Female: ', HS_UnEmp_f
    # BABS Unemployed Male
    for i in p_data_negative.index:
        if p_data_negative.at[i, 'SCHL'] in range(21, 24 + 1) \
                and p_data_negative.at[i, 'SEX'] == 1:
            BABS_UnEmp_m += p_data_negative.at[i, 'PWGTP']
    # print 'BABS UnEmp Male: ', BABS_UnEmp_m

    # BABS Unemployed Female
    for i in p_data_negative.index:
        if p_data_negative.at[i, 'SCHL'] in range(21, 24 + 1) \
                and p_data_negative.at[i, 'SEX'] == 2:
            BABS_UnEmp_f += p_data_negative.at[i, 'PWGTP']
            # print 'BABS UnEmp Female: ', BABS_UnEmp_f

    # ------------------------------------------------------------------------------------------------------------------

    # HSINC Unemployed Total
    HSINC_UnEmp_mf_t = HSINC_UnEmp_m + HSINC_UnEmp_f

    # HSINC Employed Total
    HSINC_Emp_mf_t = HSINC_emp_m + HSINC_emp_f
    #print 'HSINC Emp Total: ', HSINC_Emp_mf_t

    # HSINC Male Total
    Total_HSINC_m = HSINC_emp_m + HSINC_UnEmp_m
    #print 'HSINC Male Total: ', Total_HSINC_m

    # HSINC Female total
    Total_HSINC_f = HSINC_emp_f + HSINC_UnEmp_f
    #print 'HSINC Female Total: ', Total_HSINC_f

    # HSINC Total
    Total_HSINC = Total_HSINC_m + Total_HSINC_f
    #print 'HSINC Total: ', Total_HSINC

    # HS Employed Total
    HS_Emp_mf_t = HS_emp_m + HS_emp_f
    #print 'HS Emp Total: ', HS_Emp_mf_t

    # HS Unemployed total
    HS_UnEmp_mf_t = HS_UnEmp_m + HS_UnEmp_f
    #print 'HS UnEmp Total: ', HS_UnEmp_mf_t

    # HS Male Total
    Total_HS_m = HS_emp_m + HS_UnEmp_m
    #print 'HS Male Total: ', Total_HS_m

    # HS Female total
    Total_HS_f = HS_emp_f + HS_UnEmp_f
    #print 'HS Female Total: ', Total_HS_f

    # HS Total
    Total_HS = Total_HS_m + Total_HS_f
    #print 'HS Total: ', Total_HS

    # BABS Employed Total
    BABS_Emp_mf_t = BABS_emp_m + BABS_emp_f
    #print 'BABS Emp Total: ', BABS_Emp_mf_t

    # BABS Unemployed total
    BABS_UnEmp_mf_t = BABS_UnEmp_m + BABS_UnEmp_f
    #print 'BABS UnEmp Total: ', BABS_UnEmp_mf_t

    # BABS Male Total
    Total_BABS_m = BABS_emp_m + BABS_UnEmp_m
    #print 'BABS Male Total: ', Total_BABS_m

    # BABS Female total
    Total_BABS_f = BABS_emp_f + BABS_UnEmp_f
    #print 'BABS Female Total: ', Total_BABS_f

    # BABS Total
    Total_BABS = Total_BABS_m + Total_BABS_f
    #print 'BABS Total: ', Total_BABS


    #-----------------------------------------------------

    # Total UnEmp Female
    Total_UnEmp_f = BABS_UnEmp_f + HS_UnEmp_f + HSINC_UnEmp_f
    #print 'Total UnEMp Female: ',Total_UnEmp_f

    # Total UnEmp Male
    Total_UnEmp_m = BABS_UnEmp_m + HS_UnEmp_m + HSINC_UnEmp_m
    #print 'Total UnEMp Male: ', Total_UnEmp_m

    # Total UnEmp M+F
    Total_UnEmp_mf_t = Total_UnEmp_m + Total_UnEmp_f
    #print 'Total UnEMp Population: ', Total_UnEmp_mf_t

    # Total Emp Female
    Total_emp_f = BABS_emp_f + HS_emp_f + HSINC_emp_f
    #print 'Total Emp Female: ', Total_emp_f
    # Total Emp Male
    Total_emp_m = BABS_emp_m + HS_emp_m + HSINC_emp_m
    #print 'Total Emp Male: ', Total_emp_m
    # Total Emp M+F
    Total_Emp_mf_t = Total_emp_m + Total_emp_f
    #print 'Total Emp Population: ', Total_Emp_mf_t
    # Total Female
    Total_geo_f = Total_emp_f + Total_UnEmp_f
    #print 'Total Female Populatinon: ',Total_geo_f
    # Total Male
    Total_geo_m = Total_emp_m + Total_UnEmp_m
    #print 'Total Male Populatinon: ', Total_geo_m
    # Total M+F
    Total_geo = Total_geo_f + Total_geo_m
    #print 'Total Population: ',Total_geo


    list_to_return = [Total_geo,Total_geo_m,Total_geo_f,
                      Total_Emp_mf_t,Total_emp_m,Total_emp_f,
                      Total_UnEmp_mf_t,Total_UnEmp_m,Total_UnEmp_f,

                      Total_BABS,Total_BABS_m,Total_BABS_f,
                      BABS_Emp_mf_t,BABS_emp_m,BABS_emp_f,
                      BABS_UnEmp_mf_t,BABS_UnEmp_m,BABS_UnEmp_f,

                      Total_HS,Total_HS_m,Total_HS_f,
                      HS_Emp_mf_t,HS_emp_m,HS_emp_f,
                      HS_UnEmp_mf_t,HS_UnEmp_m,HS_UnEmp_f,

                      Total_HSINC, Total_HSINC_m, Total_HSINC_f,
                      HSINC_Emp_mf_t, HSINC_emp_m, HSINC_emp_f,
                      HSINC_UnEmp_mf_t, HSINC_UnEmp_m, HSINC_UnEmp_f]
    if GRP11 is None:
        return list_to_return
    else:
        return list_to_return[27:]

def UnEmp(NATIVITY = None, PATH = '/', RAC1P=None, HISP=None,GRP11=None,FB_WNH=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]
    col_NB_UnEmp = ['puma',
                    'Total_geo','Total_geo_m', 'Total_geo_f',
                    'Total_Emp_mf_t', 'Total_emp_m', 'Total_emp_f',
                    'Total_UnEmp_mf_t', 'Total_UnEmp_m', 'Total_UnEmp_f',

                    'Total_BABS', 'Total_BABS_m', 'Total_BABS_f',
                    'BABS_Emp_mf_t', 'BABS_emp_m', 'BABS_emp_f',
                    'BABS_UnEmp_mf_t', 'BABS_UnEmp_m', 'BABS_UnEmp_f',

                    'Total_HS', 'Total_HS_m', 'Total_HS_f',
                    'HS_Emp_mf_t', 'HS_emp_m', 'HS_emp_f',
                    'HS_UnEmp_mf_t', 'HS_UnEmp_m', 'HS_UnEmp_f',

                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_Emp_mf_t', 'HSINC_emp_m', 'HSINC_emp_f',
                    'HSINC_UnEmp_mf_t', 'HSINC_UnEmp_m', 'HSINC_UnEmp_f'
                    ]
    col_NB_UnEmp_grp_11=['puma',
                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_Emp_mf_t', 'HSINC_emp_m', 'HSINC_emp_f',
                    'HSINC_UnEmp_mf_t', 'HSINC_UnEmp_m', 'HSINC_UnEmp_f']
    full_list = []
    # First row is not a PUMA county. It is the summation row.
    print('----------------- For Total Geo')
    l = create_row_dataframe(NATIVITY=NATIVITY,ESR=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data.PUMA_Counties:
        print('----------------- For PUMA: ', puma_county(p))
        l = create_row_dataframe(PUMA=p,NATIVITY=NATIVITY,ESR=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_1_list]
        sum_region_1 = [sum(x) for x in zip(*test)] #sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_2_list]
        sum_region_2 = [sum(x) for x in zip(*test)]
        if p in region_3:
            region_3_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_3_list]
        sum_region_3 = [sum(x) for x in zip(*test)]
        if p in region_4:
            region_4_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_4_list]
        sum_region_4 = [sum(x) for x in zip(*test)]
        if p in region_5:
            region_5_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_5_list]
        sum_region_5 = [sum(x) for x in zip(*test)]
        if p in region_6:
            region_6_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_6_list]
        sum_region_6 = [sum(x) for x in zip(*test)]
        if p in region_7:
            region_7_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_7_list]
        sum_region_7 = [sum(x) for x in zip(*test)]
        if p in region_8:
            region_8_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_8_list]
        sum_region_8 = [sum(x) for x in zip(*test)]
        if p in region_9:
            region_9_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_9_list]
        sum_region_9 = [sum(x) for x in zip(*test)]
        if p in region_10:
            region_10_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_10_list]
        sum_region_10 = [sum(x) for x in zip(*test)]

    l1=["Capital Region"] + sum_region_1
    l2=["Central NY"] + sum_region_2
    l3=["Finger Lakes"] + sum_region_3
    l4=["Long Island"] + sum_region_4
    l5=["Mid-Hudson"] + sum_region_5
    l6=["Mohawk Valley"] + sum_region_6
    l7=["New York City"] +sum_region_7
    l8=["North Country"] +sum_region_8
    l9=["Southern Tier"] + sum_region_9
    l10=["Western NY"] + sum_region_10
    full_list.append(l1)
    full_list.append(l2)
    full_list.append(l3)
    full_list.append(l4)
    full_list.append(l5)
    full_list.append(l6)
    full_list.append(l7)
    full_list.append(l8)
    full_list.append(l9)
    full_list.append(l10)

    if GRP11 is None:
        result_df = pandas.DataFrame(full_list, columns=col_NB_UnEmp)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'UnEmp.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is None and HISP is None:
            global_data.NB_ALL_UnEmp_p = get_percentage(df=result_df, indicator='UnEmp')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_ALL_UnEmp_p.to_csv(PATH + 'step_2/' + 'UnEmp.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
            global_data.NB_WNH_UnEmp_p = get_percentage(df=result_df, indicator='UnEmp')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_WNH_UnEmp_p.to_csv(PATH + 'step_2/' + 'UnEmp.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is 1:
            global_data.FB_WNH_UnEmp_p = get_percentage(df=result_df, indicator='UnEmp')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_WNH_UnEmp_p.to_csv(PATH + 'step_2/' + 'UnEmp.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is None and HISP is None:
            global_data.FB_ALL_UnEmp_p = get_percentage(df=result_df, indicator='UnEmp')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_ALL_UnEmp_p.to_csv(PATH + 'step_2/' + 'UnEmp.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is None:
            global_data.FB_POC_UnEmp_p = get_percentage(df=result_df, indicator='UnEmp')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_POC_UnEmp_p.to_csv(PATH + 'step_2/' + 'UnEmp.csv', na_rep="#DIV/0!")
    else:
        result_df = pandas.DataFrame(full_list, columns=col_NB_UnEmp_grp_11)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'UnEmp.csv', na_rep="#DIV/0!")
        global_data.FB_ALL_GRP11_UnEmp_p = get_percentage(df=result_df, indicator='UnEmp',GRP11=1)
        make_sure_path_exists(PATH + 'step_2/')
        global_data.FB_ALL_GRP11_UnEmp_p.to_csv(PATH + 'step_2/' + 'UnEmp.csv', na_rep="#DIV/0!")

def FT_Work(NATIVITY = None,PATH = '/', RAC1P=None, HISP=None,GRP11=None,FB_WNH=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]
    # FT: Full Time, PT: Part Time
    col_NB_FT_Work = ['puma',
                    'Total_geo', 'Total_geo_m', 'Total_geo_f',
                    'Total_FT_mf_t', 'Total_FT_m', 'Total_FT_f',
                    'Total_PT_mf_t', 'Total_PT_m', 'Total_PT_f',

                    'Total_BABS', 'Total_BABS_m', 'Total_BABS_f',
                    'BABS_FT_mf_t', 'BABS_FT_m', 'BABS_FT_f',
                    'BABS_PT_mf_t', 'BABS_PT_m', 'BABS_PT_f',

                    'Total_HS', 'Total_HS_m', 'Total_HS_f',
                    'HS_FT_mf_t', 'HS_FT_m', 'HS_FT_f',
                    'HS_PT_mf_t', 'HS_PT_m', 'HS_PT_f',

                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_FT_mf_t', 'HSINC_FT_m', 'HSINC_FT_f',
                    'HSINC_PT_mf_t', 'HSINC_PT_m', 'HSINC_PT_f'
                    ]
    col_NB_FT_Work_grp_11= ['puma',
                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_FT_mf_t', 'HSINC_FT_m', 'HSINC_FT_f',
                    'HSINC_PT_mf_t', 'HSINC_PT_m', 'HSINC_PT_f'
                    ]
    full_list = []
    # First row is not a PUMA county. It is the summation row.
    print ('----------------- For Total Geo')
    l = create_row_dataframe(NATIVITY=NATIVITY,WKHP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data.PUMA_Counties:
        print ('----------------- For PUMA: ', puma_county(p))
        l = create_row_dataframe(PUMA=p,NATIVITY=NATIVITY,WKHP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_1_list]
        sum_region_1 = [sum(x) for x in zip(*test)] #sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_2_list]
        sum_region_2 = [sum(x) for x in zip(*test)]
        if p in region_3:
            region_3_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_3_list]
        sum_region_3 = [sum(x) for x in zip(*test)]
        if p in region_4:
            region_4_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_4_list]
        sum_region_4 = [sum(x) for x in zip(*test)]
        if p in region_5:
            region_5_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_5_list]
        sum_region_5 = [sum(x) for x in zip(*test)]
        if p in region_6:
            region_6_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_6_list]
        sum_region_6 = [sum(x) for x in zip(*test)]
        if p in region_7:
            region_7_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_7_list]
        sum_region_7 = [sum(x) for x in zip(*test)]
        if p in region_8:
            region_8_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_8_list]
        sum_region_8 = [sum(x) for x in zip(*test)]
        if p in region_9:
            region_9_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_9_list]
        sum_region_9 = [sum(x) for x in zip(*test)]
        if p in region_10:
            region_10_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_10_list]
        sum_region_10 = [sum(x) for x in zip(*test)]

    l1 = ["Capital Region"] + sum_region_1
    l2 = ["Central NY"] + sum_region_2
    l3 = ["Finger Lakes"] + sum_region_3
    l4 = ["Long Island"] + sum_region_4
    l5 = ["Mid-Hudson"] + sum_region_5
    l6 = ["Mohawk Valley"] + sum_region_6
    l7 = ["New York City"] + sum_region_7
    l8 = ["North Country"] + sum_region_8
    l9 = ["Southern Tier"] + sum_region_9
    l10 = ["Western NY"] + sum_region_10
    full_list.append(l1)
    full_list.append(l2)
    full_list.append(l3)
    full_list.append(l4)
    full_list.append(l5)
    full_list.append(l6)
    full_list.append(l7)
    full_list.append(l8)
    full_list.append(l9)
    full_list.append(l10)

    if GRP11 is None:
        result_df = pandas.DataFrame(full_list, columns=col_NB_FT_Work)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'FT_Work.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is None and HISP is None:
            global_data.NB_ALL_FT_Work_p = get_percentage(df=result_df, indicator='FT_Work')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_ALL_FT_Work_p.to_csv(PATH + 'step_2/' + 'FT_Work.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
            global_data.NB_WNH_FT_Work_p = get_percentage(df=result_df, indicator='FT_Work')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_WNH_FT_Work_p.to_csv(PATH + 'step_2/' + 'FT_Work.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is 1:
            global_data.FB_WNH_FT_Work_p = get_percentage(df=result_df, indicator='FT_Work')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_WNH_FT_Work_p.to_csv(PATH + 'step_2/' + 'FT_Work.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is None and HISP is None:
            global_data.FB_ALL_FT_Work_p = get_percentage(df=result_df, indicator='FT_Work')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_ALL_FT_Work_p.to_csv(PATH + 'step_2/' + 'FT_Work.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is None:
            global_data.FB_POC_FT_Work_p = get_percentage(df=result_df, indicator='FT_Work')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_POC_FT_Work_p.to_csv(PATH + 'step_2/' + 'FT_Work.csv', na_rep="#DIV/0!")
    else:
        result_df = pandas.DataFrame(full_list, columns=col_NB_FT_Work_grp_11)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'FT_Work.csv', na_rep="#DIV/0!")
        global_data.FB_ALL_GRP11_FT_Work_p = get_percentage(df=result_df,indicator='FT_Work',GRP11=1)
        make_sure_path_exists(PATH + 'step_2/')
        global_data.FB_ALL_GRP11_FT_Work_p.to_csv(PATH + 'step_2/' + 'FT_Work.csv', na_rep="#DIV/0!")
def Poverty(NATIVITY = None,PATH = '/', RAC1P=None, HISP=None,GRP11=None,FB_WNH=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]
    # P: Poverty, NP: Not Poverty
    col_NB_Poverty = ['puma',
                    'Total_geo', 'Total_geo_m', 'Total_geo_f',
                    'Total_NP_mf_t', 'Total_NP_m', 'Total_NP_f',
                    'Total_P_mf_t', 'Total_P_m', 'Total_P_f',

                    'Total_BABS', 'Total_BABS_m', 'Total_BABS_f',
                    'BABS_NP_mf_t', 'BABS_NP_m', 'BABS_NP_f',
                    'BABS_P_mf_t', 'BABS_P_m', 'BABS_P_f',

                    'Total_HS', 'Total_HS_m', 'Total_HS_f',
                    'HS_NP_mf_t', 'HS_NP_m', 'HS_NP_f',
                    'HS_P_mf_t', 'HS_P_m', 'HS_P_f',

                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_NP_mf_t', 'HSINC_NP_m', 'HSINC_NP_f',
                    'HSINC_P_mf_t', 'HSINC_P_m', 'HSINC_P_f'
                    ]
    col_NB_Poverty_grp_11=['puma',
                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_NP_mf_t', 'HSINC_NP_m', 'HSINC_NP_f',
                    'HSINC_P_mf_t', 'HSINC_P_m', 'HSINC_P_f'
                    ]
    full_list = []
    # First row is not a PUMA county. It is the summation row.
    print ('----------------- For Total Geo')
    l = create_row_dataframe(NATIVITY=NATIVITY,POVPIP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data.PUMA_Counties:
        print ('----------------- For PUMA: ', puma_county(p))
        l = create_row_dataframe(PUMA=p,NATIVITY=NATIVITY,POVPIP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_1_list]
        sum_region_1 = [sum(x) for x in zip(*test)] #sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_2_list]
        sum_region_2 = [sum(x) for x in zip(*test)]
        if p in region_3:
            region_3_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_3_list]
        sum_region_3 = [sum(x) for x in zip(*test)]
        if p in region_4:
            region_4_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_4_list]
        sum_region_4 = [sum(x) for x in zip(*test)]
        if p in region_5:
            region_5_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_5_list]
        sum_region_5 = [sum(x) for x in zip(*test)]
        if p in region_6:
            region_6_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_6_list]
        sum_region_6 = [sum(x) for x in zip(*test)]
        if p in region_7:
            region_7_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_7_list]
        sum_region_7 = [sum(x) for x in zip(*test)]
        if p in region_8:
            region_8_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_8_list]
        sum_region_8 = [sum(x) for x in zip(*test)]
        if p in region_9:
            region_9_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_9_list]
        sum_region_9 = [sum(x) for x in zip(*test)]
        if p in region_10:
            region_10_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_10_list]
        sum_region_10 = [sum(x) for x in zip(*test)]

    l1 = ["Capital Region"] + sum_region_1
    l2 = ["Central NY"] + sum_region_2
    l3 = ["Finger Lakes"] + sum_region_3
    l4 = ["Long Island"] + sum_region_4
    l5 = ["Mid-Hudson"] + sum_region_5
    l6 = ["Mohawk Valley"] + sum_region_6
    l7 = ["New York City"] + sum_region_7
    l8 = ["North Country"] + sum_region_8
    l9 = ["Southern Tier"] + sum_region_9
    l10 = ["Western NY"] + sum_region_10
    full_list.append(l1)
    full_list.append(l2)
    full_list.append(l3)
    full_list.append(l4)
    full_list.append(l5)
    full_list.append(l6)
    full_list.append(l7)
    full_list.append(l8)
    full_list.append(l9)
    full_list.append(l10)

    if GRP11 is None:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Poverty)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Poverty.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is None and HISP is None:
            global_data.NB_ALL_Poverty_p = get_percentage(df=result_df, indicator='Poverty')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_ALL_Poverty_p.to_csv(PATH + 'step_2/' + 'Poverty.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
            global_data.NB_WNH_Poverty_p = get_percentage(df=result_df, indicator='Poverty')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_WNH_Poverty_p.to_csv(PATH + 'step_2/' + 'Poverty.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is 1:
            global_data.FB_WNH_Poverty_p = get_percentage(df=result_df, indicator='Poverty')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_WNH_Poverty_p.to_csv(PATH + 'step_2/' + 'Poverty.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is None and HISP is None:
            global_data.FB_ALL_Poverty_p = get_percentage(df=result_df, indicator='Poverty')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_ALL_Poverty_p.to_csv(PATH + 'step_2/' + 'Poverty.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is None:
            global_data.FB_POC_Poverty_p = get_percentage(df=result_df, indicator='Poverty')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_POC_Poverty_p.to_csv(PATH + 'step_2/' + 'Poverty.csv', na_rep="#DIV/0!")
    else:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Poverty_grp_11)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Poverty.csv', na_rep="#DIV/0!")
        global_data.FB_ALL_GRP11_Poverty_p = get_percentage(df=result_df, indicator='Poverty',GRP11=1)
        make_sure_path_exists(PATH + 'step_2/')
        global_data.FB_ALL_GRP11_Poverty_p.to_csv(PATH + 'step_2/' + 'Poverty.csv', na_rep="#DIV/0!")

def Working_poor(NATIVITY = None,PATH = '/', RAC1P=None, HISP=None,GRP11=None,FB_WNH=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]
    # P: Poverty, NP: Not Poverty
    col_NB_Working_poor = ['puma',
                    'Total_geo', 'Total_geo_m', 'Total_geo_f',
                    'Total_NP_mf_t', 'Total_NP_m', 'Total_NP_f',
                    'Total_P_mf_t', 'Total_P_m', 'Total_P_f',

                    'Total_BABS', 'Total_BABS_m', 'Total_BABS_f',
                    'BABS_NP_mf_t', 'BABS_NP_m', 'BABS_NP_f',
                    'BABS_P_mf_t', 'BABS_P_m', 'BABS_P_f',

                    'Total_HS', 'Total_HS_m', 'Total_HS_f',
                    'HS_NP_mf_t', 'HS_NP_m', 'HS_NP_f',
                    'HS_P_mf_t', 'HS_P_m', 'HS_P_f',

                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_NP_mf_t', 'HSINC_NP_m', 'HSINC_NP_f',
                    'HSINC_P_mf_t', 'HSINC_P_m', 'HSINC_P_f'
                    ]
    col_NB_Working_poor_grp_11=['puma',
                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_NP_mf_t', 'HSINC_NP_m', 'HSINC_NP_f',
                    'HSINC_P_mf_t', 'HSINC_P_m', 'HSINC_P_f']
    full_list = []
    # First row is not a PUMA county. It is the summation row.
    print ('----------------- For Total Geo')
    l = create_row_dataframe(WKHP=1,NATIVITY=NATIVITY,POVPIP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data.PUMA_Counties:
        print ('----------------- For PUMA: ', puma_county(p))
        l = create_row_dataframe(PUMA=p,WKHP=1,NATIVITY=NATIVITY,POVPIP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_1_list]
        sum_region_1 = [sum(x) for x in zip(*test)] #sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_2_list]
        sum_region_2 = [sum(x) for x in zip(*test)]
        if p in region_3:
            region_3_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_3_list]
        sum_region_3 = [sum(x) for x in zip(*test)]
        if p in region_4:
            region_4_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_4_list]
        sum_region_4 = [sum(x) for x in zip(*test)]
        if p in region_5:
            region_5_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_5_list]
        sum_region_5 = [sum(x) for x in zip(*test)]
        if p in region_6:
            region_6_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_6_list]
        sum_region_6 = [sum(x) for x in zip(*test)]
        if p in region_7:
            region_7_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_7_list]
        sum_region_7 = [sum(x) for x in zip(*test)]
        if p in region_8:
            region_8_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_8_list]
        sum_region_8 = [sum(x) for x in zip(*test)]
        if p in region_9:
            region_9_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_9_list]
        sum_region_9 = [sum(x) for x in zip(*test)]
        if p in region_10:
            region_10_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_10_list]
        sum_region_10 = [sum(x) for x in zip(*test)]

    l1 = ["Capital Region"] + sum_region_1
    l2 = ["Central NY"] + sum_region_2
    l3 = ["Finger Lakes"] + sum_region_3
    l4 = ["Long Island"] + sum_region_4
    l5 = ["Mid-Hudson"] + sum_region_5
    l6 = ["Mohawk Valley"] + sum_region_6
    l7 = ["New York City"] + sum_region_7
    l8 = ["North Country"] + sum_region_8
    l9 = ["Southern Tier"] + sum_region_9
    l10 = ["Western NY"] + sum_region_10
    full_list.append(l1)
    full_list.append(l2)
    full_list.append(l3)
    full_list.append(l4)
    full_list.append(l5)
    full_list.append(l6)
    full_list.append(l7)
    full_list.append(l8)
    full_list.append(l9)
    full_list.append(l10)

    if GRP11 is None:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Working_poor)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Working_Poor.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is None and HISP is None:
            global_data.NB_ALL_Working_Poor_p = get_percentage(df=result_df, indicator='Working_Poor')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_ALL_Working_Poor_p.to_csv(PATH + 'step_2/' + 'Working_Poor.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
            global_data.NB_WNH_Working_Poor_p = get_percentage(df=result_df, indicator='Working_Poor')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_WNH_Working_Poor_p.to_csv(PATH + 'step_2/' + 'Working_Poor.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is 1:
            global_data.FB_WNH_Working_Poor_p = get_percentage(df=result_df, indicator='Working_Poor')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_WNH_Working_Poor_p.to_csv(PATH + 'step_2/' + 'Working_Poor.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is None and HISP is None:
            global_data.FB_ALL_Working_Poor_p = get_percentage(df=result_df, indicator='Working_Poor')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_ALL_Working_Poor_p.to_csv(PATH + 'step_2/' + 'Working_Poor.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is None:
            global_data.FB_POC_Working_Poor_p = get_percentage(df=result_df, indicator='Working_Poor')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_POC_Working_Poor_p.to_csv(PATH + 'step_2/' + 'Working_Poor.csv', na_rep="#DIV/0!")
    else:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Working_poor_grp_11)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Working_Poor.csv', na_rep="#DIV/0!")
        global_data.FB_ALL_GRP11_Working_Poor_p = get_percentage(df=result_df, indicator='Working_Poor',GRP11=1)
        make_sure_path_exists(PATH + 'step_2/')

        global_data.FB_ALL_GRP11_Working_Poor_p.to_csv(PATH + 'step_2/' + 'Working_Poor.csv', na_rep="#DIV/0!")
def Rent_burden(NATIVITY = None,PATH = '/', RAC1P=None, HISP=None,GRP11=None,FB_WNH=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]
    # P: Poverty, NP: Not Poverty
    col_NB_Rent_burden = ['puma',
                    'Total_geo', 'Total_geo_m', 'Total_geo_f',
                    'Total_NRB_mf_t', 'Total_NRB_m', 'Total_NRB_f',
                    'Total_RB_mf_t', 'Total_RB_m', 'Total_RB_f',

                    'Total_BABS', 'Total_BABS_m', 'Total_BABS_f',
                    'BABS_NRB_mf_t', 'BABS_NRB_m', 'BABS_NRB_f',
                    'BABS_RB_mf_t', 'BABS_RB_m', 'BABS_RB_f',

                    'Total_HS', 'Total_HS_m', 'Total_HS_f',
                    'HS_NRB_mf_t', 'HS_NRB_m', 'HS_NRB_f',
                    'HS_RB_mf_t', 'HS_RB_m', 'HS_RB_f',

                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_NRB_mf_t', 'HSINC_NRB_m', 'HSINC_NRB_f',
                    'HSINC_RB_mf_t', 'HSINC_RB_m', 'HSINC_RB_f'
                    ]
    col_NB_Rent_burden_grp_11=['puma',
                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_NRB_mf_t', 'HSINC_NRB_m', 'HSINC_NRB_f',
                    'HSINC_RB_mf_t', 'HSINC_RB_m', 'HSINC_RB_f'
                    ]
    full_list = []
    # First row is not a PUMA county. It is the summation row.
    print ('----------------- For Total Geo')
    l = create_row_dataframe(NATIVITY=NATIVITY,GRPIP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data.PUMA_Counties:
        print ('----------------- For PUMA: ', puma_county(p))
        l = create_row_dataframe(PUMA=p,NATIVITY=NATIVITY,GRPIP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_1_list]
        sum_region_1 = [sum(x) for x in zip(*test)] #sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_2_list]
        sum_region_2 = [sum(x) for x in zip(*test)]
        if p in region_3:
            region_3_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_3_list]
        sum_region_3 = [sum(x) for x in zip(*test)]
        if p in region_4:
            region_4_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_4_list]
        sum_region_4 = [sum(x) for x in zip(*test)]
        if p in region_5:
            region_5_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_5_list]
        sum_region_5 = [sum(x) for x in zip(*test)]
        if p in region_6:
            region_6_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_6_list]
        sum_region_6 = [sum(x) for x in zip(*test)]
        if p in region_7:
            region_7_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_7_list]
        sum_region_7 = [sum(x) for x in zip(*test)]
        if p in region_8:
            region_8_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_8_list]
        sum_region_8 = [sum(x) for x in zip(*test)]
        if p in region_9:
            region_9_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_9_list]
        sum_region_9 = [sum(x) for x in zip(*test)]
        if p in region_10:
            region_10_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_10_list]
        sum_region_10 = [sum(x) for x in zip(*test)]

    l1 = ["Capital Region"] + sum_region_1
    l2 = ["Central NY"] + sum_region_2
    l3 = ["Finger Lakes"] + sum_region_3
    l4 = ["Long Island"] + sum_region_4
    l5 = ["Mid-Hudson"] + sum_region_5
    l6 = ["Mohawk Valley"] + sum_region_6
    l7 = ["New York City"] + sum_region_7
    l8 = ["North Country"] + sum_region_8
    l9 = ["Southern Tier"] + sum_region_9
    l10 = ["Western NY"] + sum_region_10
    full_list.append(l1)
    full_list.append(l2)
    full_list.append(l3)
    full_list.append(l4)
    full_list.append(l5)
    full_list.append(l6)
    full_list.append(l7)
    full_list.append(l8)
    full_list.append(l9)
    full_list.append(l10)

    if GRP11 is None:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Rent_burden)

        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is None and HISP is None:
            global_data.NB_ALL_Rent_burden_p = get_percentage(df=result_df, indicator='Rent_Burden')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_ALL_Rent_burden_p.to_csv(PATH + 'step_2/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
            global_data.NB_WNH_Rent_burden_p = get_percentage(df=result_df, indicator='Rent_Burden')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_WNH_Rent_burden_p.to_csv(PATH + 'step_2/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is 1:
            global_data.FB_WNH_Rent_burden_p = get_percentage(df=result_df, indicator='Rent_Burden')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_WNH_Rent_burden_p.to_csv(PATH + 'step_2/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is None and HISP is None:
            global_data.FB_ALL_Rent_burden_p = get_percentage(df=result_df, indicator='Rent_Burden')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_ALL_Rent_burden_p.to_csv(PATH + 'step_2/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is None:
            global_data.FB_POC_Rent_burden_p = get_percentage(df=result_df, indicator='Rent_Burden')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_POC_Rent_burden_p.to_csv(PATH + 'step_2/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
    else:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Rent_burden_grp_11)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
        global_data.FB_ALL_GRP11_Rent_burden_p = get_percentage(df=result_df, indicator='Rent_Burden',GRP11=1)
        make_sure_path_exists(PATH + 'step_2/')
        global_data.FB_ALL_GRP11_Rent_burden_p.to_csv(PATH + 'step_2/' + 'Rent_Burden.csv', na_rep="#DIV/0!")
def Home_Ownership(NATIVITY = None,PATH = '/', RAC1P=None, HISP=None,GRP11=None,FB_WNH=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]
    # P: Poverty, NP: Not Poverty
    col_NB_Home_Ownership = ['puma',
                    'Total_geo', 'Total_geo_m', 'Total_geo_f',
                    'Total_Owner_mf_t', 'Total_Owner_m', 'Total_Owner_f',
                    'Total_NotOwner_mf_t', 'Total_NotOwner_m', 'Total_NotOwner_f',

                    'Total_BABS', 'Total_BABS_m', 'Total_BABS_f',
                    'BABS_Owner_mf_t', 'BABS_Owner_m', 'BABS_Owner_f',
                    'BABS_NotOwner_mf_t', 'BABS_NotOwner_m', 'BABS_NotOwner_f',

                    'Total_HS', 'Total_HS_m', 'Total_HS_f',
                    'HS_Owner_mf_t', 'HS_Owner_m', 'HS_Owner_f',
                    'HS_NotOwner_mf_t', 'HS_NotOwner_m', 'HS_NotOwner_f',

                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_Owner_mf_t', 'HSINC_Owner_m', 'HSINC_Owner_f',
                    'HSINC_NotOwner_mf_t', 'HSINC_NotOwner_m', 'HSINC_NotOwner_f'
                    ]
    col_NB_Home_Ownership_grp_11=['puma',
                    'Total_HSINC', 'Total_HSINC_m', 'Total_HSINC_f',
                    'HSINC_Owner_mf_t', 'HSINC_Owner_m', 'HSINC_Owner_f',
                    'HSINC_NotOwner_mf_t', 'HSINC_NotOwner_m', 'HSINC_NotOwner_f'
                    ]
    full_list = []
    # First row is not a PUMA county. It is the summation row.
    print ('----------------- For Total Geo')
    l = create_row_dataframe(NATIVITY=NATIVITY,TEN=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data.PUMA_Counties:
        print ('----------------- For PUMA: ', puma_county(p))
        l = create_row_dataframe(PUMA=p,NATIVITY=NATIVITY,TEN=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_1_list]
        sum_region_1 = [sum(x) for x in zip(*test)] #sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_2_list]
        sum_region_2 = [sum(x) for x in zip(*test)]
        if p in region_3:
            region_3_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_3_list]
        sum_region_3 = [sum(x) for x in zip(*test)]
        if p in region_4:
            region_4_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_4_list]
        sum_region_4 = [sum(x) for x in zip(*test)]
        if p in region_5:
            region_5_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_5_list]
        sum_region_5 = [sum(x) for x in zip(*test)]
        if p in region_6:
            region_6_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_6_list]
        sum_region_6 = [sum(x) for x in zip(*test)]
        if p in region_7:
            region_7_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_7_list]
        sum_region_7 = [sum(x) for x in zip(*test)]
        if p in region_8:
            region_8_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_8_list]
        sum_region_8 = [sum(x) for x in zip(*test)]
        if p in region_9:
            region_9_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_9_list]
        sum_region_9 = [sum(x) for x in zip(*test)]
        if p in region_10:
            region_10_list.append(l[1:])
        test = [[x if not math.isnan(x) else 0 for x in y] for y in region_10_list]
        sum_region_10 = [sum(x) for x in zip(*test)]

    l1 = ["Capital Region"] + sum_region_1
    l2 = ["Central NY"] + sum_region_2
    l3 = ["Finger Lakes"] + sum_region_3
    l4 = ["Long Island"] + sum_region_4
    l5 = ["Mid-Hudson"] + sum_region_5
    l6 = ["Mohawk Valley"] + sum_region_6
    l7 = ["New York City"] + sum_region_7
    l8 = ["North Country"] + sum_region_8
    l9 = ["Southern Tier"] + sum_region_9
    l10 = ["Western NY"] + sum_region_10
    full_list.append(l1)
    full_list.append(l2)
    full_list.append(l3)
    full_list.append(l4)
    full_list.append(l5)
    full_list.append(l6)
    full_list.append(l7)
    full_list.append(l8)
    full_list.append(l9)
    full_list.append(l10)

    if GRP11 is None:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Home_Ownership)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is None and HISP is None:
            global_data.NB_ALL_Home_Ownership_p = get_percentage(df=result_df, indicator='Home_Ownership')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_ALL_Home_Ownership_p.to_csv(PATH + 'step_2/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
            global_data.NB_WNH_Home_Ownership_p = get_percentage(df=result_df, indicator='Home_Ownership')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_WNH_Home_Ownership_p.to_csv(PATH + 'step_2/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is 1:
            global_data.FB_WNH_Home_Ownership_p = get_percentage(df=result_df, indicator='Home_Ownership')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_WNH_Home_Ownership_p.to_csv(PATH + 'step_2/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is None and HISP is None:
            global_data.FB_ALL_Home_Ownership_p = get_percentage(df=result_df, indicator='Home_Ownership')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_ALL_Home_Ownership_p.to_csv(PATH + 'step_2/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is None:
            global_data.FB_POC_Home_Ownership_p = get_percentage(df=result_df, indicator='Home_Ownership')
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_POC_Home_Ownership_p.to_csv(PATH + 'step_2/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
    else:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Home_Ownership_grp_11)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
        global_data.FB_ALL_GRP11_Home_Ownership_p = get_percentage(df=result_df, indicator='Home_Ownership',GRP11=1)
        make_sure_path_exists(PATH + 'step_2/')
        global_data.FB_ALL_GRP11_Home_Ownership_p.to_csv(PATH + 'step_2/' + 'Home_Ownership.csv', na_rep="#DIV/0!")
def Income_level_FT_Workers(NATIVITY = None,PATH = '/', RAC1P=None, HISP=None,GRP11=None,FB_WNH=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]

    col_NB_Income_Level_FT_workers = [
                                        'puma',
                                        'Total_geo','Total_geo_m','Total_geo_f',
                                        'Total_Avg_PINCP_mf_t','Total_Avg_PINCP_m','Total_Avg_PINCP_f',
                                        #Weighted Total PINCP

                                        'Total_BABS','Total_BABS_m','Total_BABS_f',
                                        'BABS_Avg_PINCP_mf_t','BABS_Avg_PINCP_m','BABS_Avg_PINCP_f',
                                        #Weighted Total PINCP BABS

                                        'Total_HS','Total_HS_m','Total_HS_f',
                                        'HS_Avg_PINCP_mf_t','HS_Avg_PINCP_m','HS_Avg_PINCP_f',
                                        # Weighted Total PINCP HS

                                        'Total_HSINC','Total_HSINC_m','Total_HSINC_f',
                                        'HSINC_Avg_PINCP_mf_t','HSINC_Avg_PINCP_m','HSINC_Avg_PINCP_f',
                                        # Weighted Total PINCP HSINC
                                        ]
    col_NB_Income_Level_FT_workers_grp_11=[
                                        'puma',
                                        'Total_HSINC','Total_HSINC_m','Total_HSINC_f',
                                        'HSINC_Avg_PINCP_mf_t','HSINC_Avg_PINCP_m','HSINC_Avg_PINCP_f',
                                        # Weighted Total PINCP HSINC
                                        ]
    full_list = []
    # First row is not a PUMA county. It is the summation row.
    print ('----------------- For Total Geo')
    l = create_row_dataframe(NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data.PUMA_Counties:
        #print ('----------------- For PUMA: ', puma_county(p))
        l = create_row_dataframe(PUMA=p,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_1_list]
        # sum_region_1 = [sum(x) for x in zip(*test)] #sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_2_list]
        # sum_region_2 = [sum(x) for x in zip(*test)]

        if p in region_3:
            region_3_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_3_list]
        # sum_region_3 = [sum(x) for x in zip(*test)]

        if p in region_4:
            region_4_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_4_list]
        # sum_region_4 = [sum(x) for x in zip(*test)]
        if p in region_5:
            region_5_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_5_list]
        # sum_region_5 = [sum(x) for x in zip(*test)]

        if p in region_6:
            region_6_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_6_list]
        # sum_region_6 = [sum(x) for x in zip(*test)]

        if p in region_7:
            region_7_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_7_list]
        # sum_region_7 = [sum(x) for x in zip(*test)]

        if p in region_8:
            region_8_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_8_list]
        # sum_region_8 = [sum(x) for x in zip(*test)]

        if p in region_9:
            region_9_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_9_list]
        # sum_region_9 = [sum(x) for x in zip(*test)]

        if p in region_10:
            region_10_list.append(p)
        # test = [[x if not math.isnan(x) else 0 for x in y] for y in region_10_list]
        # sum_region_10 = [sum(x) for x in zip(*test)]


    sum_region_1 = create_row_dataframe(PUMA=region_1_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_2 = create_row_dataframe(PUMA=region_2_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_3 = create_row_dataframe(PUMA=region_3_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_4 = create_row_dataframe(PUMA=region_4_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_5 = create_row_dataframe(PUMA=region_5_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_6 = create_row_dataframe(PUMA=region_6_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_7 = create_row_dataframe(PUMA=region_7_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_8 = create_row_dataframe(PUMA=region_8_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_9 = create_row_dataframe(PUMA=region_9_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)
    sum_region_10 = create_row_dataframe(PUMA=region_10_list,NATIVITY=NATIVITY,WKHP=1,PINCP=1,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11,FB_WNH=FB_WNH)

    l1 = ["Capital Region"] + sum_region_1
    l2 = ["Central NY"] + sum_region_2
    l3 = ["Finger Lakes"] + sum_region_3
    l4 = ["Long Island"] + sum_region_4
    l5 = ["Mid-Hudson"] + sum_region_5
    l6 = ["Mohawk Valley"] + sum_region_6
    l7 = ["New York City"] + sum_region_7
    l8 = ["North Country"] + sum_region_8
    l9 = ["Southern Tier"] + sum_region_9
    l10 = ["Western NY"] + sum_region_10
    full_list.append(l1)
    full_list.append(l2)
    full_list.append(l3)
    full_list.append(l4)
    full_list.append(l5)
    full_list.append(l6)
    full_list.append(l7)
    full_list.append(l8)
    full_list.append(l9)
    full_list.append(l10)

    if GRP11 is None:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Income_Level_FT_workers)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Income_level_FT_Workers.csv',na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is None and HISP is None:
            global_data.NB_ALL_Income_level_p = result_df
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_ALL_Income_level_p.to_csv(PATH + 'step_2/' + 'Income_level_FT_Workers.csv', na_rep="#DIV/0!")
        if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
            global_data.NB_WNH_Income_level_p = result_df
            make_sure_path_exists(PATH + 'step_2/')
            global_data.NB_WNH_Income_level_p.to_csv(PATH + 'step_2/' + 'Income_level_FT_Workers.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is 1:
            global_data.FB_WNH_Income_level_p = result_df
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_WNH_Income_level_p.to_csv(PATH + 'step_2/' + 'Income_level_FT_Workers.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is None and HISP is None:
            global_data.FB_ALL_Income_level_p = result_df
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_ALL_Income_level_p.to_csv(PATH + 'step_2/' + 'Income_level_FT_Workers.csv', na_rep="#DIV/0!")
        if NATIVITY is 2 and RAC1P is 1 and HISP is 1 and FB_WNH is None:
            global_data.FB_POC_Income_level_p = result_df
            make_sure_path_exists(PATH + 'step_2/')
            global_data.FB_POC_Income_level_p.to_csv(PATH + 'step_2/' + 'Income_level_FT_Workers.csv', na_rep="#DIV/0!")
    else:
        result_df = pandas.DataFrame(full_list, columns=col_NB_Income_Level_FT_workers_grp_11)
        make_sure_path_exists(PATH + 'step_1/')
        result_df.to_csv(PATH + 'step_1/' + 'Income_level_FT_Workers.csv', na_rep="#DIV/0!")
        global_data.FB_ALL_GRP11_Income_level_p = result_df
        make_sure_path_exists(PATH + 'step_2/')
        global_data.FB_ALL_GRP11_Income_level_p.to_csv(PATH + 'step_2/' + 'Income_level_FT_Workers.csv', na_rep="#DIV/0!")

def NB_ALL():
    print ('NB_ALL')
    PATH = 'data/2014/NB_All/'
    UnEmp(NATIVITY=1,PATH=PATH)
    FT_Work(NATIVITY=1,PATH=PATH)
    Poverty(NATIVITY=1,PATH=PATH)
    Working_poor(NATIVITY=1,PATH=PATH)
    Rent_burden(NATIVITY=1,PATH=PATH)
    Home_Ownership(NATIVITY=1,PATH=PATH)
    Income_level_FT_Workers(NATIVITY=1,PATH=PATH)

def FB_ALL():

    print ('FB_ALL')
    PATH = 'data/2014/FB_All/'
    UnEmp(NATIVITY=2,PATH=PATH)
    FT_Work(NATIVITY=2,PATH=PATH)
    Poverty(NATIVITY=2,PATH=PATH)
    Working_poor(NATIVITY=2,PATH=PATH)
    Rent_burden(NATIVITY=2,PATH=PATH)
    Home_Ownership(NATIVITY=2,PATH=PATH)
    Income_level_FT_Workers(NATIVITY=2, PATH=PATH)

    #GRP 11
    print('GRP11')
    PATH = 'data/2014/GRP11/'
    UnEmp(NATIVITY=2, PATH=PATH,GRP11=1)
    FT_Work(NATIVITY=2, PATH=PATH,GRP11=1)
    Poverty(NATIVITY=2, PATH=PATH,GRP11=1)
    Working_poor(NATIVITY=2, PATH=PATH,GRP11=1)
    Rent_burden(NATIVITY=2, PATH=PATH,GRP11=1)
    Home_Ownership(NATIVITY=2, PATH=PATH,GRP11=1)
    Income_level_FT_Workers(NATIVITY=2, PATH=PATH,GRP11=1)

def NB_WNH():
    print('NB_WNH')
    PATH = 'data/2014/NB_WNH/'
    UnEmp(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1)
    FT_Work(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1)
    Poverty(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1)
    Working_poor(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1)
    Rent_burden(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1)
    Home_Ownership(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1)
    Income_level_FT_Workers(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1)

def FB_WNH():
    print('FB_WNH')
    PATH = 'data/2014/FB_WNH/'
    UnEmp(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,FB_WNH=1)
    FT_Work(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,FB_WNH=1)
    Poverty(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,FB_WNH=1)
    Working_poor(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,FB_WNH=1)
    Rent_burden(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,FB_WNH=1)
    Home_Ownership(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,FB_WNH=1)
    Income_level_FT_Workers(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,FB_WNH=1)

def FB_POC(): # TODO Need to check result
    print('FB_POC')
    PATH = 'data/2014/FB_POC/'
    #RAC1P=1,HISP=1 used for People of color
    UnEmp(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1)
    FT_Work(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1)
    Poverty(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1)
    Working_poor(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1)
    Rent_burden(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1)
    Home_Ownership(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1)
    Income_level_FT_Workers(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1)


def get_percentage(df = None,indicator = None, GRP11=None):
    new_df = pandas.DataFrame()

    columns = list(df)
    if GRP11 is None and indicator is not 'FT_Work' and indicator is not 'Home_Ownership' and indicator is not 'Income_Level_FT_Workers':
        print (indicator)
        for i in df.index:
            new_df.at[i, 'puma'] = df.at[i, 'puma']
            new_df.at[i, 'BABS_'+indicator+'_Total'] = ((df.at[i, columns[16]] * (1.0)) / df.at[i, columns[10]])*100
            new_df.at[i, 'BABS_'+indicator+'_M'] = ((df.at[i, columns[17]] * (1.0)) / df.at[i, columns[11]])*100
            new_df.at[i, 'BABS_'+indicator+'_F'] = ((df.at[i, columns[18]] * (1.0)) / df.at[i, columns[12]])*100

            new_df.at[i, 'HS_'+indicator+'_Total'] = ((df.at[i, columns[25]] * (1.0)) / df.at[i, columns[19]])*100
            new_df.at[i, 'HS_'+indicator+'_M'] = ((df.at[i, columns[26]] * (1.0)) / df.at[i, columns[20]])*100
            new_df.at[i, 'HS_'+indicator+'_F'] = ((df.at[i, columns[27]] * (1.0)) / df.at[i, columns[21]])*100

            #print df.at[i, columns[27]] * (1.0),df.at[i, columns[21]],(df.at[i, columns[27]] * (1.0)/df.at[i, columns[21]])*100

            new_df.at[i, 'HSINC_'+indicator+'_Total'] = ((df.at[i, columns[34]] * (1.0)) / df.at[i, columns[28]])*100
            new_df.at[i, 'HSINC_'+indicator+'_M'] = ((df.at[i, columns[35]] * (1.0)) / df.at[i, columns[29]])*100
            new_df.at[i, 'HSINC_'+indicator+'_F'] = ((df.at[i, columns[36]] * (1.0)) / df.at[i, columns[30]])*100
    elif GRP11 is None and indicator is 'Income_level_FT_Workers':
        return new_df
    elif GRP11 is None and (indicator is 'FT_Work' or indicator is 'Home_Ownership'):
        for i in df.index:
            new_df.at[i, 'puma'] = df.at[i, 'puma']
            new_df.at[i, 'BABS_' + indicator + '_Total'] = ((df.at[i, columns[13]] * (1.0)) / df.at[i, columns[10]])*100
            new_df.at[i, 'BABS_' + indicator + '_M'] = ((df.at[i, columns[14]] * (1.0)) / df.at[i, columns[11]])*100
            new_df.at[i, 'BABS_' + indicator + '_F'] = ((df.at[i, columns[15]] * (1.0)) / df.at[i, columns[12]])*100

            new_df.at[i, 'HS_' + indicator + '_Total'] = ((df.at[i, columns[22]] * (1.0)) / df.at[i, columns[19]])*100
            new_df.at[i, 'HS_' + indicator + '_M'] = ((df.at[i, columns[23]] * (1.0)) / df.at[i, columns[20]])*100
            new_df.at[i, 'HS_' + indicator + '_F'] = ((df.at[i, columns[24]] * (1.0)) / df.at[i, columns[21]])*100

            new_df.at[i, 'HSINC_' + indicator + '_Total'] = ((df.at[i, columns[31]] * (1.0)) / df.at[i, columns[28]])*100
            new_df.at[i, 'HSINC_' + indicator + '_M'] = ((df.at[i, columns[32]] * (1.0)) / df.at[i, columns[29]])*100
            new_df.at[i, 'HSINC_' + indicator + '_F'] = ((df.at[i, columns[33]] * (1.0)) / df.at[i, columns[30]])*100
    elif GRP11 is 1 and indicator is not 'FT_Work' and indicator is not 'Home_Ownership' and indicator is not 'Income_Level_FT_Workers':
        for i in df.index:
            new_df.at[i, 'puma'] = df.at[i, 'puma']
            new_df.at[i, 'HSINC_' + indicator + '_Total'] = ((df.at[i, columns[7]] * (1.0)) / df.at[i, columns[1]]) * 100
            new_df.at[i, 'HSINC_' + indicator + '_M'] = ((df.at[i, columns[8]] * (1.0)) / df.at[i, columns[2]]) * 100
            new_df.at[i, 'HSINC_' + indicator + '_F'] = ((df.at[i, columns[9]] * (1.0)) / df.at[i, columns[3]]) * 100
    elif GRP11 is 1 and indicator is 'Income_level_FT_Workers':
        return new_df
    elif GRP11 is 1 and (indicator is 'FT_Work' or indicator is 'Home_Ownership'):
        for i in df.index:
            new_df.at[i, 'puma'] = df.at[i, 'puma']
            new_df.at[i, 'HSINC_' + indicator + '_Total'] = ((df.at[i, columns[4]] * (1.0)) / df.at[i, columns[1]]) * 100
            new_df.at[i, 'HSINC_' + indicator + '_M'] = ((df.at[i, columns[5]] * (1.0)) / df.at[i, columns[2]]) * 100
            new_df.at[i, 'HSINC_' + indicator + '_F'] = ((df.at[i, columns[6]] * (1.0)) / df.at[i, columns[3]]) * 100

    print (indicator)
    return new_df
def get_disparity_score_grade_grp_11(PATH=None):
    disparity_grp11=pandas.DataFrame()
    disparity_grp11_final=pandas.DataFrame()
    UnEmp=pandas.read_csv('data/2014/GRP11/step_2/UnEmp.csv',usecols=['puma','HSINC_UnEmp_Total'])
    FT_work=pandas.read_csv('data/2014/GRP11/step_2/FT_Work.csv',usecols=['HSINC_FT_Work_Total'])
    Poverty=pandas.read_csv('data/2014/GRP11/step_2/Poverty.csv',usecols=['HSINC_Poverty_Total'])
    Working_Poor=pandas.read_csv('data/2014/GRP11/step_2/Working_Poor.csv',usecols=['HSINC_Working_Poor_Total'])
    Rent_Burden=pandas.read_csv('data/2014/GRP11/step_2/Rent_Burden.csv',usecols=['HSINC_Rent_Burden_Total'])
    Home_Ownership=pandas.read_csv('data/2014/GRP11/step_2/Home_Ownership.csv',usecols=['HSINC_Home_Ownership_Total'])
    Income_level=pandas.read_csv('data/2014/GRP11/step_2/Income_level_FT_Workers.csv',usecols=['HSINC_Avg_PINCP_mf_t'])

    disparity_grp11=pandas.concat([UnEmp,FT_work,Poverty,Working_Poor,Rent_Burden,Home_Ownership,Income_level],axis=1)
    score_columns_list=['Unemployment_HSINC_score','FT_Work_HSINC_score','Poverty_HSINC_score','Working_Poor_HSINC_score','Rent_Burden_HSINC_score','Home_Ownership_HSINC_score','Income_level_HSINC_score','Overall_HSINC_score']

    Unemp_grp11 = disparity_grp11[(disparity_grp11.HSINC_UnEmp_Total != '#DIV/0!') & (disparity_grp11.HSINC_UnEmp_Total != 'inf') & (
    disparity_grp11.HSINC_UnEmp_Total != '-inf')]
    mean_UnEmp_grp11_county = pandas.to_numeric(Unemp_grp11['HSINC_UnEmp_Total']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_grp11_region = pandas.to_numeric(Unemp_grp11['HSINC_UnEmp_Total']).reindex(index=range(146,156)).mean()
    stddev_UnEmp_grp11_county = pandas.to_numeric(Unemp_grp11['HSINC_UnEmp_Total']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_grp11_region = pandas.to_numeric(Unemp_grp11['HSINC_UnEmp_Total']).reindex(index=range(146,156)).std()

    FT_Work_grp11 = disparity_grp11[(disparity_grp11.HSINC_FT_Work_Total != '#DIV/0!') & (disparity_grp11.HSINC_FT_Work_Total != 'inf') & (
    disparity_grp11.HSINC_FT_Work_Total != '-inf')]
    mean_FT_Work_grp11_county = pandas.to_numeric(FT_Work_grp11['HSINC_FT_Work_Total']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_grp11_region = pandas.to_numeric(FT_Work_grp11['HSINC_FT_Work_Total']).reindex(index=range(146,156)).mean()
    stddev_FT_Work_grp11_county = pandas.to_numeric(FT_Work_grp11['HSINC_FT_Work_Total']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_grp11_region = pandas.to_numeric(FT_Work_grp11['HSINC_FT_Work_Total']).reindex(index=range(146,156)).std()

    Poverty_grp11 = disparity_grp11[(disparity_grp11.HSINC_Poverty_Total != '#DIV/0!') & (disparity_grp11.HSINC_Poverty_Total != 'inf') & (
    disparity_grp11.HSINC_Poverty_Total != '-inf')]
    mean_Poverty_grp11_county = pandas.to_numeric(Poverty_grp11['HSINC_Poverty_Total']).reindex(index=range(0, 146)).mean()
    mean_Poverty_grp11_region = pandas.to_numeric(Poverty_grp11['HSINC_Poverty_Total']).reindex(index=range(146,156)).mean()
    stddev_Poverty_grp11_county = pandas.to_numeric(Poverty_grp11['HSINC_Poverty_Total']).reindex(index=range(0, 146)).std()
    stddev_Poverty_grp11_region = pandas.to_numeric(Poverty_grp11['HSINC_Poverty_Total']).reindex(index=range(146,156)).std()

    Working_Poor_grp11 = disparity_grp11[
        (disparity_grp11.HSINC_Working_Poor_Total != '#DIV/0!') & (disparity_grp11.HSINC_Working_Poor_Total != 'inf') & (
        disparity_grp11.HSINC_Working_Poor_Total != '-inf')]
    mean_Working_Poor_grp11_county = pandas.to_numeric(Working_Poor_grp11['HSINC_Working_Poor_Total']).reindex(index=range(0, 146)).mean()
    mean_Working_Poor_grp11_region = pandas.to_numeric(Working_Poor_grp11['HSINC_Working_Poor_Total']).reindex(index=range(146,156)).mean()
    stddev_Working_Poor_grp11_county = pandas.to_numeric(Working_Poor_grp11['HSINC_Working_Poor_Total']).reindex(index=range(0, 146)).std()
    stddev_Working_Poor_grp11_region = pandas.to_numeric(Working_Poor_grp11['HSINC_Working_Poor_Total']).reindex(index=range(146,156)).std()
    
    Rent_Burden_grp11 = disparity_grp11[
        (disparity_grp11.HSINC_Rent_Burden_Total != '#DIV/0!') & (disparity_grp11.HSINC_Rent_Burden_Total != 'inf') & (
        disparity_grp11.HSINC_Rent_Burden_Total != '-inf')]
    mean_Rent_Burden_grp11_county = pandas.to_numeric(Rent_Burden_grp11['HSINC_Rent_Burden_Total']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_grp11_region = pandas.to_numeric(Rent_Burden_grp11['HSINC_Rent_Burden_Total']).reindex(index=range(146,156)).mean()
    stddev_Rent_Burden_grp11_county = pandas.to_numeric(Rent_Burden_grp11['HSINC_Rent_Burden_Total']).reindex(index=range(0,146)).std()
    stddev_Rent_Burden_grp11_region = pandas.to_numeric(Rent_Burden_grp11['HSINC_Rent_Burden_Total']).reindex(index=range(146,156)).std()

    Home_Ownership_grp11 = disparity_grp11[
        (disparity_grp11.HSINC_Home_Ownership_Total != '#DIV/0!') & (disparity_grp11.HSINC_Home_Ownership_Total != 'inf') & (
        disparity_grp11.HSINC_Home_Ownership_Total != '-inf')]
    mean_Home_Ownership_grp11_county = pandas.to_numeric(Home_Ownership_grp11['HSINC_Home_Ownership_Total']).reindex(index=range(0, 146)).mean()
    mean_Home_Ownership_grp11_region = pandas.to_numeric(Home_Ownership_grp11['HSINC_Home_Ownership_Total']).reindex(index=range(146,156)).mean()
    stddev_Home_Ownership_grp11_county = pandas.to_numeric(Home_Ownership_grp11['HSINC_Home_Ownership_Total']).reindex(index=range(0, 146)).std()
    stddev_Home_Ownership_grp11_region = pandas.to_numeric(Home_Ownership_grp11['HSINC_Home_Ownership_Total']).reindex(index=range(146,156)).std()

    Income_level_grp11 = disparity_grp11[
        (disparity_grp11.HSINC_Avg_PINCP_mf_t != '#DIV/0!') & (disparity_grp11.HSINC_Avg_PINCP_mf_t != 'inf') & (
        disparity_grp11.HSINC_Avg_PINCP_mf_t != '-inf')]
    mean_Income_level_grp11_county = pandas.to_numeric(Income_level_grp11['HSINC_Avg_PINCP_mf_t']).reindex(index=range(0, 146)).mean()
    mean_Income_level_grp11_region = pandas.to_numeric(Income_level_grp11['HSINC_Avg_PINCP_mf_t']).reindex(index=range(146,156)).mean()
    stddev_Income_level_grp11_county = pandas.to_numeric(Income_level_grp11['HSINC_Avg_PINCP_mf_t']).reindex(index=range(0, 146)).std()
    stddev_Income_level_grp11_region = pandas.to_numeric(Income_level_grp11['HSINC_Avg_PINCP_mf_t']).reindex(index=range(146,156)).std()

    for i in disparity_grp11.index:
        if i >= 0 and i<= 146:
            disparity_grp11_final.at[i, 'puma'] = disparity_grp11.at[i, 'puma']
            if disparity_grp11.at[i, 'HSINC_UnEmp_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_UnEmp_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_UnEmp_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Unemployment_HSINC_score'] = ((float(disparity_grp11.at[i, 'HSINC_UnEmp_Total']) - (
                mean_UnEmp_grp11_county)) * (-1.0)) / stddev_UnEmp_grp11_county
            else:
                disparity_grp11_final.at[i, 'Unemployment_HSINC_score'] = np.nan
    
            if disparity_grp11.at[i, 'HSINC_FT_Work_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_FT_Work_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_FT_Work_Total'] != '-inf':
                disparity_grp11_final.at[i, 'FT_Work_HSINC_score'] = ((float(disparity_grp11.at[i, 'HSINC_FT_Work_Total']) - (
                mean_FT_Work_grp11_county)) * (1.0)) / stddev_FT_Work_grp11_county
            else:
                disparity_grp11_final.at[i, 'FT_Work_HSINC_score'] = np.nan
    
            if disparity_grp11.at[i, 'HSINC_Poverty_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Poverty_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Poverty_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Poverty_HSINC_score'] = ((float(disparity_grp11.at[i, 'HSINC_Poverty_Total']) - (
                mean_Poverty_grp11_county)) * (-1.0)) / stddev_Poverty_grp11_county
            else:
                disparity_grp11_final.at[i, 'Poverty_HSINC_score'] = np.nan
    
            if disparity_grp11.at[i, 'HSINC_Working_Poor_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Working_Poor_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Working_Poor_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Working_Poor_HSINC_score'] = ((float(disparity_grp11.at[i, 'HSINC_Working_Poor_Total']) - (
                mean_Working_Poor_grp11_county)) * (-1.0)) / stddev_Working_Poor_grp11_county
            else:
                disparity_grp11_final.at[i, 'Working_Poor_HSINC_score'] = np.nan
    
            if disparity_grp11.at[i, 'HSINC_Rent_Burden_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Rent_Burden_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Rent_Burden_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Rent_Burden_HSINC_score'] = ((float(disparity_grp11.at[i, 'HSINC_Rent_Burden_Total']) - (
                mean_Rent_Burden_grp11_county)) * (-1.0)) / stddev_Rent_Burden_grp11_county
            else:
                disparity_grp11_final.at[i, 'Rent_Burden_HSINC_score'] = np.nan
    
            if disparity_grp11.at[i, 'HSINC_Home_Ownership_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Home_Ownership_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Home_Ownership_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Home_Ownership_HSINC_score'] = ((float(disparity_grp11.at[i, 'HSINC_Home_Ownership_Total']) - (
                mean_Home_Ownership_grp11_county)) * (1.0)) / stddev_Home_Ownership_grp11_county
            else:
                disparity_grp11_final.at[i, 'Home_Ownership_HSINC_score'] = np.nan
    
            if disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t'] != '-inf':
                disparity_grp11_final.at[i, 'Income_level_HSINC_score'] = ((float(disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t']) - (
                mean_Income_level_grp11_county)) * (1.0)) / stddev_Income_level_grp11_county
            else:
                disparity_grp11_final.at[i, 'Income_level_HSINC_score'] = np.nan
    
            disparity_grp11_final.at[i, 'Overall_HSINC_score'] = ((disparity_grp11_final.at[i, 'Unemployment_HSINC_score'] +
                                                              disparity_grp11_final.at[i, 'FT_Work_HSINC_score'] +
                                                              disparity_grp11_final.at[i, 'Poverty_HSINC_score'] +
                                                              disparity_grp11_final.at[i, 'Working_Poor_HSINC_score'] +
                                                              disparity_grp11_final.at[i, 'Rent_Burden_HSINC_score'] +
                                                              disparity_grp11_final.at[i, 'Home_Ownership_HSINC_score'] +
                                                              disparity_grp11_final.at[i, 'Income_level_HSINC_score']) * 1.0) / 7
            for column in score_columns_list:
                indicator = str(column).split('_score')
                if disparity_grp11_final.at[i, column] >= (1.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'A'
                elif disparity_grp11_final.at[i, column] >= (1.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif disparity_grp11_final.at[i, column] >= (0.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'B'
                elif disparity_grp11_final.at[i, column] >= (0.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif disparity_grp11_final.at[i, column] >= (-0.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'C'
                elif disparity_grp11_final.at[i, column] >= (-0.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif disparity_grp11_final.at[i, column] >= (-1.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'D'
                elif disparity_grp11_final.at[i, column] >= (-1.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif disparity_grp11_final.at[i, column] < (-1.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            disparity_grp11_final.at[i, 'puma'] = disparity_grp11.at[i, 'puma']
            if disparity_grp11.at[i, 'HSINC_UnEmp_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_UnEmp_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_UnEmp_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Unemployment_HSINC_score'] = ((float(
                    disparity_grp11.at[i, 'HSINC_UnEmp_Total']) - (
                                                                                mean_UnEmp_grp11_region)) * (
                                                                           -1.0)) / stddev_UnEmp_grp11_region
            else:
                disparity_grp11_final.at[i, 'Unemployment_HSINC_score'] = np.nan

            if disparity_grp11.at[i, 'HSINC_FT_Work_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_FT_Work_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_FT_Work_Total'] != '-inf':
                disparity_grp11_final.at[i, 'FT_Work_HSINC_score'] = ((float(
                    disparity_grp11.at[i, 'HSINC_FT_Work_Total']) - (
                                                                           mean_FT_Work_grp11_region)) * (
                                                                      1.0)) / stddev_FT_Work_grp11_region
            else:
                disparity_grp11_final.at[i, 'FT_Work_HSINC_score'] = np.nan

            if disparity_grp11.at[i, 'HSINC_Poverty_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Poverty_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Poverty_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Poverty_HSINC_score'] = ((float(
                    disparity_grp11.at[i, 'HSINC_Poverty_Total']) - (
                                                                           mean_Poverty_grp11_region)) * (
                                                                      -1.0)) / stddev_Poverty_grp11_region
            else:
                disparity_grp11_final.at[i, 'Poverty_HSINC_score'] = np.nan

            if disparity_grp11.at[i, 'HSINC_Working_Poor_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Working_Poor_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Working_Poor_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Working_Poor_HSINC_score'] = ((float(
                    disparity_grp11.at[i, 'HSINC_Working_Poor_Total']) - (
                                                                                mean_Working_Poor_grp11_region)) * (
                                                                           -1.0)) / stddev_Working_Poor_grp11_region
            else:
                disparity_grp11_final.at[i, 'Working_Poor_HSINC_score'] = np.nan

            if disparity_grp11.at[i, 'HSINC_Rent_Burden_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Rent_Burden_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Rent_Burden_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Rent_Burden_HSINC_score'] = ((float(
                    disparity_grp11.at[i, 'HSINC_Rent_Burden_Total']) - (
                                                                               mean_Rent_Burden_grp11_region)) * (
                                                                          -1.0)) / stddev_Rent_Burden_grp11_region
            else:
                disparity_grp11_final.at[i, 'Rent_Burden_HSINC_score'] = np.nan

            if disparity_grp11.at[i, 'HSINC_Home_Ownership_Total'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Home_Ownership_Total'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Home_Ownership_Total'] != '-inf':
                disparity_grp11_final.at[i, 'Home_Ownership_HSINC_score'] = ((float(
                    disparity_grp11.at[i, 'HSINC_Home_Ownership_Total']) - (
                                                                                  mean_Home_Ownership_grp11_region)) * (
                                                                             1.0)) / stddev_Home_Ownership_grp11_region
            else:
                disparity_grp11_final.at[i, 'Home_Ownership_HSINC_score'] = np.nan

            if disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t'] != '#DIV/0!' and \
                            disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t'] != 'inf' and \
                            disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t'] != '-inf':
                disparity_grp11_final.at[i, 'Income_level_HSINC_score'] = ((float(
                    disparity_grp11.at[i, 'HSINC_Avg_PINCP_mf_t']) - (
                                                                                mean_Income_level_grp11_region)) * (
                                                                           1.0)) / stddev_Income_level_grp11_region
            else:
                disparity_grp11_final.at[i, 'Income_level_HSINC_score'] = np.nan

            disparity_grp11_final.at[i, 'Overall_HSINC_score'] = ((disparity_grp11_final.at[
                                                                       i, 'Unemployment_HSINC_score'] +
                                                                   disparity_grp11_final.at[i, 'FT_Work_HSINC_score'] +
                                                                   disparity_grp11_final.at[i, 'Poverty_HSINC_score'] +
                                                                   disparity_grp11_final.at[
                                                                       i, 'Working_Poor_HSINC_score'] +
                                                                   disparity_grp11_final.at[
                                                                       i, 'Rent_Burden_HSINC_score'] +
                                                                   disparity_grp11_final.at[
                                                                       i, 'Home_Ownership_HSINC_score'] +
                                                                   disparity_grp11_final.at[
                                                                       i, 'Income_level_HSINC_score']) * 1.0) / 7
            for column in score_columns_list:
                indicator = str(column).split('_score')
                if disparity_grp11_final.at[i, column] >= (1.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'A'
                elif disparity_grp11_final.at[i, column] >= (1.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif disparity_grp11_final.at[i, column] >= (0.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'B'
                elif disparity_grp11_final.at[i, column] >= (0.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif disparity_grp11_final.at[i, column] >= (-0.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'C'
                elif disparity_grp11_final.at[i, column] >= (-0.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif disparity_grp11_final.at[i, column] >= (-1.25):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'D'
                elif disparity_grp11_final.at[i, column] >= (-1.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif disparity_grp11_final.at[i, column] < (-1.75):
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    disparity_grp11_final.at[i, indicator[0] + '_grade'] = "#DIV!0"
    make_sure_path_exists(PATH)
    disparity_grp11.to_csv(PATH + 'FB_HSINC_Disparity_percentage.csv', na_rep="#DIV/0!")
    disparity_grp11_final.to_csv(PATH + 'FB_HSINC_Scores_Grades.csv', na_rep="#DIV/0!")

def get_disparity(PATH=None,call_string=None): #you cannot call this function without getting all the relevant percentages.
    NB_UnEmp, FB_UnEmp, NB_FT_Work, FB_FT_Work, NB_Poverty, FB_Poverty, NB_Working_Poor, FB_Working_Poor, NB_Income_level, FB_Income_level, \
    NB_Rent_burden, FB_Rent_burden, NB_Home_Ownership, FB_Home_Ownership = None, None, None, None, None, None, None, None, None, None, None, None, None, None
    if call_string is 'NB_ALL_FB_ALL' or call_string is 'NB_ALL_F_FB_ALL_F' or call_string is 'FB_ALL_F_M':
        NB_UnEmp = global_data.NB_ALL_UnEmp_p
        NB_FT_Work = global_data.NB_ALL_FT_Work_p
        NB_Poverty = global_data.NB_ALL_Poverty_p
        NB_Working_Poor = global_data.NB_ALL_Working_Poor_p
        NB_Income_level = global_data.NB_ALL_Income_level_p
        NB_Rent_burden = global_data.NB_ALL_Rent_burden_p
        NB_Home_Ownership = global_data.NB_ALL_Home_Ownership_p
        FB_UnEmp = global_data.FB_ALL_UnEmp_p
        FB_FT_Work = global_data.FB_ALL_FT_Work_p
        FB_Poverty = global_data.FB_ALL_Poverty_p
        FB_Working_Poor = global_data.FB_ALL_Working_Poor_p
        FB_Income_level = global_data.FB_ALL_Income_level_p
        FB_Rent_burden = global_data.FB_ALL_Rent_burden_p
        FB_Home_Ownership = global_data.FB_ALL_Home_Ownership_p
    elif call_string is 'NB_WNH_FB_POC' or call_string is 'NB_WNH_FB_POC_F':
        NB_UnEmp = global_data.NB_WNH_UnEmp_p
        NB_FT_Work = global_data.NB_WNH_FT_Work_p
        NB_Poverty = global_data.NB_WNH_Poverty_p
        NB_Working_Poor = global_data.NB_WNH_Working_Poor_p
        NB_Income_level = global_data.NB_WNH_Income_level_p
        NB_Rent_burden = global_data.NB_WNH_Rent_burden_p
        NB_Home_Ownership = global_data.NB_WNH_Home_Ownership_p
        FB_UnEmp = global_data.FB_POC_UnEmp_p
        FB_FT_Work = global_data.FB_POC_FT_Work_p
        FB_Poverty = global_data.FB_POC_Poverty_p
        FB_Working_Poor = global_data.FB_POC_Working_Poor_p
        FB_Income_level = global_data.FB_POC_Income_level_p
        FB_Rent_burden = global_data.FB_POC_Rent_burden_p
        FB_Home_Ownership = global_data.FB_POC_Home_Ownership_p
    elif call_string is 'FB_WNH_FB_POC' or call_string is 'FB_WNH_FB_POC_F':
        NB_UnEmp = global_data.FB_WNH_UnEmp_p
        NB_FT_Work = global_data.FB_WNH_FT_Work_p
        NB_Poverty = global_data.FB_WNH_Poverty_p
        NB_Working_Poor = global_data.FB_WNH_Working_Poor_p
        NB_Income_level = global_data.FB_WNH_Income_level_p
        NB_Rent_burden = global_data.FB_WNH_Rent_burden_p
        NB_Home_Ownership = global_data.FB_WNH_Home_Ownership_p
        FB_UnEmp = global_data.FB_POC_UnEmp_p
        FB_FT_Work = global_data.FB_POC_FT_Work_p
        FB_Poverty = global_data.FB_POC_Poverty_p
        FB_Working_Poor = global_data.FB_POC_Working_Poor_p
        FB_Income_level = global_data.FB_POC_Income_level_p
        FB_Rent_burden = global_data.FB_POC_Rent_burden_p
        FB_Home_Ownership = global_data.FB_POC_Home_Ownership_p


    if call_string is 'NB_ALL_FB_ALL':
        #if(global_data.NB_ALL_FB_ALL_d is not None and NB_UnEmp is not None and FB_UnEmp is not None):

        for i in NB_UnEmp.index:
            global_data.NB_ALL_FB_ALL_d.at[i, 'puma'] = NB_UnEmp.at[i,'puma']
            global_data.NB_ALL_FB_ALL_d.at[i,'Unemployment_BABS'] = (FB_UnEmp.at[i,'BABS_UnEmp_Total'] * (1.0) ) / NB_UnEmp.at[i,'BABS_UnEmp_Total']
            global_data.NB_ALL_FB_ALL_d.at[i,'Unemployment_HS'] = (FB_UnEmp.at[i,'HS_UnEmp_Total'] * (1.0) ) / NB_UnEmp.at[i,'HS_UnEmp_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'FT_Work_BABS'] = (FB_FT_Work.at[i, 'BABS_FT_Work_Total'] * (1.0)) / NB_FT_Work.at[i, 'BABS_FT_Work_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'FT_Work_HS'] = (FB_FT_Work.at[i, 'HS_FT_Work_Total'] * (1.0)) / NB_FT_Work.at[i, 'HS_FT_Work_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Poverty_BABS'] = (FB_Poverty.at[i, 'BABS_Poverty_Total'] * (1.0)) / NB_Poverty.at[i, 'BABS_Poverty_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Poverty_HS'] = (FB_Poverty.at[i, 'HS_Poverty_Total'] * (1.0)) / NB_Poverty.at[i, 'HS_Poverty_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Working_Poor_BABS'] = (FB_Working_Poor.at[i, 'BABS_Working_Poor_Total'] * (1.0)) / NB_Working_Poor.at[i, 'BABS_Working_Poor_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Working_Poor_HS'] = (FB_Working_Poor.at[i, 'HS_Working_Poor_Total'] * (1.0)) / NB_Working_Poor.at[i, 'HS_Working_Poor_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Income_level_BABS'] = (FB_Income_level.at[i, 'BABS_Avg_PINCP_mf_t']* (1.0)) / NB_Income_level.at[i, 'BABS_Avg_PINCP_mf_t']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Income_level_HS'] = (FB_Income_level.at[i, 'HS_Avg_PINCP_mf_t']* (1.0)) / NB_Income_level.at[i, 'HS_Avg_PINCP_mf_t']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Rent_Burden_BABS'] = (FB_Rent_burden.at[i, 'BABS_Rent_Burden_Total'] * (1.0)) / NB_Rent_burden.at[i, 'BABS_Rent_Burden_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Rent_Burden_HS'] = (FB_Rent_burden.at[i, 'HS_Rent_Burden_Total'] * (1.0)) / NB_Rent_burden.at[i, 'HS_Rent_Burden_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Home_Ownership_BABS'] = (FB_Home_Ownership.at[i, 'BABS_Home_Ownership_Total'] * (1.0)) / NB_Home_Ownership.at[i, 'BABS_Home_Ownership_Total']
            global_data.NB_ALL_FB_ALL_d.at[i, 'Home_Ownership_HS'] = (FB_Home_Ownership.at[i, 'HS_Home_Ownership_Total'] * (1.0)) / NB_Home_Ownership.at[i, 'HS_Home_Ownership_Total']
        make_sure_path_exists(PATH)
        global_data.NB_ALL_FB_ALL_d.to_csv(PATH + 'NB_ALL_FB_ALL_Disparity.csv',na_rep="#DIV/0!")
    elif call_string is 'NB_ALL_F_FB_ALL_F':
        for i in NB_UnEmp.index:
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'puma'] = NB_UnEmp.at[i, 'puma']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i,'Unemployment_BABS_F'] = (FB_UnEmp.at[i,'BABS_UnEmp_F'] * (1.0) ) / NB_UnEmp.at[i,'BABS_UnEmp_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i,'Unemployment_HS_F'] = (FB_UnEmp.at[i,'HS_UnEmp_F'] * (1.0) ) / NB_UnEmp.at[i,'HS_UnEmp_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i,'FT_Work_BABS_F'] = (FB_FT_Work.at[i, 'BABS_FT_Work_F'] * (1.0)) / NB_FT_Work.at[i, 'BABS_FT_Work_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i,'FT_Work_HS_F'] = (FB_FT_Work.at[i, 'HS_FT_Work_F'] * (1.0)) / NB_FT_Work.at[i, 'HS_FT_Work_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i,'Poverty_BABS_F'] = (FB_Poverty.at[i, 'BABS_Poverty_F'] * (1.0)) / NB_Poverty.at[i, 'BABS_Poverty_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i,'Poverty_HS_F'] = (FB_Poverty.at[i, 'HS_Poverty_F'] * (1.0)) / NB_Poverty.at[i, 'HS_Poverty_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Working_Poor_BABS_F'] = (FB_Working_Poor.at[i, 'BABS_Working_Poor_F'] * (1.0)) / NB_Working_Poor.at[i, 'BABS_Working_Poor_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Working_Poor_HS_F'] = (FB_Working_Poor.at[i, 'HS_Working_Poor_F'] * (1.0)) / NB_Working_Poor.at[i, 'HS_Working_Poor_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Income_level_BABS_F'] = (FB_Income_level.at[i, 'BABS_Avg_PINCP_f']* (1.0)) / NB_Income_level.at[i, 'BABS_Avg_PINCP_f']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Income_level_HS_F'] = (FB_Income_level.at[i, 'HS_Avg_PINCP_f']* (1.0)) / NB_Income_level.at[i, 'HS_Avg_PINCP_f']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Rent_Burden_BABS_F'] = (FB_Rent_burden.at[i, 'BABS_Rent_Burden_F'] * (1.0)) / NB_Rent_burden.at[i, 'BABS_Rent_Burden_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Rent_Burden_HS_F'] = (FB_Rent_burden.at[i, 'HS_Rent_Burden_F'] * (1.0)) / NB_Rent_burden.at[i, 'HS_Rent_Burden_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Home_Ownership_BABS_F'] = (FB_Home_Ownership.at[i, 'BABS_Home_Ownership_F'] * (1.0)) / NB_Home_Ownership.at[i,'BABS_Home_Ownership_F']
            global_data.NB_ALL_F_FB_ALL_F_d.at[i, 'Home_Ownership_HS_F'] = (FB_Home_Ownership.at[i, 'HS_Home_Ownership_F'] * (1.0)) /NB_Home_Ownership.at[i, 'HS_Home_Ownership_F']
        make_sure_path_exists(PATH)
        global_data.NB_ALL_F_FB_ALL_F_d.to_csv(PATH + 'NB_ALL_F_FB_ALL_F_Disparity.csv',na_rep="#DIV/0!")

    elif call_string is 'FB_ALL_F_M':
        for i in FB_UnEmp.index:
            global_data.FB_ALL_F_M_d.at[i,'puma'] = FB_UnEmp.at[i, 'puma']
            global_data.FB_ALL_F_M_d.at[i,'Unemployment_BABS'] = (FB_UnEmp.at[i,'BABS_UnEmp_F']) * (1.0) / FB_UnEmp.at[i,'BABS_UnEmp_M']
            global_data.FB_ALL_F_M_d.at[i, 'Unemployment_HS'] = (FB_UnEmp.at[i, 'HS_UnEmp_F']) * (1.0) / FB_UnEmp.at[i, 'HS_UnEmp_M']
            global_data.FB_ALL_F_M_d.at[i, 'FT_Work_BABS'] = (FB_FT_Work.at[i, 'BABS_FT_Work_F']) * (1.0) / FB_FT_Work.at[i, 'BABS_FT_Work_M']
            global_data.FB_ALL_F_M_d.at[i, 'FT_Work_HS'] = (FB_FT_Work.at[i, 'HS_FT_Work_F']) * (1.0) / FB_FT_Work.at[i, 'HS_FT_Work_M']
            global_data.FB_ALL_F_M_d.at[i, 'Poverty_BABS'] = (FB_Poverty.at[i, 'BABS_Poverty_F'] * (1.0)) / FB_Poverty.at[i, 'BABS_Poverty_M']
            global_data.FB_ALL_F_M_d.at[i, 'Poverty_HS'] = (FB_Poverty.at[i, 'HS_Poverty_F'] * (1.0)) / FB_Poverty.at[i, 'HS_Poverty_M']
            global_data.FB_ALL_F_M_d.at[i, 'Working_Poor_BABS'] = (FB_Working_Poor.at[i, 'BABS_Working_Poor_F'] * (1.0)) / FB_Working_Poor.at[i, 'BABS_Working_Poor_M']
            global_data.FB_ALL_F_M_d.at[i, 'Working_Poor_HS'] = (FB_Working_Poor.at[i, 'HS_Working_Poor_F'] * (1.0)) / FB_Working_Poor.at[i, 'HS_Working_Poor_M']
            global_data.FB_ALL_F_M_d.at[i, 'Income_level_BABS'] = (FB_Income_level.at[i, 'BABS_Avg_PINCP_f']* (1.0)) / FB_Income_level.at[i, 'BABS_Avg_PINCP_m']
            global_data.FB_ALL_F_M_d.at[i, 'Income_level_HS'] = (FB_Income_level.at[i, 'HS_Avg_PINCP_f']* (1.0)) / FB_Income_level.at[i, 'HS_Avg_PINCP_m']
            global_data.FB_ALL_F_M_d.at[i, 'Rent_Burden_BABS'] = (FB_Rent_burden.at[i, 'BABS_Rent_Burden_F'] * (1.0)) / FB_Rent_burden.at[i, 'BABS_Rent_Burden_M']
            global_data.FB_ALL_F_M_d.at[i, 'Rent_Burden_HS'] = (FB_Rent_burden.at[i, 'HS_Rent_Burden_F'] * (1.0)) / FB_Rent_burden.at[i, 'HS_Rent_Burden_M']
            global_data.FB_ALL_F_M_d.at[i, 'Home_Ownership_BABS'] = (FB_Home_Ownership.at[i, 'BABS_Home_Ownership_F'] * (1.0)) / FB_Home_Ownership.at[i, 'BABS_Home_Ownership_M']
            global_data.FB_ALL_F_M_d.at[i, 'Home_Ownership_HS'] = (FB_Home_Ownership.at[i, 'HS_Home_Ownership_F'] * (1.0)) / FB_Home_Ownership.at[i, 'HS_Home_Ownership_M']
        make_sure_path_exists(PATH)
        global_data.FB_ALL_F_M_d.to_csv(PATH + 'FB_ALL_F_M_Disparity.csv',na_rep="#DIV/0!")


    elif call_string is 'NB_WNH_FB_POC':
        for i in FB_UnEmp.index:
            global_data.NB_WNH_FB_POC_d.at[i, 'puma'] = FB_UnEmp.at[i, 'puma']
            global_data.NB_WNH_FB_POC_d.at[i,'Unemployment_BABS'] = (FB_UnEmp.at[i,'BABS_UnEmp_Total'] * (1.0)) / NB_UnEmp.at[i,'BABS_UnEmp_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'Unemployment_HS'] = (FB_UnEmp.at[i, 'HS_UnEmp_Total'] * (1.0)) / NB_UnEmp.at[i, 'HS_UnEmp_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'FT_Work_BABS'] = (FB_FT_Work.at[i, 'BABS_FT_Work_Total'] * (1.0)) / NB_FT_Work.at[i, 'BABS_FT_Work_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'FT_Work_HS'] = (FB_FT_Work.at[i, 'HS_FT_Work_Total'] * (1.0)) / NB_FT_Work.at[i, 'HS_FT_Work_Total']
            global_data.NB_WNH_FB_POC_d.at[i,'Poverty_BABS'] = (FB_Poverty.at[i, 'BABS_Poverty_Total'] * (1.0)) / NB_Poverty.at[i, 'BABS_Poverty_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'Poverty_HS'] = (FB_Poverty.at[i, 'HS_Poverty_Total'] * (1.0)) / NB_Poverty.at[i, 'HS_Poverty_Total']
            global_data.NB_WNH_FB_POC_d.at[i,'Working_Poor_BABS'] = (FB_Working_Poor.at[i, 'BABS_Working_Poor_Total'] * (1.0)) / NB_Working_Poor.at[i, 'BABS_Working_Poor_Total']
            global_data.NB_WNH_FB_POC_d.at[i,'Working_Poor_HS'] = (FB_Working_Poor.at[i, 'HS_Working_Poor_Total'] * (1.0)) / NB_Working_Poor.at[i, 'HS_Working_Poor_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'Income_level_BABS'] = (FB_Income_level.at[i, 'BABS_Avg_PINCP_mf_t']* (1.0)) / NB_Income_level.at[i, 'BABS_Avg_PINCP_mf_t']
            global_data.NB_WNH_FB_POC_d.at[i,'Income_level_HS'] = (FB_Income_level.at[i, 'HS_Avg_PINCP_mf_t']* (1.0)) / NB_Income_level.at[i, 'HS_Avg_PINCP_mf_t']
            global_data.NB_WNH_FB_POC_d.at[i,'Rent_Burden_BABS'] = (FB_Rent_burden.at[i, 'BABS_Rent_Burden_Total'] * (1.0)) / NB_Rent_burden.at[i, 'BABS_Rent_Burden_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'Rent_Burden_HS'] = (FB_Rent_burden.at[i, 'HS_Rent_Burden_Total'] * (1.0)) / NB_Rent_burden.at[i, 'HS_Rent_Burden_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'Home_Ownership_BABS'] = (FB_Home_Ownership.at[i, 'BABS_Home_Ownership_Total'] * (1.0)) / NB_Home_Ownership.at[i, 'BABS_Home_Ownership_Total']
            global_data.NB_WNH_FB_POC_d.at[i, 'Home_Ownership_HS'] = (FB_Home_Ownership.at[i, 'HS_Home_Ownership_Total'] * (1.0)) / NB_Home_Ownership.at[i, 'HS_Home_Ownership_Total']
        make_sure_path_exists(PATH)
        global_data.NB_WNH_FB_POC_d.to_csv(PATH + 'NB_WNH_FB_POC_Disparity.csv',na_rep="#DIV/0!")
        
    elif call_string is 'FB_WNH_FB_POC':
        for i in FB_UnEmp.index:
            global_data.FB_WNH_FB_POC_d.at[i, 'puma'] = FB_UnEmp.at[i, 'puma']
            global_data.FB_WNH_FB_POC_d.at[i,'Unemployment_BABS'] = (FB_UnEmp.at[i,'BABS_UnEmp_Total'] * (1.0)) / NB_UnEmp.at[i,'BABS_UnEmp_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'Unemployment_HS'] = (FB_UnEmp.at[i, 'HS_UnEmp_Total'] * (1.0)) / NB_UnEmp.at[i, 'HS_UnEmp_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'FT_Work_BABS'] = (FB_FT_Work.at[i, 'BABS_FT_Work_Total'] * (1.0)) / NB_FT_Work.at[i, 'BABS_FT_Work_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'FT_Work_HS'] = (FB_FT_Work.at[i, 'HS_FT_Work_Total'] * (1.0)) / NB_FT_Work.at[i, 'HS_FT_Work_Total']
            global_data.FB_WNH_FB_POC_d.at[i,'Poverty_BABS'] = (FB_Poverty.at[i, 'BABS_Poverty_Total'] * (1.0)) / NB_Poverty.at[i, 'BABS_Poverty_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'Poverty_HS'] = (FB_Poverty.at[i, 'HS_Poverty_Total'] * (1.0)) / NB_Poverty.at[i, 'HS_Poverty_Total']
            global_data.FB_WNH_FB_POC_d.at[i,'Working_Poor_BABS'] = (FB_Working_Poor.at[i, 'BABS_Working_Poor_Total'] * (1.0)) / NB_Working_Poor.at[i, 'BABS_Working_Poor_Total']
            global_data.FB_WNH_FB_POC_d.at[i,'Working_Poor_HS'] = (FB_Working_Poor.at[i, 'HS_Working_Poor_Total'] * (1.0)) / NB_Working_Poor.at[i, 'HS_Working_Poor_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'Income_level_BABS'] = (FB_Income_level.at[i, 'BABS_Avg_PINCP_mf_t']* (1.0)) / NB_Income_level.at[i, 'BABS_Avg_PINCP_mf_t']
            global_data.FB_WNH_FB_POC_d.at[i,'Income_level_HS'] = (FB_Income_level.at[i, 'HS_Avg_PINCP_mf_t']* (1.0)) / NB_Income_level.at[i, 'HS_Avg_PINCP_mf_t']
            global_data.FB_WNH_FB_POC_d.at[i,'Rent_Burden_BABS'] = (FB_Rent_burden.at[i, 'BABS_Rent_Burden_Total'] * (1.0)) / NB_Rent_burden.at[i, 'BABS_Rent_Burden_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'Rent_Burden_HS'] = (FB_Rent_burden.at[i, 'HS_Rent_Burden_Total'] * (1.0)) / NB_Rent_burden.at[i, 'HS_Rent_Burden_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'Home_Ownership_BABS'] = (FB_Home_Ownership.at[i, 'BABS_Home_Ownership_Total'] * (1.0)) / NB_Home_Ownership.at[i, 'BABS_Home_Ownership_Total']
            global_data.FB_WNH_FB_POC_d.at[i, 'Home_Ownership_HS'] = (FB_Home_Ownership.at[i, 'HS_Home_Ownership_Total'] * (1.0)) / NB_Home_Ownership.at[i, 'HS_Home_Ownership_Total']
        make_sure_path_exists(PATH)
        global_data.FB_WNH_FB_POC_d.to_csv(PATH + 'FB_WNH_FB_POC_Disparity.csv',na_rep="#DIV/0!")

    elif call_string is 'NB_WNH_FB_POC_F':
        for i in FB_UnEmp.index:
            global_data.NB_WNH_FB_POC_d.at[i, 'puma'] = FB_UnEmp.at[i, 'puma']
            global_data.NB_WNH_FB_POC_d.at[i, 'Unemployment_BABS'] = (FB_UnEmp.at[i, 'BABS_UnEmp_F'] * (1.0)) / NB_UnEmp.at[i, 'BABS_UnEmp_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Unemployment_HS'] = (FB_UnEmp.at[i, 'HS_UnEmp_F'] * (1.0)) / NB_UnEmp.at[i, 'HS_UnEmp_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'FT_Work_BABS'] = (FB_FT_Work.at[i, 'BABS_FT_Work_F'] * (1.0)) / NB_FT_Work.at[i, 'BABS_FT_Work_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'FT_Work_HS'] = (FB_FT_Work.at[i, 'HS_FT_Work_F'] * (1.0)) / NB_FT_Work.at[i, 'HS_FT_Work_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Poverty_BABS'] = (FB_Poverty.at[i, 'BABS_Poverty_F'] * (1.0)) / NB_Poverty.at[i, 'BABS_Poverty_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Poverty_HS'] = (FB_Poverty.at[i, 'HS_Poverty_F'] * (1.0)) / NB_Poverty.at[i, 'HS_Poverty_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Working_Poor_BABS'] = (FB_Working_Poor.at[i, 'BABS_Working_Poor_F'] * (1.0)) / NB_Working_Poor.at[i, 'BABS_Working_Poor_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Working_Poor_HS'] = (FB_Working_Poor.at[i, 'HS_Working_Poor_F'] * (1.0)) / NB_Working_Poor.at[i, 'HS_Working_Poor_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Income_level_BABS'] = (FB_Income_level.at[i, 'BABS_Avg_PINCP_f']* (1.0)) / NB_Income_level.at[i, 'BABS_Avg_PINCP_f']
            global_data.NB_WNH_FB_POC_d.at[i, 'Income_level_HS'] = (FB_Income_level.at[i, 'HS_Avg_PINCP_f']* (1.0)) / NB_Income_level.at[i, 'HS_Avg_PINCP_f']
            global_data.NB_WNH_FB_POC_d.at[i, 'Rent_Burden_BABS'] = (FB_Rent_burden.at[i, 'BABS_Rent_Burden_F'] * (1.0)) / NB_Rent_burden.at[i, 'BABS_Rent_Burden_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Rent_Burden_HS'] = (FB_Rent_burden.at[i, 'HS_Rent_Burden_F'] * (1.0)) / NB_Rent_burden.at[i, 'HS_Rent_Burden_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Home_Ownership_BABS'] = (FB_Home_Ownership.at[i, 'BABS_Home_Ownership_F'] * (1.0)) / NB_Home_Ownership.at[i, 'BABS_Home_Ownership_F']
            global_data.NB_WNH_FB_POC_d.at[i, 'Home_Ownership_HS'] = (FB_Home_Ownership.at[i, 'HS_Home_Ownership_F'] * (1.0)) / NB_Home_Ownership.at[i,'HS_Home_Ownership_F']
        make_sure_path_exists(PATH)
        global_data.NB_WNH_FB_POC_d.to_csv(PATH + 'NB_WNH_FB_POC_F_Disparity.csv',na_rep="#DIV/0!")

    elif call_string is 'FB_WNH_FB_POC_F':
            for i in FB_UnEmp.index:
                global_data.FB_WNH_FB_POC_d.at[i, 'puma'] = FB_UnEmp.at[i, 'puma']
                global_data.FB_WNH_FB_POC_d.at[i, 'Unemployment_BABS'] = (FB_UnEmp.at[i, 'BABS_UnEmp_F'] * (1.0)) / NB_UnEmp.at[i, 'BABS_UnEmp_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Unemployment_HS'] = (FB_UnEmp.at[i, 'HS_UnEmp_F'] * (1.0)) / NB_UnEmp.at[i, 'HS_UnEmp_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'FT_Work_BABS'] = (FB_FT_Work.at[i, 'BABS_FT_Work_F'] * (1.0)) / NB_FT_Work.at[i, 'BABS_FT_Work_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'FT_Work_HS'] = (FB_FT_Work.at[i, 'HS_FT_Work_F'] * (1.0)) / NB_FT_Work.at[i, 'HS_FT_Work_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Poverty_BABS'] = (FB_Poverty.at[i, 'BABS_Poverty_F'] * (1.0)) / NB_Poverty.at[i, 'BABS_Poverty_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Poverty_HS'] = (FB_Poverty.at[i, 'HS_Poverty_F'] * (1.0)) / NB_Poverty.at[i, 'HS_Poverty_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Working_Poor_BABS'] = (FB_Working_Poor.at[i, 'BABS_Working_Poor_F'] * (1.0)) / NB_Working_Poor.at[i, 'BABS_Working_Poor_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Working_Poor_HS'] = (FB_Working_Poor.at[i, 'HS_Working_Poor_F'] * (1.0)) / NB_Working_Poor.at[i, 'HS_Working_Poor_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Income_level_BABS'] = (FB_Income_level.at[i, 'BABS_Avg_PINCP_f']* (1.0)) / NB_Income_level.at[i, 'BABS_Avg_PINCP_f']
                global_data.FB_WNH_FB_POC_d.at[i, 'Income_level_HS'] = (FB_Income_level.at[i, 'HS_Avg_PINCP_f']* (1.0)) / NB_Income_level.at[i, 'HS_Avg_PINCP_f']
                global_data.FB_WNH_FB_POC_d.at[i, 'Rent_Burden_BABS'] = (FB_Rent_burden.at[i, 'BABS_Rent_Burden_F'] * (1.0)) / NB_Rent_burden.at[i, 'BABS_Rent_Burden_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Rent_Burden_HS'] = (FB_Rent_burden.at[i, 'HS_Rent_Burden_F'] * (1.0)) / NB_Rent_burden.at[i, 'HS_Rent_Burden_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Home_Ownership_BABS'] = (FB_Home_Ownership.at[i, 'BABS_Home_Ownership_F'] * (1.0)) / NB_Home_Ownership.at[i, 'BABS_Home_Ownership_F']
                global_data.FB_WNH_FB_POC_d.at[i, 'Home_Ownership_HS'] = (FB_Home_Ownership.at[i, 'HS_Home_Ownership_F'] * (1.0)) / NB_Home_Ownership.at[i,'HS_Home_Ownership_F']
            make_sure_path_exists(PATH)
            global_data.FB_WNH_FB_POC_d.to_csv(PATH + 'FB_WNH_FB_POC_F_Disparity.csv',na_rep="#DIV/0!")

def get_NB_All_FB_All_disparity():
    get_disparity('data/2014/Disparities/','NB_ALL_FB_ALL')

def get_NB_ALL_F_FB_ALL_F_disparity():
    get_disparity('data/2014/Disparities/', 'NB_ALL_F_FB_ALL_F')

def get_FB_ALL_F_M_disparity():
    get_disparity('data/2014/Disparities/', 'FB_ALL_F_M')

def get_NB_WNH_FB_POC_disparity():
    get_disparity('data/2014/Disparities/', 'NB_WNH_FB_POC')
    
def get_FB_WNH_FB_POC_disparity():
    get_disparity('data/2014/Disparities/', 'FB_WNH_FB_POC')

def get_NB_WNH_FB_POC_F_disparity():
    get_disparity('data/2014/Disparities/', 'NB_WNH_FB_POC_F')

def get_FB_WNH_FB_POC_F_disparity():
    get_disparity('data/2014/Disparities/', 'FB_WNH_FB_POC_F')

def get_score_grade_all(PATH=None):
    sg_data_NB_ALL_FB_ALL_final = pandas.DataFrame()
    sg_data_NB_ALL_FB_ALL = pandas.read_csv(
        'data/2014/Disparities/NB_ALL_FB_ALL_Disparity.csv').replace([np.inf, -np.inf],
                                                                                          np.nan)
    sg_data_NB_ALL_FB_ALL = sg_data_NB_ALL_FB_ALL.astype('object')

    score_columns_list = ['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                          'Working_Poor_BABS_score', 'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
                          'Income_level_BABS_score', 'Overall_BABS_score',
                          'Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score', 'Working_Poor_HS_score',
                          'Income_level_HS_score',
                          'Rent_Burden_HS_score', 'Home_Ownership_HS_score', 'Overall_HS_score']

    # Calculate all mean and std dev first, ignoring #DIV/0!, inf, -inf
    Unemp_BABS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Unemployment_BABS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Unemployment_BABS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Unemployment_BABS != '-inf')]
    mean_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).std()

    FT_Work_BABS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.FT_Work_BABS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.FT_Work_BABS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.FT_Work_BABS != '-inf')]
    mean_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).std()

    Poverty_BABS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Poverty_BABS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Poverty_BABS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Poverty_BABS != '-inf')]
    mean_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).std()

    Working_Poor_BABS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Working_Poor_BABS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Working_Poor_BABS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Working_Poor_BABS != '-inf')]
    mean_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_BABS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Rent_Burden_BABS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Rent_Burden_BABS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Rent_Burden_BABS != '-inf')]
    mean_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_BABS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Home_Ownership_BABS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Home_Ownership_BABS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Home_Ownership_BABS != '-inf')]
    mean_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).std()

    Income_level_BABS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Income_level_BABS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Income_level_BABS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Income_level_BABS != '-inf')]
    mean_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).std()

    Unemp_HS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Unemployment_HS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Unemployment_HS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Unemployment_HS != '-inf')]
    mean_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).std()

    FT_Work_HS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.FT_Work_HS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.FT_Work_HS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.FT_Work_HS != '-inf')]
    mean_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).std()

    Poverty_HS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Poverty_HS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Poverty_HS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Poverty_HS != '-inf')]
    mean_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).std()

    Working_Poor_HS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Working_Poor_HS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Working_Poor_HS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Working_Poor_HS != '-inf')]
    mean_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_HS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Rent_Burden_HS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Rent_Burden_HS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Rent_Burden_HS != '-inf')]
    mean_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_HS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Home_Ownership_HS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Home_Ownership_HS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Home_Ownership_HS != '-inf')]
    mean_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).std()

    Income_level_HS = sg_data_NB_ALL_FB_ALL[
        (sg_data_NB_ALL_FB_ALL.Income_level_HS != '#DIV/0!') & (
            sg_data_NB_ALL_FB_ALL.Income_level_HS != 'inf') & (
            sg_data_NB_ALL_FB_ALL.Income_level_HS != '-inf')]
    mean_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).std()

    for i in sg_data_NB_ALL_FB_ALL.index:
        if i >= 0 and i <= 146:
            sg_data_NB_ALL_FB_ALL_final.at[i, 'puma'] = sg_data_NB_ALL_FB_ALL.at[i, 'puma']
            if sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_county)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_county)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_county)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_county)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_county)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_county)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_county)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_NB_ALL_FB_ALL_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_county)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_county)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_county)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_county)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_county)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_county)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_county)) * (
                                                                                           1.0)) / stddev_Income_level_HS_county
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_NB_ALL_FB_ALL_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (1.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (1.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (0.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (0.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-0.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-0.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-1.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-1.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] < (-1.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            sg_data_NB_ALL_FB_ALL_final.at[i, 'puma'] = sg_data_NB_ALL_FB_ALL.at[i, 'puma']
            if sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_region)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_region)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_region)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_region)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_region)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_region)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_region)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_NB_ALL_FB_ALL_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_NB_ALL_FB_ALL_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_region)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_region)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_region)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_region)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_region)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_region)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS'] != '-inf':
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_NB_ALL_FB_ALL.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_region)) * (
                                                                                           1.0)) / stddev_Income_level_HS_region
            else:
                sg_data_NB_ALL_FB_ALL_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_NB_ALL_FB_ALL_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_NB_ALL_FB_ALL_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (1.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (1.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (0.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (0.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-0.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-0.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-1.25):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] >= (-1.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_NB_ALL_FB_ALL_final.at[i, column] < (-1.75):
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_NB_ALL_FB_ALL_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

    make_sure_path_exists(PATH)
    sg_data_NB_ALL_FB_ALL_final.to_csv(PATH + 'NB_ALL_FB_ALL_Scores_Grades.csv', na_rep="#DIV/0!")

def get_score_grade_f(PATH=None):
    sg_data_f_final = pandas.DataFrame()
    sg_data_f = pandas.read_csv(
        'data/2014/Disparities/NB_ALL_F_FB_ALL_F_Disparity.csv').replace([np.inf, -np.inf],
                                                                                          np.nan)
    sg_data_f = sg_data_f.astype('object')

    score_columns_list = ['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                          'Working_Poor_BABS_score', 'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
                          'Income_level_BABS_score', 'Overall_BABS_score',
                          'Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score', 'Working_Poor_HS_score',
                          'Income_level_HS_score',
                          'Rent_Burden_HS_score', 'Home_Ownership_HS_score', 'Overall_HS_score']

    # Calculate all mean and std dev first, ignoring #DIV/0!, inf, -inf
    Unemp_BABS = sg_data_f[
        (sg_data_f.Unemployment_BABS_F != '#DIV/0!') & (
            sg_data_f.Unemployment_BABS_F != 'inf') & (
            sg_data_f.Unemployment_BABS_F != '-inf')]
    mean_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS_F']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS_F']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS_F']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS_F']).reindex(index=range(146, 156)).std()

    FT_Work_BABS = sg_data_f[
        (sg_data_f.FT_Work_BABS_F != '#DIV/0!') & (
            sg_data_f.FT_Work_BABS_F != 'inf') & (
            sg_data_f.FT_Work_BABS_F != '-inf')]
    mean_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS_F']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS_F']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS_F']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS_F']).reindex(index=range(146, 156)).std()

    Poverty_BABS = sg_data_f[
        (sg_data_f.Poverty_BABS_F != '#DIV/0!') & (
            sg_data_f.Poverty_BABS_F != 'inf') & (
            sg_data_f.Poverty_BABS_F != '-inf')]
    mean_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS_F']).reindex(index=range(0, 146)).mean()
    mean_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS_F']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS_F']).reindex(index=range(0, 146)).std()
    stddev_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS_F']).reindex(index=range(146, 156)).std()

    Working_Poor_BABS = sg_data_f[
        (sg_data_f.Working_Poor_BABS_F != '#DIV/0!') & (
            sg_data_f.Working_Poor_BABS_F != 'inf') & (
            sg_data_f.Working_Poor_BABS_F != '-inf')]
    mean_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS_F']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS_F']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_BABS = sg_data_f[
        (sg_data_f.Rent_Burden_BABS_F != '#DIV/0!') & (
            sg_data_f.Rent_Burden_BABS_F != 'inf') & (
            sg_data_f.Rent_Burden_BABS_F != '-inf')]
    mean_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS_F']).reindex(
        index=range(0, 146)).mean()
    mean_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS_F']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_BABS = sg_data_f[
        (sg_data_f.Home_Ownership_BABS_F != '#DIV/0!') & (
            sg_data_f.Home_Ownership_BABS_F != 'inf') & (
            sg_data_f.Home_Ownership_BABS_F != '-inf')]
    mean_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS_F']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS_F']).reindex(
        index=range(146, 156)).std()

    Income_level_BABS = sg_data_f[
        (sg_data_f.Income_level_BABS_F != '#DIV/0!') & (
            sg_data_f.Income_level_BABS_F != 'inf') & (
            sg_data_f.Income_level_BABS_F != '-inf')]
    mean_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS_F']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS_F']).reindex(
        index=range(146, 156)).std()

    Unemp_HS = sg_data_f[
        (sg_data_f.Unemployment_HS_F != '#DIV/0!') & (
            sg_data_f.Unemployment_HS_F != 'inf') & (
            sg_data_f.Unemployment_HS_F != '-inf')]
    mean_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS_F']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS_F']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS_F']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS_F']).reindex(index=range(146, 156)).std()

    FT_Work_HS = sg_data_f[
        (sg_data_f.FT_Work_HS_F != '#DIV/0!') & (
            sg_data_f.FT_Work_HS_F != 'inf') & (
            sg_data_f.FT_Work_HS_F != '-inf')]
    mean_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS_F']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS_F']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS_F']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS_F']).reindex(index=range(146, 156)).std()

    Poverty_HS = sg_data_f[
        (sg_data_f.Poverty_HS_F != '#DIV/0!') & (
            sg_data_f.Poverty_HS_F != 'inf') & (
            sg_data_f.Poverty_HS_F != '-inf')]
    mean_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS_F']).reindex(index=range(0, 146)).mean()
    mean_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS_F']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS_F']).reindex(index=range(0, 146)).std()
    stddev_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS_F']).reindex(index=range(146, 156)).std()

    Working_Poor_HS = sg_data_f[
        (sg_data_f.Working_Poor_HS_F != '#DIV/0!') & (
            sg_data_f.Working_Poor_HS_F != 'inf') & (
            sg_data_f.Working_Poor_HS_F != '-inf')]
    mean_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS_F']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS_F']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_HS = sg_data_f[
        (sg_data_f.Rent_Burden_HS_F != '#DIV/0!') & (
            sg_data_f.Rent_Burden_HS_F != 'inf') & (
            sg_data_f.Rent_Burden_HS_F != '-inf')]
    mean_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS_F']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS_F']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_HS = sg_data_f[
        (sg_data_f.Home_Ownership_HS_F != '#DIV/0!') & (
            sg_data_f.Home_Ownership_HS_F != 'inf') & (
            sg_data_f.Home_Ownership_HS_F != '-inf')]
    mean_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS_F']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS_F']).reindex(
        index=range(146, 156)).std()

    Income_level_HS = sg_data_f[
        (sg_data_f.Income_level_HS_F != '#DIV/0!') & (
            sg_data_f.Income_level_HS_F != 'inf') & (
            sg_data_f.Income_level_HS_F != '-inf')]
    mean_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS_F']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS_F']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS_F']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS_F']).reindex(
        index=range(146, 156)).std()

    for i in sg_data_f.index:
        if i >= 0 and i <= 146:
            sg_data_f_final.at[i, 'puma'] = sg_data_f.at[i, 'puma']
            if sg_data_f.at[i, 'Unemployment_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Unemployment_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Unemployment_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Unemployment_BABS_F']) - (mean_UnEmp_BABS_county)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_county
            else:
                sg_data_f_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_f.at[i, 'FT_Work_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'FT_Work_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'FT_Work_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_f.at[i, 'FT_Work_BABS_F']) - (mean_FT_Work_BABS_county)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_county
            else:
                sg_data_f_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Poverty_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Poverty_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Poverty_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Poverty_BABS_F']) - (mean_Poverty_BABS_county)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_county
            else:
                sg_data_f_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Working_Poor_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Working_Poor_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Working_Poor_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Working_Poor_BABS_F']) - (mean_Working_Poor_BABS_county)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_county
            else:
                sg_data_f_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Rent_Burden_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Rent_Burden_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Rent_Burden_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Rent_Burden_BABS_F']) - (mean_Rent_Burden_BABS_county)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_county
            else:
                sg_data_f_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Home_Ownership_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Home_Ownership_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Home_Ownership_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Home_Ownership_BABS_F']) - (
                                                                                            mean_Home_Ownership_BABS_county)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_county
            else:
                sg_data_f_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Income_level_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Income_level_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Income_level_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Income_level_BABS_F']) - (mean_Income_level_BABS_county)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_county
            else:
                sg_data_f_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_f_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_f.at[i, 'Unemployment_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Unemployment_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Unemployment_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_f.at[i, 'Unemployment_HS_F']) - (
                                                                                            mean_UnEmp_HS_county)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_county
            else:
                sg_data_f_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_f.at[i, 'FT_Work_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'FT_Work_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'FT_Work_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_f.at[i, 'FT_Work_HS_F']) - (
                                                                                       mean_FT_Work_HS_county)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_county
            else:
                sg_data_f_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_f.at[i, 'Poverty_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Poverty_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Poverty_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_f.at[i, 'Poverty_HS_F']) - (
                                                                                       mean_Poverty_HS_county)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_county
            else:
                sg_data_f_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_f.at[i, 'Working_Poor_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Working_Poor_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Working_Poor_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_f.at[i, 'Working_Poor_HS_F']) - (
                                                                                            mean_Working_Poor_HS_county)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_county
            else:
                sg_data_f_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_f.at[i, 'Rent_Burden_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Rent_Burden_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Rent_Burden_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_f.at[i, 'Rent_Burden_HS_F']) - (
                                                                                           mean_Rent_Burden_HS_county)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_county
            else:
                sg_data_f_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_f.at[i, 'Home_Ownership_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Home_Ownership_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Home_Ownership_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_f.at[i, 'Home_Ownership_HS_F']) - (
                                                                                              mean_Home_Ownership_HS_county)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_county
            else:
                sg_data_f_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_f.at[i, 'Income_level_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Income_level_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Income_level_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_f.at[i, 'Income_level_HS_F']) - (
                                                                                            mean_Income_level_HS_county)) * (
                                                                                           1.0)) / stddev_Income_level_HS_county
            else:
                sg_data_f_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_f_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_f_final.at[i, column] >= (1.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_f_final.at[i, column] >= (1.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_f_final.at[i, column] >= (0.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_f_final.at[i, column] >= (0.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_f_final.at[i, column] >= (-0.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_f_final.at[i, column] >= (-0.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_f_final.at[i, column] >= (-1.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_f_final.at[i, column] >= (-1.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_f_final.at[i, column] < (-1.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            sg_data_f_final.at[i, 'puma'] = sg_data_f.at[i, 'puma']
            if sg_data_f.at[i, 'Unemployment_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Unemployment_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Unemployment_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Unemployment_BABS_F']) - (mean_UnEmp_BABS_region)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_region
            else:
                sg_data_f_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_f.at[i, 'FT_Work_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'FT_Work_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'FT_Work_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_f.at[i, 'FT_Work_BABS_F']) - (mean_FT_Work_BABS_region)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_region
            else:
                sg_data_f_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Poverty_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Poverty_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Poverty_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Poverty_BABS_F']) - (mean_Poverty_BABS_region)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_region
            else:
                sg_data_f_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Working_Poor_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Working_Poor_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Working_Poor_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Working_Poor_BABS_F']) - (mean_Working_Poor_BABS_region)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_region
            else:
                sg_data_f_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Rent_Burden_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Rent_Burden_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Rent_Burden_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Rent_Burden_BABS_F']) - (mean_Rent_Burden_BABS_region)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_region
            else:
                sg_data_f_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Home_Ownership_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Home_Ownership_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Home_Ownership_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Home_Ownership_BABS_F']) - (
                                                                                            mean_Home_Ownership_BABS_region)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_region
            else:
                sg_data_f_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_f.at[i, 'Income_level_BABS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Income_level_BABS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Income_level_BABS_F'] != '-inf':
                sg_data_f_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_f.at[i, 'Income_level_BABS_F']) - (mean_Income_level_BABS_region)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_region
            else:
                sg_data_f_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_f_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_f_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_f.at[i, 'Unemployment_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Unemployment_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Unemployment_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_f.at[i, 'Unemployment_HS_F']) - (
                                                                                            mean_UnEmp_HS_region)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_region
            else:
                sg_data_f_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_f.at[i, 'FT_Work_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'FT_Work_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'FT_Work_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_f.at[i, 'FT_Work_HS_F']) - (
                                                                                       mean_FT_Work_HS_region)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_region
            else:
                sg_data_f_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_f.at[i, 'Poverty_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Poverty_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Poverty_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_f.at[i, 'Poverty_HS_F']) - (
                                                                                       mean_Poverty_HS_region)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_region
            else:
                sg_data_f_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_f.at[i, 'Working_Poor_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Working_Poor_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Working_Poor_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_f.at[i, 'Working_Poor_HS_F']) - (
                                                                                            mean_Working_Poor_HS_region)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_region
            else:
                sg_data_f_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_f.at[i, 'Rent_Burden_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Rent_Burden_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Rent_Burden_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_f.at[i, 'Rent_Burden_HS_F']) - (
                                                                                           mean_Rent_Burden_HS_region)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_region
            else:
                sg_data_f_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_f.at[i, 'Home_Ownership_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Home_Ownership_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Home_Ownership_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_f.at[i, 'Home_Ownership_HS_F']) - (
                                                                                              mean_Home_Ownership_HS_region)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_region
            else:
                sg_data_f_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_f.at[i, 'Income_level_HS_F'] != '#DIV/0!' and \
                            sg_data_f.at[i, 'Income_level_HS_F'] != 'inf' and \
                            sg_data_f.at[i, 'Income_level_HS_F'] != '-inf':
                sg_data_f_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_f.at[i, 'Income_level_HS_F']) - (
                                                                                            mean_Income_level_HS_region)) * (
                                                                                           1.0)) / stddev_Income_level_HS_region
            else:
                sg_data_f_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_f_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_f_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_f_final.at[i, column] >= (1.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_f_final.at[i, column] >= (1.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_f_final.at[i, column] >= (0.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_f_final.at[i, column] >= (0.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_f_final.at[i, column] >= (-0.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_f_final.at[i, column] >= (-0.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_f_final.at[i, column] >= (-1.25):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_f_final.at[i, column] >= (-1.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_f_final.at[i, column] < (-1.75):
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_f_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

    make_sure_path_exists(PATH)
    sg_data_f_final.to_csv(PATH + 'NB_ALL_F_FB_ALL_F_Scores_Grades.csv', na_rep="#DIV/0!")

def get_score_grade_FB_ALL_F_M(PATH=None):
    sg_data_f_m_final = pandas.DataFrame()
    sg_data_f_m = pandas.read_csv(
        'data/2014/Disparities/FB_ALL_F_M_Disparity.csv').replace([np.inf, -np.inf],
                                                                                          np.nan)
    sg_data_f_m = sg_data_f_m.astype('object')

    score_columns_list = ['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                          'Working_Poor_BABS_score', 'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
                          'Income_level_BABS_score', 'Overall_BABS_score',
                          'Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score', 'Working_Poor_HS_score',
                          'Income_level_HS_score',
                          'Rent_Burden_HS_score', 'Home_Ownership_HS_score', 'Overall_HS_score']

    # Calculate all mean and std dev first, ignoring #DIV/0!, inf, -inf
    Unemp_BABS = sg_data_f_m[
        (sg_data_f_m.Unemployment_BABS != '#DIV/0!') & (
            sg_data_f_m.Unemployment_BABS != 'inf') & (
            sg_data_f_m.Unemployment_BABS != '-inf')]
    mean_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).std()

    FT_Work_BABS = sg_data_f_m[
        (sg_data_f_m.FT_Work_BABS != '#DIV/0!') & (
            sg_data_f_m.FT_Work_BABS != 'inf') & (
            sg_data_f_m.FT_Work_BABS != '-inf')]
    mean_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).std()

    Poverty_BABS = sg_data_f_m[
        (sg_data_f_m.Poverty_BABS != '#DIV/0!') & (
            sg_data_f_m.Poverty_BABS != 'inf') & (
            sg_data_f_m.Poverty_BABS != '-inf')]
    mean_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).std()

    Working_Poor_BABS = sg_data_f_m[
        (sg_data_f_m.Working_Poor_BABS != '#DIV/0!') & (
            sg_data_f_m.Working_Poor_BABS != 'inf') & (
            sg_data_f_m.Working_Poor_BABS != '-inf')]
    mean_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_BABS = sg_data_f_m[
        (sg_data_f_m.Rent_Burden_BABS != '#DIV/0!') & (
            sg_data_f_m.Rent_Burden_BABS != 'inf') & (
            sg_data_f_m.Rent_Burden_BABS != '-inf')]
    mean_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_BABS = sg_data_f_m[
        (sg_data_f_m.Home_Ownership_BABS != '#DIV/0!') & (
            sg_data_f_m.Home_Ownership_BABS != 'inf') & (
            sg_data_f_m.Home_Ownership_BABS != '-inf')]
    mean_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).std()

    Income_level_BABS = sg_data_f_m[
        (sg_data_f_m.Income_level_BABS != '#DIV/0!') & (
            sg_data_f_m.Income_level_BABS != 'inf') & (
            sg_data_f_m.Income_level_BABS != '-inf')]
    mean_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).std()

    Unemp_HS = sg_data_f_m[
        (sg_data_f_m.Unemployment_HS != '#DIV/0!') & (
            sg_data_f_m.Unemployment_HS != 'inf') & (
            sg_data_f_m.Unemployment_HS != '-inf')]
    mean_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).std()

    FT_Work_HS = sg_data_f_m[
        (sg_data_f_m.FT_Work_HS != '#DIV/0!') & (
            sg_data_f_m.FT_Work_HS != 'inf') & (
            sg_data_f_m.FT_Work_HS != '-inf')]
    mean_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).std()

    Poverty_HS = sg_data_f_m[
        (sg_data_f_m.Poverty_HS != '#DIV/0!') & (
            sg_data_f_m.Poverty_HS != 'inf') & (
            sg_data_f_m.Poverty_HS != '-inf')]
    mean_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).std()

    Working_Poor_HS = sg_data_f_m[
        (sg_data_f_m.Working_Poor_HS != '#DIV/0!') & (
            sg_data_f_m.Working_Poor_HS != 'inf') & (
            sg_data_f_m.Working_Poor_HS != '-inf')]
    mean_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_HS = sg_data_f_m[
        (sg_data_f_m.Rent_Burden_HS != '#DIV/0!') & (
            sg_data_f_m.Rent_Burden_HS != 'inf') & (
            sg_data_f_m.Rent_Burden_HS != '-inf')]
    mean_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_HS = sg_data_f_m[
        (sg_data_f_m.Home_Ownership_HS != '#DIV/0!') & (
            sg_data_f_m.Home_Ownership_HS != 'inf') & (
            sg_data_f_m.Home_Ownership_HS != '-inf')]
    mean_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).std()

    Income_level_HS = sg_data_f_m[
        (sg_data_f_m.Income_level_HS != '#DIV/0!') & (
            sg_data_f_m.Income_level_HS != 'inf') & (
            sg_data_f_m.Income_level_HS != '-inf')]
    mean_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).std()

    for i in sg_data_f_m.index:
        if i >= 0 and i <= 146:
            sg_data_f_m_final.at[i, 'puma'] = sg_data_f_m.at[i, 'puma']
            if sg_data_f_m.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_county)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_county
            else:
                sg_data_f_m_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_county)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_county
            else:
                sg_data_f_m_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_county)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_county
            else:
                sg_data_f_m_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_county)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_county
            else:
                sg_data_f_m_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_county)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_county
            else:
                sg_data_f_m_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_county)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_county
            else:
                sg_data_f_m_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_county)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_county
            else:
                sg_data_f_m_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_f_m_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_f_m.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_county)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_county
            else:
                sg_data_f_m_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_county)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_county
            else:
                sg_data_f_m_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Poverty_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_county)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_county
            else:
                sg_data_f_m_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_county)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_county
            else:
                sg_data_f_m_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_county)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_county
            else:
                sg_data_f_m_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_county)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_county
            else:
                sg_data_f_m_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Income_level_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_county)) * (
                                                                                           1.0)) / stddev_Income_level_HS_county
            else:
                sg_data_f_m_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_f_m_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_f_m_final.at[i, column] >= (1.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_f_m_final.at[i, column] >= (1.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_f_m_final.at[i, column] >= (0.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_f_m_final.at[i, column] >= (0.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_f_m_final.at[i, column] >= (-0.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_f_m_final.at[i, column] >= (-0.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_f_m_final.at[i, column] >= (-1.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_f_m_final.at[i, column] >= (-1.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_f_m_final.at[i, column] < (-1.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            sg_data_f_m_final.at[i, 'puma'] = sg_data_f_m.at[i, 'puma']
            if sg_data_f_m.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_region)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_region
            else:
                sg_data_f_m_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_region)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_region
            else:
                sg_data_f_m_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_region)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_region
            else:
                sg_data_f_m_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_region)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_region
            else:
                sg_data_f_m_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_region)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_region
            else:
                sg_data_f_m_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_region)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_region
            else:
                sg_data_f_m_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_f_m.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_f_m_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_f_m.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_region)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_region
            else:
                sg_data_f_m_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_f_m_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_f_m_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_f_m.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_region)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_region
            else:
                sg_data_f_m_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_region)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_region
            else:
                sg_data_f_m_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Poverty_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_region)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_region
            else:
                sg_data_f_m_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_region)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_region
            else:
                sg_data_f_m_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_region)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_region
            else:
                sg_data_f_m_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_region)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_region
            else:
                sg_data_f_m_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_f_m.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_f_m.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_f_m.at[i, 'Income_level_HS'] != '-inf':
                sg_data_f_m_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_f_m.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_region)) * (
                                                                                           1.0)) / stddev_Income_level_HS_region
            else:
                sg_data_f_m_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_f_m_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_f_m_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_f_m_final.at[i, column] >= (1.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_f_m_final.at[i, column] >= (1.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_f_m_final.at[i, column] >= (0.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_f_m_final.at[i, column] >= (0.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_f_m_final.at[i, column] >= (-0.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_f_m_final.at[i, column] >= (-0.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_f_m_final.at[i, column] >= (-1.25):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_f_m_final.at[i, column] >= (-1.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_f_m_final.at[i, column] < (-1.75):
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_f_m_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

    make_sure_path_exists(PATH)
    sg_data_f_m_final.to_csv(PATH + 'FB_ALL_F_M_Scores_Grades.csv', na_rep="#DIV/0!")

def get_score_grade_NB_WNH_FB_POC(PATH=None):
    sg_data_NB_WNH_FB_POC_final = pandas.DataFrame()
    sg_data_NB_WNH_FB_POC = pandas.read_csv(
        'data/2014/Disparities/NB_WNH_FB_POC_Disparity.csv').replace([np.inf, -np.inf],
                                                                                          np.nan)
    sg_data_NB_WNH_FB_POC = sg_data_NB_WNH_FB_POC.astype('object')

    score_columns_list = ['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                          'Working_Poor_BABS_score', 'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
                          'Income_level_BABS_score', 'Overall_BABS_score',
                          'Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score', 'Working_Poor_HS_score',
                          'Income_level_HS_score',
                          'Rent_Burden_HS_score', 'Home_Ownership_HS_score', 'Overall_HS_score']

    # Calculate all mean and std dev first, ignoring #DIV/0!, inf, -inf
    Unemp_BABS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Unemployment_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Unemployment_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Unemployment_BABS != '-inf')]
    mean_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).std()

    FT_Work_BABS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.FT_Work_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.FT_Work_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC.FT_Work_BABS != '-inf')]
    mean_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).std()

    Poverty_BABS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Poverty_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Poverty_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Poverty_BABS != '-inf')]
    mean_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).std()

    Working_Poor_BABS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Working_Poor_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Working_Poor_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Working_Poor_BABS != '-inf')]
    mean_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_BABS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Rent_Burden_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Rent_Burden_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Rent_Burden_BABS != '-inf')]
    mean_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_BABS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Home_Ownership_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Home_Ownership_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Home_Ownership_BABS != '-inf')]
    mean_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).std()

    Income_level_BABS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Income_level_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Income_level_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Income_level_BABS != '-inf')]
    mean_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).std()

    Unemp_HS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Unemployment_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Unemployment_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Unemployment_HS != '-inf')]
    mean_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).std()

    FT_Work_HS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.FT_Work_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.FT_Work_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC.FT_Work_HS != '-inf')]
    mean_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).std()

    Poverty_HS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Poverty_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Poverty_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Poverty_HS != '-inf')]
    mean_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).std()

    Working_Poor_HS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Working_Poor_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Working_Poor_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Working_Poor_HS != '-inf')]
    mean_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_HS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Rent_Burden_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Rent_Burden_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Rent_Burden_HS != '-inf')]
    mean_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_HS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Home_Ownership_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Home_Ownership_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Home_Ownership_HS != '-inf')]
    mean_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).std()

    Income_level_HS = sg_data_NB_WNH_FB_POC[
        (sg_data_NB_WNH_FB_POC.Income_level_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC.Income_level_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC.Income_level_HS != '-inf')]
    mean_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).std()

    for i in sg_data_NB_WNH_FB_POC.index:
        if i >= 0 and i <= 146:
            sg_data_NB_WNH_FB_POC_final.at[i, 'puma'] = sg_data_NB_WNH_FB_POC.at[i, 'puma']
            if sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_county)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_county)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_county)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_county)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_county)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_county)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_county)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_county)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_county)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_county)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_county)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_county)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_county)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_county)) * (
                                                                                           1.0)) / stddev_Income_level_HS_county
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_NB_WNH_FB_POC_final.at[i, column] >= (1.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (1.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (0.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (0.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-0.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-0.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-1.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-1.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] < (-1.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            sg_data_NB_WNH_FB_POC_final.at[i, 'puma'] = sg_data_NB_WNH_FB_POC.at[i, 'puma']
            if sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_region)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_region)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_region)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_region)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_region)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_region)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_region)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_region)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_region)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_region)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_region)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_region)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_region)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_region)) * (
                                                                                           1.0)) / stddev_Income_level_HS_region
            else:
                sg_data_NB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_NB_WNH_FB_POC_final.at[i, column] >= (1.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (1.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (0.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (0.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-0.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-0.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-1.25):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] >= (-1.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_NB_WNH_FB_POC_final.at[i, column] < (-1.75):
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_NB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

    make_sure_path_exists(PATH)
    sg_data_NB_WNH_FB_POC_final.to_csv(PATH + 'NB_WNH_FB_POC_Scores_Grades.csv', na_rep="#DIV/0!")

def get_score_grade_FB_WNH_FB_POC(PATH=None):
    sg_data_FB_WNH_FB_POC_final = pandas.DataFrame()
    sg_data_FB_WNH_FB_POC = pandas.read_csv(
        'data/2014/Disparities/FB_WNH_FB_POC_Disparity.csv').replace([np.inf, -np.inf],
                                                                                          np.nan)
    sg_data_FB_WNH_FB_POC = sg_data_FB_WNH_FB_POC.astype('object')

    score_columns_list = ['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                          'Working_Poor_BABS_score', 'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
                          'Income_level_BABS_score', 'Overall_BABS_score',
                          'Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score', 'Working_Poor_HS_score',
                          'Income_level_HS_score',
                          'Rent_Burden_HS_score', 'Home_Ownership_HS_score', 'Overall_HS_score']

    # Calculate all mean and std dev first, ignoring #DIV/0!, inf, -inf
    Unemp_BABS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Unemployment_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Unemployment_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Unemployment_BABS != '-inf')]
    mean_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).std()

    FT_Work_BABS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.FT_Work_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.FT_Work_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC.FT_Work_BABS != '-inf')]
    mean_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).std()

    Poverty_BABS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Poverty_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Poverty_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Poverty_BABS != '-inf')]
    mean_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).std()

    Working_Poor_BABS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Working_Poor_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Working_Poor_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Working_Poor_BABS != '-inf')]
    mean_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_BABS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Rent_Burden_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Rent_Burden_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Rent_Burden_BABS != '-inf')]
    mean_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_BABS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Home_Ownership_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Home_Ownership_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Home_Ownership_BABS != '-inf')]
    mean_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).std()

    Income_level_BABS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Income_level_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Income_level_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Income_level_BABS != '-inf')]
    mean_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).std()

    Unemp_HS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Unemployment_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Unemployment_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Unemployment_HS != '-inf')]
    mean_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).std()

    FT_Work_HS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.FT_Work_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.FT_Work_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC.FT_Work_HS != '-inf')]
    mean_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).std()

    Poverty_HS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Poverty_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Poverty_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Poverty_HS != '-inf')]
    mean_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).std()

    Working_Poor_HS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Working_Poor_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Working_Poor_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Working_Poor_HS != '-inf')]
    mean_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_HS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Rent_Burden_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Rent_Burden_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Rent_Burden_HS != '-inf')]
    mean_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_HS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Home_Ownership_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Home_Ownership_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Home_Ownership_HS != '-inf')]
    mean_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).std()

    Income_level_HS = sg_data_FB_WNH_FB_POC[
        (sg_data_FB_WNH_FB_POC.Income_level_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC.Income_level_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC.Income_level_HS != '-inf')]
    mean_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).std()

    for i in sg_data_FB_WNH_FB_POC.index:
        if i >= 0 and i <= 146:
            sg_data_FB_WNH_FB_POC_final.at[i, 'puma'] = sg_data_FB_WNH_FB_POC.at[i, 'puma']
            if sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_county)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_county)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_county)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_county)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_county)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_county)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_county)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_county)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_county)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_county)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_county)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_county)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_county)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_county)) * (
                                                                                           1.0)) / stddev_Income_level_HS_county
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_FB_WNH_FB_POC_final.at[i, column] >= (1.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (1.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (0.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (0.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-0.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-0.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-1.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-1.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] < (-1.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            sg_data_FB_WNH_FB_POC_final.at[i, 'puma'] = sg_data_FB_WNH_FB_POC.at[i, 'puma']
            if sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_region)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_region)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_region)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_region)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_region)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_region)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_region)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_region)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_region)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_region)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_region)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_region)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_region)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_region)) * (
                                                                                           1.0)) / stddev_Income_level_HS_region
            else:
                sg_data_FB_WNH_FB_POC_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_FB_WNH_FB_POC_final.at[i, column] >= (1.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (1.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (0.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (0.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-0.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-0.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-1.25):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] >= (-1.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_FB_WNH_FB_POC_final.at[i, column] < (-1.75):
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_FB_WNH_FB_POC_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

    make_sure_path_exists(PATH)
    sg_data_FB_WNH_FB_POC_final.to_csv(PATH + 'FB_WNH_FB_POC_Scores_Grades.csv', na_rep="#DIV/0!")

def get_score_grade_NB_WNH_FB_POC_F(PATH=None):
    sg_data_NB_WNH_FB_POC_F_final = pandas.DataFrame()
    sg_data_NB_WNH_FB_POC_F = pandas.read_csv(
        'data/2014/Disparities/NB_WNH_FB_POC_F_Disparity.csv').replace([np.inf, -np.inf],
                                                                                          np.nan)
    sg_data_NB_WNH_FB_POC_F = sg_data_NB_WNH_FB_POC_F.astype('object')

    score_columns_list = ['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                          'Working_Poor_BABS_score', 'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
                          'Income_level_BABS_score', 'Overall_BABS_score',
                          'Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score', 'Working_Poor_HS_score',
                          'Income_level_HS_score',
                          'Rent_Burden_HS_score', 'Home_Ownership_HS_score', 'Overall_HS_score']

    # Calculate all mean and std dev first, ignoring #DIV/0!, inf, -inf
    Unemp_BABS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Unemployment_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Unemployment_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Unemployment_BABS != '-inf')]
    mean_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).std()

    FT_Work_BABS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.FT_Work_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.FT_Work_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.FT_Work_BABS != '-inf')]
    mean_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).std()

    Poverty_BABS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Poverty_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Poverty_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Poverty_BABS != '-inf')]
    mean_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).std()

    Working_Poor_BABS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Working_Poor_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Working_Poor_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Working_Poor_BABS != '-inf')]
    mean_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_BABS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Rent_Burden_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Rent_Burden_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Rent_Burden_BABS != '-inf')]
    mean_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_BABS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Home_Ownership_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Home_Ownership_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Home_Ownership_BABS != '-inf')]
    mean_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).std()

    Income_level_BABS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Income_level_BABS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Income_level_BABS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Income_level_BABS != '-inf')]
    mean_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).std()

    Unemp_HS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Unemployment_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Unemployment_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Unemployment_HS != '-inf')]
    mean_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).std()

    FT_Work_HS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.FT_Work_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.FT_Work_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.FT_Work_HS != '-inf')]
    mean_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).std()

    Poverty_HS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Poverty_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Poverty_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Poverty_HS != '-inf')]
    mean_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).std()

    Working_Poor_HS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Working_Poor_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Working_Poor_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Working_Poor_HS != '-inf')]
    mean_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_HS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Rent_Burden_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Rent_Burden_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Rent_Burden_HS != '-inf')]
    mean_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_HS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Home_Ownership_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Home_Ownership_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Home_Ownership_HS != '-inf')]
    mean_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).std()

    Income_level_HS = sg_data_NB_WNH_FB_POC_F[
        (sg_data_NB_WNH_FB_POC_F.Income_level_HS != '#DIV/0!') & (
            sg_data_NB_WNH_FB_POC_F.Income_level_HS != 'inf') & (
            sg_data_NB_WNH_FB_POC_F.Income_level_HS != '-inf')]
    mean_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).std()

    for i in sg_data_NB_WNH_FB_POC_F.index:
        if i >= 0 and i <= 146:
            sg_data_NB_WNH_FB_POC_F_final.at[i, 'puma'] = sg_data_NB_WNH_FB_POC_F.at[i, 'puma']
            if sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_county)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_county)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_county)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_county)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_county)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_county)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_county)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_F_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_county)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_county)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_county)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_county)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_county)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_county)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_county)) * (
                                                                                           1.0)) / stddev_Income_level_HS_county
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_F_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (1.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (1.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (0.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (0.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-0.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-0.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-1.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-1.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] < (-1.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            sg_data_NB_WNH_FB_POC_F_final.at[i, 'puma'] = sg_data_NB_WNH_FB_POC_F.at[i, 'puma']
            if sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_region)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_region)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_region)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_region)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_region)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_region)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_region)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_F_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_region)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_region)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_region)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_region)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_region)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_region)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '-inf':
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_NB_WNH_FB_POC_F.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_region)) * (
                                                                                           1.0)) / stddev_Income_level_HS_region
            else:
                sg_data_NB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_NB_WNH_FB_POC_F_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_NB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (1.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (1.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (0.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (0.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-0.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-0.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-1.25):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] >= (-1.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_NB_WNH_FB_POC_F_final.at[i, column] < (-1.75):
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_NB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

    make_sure_path_exists(PATH)
    sg_data_NB_WNH_FB_POC_F_final.to_csv(PATH + 'NB_WNH_FB_POC_F_Scores_Grades.csv', na_rep="#DIV/0!")
    
    
def get_score_grade_FB_WNH_FB_POC_F(PATH=None):
    sg_data_FB_WNH_FB_POC_F_final = pandas.DataFrame()
    sg_data_FB_WNH_FB_POC_F = pandas.read_csv(
        'data/2014/Disparities/FB_WNH_FB_POC_F_Disparity.csv').replace([np.inf, -np.inf],
                                                                                          np.nan)
    sg_data_FB_WNH_FB_POC_F = sg_data_FB_WNH_FB_POC_F.astype('object')

    score_columns_list = ['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                          'Working_Poor_BABS_score', 'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
                          'Income_level_BABS_score', 'Overall_BABS_score',
                          'Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score', 'Working_Poor_HS_score',
                          'Income_level_HS_score',
                          'Rent_Burden_HS_score', 'Home_Ownership_HS_score', 'Overall_HS_score']

    # Calculate all mean and std dev first, ignoring #DIV/0!, inf, -inf
    Unemp_BABS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Unemployment_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Unemployment_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Unemployment_BABS != '-inf')]
    mean_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_BABS_county = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_BABS_region = pandas.to_numeric(Unemp_BABS['Unemployment_BABS']).reindex(index=range(146, 156)).std()

    FT_Work_BABS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.FT_Work_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.FT_Work_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.FT_Work_BABS != '-inf')]
    mean_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_BABS_county = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_BABS_region = pandas.to_numeric(FT_Work_BABS['FT_Work_BABS']).reindex(index=range(146, 156)).std()

    Poverty_BABS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Poverty_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Poverty_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Poverty_BABS != '-inf')]
    mean_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_BABS_county = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_BABS_region = pandas.to_numeric(Poverty_BABS['Poverty_BABS']).reindex(index=range(146, 156)).std()

    Working_Poor_BABS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Working_Poor_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Working_Poor_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Working_Poor_BABS != '-inf')]
    mean_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_BABS_county = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_BABS_region = pandas.to_numeric(Working_Poor_BABS['Working_Poor_BABS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_BABS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Rent_Burden_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Rent_Burden_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Rent_Burden_BABS != '-inf')]
    mean_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_BABS_county = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_BABS_region = pandas.to_numeric(Rent_Burden_BABS['Rent_Burden_BABS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_BABS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Home_Ownership_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Home_Ownership_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Home_Ownership_BABS != '-inf')]
    mean_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_BABS_county = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_BABS_region = pandas.to_numeric(Home_Ownership_BABS['Home_Ownership_BABS']).reindex(
        index=range(146, 156)).std()

    Income_level_BABS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Income_level_BABS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Income_level_BABS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Income_level_BABS != '-inf')]
    mean_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_BABS_county = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_BABS_region = pandas.to_numeric(Income_level_BABS['Income_level_BABS']).reindex(
        index=range(146, 156)).std()

    Unemp_HS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Unemployment_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Unemployment_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Unemployment_HS != '-inf')]
    mean_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).mean()
    mean_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).mean()
    stddev_UnEmp_HS_county = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(0, 146)).std()
    stddev_UnEmp_HS_region = pandas.to_numeric(Unemp_HS['Unemployment_HS']).reindex(index=range(146, 156)).std()

    FT_Work_HS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.FT_Work_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.FT_Work_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.FT_Work_HS != '-inf')]
    mean_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).mean()
    mean_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).mean()
    stddev_FT_Work_HS_county = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(0, 146)).std()
    stddev_FT_Work_HS_region = pandas.to_numeric(FT_Work_HS['FT_Work_HS']).reindex(index=range(146, 156)).std()

    Poverty_HS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Poverty_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Poverty_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Poverty_HS != '-inf')]
    mean_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).mean()
    mean_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).mean()
    stddev_Poverty_HS_county = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(0, 146)).std()
    stddev_Poverty_HS_region = pandas.to_numeric(Poverty_HS['Poverty_HS']).reindex(index=range(146, 156)).std()

    Working_Poor_HS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Working_Poor_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Working_Poor_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Working_Poor_HS != '-inf')]
    mean_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Working_Poor_HS_county = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Working_Poor_HS_region = pandas.to_numeric(Working_Poor_HS['Working_Poor_HS']).reindex(
        index=range(146, 156)).std()

    Rent_Burden_HS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Rent_Burden_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Rent_Burden_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Rent_Burden_HS != '-inf')]
    mean_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(index=range(0, 146)).mean()
    mean_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Rent_Burden_HS_county = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Rent_Burden_HS_region = pandas.to_numeric(Rent_Burden_HS['Rent_Burden_HS']).reindex(
        index=range(146, 156)).std()

    Home_Ownership_HS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Home_Ownership_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Home_Ownership_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Home_Ownership_HS != '-inf')]
    mean_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Home_Ownership_HS_county = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Home_Ownership_HS_region = pandas.to_numeric(Home_Ownership_HS['Home_Ownership_HS']).reindex(
        index=range(146, 156)).std()

    Income_level_HS = sg_data_FB_WNH_FB_POC_F[
        (sg_data_FB_WNH_FB_POC_F.Income_level_HS != '#DIV/0!') & (
            sg_data_FB_WNH_FB_POC_F.Income_level_HS != 'inf') & (
            sg_data_FB_WNH_FB_POC_F.Income_level_HS != '-inf')]
    mean_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).mean()
    mean_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).mean()
    stddev_Income_level_HS_county = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(0, 146)).std()
    stddev_Income_level_HS_region = pandas.to_numeric(Income_level_HS['Income_level_HS']).reindex(
        index=range(146, 156)).std()

    for i in sg_data_FB_WNH_FB_POC_F.index:
        if i >= 0 and i <= 146:
            sg_data_FB_WNH_FB_POC_F_final.at[i, 'puma'] = sg_data_FB_WNH_FB_POC_F.at[i, 'puma']
            if sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_county)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_county)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_county)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_county)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_county)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_county)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_county)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_F_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_county)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_county)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_county)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_county)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_county)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_county)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_county)) * (
                                                                                           1.0)) / stddev_Income_level_HS_county
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_F_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (1.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (1.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (0.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (0.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-0.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-0.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-1.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-1.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] < (-1.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

        elif i >= 146 and i <= 156:
            sg_data_FB_WNH_FB_POC_F_final.at[i, 'puma'] = sg_data_FB_WNH_FB_POC_F.at[i, 'puma']
            if sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_BABS']) - (mean_UnEmp_BABS_region)) * (
                                                                                             -1.0)) / stddev_UnEmp_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_BABS']) - (mean_FT_Work_BABS_region)) * (
                                                                                        1.0)) / stddev_FT_Work_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_BABS']) - (mean_Poverty_BABS_region)) * (
                                                                                        -1.0)) / stddev_Poverty_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_BABS']) - (mean_Working_Poor_BABS_region)) * (
                                                                                             -1.0)) / stddev_Working_Poor_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_BABS']) - (mean_Rent_Burden_BABS_region)) * (
                                                                                            -1.0)) / stddev_Rent_Burden_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_BABS']) - (
                                                                                            mean_Home_Ownership_BABS_region)) * (
                                                                                               1.0)) / stddev_Home_Ownership_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_BABS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_BABS']) - (mean_Income_level_BABS_region)) * (
                                                                                             1.0)) / stddev_Income_level_BABS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_BABS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_F_final.at[i, 'Overall_BABS_score'] = ((
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Unemployment_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'FT_Work_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Poverty_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Working_Poor_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Rent_Burden_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Home_Ownership_BABS_score'] +
                                                                                    sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                        i, 'Income_level_BABS_score']) * 1.0) / 7

            # checking for scores for HS
            if sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Unemployment_HS']) - (
                                                                                            mean_UnEmp_HS_region)) * (
                                                                                           -1.0)) / stddev_UnEmp_HS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Unemployment_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'FT_Work_HS']) - (
                                                                                       mean_FT_Work_HS_region)) * (
                                                                                      1.0)) / stddev_FT_Work_HS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'FT_Work_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Poverty_HS']) - (
                                                                                       mean_Poverty_HS_region)) * (
                                                                                      -1.0)) / stddev_Poverty_HS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Poverty_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Working_Poor_HS']) - (
                                                                                            mean_Working_Poor_HS_region)) * (
                                                                                           -1.0)) / stddev_Working_Poor_HS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Working_Poor_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Rent_Burden_HS']) - (
                                                                                           mean_Rent_Burden_HS_region)) * (
                                                                                          -1.0)) / stddev_Rent_Burden_HS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Rent_Burden_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Home_Ownership_HS']) - (
                                                                                              mean_Home_Ownership_HS_region)) * (
                                                                                             1.0)) / stddev_Home_Ownership_HS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Home_Ownership_HS_score'] = np.nan

            if sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '#DIV/0!' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != 'inf' and \
                            sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS'] != '-inf':
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = ((float(
                    sg_data_FB_WNH_FB_POC_F.at[i, 'Income_level_HS']) - (
                                                                                            mean_Income_level_HS_region)) * (
                                                                                           1.0)) / stddev_Income_level_HS_region
            else:
                sg_data_FB_WNH_FB_POC_F_final.at[i, 'Income_level_HS_score'] = np.nan

            sg_data_FB_WNH_FB_POC_F_final.at[i, 'Overall_HS_score'] = ((
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Unemployment_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'FT_Work_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Poverty_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Working_Poor_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Rent_Burden_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Home_Ownership_HS_score'] +
                                                                                  sg_data_FB_WNH_FB_POC_F_final.at[
                                                                                      i, 'Income_level_HS_score']) * 1.0) / 7

            for column in score_columns_list:
                indicator = str(column).split('_score')
                if sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (1.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (1.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'A-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (0.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (0.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'B-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-0.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-0.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'C-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-1.25):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] >= (-1.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'D-'
                elif sg_data_FB_WNH_FB_POC_F_final.at[i, column] < (-1.75):
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = 'E'
                else:
                    sg_data_FB_WNH_FB_POC_F_final.at[i, indicator[0] + '_grade'] = "#DIV!0"

    make_sure_path_exists(PATH)
    sg_data_FB_WNH_FB_POC_F_final.to_csv(PATH + 'FB_WNH_FB_POC_F_Scores_Grades.csv', na_rep="#DIV/0!")

def make_sure_path_exists(path):
    # https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory-in-python
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
def run():
    print ('Creating DataFrames...')

    NB_ALL()
    NB_WNH()
    FB_ALL()
    FB_WNH()
    FB_POC()
    get_NB_All_FB_All_disparity()
    get_NB_ALL_F_FB_ALL_F_disparity()
    get_FB_ALL_F_M_disparity()
    get_NB_WNH_FB_POC_disparity()
    get_FB_WNH_FB_POC_disparity()
    get_disparity_score_grade_grp_11('data/2014/GRP11/Scores_Grades/')
    get_NB_WNH_FB_POC_F_disparity()
    get_FB_WNH_FB_POC_F_disparity()
    get_score_grade_all('data/2014/Scores_Grades/')
    get_score_grade_f('data/2014/Scores_Grades/')
    get_score_grade_FB_ALL_F_M('data/2014/Scores_Grades/')
    get_score_grade_NB_WNH_FB_POC('data/2014/Scores_Grades/')
    get_score_grade_FB_WNH_FB_POC('data/2014/Scores_Grades/') #make this
    get_score_grade_FB_WNH_FB_POC_F('data/2014/Scores_Grades/')
    get_score_grade_NB_WNH_FB_POC_F('data/2014/Scores_Grades/')
    cj.csv_to_json()


if __name__ == '__main__':
    run()
