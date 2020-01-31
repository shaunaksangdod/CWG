#from StdSuites.Type_Names_Suite import null

import pandas,os,errno
import timeit as t
import global_data_edu
import numpy as np
import glob
import collections
import csv
import json
import csv_to_json as cj


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

    if puma_maps_dict.has_key(county_num):
        return puma_maps_dict.get(county_num)
    else:
        return null

"""
# for mapping the counties defined above to 10 regions
def puma_region(rcounty_num):
    # check_region = [puma_maps_dict[k] for k in region_1 if k in puma_maps_dict]
    if rcounty_num in region_1 and puma_maps_dict.has_key(rcounty_num):
        return "Capital Region"
    elif rcounty_num in region_2 and puma_maps_dict.has_key(rcounty_num):
        return "Central NY"
    elif rcounty_num in region_3 and puma_maps_dict.has_key(rcounty_num):
        return "Finger Lakes"
    elif rcounty_num in region_4 and puma_maps_dict.has_key(rcounty_num):
        return "Long Island"
    elif rcounty_num in region_5 and puma_maps_dict.has_key(rcounty_num):
        return "Mid-Hudson"
    elif rcounty_num in region_6 and puma_maps_dict.has_key(rcounty_num):
        return "Mohawk"
    elif rcounty_num in region_7 and puma_maps_dict.has_key(rcounty_num):
        return "New York"
    elif rcounty_num in region_8 and puma_maps_dict.has_key(rcounty_num):
        return "North Country"
    elif rcounty_num in region_9 and puma_maps_dict.has_key(rcounty_num):
        return "Souther Tier"
    elif rcounty_num in region_10 and puma_maps_dict.has_key(rcounty_num):
        return "Western NY"
    else:
        return null
"""
def get_PUMA_from_CSV():
    PUMA = list()
    for p in p_data['PUMA']:
        if p not in PUMA:
            PUMA.append(p)
    return PUMA
def create_row_dataframe(PUMA=None,NATIVITY=None,AGEP=None,SCHL=None,SEX=None,WKHP=None,ESR=None,PINCP=None,POVPIP=None,GRPIP=None,TEN=None,RAC1P=None,HISP=None,GRP11=None):
    #   GRPIP and TEN are from housing data set.
    arguments = locals()
    #print 'args: ',arguments
    count = 0
    for a in arguments.keys():
        if a is not 'PUMA' \
                and a is not 'NATIVITY' \
                and a is not 'RAC1P' \
                and a is not 'HISP'\
                and a is not 'GRP11':
            if arguments[a] is not None:
                count += 1


    #   Each Variable following is a column in the data frame
    HSINC_m,HSINC_f= 0, 0 #   from df
    HSINC_mf_t = 0 #   derived from sum

    HS_m, HS_f= 0, 0 #   from df
    HS_mf_t= 0 #   derived from sum

    BABS_m, BABS_f = 0, 0 #   from df
    BABS_mf_t= 0#   derived from sum


    # Filter out Nativity and Age
    p_data_tmp = p_data[p_data['NATIVITY'] == NATIVITY]
    p_data1 = p_data_tmp[p_data_tmp['AGEP'].isin(range(25, 64 + 1))]
    # Filter out PUMA
    if PUMA is not None:
        p_data1 = p_data1[p_data1['PUMA'] == PUMA]
    # English Ability
    if NATIVITY is 2:
        p_data1 = p_data1[p_data1['ENG'].isin(range(1, 2 + 1))]

    # Filter out Race for NB_WNH
    if RAC1P is 1 and NATIVITY is 1:
        p_data1 = p_data1[p_data1['RAC1P'] == RAC1P]
    if HISP is 1 and NATIVITY is 1:
        p_data1 = p_data1[p_data1['HISP'] == HISP]

    # Filter out Race for FB_POC
    if HISP is 1 and RAC1P is 1 and NATIVITY is 2:
        # FB_NW : nothing to do
        p_data1_FB_NW = p_data1[p_data1['RAC1P'].isin(range(2, 9 + 1))]  # FB_NW
        p_data1_FB_WH = p_data1[p_data1['RAC1P'] == 1]  # FB_WH
        p_data_FB_WH = p_data1_FB_WH[p_data1_FB_WH['HISP'].isin(range(2, 24 + 1))]  # FB_WH
        # p_data1 = p_data_FB_WH + p_data1_FB_NW -- concate them.
        ''' # Check for intersection. None means no overlapping
        s1 = pandas.merge(p_data1_FB_WH, p_data1_FB_NW, how='right', on=['SERIALNO'])
        s1 = s1.dropna(inplace=True)
        print s1
        '''
        # p_data1 = pandas.concat([p_data1_FB_NW,p_data1_FB_WH],ignore_index=True)
        p_data1 = p_data1_FB_NW.append(p_data_FB_WH, ignore_index=True)

    # HSINC Male
    for i in p_data1.index:
        if p_data1.at[i, 'SCHL'] in range(0, 15 + 1) \
                and p_data1.at[i, 'SEX'] == 1:
            HSINC_m += p_data1.at[i, 'PWGTP']


    # HSINC Female
    for i in p_data1.index:
        if p_data1.at[i, 'SCHL'] in range(0, 15 + 1) \
                and p_data1.at[i, 'SEX'] == 2:
            HSINC_f += p_data1.at[i, 'PWGTP']


    # HS Male
    for i in p_data1.index:
        if p_data1.at[i, 'SCHL'] in range(16, 20 + 1) \
                and p_data1.at[i, 'SEX'] == 1:
            HS_m += p_data1.at[i, 'PWGTP']


    # HS Female
    for i in p_data1.index:
        if p_data1.at[i, 'SCHL'] in range(16, 20 + 1) \
                and p_data1.at[i, 'SEX'] == 2:
            HS_f += p_data1.at[i, 'PWGTP']


    # BABS Male
    for i in p_data1.index:
        if p_data1.at[i, 'SCHL'] in range(21, 24 + 1) \
                and p_data1.at[i, 'SEX'] == 1:
            BABS_m += p_data1.at[i, 'PWGTP']


    # BABS Female
    for i in p_data1.index:
        if p_data1.at[i, 'SCHL'] in range(21, 24 + 1) \
                and p_data1.at[i, 'SEX'] == 2:
            BABS_f += p_data1.at[i, 'PWGTP']


    # ------------------------------------------------------------------------------------------------------------------

    # HSINC Total
    HSINC_mf_t = HSINC_m + HSINC_f

    # HS Employed Total
    HS_mf_t = HS_m + HS_f
    # print 'HS Emp Total: ', HS_Emp_mf_t


    # BABS Employed Total
    BABS_mf_t = BABS_m + BABS_f
    # print 'BABS Emp Total: ', BABS_Emp_mf_t

    Total_Population = BABS_mf_t + HS_mf_t + HSINC_mf_t
    list_to_return = [Total_Population,
                      BABS_mf_t, BABS_m, BABS_f,

                      HS_mf_t, HS_m, HS_f,

                      HSINC_mf_t, HSINC_m, HSINC_f]
    if GRP11 is None:
        return list_to_return


def Edu_Attainment(NATIVITY = None, PATH = '/', RAC1P=None, HISP=None,GRP11=None,name=None):
    region_1_list,region_2_list,region_3_list,region_4_list,region_5_list = [],[],[],[],[]
    region_6_list,region_7_list,region_8_list,region_9_list,region_10_list = [],[],[],[],[]
    col_NB = ['puma',
                    'Total_Population',
                    'BABS_mf_t', 'BABS_m', 'BABS_f',

                    'HS_mf_t', 'HS_m', 'HS_f',

                    'HSINC_mf_t', 'HSINC_m', 'HSINC_f'
                    ]
    full_list = []

    # First row is not a PUMA county. It is the summation row.
    print '----------------- For Total Geo'
    l = create_row_dataframe(NATIVITY=NATIVITY,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11)
    l.insert(0, "Total Geo")
    full_list.append(l)
    # For all the PUMA counties.
    for p in global_data_edu.PUMA_Counties:
        print '----------------- For PUMA: ', puma_county(p)
        l = create_row_dataframe(PUMA=p,NATIVITY=NATIVITY,RAC1P=RAC1P,HISP=HISP,GRP11=GRP11)
        l.insert(0,puma_county(p))
        full_list.append(l)
        # add list to appropriate region list
        if p in region_1:
            region_1_list.append(l[1:])
            sum_region_1 = [sum(x) for x in zip(*region_1_list)]
        if p in region_2:
            region_2_list.append(l[1:])
            sum_region_2 = [sum(x) for x in zip(*region_2_list)]
        if p in region_3:
            region_3_list.append(l[1:])
            sum_region_3 = [sum(x) for x in zip(*region_3_list)]
        if p in region_4:
            region_4_list.append(l[1:])
            sum_region_4 = [sum(x) for x in zip(*region_4_list)]
        if p in region_5:
            region_5_list.append(l[1:])
            sum_region_5 = [sum(x) for x in zip(*region_5_list)]
        if p in region_6:
            region_6_list.append(l[1:])
            sum_region_6 = [sum(x) for x in zip(*region_6_list)]
        if p in region_7:
            region_7_list.append(l[1:])
            sum_region_7 = [sum(x) for x in zip(*region_7_list)]
        if p in region_8:
            region_8_list.append(l[1:])
            sum_region_8 = [sum(x) for x in zip(*region_8_list)]
        if p in region_9:
            region_9_list.append(l[1:])
            sum_region_9 = [sum(x) for x in zip(*region_9_list)]
        if p in region_10:
            region_10_list.append(l[1:])
            sum_region_10 = [sum(x) for x in zip(*region_10_list)]

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


    result_df = pandas.DataFrame(full_list, columns=col_NB)
    make_sure_path_exists(PATH + 'step_1/')
    result_df.to_csv(PATH + 'step_1/' + name, na_rep="#DIV/0!")

    if NATIVITY is 1 and RAC1P is None and HISP is None:
        global_data_edu.NB_ALL_p = get_percentage(df=result_df)
        make_sure_path_exists(PATH + 'step_2/')
        global_data_edu.NB_ALL_p.to_csv(PATH + 'step_2/' + 'NB_ALL_Edu_percent.csv', na_rep="#DIV/0!")
    if NATIVITY is 1 and RAC1P is 1 and HISP is 1:
        global_data_edu.NB_WNH_p = get_percentage(df=result_df)
        make_sure_path_exists(PATH + 'step_2/')
        global_data_edu.NB_WNH_p.to_csv(PATH + 'step_2/' + 'NB_WNH_Edu_percent.csv', na_rep="#DIV/0!")
    if NATIVITY is 2 and RAC1P is None and HISP is None:
        global_data_edu.FB_ALL_p = get_percentage(df=result_df)
        make_sure_path_exists(PATH + 'step_2/')
        global_data_edu.FB_ALL_p.to_csv(PATH + 'step_2/' + 'FB_ALL_Edu_percent.csv', na_rep="#DIV/0!")
    if NATIVITY is 2 and RAC1P is 1 and HISP is 1:
        global_data_edu.FB_POC_p = get_percentage(df=result_df)
        make_sure_path_exists(PATH + 'step_2/')
        global_data_edu.FB_POC_p.to_csv(PATH + 'step_2/' + 'FB_POC_Edu_percent.csv', na_rep="#DIV/0!")

def NB_ALL():
    print 'NB_ALL'
    PATH = 'data/2014/NB_All/'
    Edu_Attainment(NATIVITY=1,PATH=PATH,name='NB_Edu.csv')
def FB_ALL():
    
    print 'FB_ALL'
    PATH = 'data/2014/FB_All/'
    Edu_Attainment(NATIVITY=2,PATH=PATH,name='FB_Edu.csv')
def NB_WNH():
    print 'NB_WNH'
    PATH = 'data/2014/NB_WNH/'
    Edu_Attainment(NATIVITY=1,PATH=PATH,RAC1P=1,HISP=1,name='NB_WNH_Edu.csv')

def FB_POC(): # TODO Need to check result
    print 'FB_POC'
    PATH = 'data/2014/FB_POC/'
    Edu_Attainment(NATIVITY=2,PATH=PATH,RAC1P=1,HISP=1,name='FB_POC_Edu.csv')

def get_percentage(df = None):
    new_df = pandas.DataFrame()

    columns = list(df)

    for i in df.index:
        new_df.at[i, 'puma'] = df.at[i, 'puma']
        new_df.at[i, 'BABS_mf_t'] = ((df.at[i, 'BABS_mf_t'] * (1.0)) / df.at[i,'Total_Population'])*100
        new_df.at[i, 'BABS_m'] = ((df.at[i,'BABS_m'] * (1.0)) / df.at[i,'BABS_mf_t'])*100
        new_df.at[i, 'BABS_f'] = ((df.at[i,'BABS_f'] * (1.0)) / df.at[i,'BABS_mf_t'])*100

        new_df.at[i, 'HS_mf_t'] = ((df.at[i, 'HS_mf_t'] * (1.0)) / df.at[i, 'Total_Population']) * 100
        new_df.at[i, 'HS_m'] = ((df.at[i, 'HS_m'] * (1.0)) / df.at[i, 'HS_mf_t']) * 100
        new_df.at[i, 'HS_f'] = ((df.at[i, 'HS_f'] * (1.0)) / df.at[i, 'HS_mf_t']) * 100

        new_df.at[i, 'HSINC_mf_t'] = ((df.at[i, 'HSINC_mf_t'] * (1.0)) / df.at[i, 'Total_Population']) * 100
        new_df.at[i, 'HSINC_m'] = ((df.at[i, 'HSINC_m'] * (1.0)) / df.at[i, 'HSINC_mf_t']) * 100
        new_df.at[i, 'HSINC_f'] = ((df.at[i, 'HSINC_f'] * (1.0)) / df.at[i, 'HSINC_mf_t']) * 100

    return new_df

def make_sure_path_exists(path):
    # https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory-in-python
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
def run():
    print 'Creating DataFrames...'
    
    NB_ALL()
    FB_ALL()
    NB_WNH()
    FB_POC()

if __name__ == '__main__':
    run()