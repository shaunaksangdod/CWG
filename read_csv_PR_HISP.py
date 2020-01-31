import pandas

p_columns = ['SERIALNO','SPORDER','PUMA','PWGTP','AGEP','SCHL','SEX','WKHP','ESR','NATIVITY','PINCP','POVPIP','ENG','RAC1P','HISP','POBP']
h_columns = ['SERIALNO','GRPIP','TEN']

p_data = pandas.read_csv('data_PR_HISP/ss16pny.csv',usecols=p_columns)
h_data = pandas.read_csv('data_PR_HISP/ss16hny.csv',usecols=h_columns)


'''
--General variables(Hispanic Data):
1.NB_ALL:
    Education: SCHL:
                BABS: 21(BS),22(MS),23(Professional Degree),24(PHD)
                HS: 16(Reg. HS),17(GED or alternate credentials),18(College less than 1 yr),19(1 or more years of clg, no degree),20(associate degree)
                HSINC: 0-15(0-12 no diploma)

    Geo-Items: PUMA (region codes: https://www.census.gov/geo/maps-data/maps/2010puma/st36_ny.html )
    Nativity: NATIVITY:
                1 (NB) -- select this only
                2 (FB)
    SEX:
                1 (male)
                2 (female)
    Age: AGEP:
                25-64
2.FB_Hispanic:
    Education: SCHL:
                BABS: 21(BS),22(MS),23(Professional Degree),24(PHD)
                HS: 16(Reg. HS),17(GED or alternate credentials),18(College less than 1 yr),19(1 or more years of clg, no degree),20(associate degree)
                HSINC: 0-15(0-12 no diploma)
    
    Geo-Items: PUMA (region codes: https://www.census.gov/geo/maps-data/maps/2010puma/st36_ny.html )
    Nativity: NATIVITY:
                1 (NB) 
                2 (FB) -- select this only
    SEX:
                1 (male)
                2 (female)
    Age: AGEP:
                25-64
    English Ability: ENG:
                1: Very well
                2: well
    HISP(Recoded Detailed Hispanic Origin): Select ALL, 
                                            except "Not Spanish/Hispanic/Latino"
3.FB-White Non-Hispanic(FB-WNH):
    
    Education: SCHL:
                BABS: 21(BS),22(MS),23(Professional Degree),24(PHD)
                HS: 16(Reg. HS),17(GED or alternate credentials),18(College less than 1 yr),19(1 or more years of clg, no degree),20(associate degree)
                HSINC: 0-15(0-12 no diploma)
    Geo-Items: PUMA (region codes: https://www.census.gov/geo/maps-data/maps/2010puma/st36_ny.html )
    Nativity: NATIVITY:
                1 (NB) 
                2 (FB) -- select this only
    SEX:
                1 (male)
                2 (female)
    Age: AGEP:
                25-64
    
    English Ability: ENG:
                1: Very well
                2: well
    
    RAC1P : Select ONLY "White Alone" 
    HISP(Recoded Detailed Hispanic Origin): Select ONLY "Not Spanish/Hispanic/Latino"

4.FB-Hispanic POC: To be asked 
    HISP(Recoded Detailed Hispanic Origin): Select ONLY "Not Spanish/Hispanic/Latino"
    RAC1P : Select All except "White Alone"

--General variables(Puerto Rican Data):
1. Born in PR- Hispanic: 
    Education: SCHL:
                BABS: 21(BS),22(MS),23(Professional Degree),24(PHD)
                HS: 16(Reg. HS),17(GED or alternate credentials),18(College less than 1 yr),19(1 or more years of clg, no degree),20(associate degree)
                HSINC: 0-15(0-12 no diploma)
    Geo-Items: PUMA (region codes: https://www.census.gov/geo/maps-data/maps/2010puma/st36_ny.html )
    Nativity: NATIVITY:
                1 (NB) -- select this only
                2 (FB)
    SEX:
                1 (male)
                2 (female)
    Age: AGEP:
                25-64
    
    HISP: Select ONLY "Puerto Rican"
    Place of Birth:  Select ONLY "Puerto Rico"
2. NB in Mainland:
    Education: SCHL:
                BABS: 21(BS),22(MS),23(Professional Degree),24(PHD)
                HS: 16(Reg. HS),17(GED or alternate credentials),18(College less than 1 yr),19(1 or more years of clg, no degree),20(associate degree)
                HSINC: 0-15(0-12 no diploma)
    Geo-Items: PUMA (region codes: https://www.census.gov/geo/maps-data/maps/2010puma/st36_ny.html )
    Nativity: NATIVITY:
                1 (NB) -- select this only
                2 (FB)
    SEX:
                1 (male)
                2 (female)
    Age: AGEP:
                25-64
    
    Place of Birth : Select ONLY 001-056 (i.e. 50 States and D.C. THE NUMBERS SKIP IN THE DATAFERRETT)

3. NB in Mainland identifying as Puerto Rican(PR born in Mainland):
    Education: SCHL:
                BABS: 21(BS),22(MS),23(Professional Degree),24(PHD)
                HS: 16(Reg. HS),17(GED or alternate credentials),18(College less than 1 yr),19(1 or more years of clg, no degree),20(associate degree)
                HSINC: 0-15(0-12 no diploma)
    Geo-Items: PUMA (region codes: https://www.census.gov/geo/maps-data/maps/2010puma/st36_ny.html )
    Nativity: NATIVITY:
                1 (NB) -- select this only
                2 (FB)
    SEX:
                1 (male)
                2 (female)
    Age: AGEP:
                25-64
    HISP: Select only "Puerto rican"
    Place of Birth : Select ONLY 001-056 (i.e. 50 States and D.C. THE NUMBERS SKIP IN THE DATAFERRETT)
--Special variables:
    1.
    Unemployment: ESR:
                3: Unemployed
                1,2,4,5: Employed
    2.
    FT work access: WKHP
                1-34 : part-time
                35-99 : full-time
    3.
    Poverty: POVPIP
                0-150: poverty
                150+: not poverty
    4.
    Working Poor:
                WKHP:   35+
                POVPIP: 0-150: poverty
                        150+: not poverty
    5.
    Income level for FT workers:
                WKHP:   35+
                PINCP:  all except N/A (All income)
    6.
    Rent burden: GRPIP:
                0-50: Not rent burdened
                50+: Rent burdened
    7.
    Home ownership: TEN
                1,2: Owned with a mortgage (TEN=1),Owned free and clear (TEN=2)
                3,4: Not owned, Rented for cash (TEN=3),No cash rent (TEN=4)

Group 11: 
    Education: HSINC(0-12)
    Nativity: Only foreign
    Sex: 1-Male
         2- Female
    Age: 25-64
    English Ability: Not well and Not at all(ENG=3,4)
    Indicators are all same 
    for the disparties: take percentages, calculate mean and std
                        then (percentage-mean)/stddev
                        the scores criterias will remain same. 

'''