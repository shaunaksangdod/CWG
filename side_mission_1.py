import pandas
hp_data = pandas.DataFrame()
state_names = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
               'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii',
               'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
               'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
               'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
               'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
               'Oklahoma', 'Oregon', 'Pennslyvania', 'Rhode Island', 'South Carolina', 'South Dakota',
               'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
               'West Virginia', 'Wisconsin', 'Wyoming', 'U.S. state not specified']
def setup():
    state_numbers = range(57+1)
    state_numbers.remove(0)
    state_numbers.remove(3)
    state_numbers.remove(7)
    state_numbers.remove(14)
    state_numbers.remove(43)
    state_numbers.remove(52)

    h_columns = ['SERIALNO','ST','HHT','HUPAC']
    p_columns = ['SERIALNO','SEX','PWGTP']
    # df.groupby(['Fruit','Name'])['Number'].sum()
    h_data = pandas.DataFrame()
    h_data_a = pandas.read_csv('data/psam_husa.csv',usecols=h_columns)
    h_data_b = pandas.read_csv('data/psam_husb.csv',usecols=h_columns)
    p_data_a = pandas.read_csv('data/psam_pusa.csv',usecols=p_columns)
    p_data_b = pandas.read_csv('data/psam_pusb.csv',usecols=p_columns)
    h_data = h_data_a.append(h_data_b) # used append instead of concat because reindexing is needed
    p_data = p_data_a.append(p_data_b) # used append instead of concat because reindexing is needed
    hp_data = p_data.join(h_data.set_index('SERIALNO'), on='SERIALNO', how='right',
                          lsuffix='_population',rsuffix='_housing').reset_index(drop=True)
    hp_data = hp_data[hp_data['HHT'] != float(0)]
    hp_data = hp_data[hp_data['HUPAC'] != float(0)]
    hp_data = hp_data[hp_data['HUPAC'] != float(4)]
    hp_data = hp_data.drop(columns=['HUPAC'])
    hp_data = hp_data.rename(columns={'ST': 'State', 'HHT': 'Housing Type'})
    hp_data = hp_data.rename(columns={'PWGTP': 'Total Household'})
    hp_data['State'] = hp_data['State'].replace(state_numbers,state_names)


    #h_data = h_data.groupby(['State','Housing Type'])['Total Household'].sum().to_frame()
    hp_data.to_csv('data/US_State_housing_data.csv')

def format(ST=None,data1=None):
    data = data1
    HHT_1_M, HHT_1_F, HHT_1_T = 0,0,0
    HHT_2_M, HHT_2_F, HHT_2_T = 0,0,0
    HHT_3_M, HHT_3_F, HHT_3_T = 0,0,0
    HHT_4_M, HHT_4_F, HHT_4_T = 0,0,0
    HHT_5_M, HHT_5_F, HHT_5_T = 0,0,0
    HHT_6_M, HHT_6_F, HHT_6_T = 0,0,0
    HHT_7_M, HHT_7_F, HHT_7_T = 0,0,0
    HHT_T_M, HHT_T_F, HHT_T_T = 0,0,0
    print 'entering for loop'
    for i in data.index:
        #print [data.at[i,'SERIALNO'],data.at[i,'SEX'],data.at[i,'Housing Type']], ' added '
            if data.at[i,'Housing Type'] == 1:
                HHT_1_T += data.at[i,'Total Household']
                if data.at[i,'SEX'] == 1.0:
                    HHT_1_M += data.at[i,'Total Household']
                elif data.at[i,'SEX'] == 2.0:
                    HHT_1_F += data.at[i,'Total Household']
            elif data.at[i,'Housing Type'] == 2:
                HHT_2_T += data.at[i,'Total Household']
                if data.at[i,'SEX'] == 1.0:
                    HHT_2_M += data.at[i,'Total Household']
                elif data.at[i,'SEX'] == 2.0:
                    HHT_2_F += data.at[i,'Total Household']
            elif data.at[i,'Housing Type'] == 3:
                HHT_3_T += data.at[i,'Total Household']
                if data.at[i,'SEX'] == 1.0:
                    HHT_3_M += data.at[i,'Total Household']
                elif data.at[i,'SEX'] == 2.0:
                    HHT_3_F += data.at[i,'Total Household']
            elif data.at[i,'Housing Type'] == 4:
                HHT_4_T += data.at[i,'Total Household']
                if data.at[i,'SEX'] == 1.0:
                    HHT_4_M += data.at[i,'Total Household']
                elif data.at[i,'SEX'] == 2.0:
                    HHT_4_F += data.at[i,'Total Household']
            elif data.at[i,'Housing Type'] == 5:
                HHT_5_T += data.at[i,'Total Household']
                if data.at[i,'SEX'] == 1.0:
                    HHT_5_M += data.at[i,'Total Household']
                elif data.at[i,'SEX'] == 2.0:
                    HHT_5_F += data.at[i,'Total Household']
            elif data.at[i,'Housing Type'] == 6:
                HHT_6_T += data.at[i,'Total Household']
                if data.at[i,'SEX'] == 1.0:
                    HHT_6_M += data.at[i,'Total Household']
                elif data.at[i,'SEX'] == 2.0:
                    HHT_6_F += data.at[i,'Total Household']
            elif data.at[i,'Housing Type'] == 7:
                HHT_7_T += data.at[i,'Total Household']
                if data.at[i,'SEX'] == 1.0:
                    HHT_7_M += data.at[i,'Total Household']
                elif data.at[i,'SEX'] == 2.0:
                    HHT_7_F += data.at[i,'Total Household']
    print '---------------------------------out of for'
    HHT_T_M = HHT_1_M + HHT_2_M + HHT_3_M + HHT_4_M + HHT_5_M + HHT_6_M + HHT_7_M
    HHT_T_F = HHT_1_F + HHT_2_F + HHT_3_F + HHT_4_F + HHT_5_F + HHT_6_F + HHT_7_F
    HHT_T_T = HHT_1_T + HHT_2_T + HHT_3_T + HHT_4_T + HHT_5_T + HHT_6_T + HHT_7_T
    state = ''
    if ST is None:
        state = 'Total'
    else:
        state = ST
    print state
    return [ state,
             HHT_T_T, HHT_T_M, HHT_T_F,
             HHT_1_T, HHT_1_M, HHT_1_F,
             HHT_2_T, HHT_2_M, HHT_2_F,
             HHT_3_T, HHT_3_M, HHT_3_F,
             HHT_4_T, HHT_4_M, HHT_4_F,
             HHT_5_T, HHT_5_M, HHT_5_F,
             HHT_6_T, HHT_6_M, HHT_6_F,
             HHT_7_T, HHT_7_M, HHT_7_F
    ]


def get_data():
    data = pandas.read_csv('data/US_State_housing_data.csv')
    print 'Data read...'
    cols = ['State',
            'Total_HHT','Total_HHT_M','Total_HHT_F',
            'Married_couple_household_T','Married_couple_household_M','Married_couple_household_F',
            'Other family household:Male householder, no wife present_T','Other family household:Male householder, no wife present_M','Other family household:Male householder, no wife present_F',
            'Other family household:Female householder, no husband present_T','Other family household:Female householder, no husband present_M','Other family household:Female householder, no husband present_F',
            'Nonfamily household:Male householder:Living alone_T','Nonfamily household:Male householder:Living alone_M','Nonfamily household:Male householder:Living alone_F',
            'Nonfamily household:Male householder:not living alone_T','Nonfamily household:Male householder:not living alone_M','Nonfamily household:Male householder:not living alone_F',
            'Nonfamily household:Female householder:Living alone_T','Nonfamily household:Female householder:Living alone_M','Nonfamily household:Female householder:Living alone_F',
            'Nonfamily household:Female householder:not living alone_T','Nonfamily household:Female householder:not living alone_M','Nonfamily household:Female householder:not living alone_F'
            ]
    final_list = []

    # total

    l = format(ST=None,data1=data)
    final_list.append(l)
    print 'for total: ', l

    for state in state_names:
        l = format(ST=state,data1=data[data['State'] == state])
        final_list.append(l)
        print 'for ',state,l
    #ToDo check if it's going in infinite for loop
    print '-------------------------------------'
    print 'final DF:'
    print final_list
    final_df = pandas.DataFrame(final_list,columns=cols)
    final_df.to_csv('data/US_State_housing_data_final.csv')
if __name__ == '__main__':
    setup()
    #format()
    get_data()