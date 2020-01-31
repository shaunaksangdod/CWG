import pandas as pd
import matplotlib.cm as cm
import numpy as np
import matplotlib.pyplot as plt
import matplotlib,os,errno


# Data
'''
r = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
data_0 = 0 # Western NY
raw_data = {'greenBars': [20, 1.5, 7, 10, 5,20, 1.5, 7, 10, 5], 'orangeBars': [5, 15, 5, 10, 15,5, 15, 5, 10, 15],
            'blueBars': [2, 15, 18, 5, 10,2, 15, 18, 5, 10]}
df = pd.DataFrame(raw_data)

# From raw value to percentage
totals = [i + j + k for i, j, k in zip(df['greenBars'], df['orangeBars'], df['blueBars'])]
greenBars = [i / j * 100 for i, j in zip(df['greenBars'], totals)]
orangeBars = [i / j * 100 for i, j in zip(df['orangeBars'], totals)]
blueBars = [i / j * 100 for i, j in zip(df['blueBars'], totals)]

# plot
barWidth = 0.85
names = ('Western NY', 'Southern NY', 'North Contry', 'New York City', 'Mohawk Valley', 'Mid-Hudon', 'Long Island',
         'Fingerlakes', 'Central NY', 'Capital Region')
# Create green Bars
bar1 = plt.bar(r, greenBars, color='#b5ffb9', edgecolor='white', width=barWidth, label="group A")
# Create orange Bars
bar2 = plt.bar(r, orangeBars, bottom=greenBars, color='#f9bc86', edgecolor='white', width=barWidth, label="group B")
# Create blue Bars
bar3 = plt.bar(r, blueBars, bottom=[i + j for i, j in zip(greenBars, orangeBars)], color='#a3acff', edgecolor='white', width=barWidth, label="group C")

for r1, r2, r3 in zip(bar1, bar2, bar3):
    h1 = r1.get_height()
    h2 = r2.get_height()
    h3 = r3.get_height()
    plt.text(r1.get_x() + r1.get_width() / 2., h1 / 2., "%d" % h1, ha="center", va="center", color="white", fontsize=16, fontweight="bold")
    plt.text(r2.get_x() + r2.get_width() / 2., h1 + h2 / 2., "%d" % h2, ha="center", va="center", color="white", fontsize=16, fontweight="bold")
    plt.text(r3.get_x() + r3.get_width() / 2., h1 + h2 + h3 / 2., "%d" % h3, ha="center", va="center", color="white", fontsize=16, fontweight="bold")

# Custom x axis
plt.xticks(r, names)
plt.xlabel("group")

plt.legend(loc='upper left', bbox_to_anchor=(1,1), ncol=1)

# Show graphic
plt.show()
'''

'''
PR and HISP: PR_HISP_DATA does not include eng prof and edu att '''
def plot_clustered_stacked(dfall, labels=None, legend1_loc = None, legend2_loc = None, title=None,  H=".", save_name=None, **kwargs):
    """Given a list of dataframes, with identical columns and index, create a clustered stacked bar plot.
labels is a list of the names of the dataframe, used for the legend
title is a string for the title of the plot
H is the hatch used for identification of the different dataframe"""

    n_df = len(dfall)
    n_col = len(dfall[0].columns)
    n_ind = len(dfall[0].index)

    plt.figure(num=1,figsize=(20,10),dpi=80)
    axe = plt.subplot(111)


    h1,h2 = 0,0
    for df in dfall : # for each data frame
        axe = df.plot(kind="bar",
                      color = ['#b5ffb9', '#f9bc86','#a3acff'],
                      linewidth=0.5,
                      stacked=True,
                      ax=axe,
                      legend=False,
                      grid=False,
                      **kwargs)  # make bar plots

    h,l = axe.get_legend_handles_labels() # get the handles we want to modify
    for i in range(0, n_df * n_col, n_col): # len(h) = n_col * n_df
        for j, pa in enumerate(h[i:i+n_col]):
            for rect in pa.patches: # for each index
                rect.set_x(rect.get_x() + 1 / float(n_df + 1) * i / float(n_col))
                rect.set_hatch(H * int(i / n_col)) #edited part
                rect.set_width(1 / float(n_df + 1))

                bl = rect.get_xy()
                x = 0.5 * rect.get_width() + bl[0]
                y = 0.5 * rect.get_height() + bl[1]
                label = rect.get_height()
                axe.text(x, y, "%d" % (label), ha='center',va='center', bbox=dict(facecolor='#FFEBCD', alpha=1,boxstyle='round,pad=0.3'))
    axe.set_xticks((np.arange(0, 2 * n_ind, 2) + 1 / float(n_df + 1)) / 2.)
    axe.set_xticklabels(df.index, rotation = 0)
    axe.set_title(title)

    # Add invisible data to add another legend
    n=[]
    for i in range(n_df):
        n.append(axe.bar(0, 0, color="w", hatch=H * i))

    l1 = axe.legend(h[:n_col], l[:n_col], loc=legend1_loc)
    if labels is not None:
        l2 = plt.legend(n, labels, loc=legend2_loc)
    axe.add_artist(l1)


    PATH = 'plots/' + save_name.split('/')[0] + '/'
    make_sure_path_exists(PATH)
    plt.savefig(PATH + save_name.split('/')[1] + '.png')
    #plt.show()
    plt.close()
    return axe

def pie_plot(names=None,sizes=None, PATH=None, save_name=None,title=None):
    import matplotlib.pyplot as plt

    # Data to plot
    regions = names
    sizes = sizes
    colors = ['#B8860B', 'lightcoral', 'lightskyblue','#0066cc','#4d4d00','#004d00','#993333','#999966','#4d004d','#666699']
    explode = 1 - (sizes / sizes.sum())
    percent = 100. * sizes / sizes.sum()
    labels = ['{0} - {1:1.2f} %'.format(i, j) for i, j in zip(regions, percent)]
    # Plot

    plt.figure(num=1,figsize=(20,10),dpi=80)
    plt.title(title,y=1.08)
    patches, texts= plt.pie(sizes, explode=explode,labels=labels,colors=colors, startangle=25)

    # plt.legend(patches, labels,loc='right',fontsize=12)

    #plt.legend(patches, labels, loc="best")
    plt.axis('equal')
    PATH = 'plots/' + save_name.split('/')[0] + '/'
    make_sure_path_exists(PATH)
    plt.savefig(PATH + save_name.split('/')[1] + '.png')
    #plt.show()
    plt.close()

def func(pct, allvals):
    absolute = int(pct/100.*np.sum(allvals))
    return "{:.1f}%".format(pct, absolute)

def make_sure_path_exists(path):
    # https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory-in-python
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
if __name__ == '__main__':
    names = ['Capital Region','Central NY', 'Fingerlakes', 'Long Island', 'Mid-Hudon', 'Mohawk Valley', 'New York City',
             'North Contry', 'Southern NY','Western NY']
    years = [2014,2016,2017,'HISP_2016','PR_2012_2016','PR_new']

    for year in years:
        print year

        '''
                   [BABS HS HSINC]
        [[0.43318102 0.58696186 0.9867504 ]
         [0.66154522 0.61986549 0.63301052]
         [0.50228802 0.82427909 0.21085254]
         [0.96895778 0.75051893 0.59345177]
         [0.89791523 0.67850337 0.68462278]
         [0.87258123 0.36720837 0.50053732]
         [0.67052441 0.77855857 0.29220114]
         [0.20936551 0.36975588 0.12474265]
         [0.46917573 0.93054858 0.65642399]
         [0.60664917 0.13121359 0.12592089]]'''


        #'''
        print 'Edu att'
        # Education attainment
        if year in [2014, 2016, 2017]:
            print '----------------------------'
            # fetch data
            FB = pd.read_csv('data/edu_att/'+str(year)+'/FB_All/step_2/FB_ALL_Edu_percent.csv')
            NB = pd.read_csv('data/edu_att/' + str(year) + '/NB_All/step_2/NB_ALL_Edu_percent.csv')
            FB_POC = pd.read_csv('data/edu_att/' + str(year) + '/FB_POC/step_2/FB_POC_Edu_percent.csv')
            NB_WNH = pd.read_csv('data/edu_att/' + str(year) + '/NB_WNH/step_2/NB_WNH_Edu_percent.csv')

            # clean data
            FB_BABS = FB[146:]['BABS_mf_t']
            FB_HS = FB[146:]['HS_mf_t']
            FB_HSINC = FB[146:]['HSINC_mf_t']
            NB_BABS = NB[146:]['BABS_mf_t']
            NB_HS = NB[146:]['HS_mf_t']
            NB_HSINC = NB[146:]['HSINC_mf_t']


            FB_POC_BABS = FB_POC[146:]['BABS_mf_t']
            FB_POC_HS = FB_POC[146:]['HS_mf_t']
            FB_POC_HSINC = FB_POC[146:]['HSINC_mf_t']
            NB_WNH_BABS = NB_WNH[146:]['BABS_mf_t']
            NB_WNH_HS = NB_WNH[146:]['HS_mf_t']
            NB_WNH_HSINC = NB_WNH[146:]['HSINC_mf_t']

            FB_M_BABS = FB[146:]['BABS_m']
            FB_M_HS = FB[146:]['HS_m']
            FB_M_HSINC = FB[146:]['HSINC_m']
            FB_F_BABS = FB[146:]['BABS_f']
            FB_F_HS = FB[146:]['HS_f']
            FB_F_HSINC = FB[146:]['HSINC_f']

            # zip data
            FB_data = zip(FB_BABS, FB_HS, FB_HSINC)
            NB_data = zip(NB_BABS, NB_HS, NB_HSINC)

            FB_POC_data = zip(FB_POC_BABS.astype('float64'), FB_POC_HS.astype('float64'), FB_POC_HSINC.astype('float64'))
            NB_WNH_data = zip(NB_WNH_BABS.astype('float64'), NB_WNH_HS.astype('float64'), NB_WNH_HSINC.astype('float64'))

            FB_M_data = zip(FB_M_BABS.astype('float64'), FB_M_HS.astype('float64'), FB_M_HSINC.astype('float64'))
            FB_F_data = zip(FB_F_BABS.astype('float64'), FB_F_HS.astype('float64'), FB_F_HSINC.astype('float64'))

            # create dataframes


            # plot 1
            FB_ALL = pd.DataFrame(FB_data,
                               index=names,
                               columns=["College degree or better", "High School diploma / some college", "Without High school diploma"])
            NB_ALL = pd.DataFrame(NB_data,
                               index=names,
                               columns=["College degree or better", "High School diploma / some college", "Without High school diploma"])

            plot_clustered_stacked([FB_ALL, NB_ALL],["Foreign born", "Native born"],title='% of Foreign(English Proficient) '

                                                                                    'and Native born - ' + str(year),save_name=str(year)+'/img1',
                                   legend1_loc=[0.3, 0.85],legend2_loc=[0.6, 0.85])
            # plot 2
            FB_POC = pd.DataFrame(FB_POC_data,
                               index=names,
                               columns=["College degree or better", "High School diploma / some college", "Without High school diploma"])
            NB_WNH = pd.DataFrame(NB_WNH_data,
                               index=names,
                               columns=["College degree or better", "High School diploma / some college", "Without High school diploma"])

            plot_clustered_stacked([FB_POC, NB_WNH],["FB-People of color", "NB-White non hispanic"],title='% of Foreign born people of color(English Proficient) '
                                                                                    'and Native born white non hispanic - ' + str(year),save_name=str(year)+'/img2',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_M = pd.DataFrame(FB_M_data,
                               index=names,
                               columns=["College degree or better", "High School diploma / some college", "Without High school diploma"])
            FB_F = pd.DataFrame(FB_F_data,
                               index=names,
                               columns=["College degree or better", "High School diploma / some college", "Without High school diploma"])

            plot_clustered_stacked([FB_M, FB_F],["Male", "Female"],title='% of Foreign born Male '
                                                                                    'and Female (English Proficient) - ' + str(year),save_name=str(year)+'/img3',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'HISP_2016':
            # fetch data
            FB_HISP = pd.read_csv('data/edu_att/' + str(year) + '/FB_Hispanic/step_2/FB_Hispanic_Edu_percent.csv')
            NB_HISP = pd.read_csv('data/edu_att/' + str(year) + '/NB_Hispanic/step_2/NB_Hispanic_Edu_percent.csv')
            FB_HISP_POC = pd.read_csv('data/edu_att/' + str(year) + '/FB_Hispanic_POC/step_2/FB_Hispanic_POC_Edu_percent.csv')
            FB_WNH = pd.read_csv('data/edu_att/' + str(year) + '/FB_WNH/step_2/FB_WNH_Edu_percent.csv')

            # clean data
            FB_BABS = FB_HISP[146:]['BABS_mf_t']
            FB_HS = FB_HISP[146:]['HS_mf_t']
            FB_HSINC = FB_HISP[146:]['HSINC_mf_t']
            NB_BABS = NB_HISP[146:]['BABS_mf_t']
            NB_HS = NB_HISP[146:]['HS_mf_t']
            NB_HSINC = NB_HISP[146:]['HSINC_mf_t']

            FB_HISP_POC_BABS = FB_HISP_POC[146:]['BABS_mf_t']
            FB_HISP_POC_HS = FB_HISP_POC[146:]['HS_mf_t']
            FB_HISP_POC_HSINC = FB_HISP_POC[146:]['HSINC_mf_t']
            FB_WNH_BABS = FB_WNH[146:]['BABS_mf_t']
            FB_WNH_HS = FB_WNH[146:]['HS_mf_t']
            FB_WNH_HSINC = FB_WNH[146:]['HSINC_mf_t']

            FB_M_BABS = FB_HISP[146:]['BABS_m']
            FB_M_HS = FB_HISP[146:]['HS_m']
            FB_M_HSINC = FB_HISP[146:]['HSINC_m']
            FB_F_BABS = FB_HISP[146:]['BABS_f']
            FB_F_HS = FB_HISP[146:]['HS_f']
            FB_F_HSINC = FB_HISP[146:]['HSINC_f']

            # zip data
            FB_HISP_data = zip(FB_BABS, FB_HS, FB_HSINC)
            NB_HISP_data = zip(NB_BABS, NB_HS, NB_HSINC)

            FB_HISP_POC_data = zip(FB_HISP_POC_BABS.astype('float64'), FB_HISP_POC_HS.astype('float64'), FB_HISP_POC_HSINC.astype('float64'))
            FB_WNH_data = zip(FB_WNH_BABS.astype('float64'), FB_WNH_HS.astype('float64'), FB_WNH_HSINC.astype('float64'))

            FB_M_data = zip(FB_M_BABS.astype('float64'), FB_M_HS.astype('float64'), FB_M_HSINC.astype('float64'))
            FB_F_data = zip(FB_F_BABS.astype('float64'), FB_F_HS.astype('float64'), FB_F_HSINC.astype('float64'))

            # plot 1
            FB_ALL = pd.DataFrame(FB_HISP_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])
            NB_ALL = pd.DataFrame(NB_HISP_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])

            plot_clustered_stacked([FB_ALL, NB_ALL], ["Foreign born Hispanic", "Native born Hispanic"],
                                   title='% of Foreign(English Proficient) '

                                         'and Native born Hispanic- ' + str(year), save_name=str(year) + '/img1',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
            # plot 2
            FB_POC = pd.DataFrame(FB_HISP_POC_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])
            NB_WNH = pd.DataFrame(FB_WNH_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])

            plot_clustered_stacked([FB_POC, NB_WNH], ["FB Hispanic People of color", "FB White non hispanic"],
                                   title='% of Foreign born Hispanic people of color(English Proficient) '
                                         'and Foreign born white non hispanic - ' + str(year),
                                   save_name=str(year) + '/img2',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college",
                                         "Without High school diploma"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college",
                                         "Without High school diploma"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"], title='% of Foreign born Hispanic Male '
                                                                           'and Female (English Proficient) - ' + str(
                year), save_name=str(year) + '/img3',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_2012_2016':
            # fetch data
            FB_HISP = pd.read_csv('data/edu_att/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_Edu_percent.csv')
            NB_HISP = pd.read_csv('data/edu_att/PR_2012_2016/NB_Mainland/step_2/NB_Mainland_Edu_percent.csv')

            FB_HISP_POC = pd.read_csv(
                'data/edu_att/HISP_2016/FB_Hispanic/step_2/FB_Hispanic_Edu_percent.csv')
            FB_WNH = pd.read_csv('data/edu_att/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_Edu_percent.csv')

            NB_Mainland_PR = pd.read_csv('data/edu_att/PR_2012_2016/NB_Mainland_PR/step_2/NB_Mainland_PR_Edu_percent.csv')
            PR_HISP = pd.read_csv('data/edu_att/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_Edu_percent.csv')
            # clean data
            FB_BABS = FB_HISP[146:]['BABS_mf_t']
            FB_HS = FB_HISP[146:]['HS_mf_t']
            FB_HSINC = FB_HISP[146:]['HSINC_mf_t']
            NB_BABS = NB_HISP[146:]['BABS_mf_t']
            NB_HS = NB_HISP[146:]['HS_mf_t']
            NB_HSINC = NB_HISP[146:]['HSINC_mf_t']

            FB_HISP_POC_BABS = FB_HISP_POC[146:]['BABS_mf_t']
            FB_HISP_POC_HS = FB_HISP_POC[146:]['HS_mf_t']
            FB_HISP_POC_HSINC = FB_HISP_POC[146:]['HSINC_mf_t']
            FB_WNH_BABS = FB_WNH[146:]['BABS_mf_t']
            FB_WNH_HS = FB_WNH[146:]['HS_mf_t']
            FB_WNH_HSINC = FB_WNH[146:]['HSINC_mf_t']

            NB_Mainland_PR_BABS = NB_Mainland_PR[146:]['BABS_mf_t']
            NB_Mainland_PR_HS = NB_Mainland_PR[146:]['HS_mf_t']
            NB_Mainland_PR_HSINC = NB_Mainland_PR[146:]['HSINC_mf_t']
            PR_HISP_BABS = PR_HISP[146:]['BABS_mf_t']
            PR_HISP_HS = PR_HISP[146:]['HS_mf_t']
            PR_HISP_HSINC = PR_HISP[146:]['HSINC_mf_t']

            # zip data
            FB_HISP_data = zip(FB_BABS, FB_HS, FB_HSINC)
            NB_HISP_data = zip(NB_BABS, NB_HS, NB_HSINC)

            FB_HISP_POC_data = zip(FB_HISP_POC_BABS.astype('float64'), FB_HISP_POC_HS.astype('float64'),
                                   FB_HISP_POC_HSINC.astype('float64'))
            FB_WNH_data = zip(FB_WNH_BABS.astype('float64'), FB_WNH_HS.astype('float64'),
                              FB_WNH_HSINC.astype('float64'))

            FB_M_data = zip(NB_Mainland_PR_BABS.astype('float64'), NB_Mainland_PR_HS.astype('float64'), NB_Mainland_PR_HSINC.astype('float64'))
            FB_F_data = zip(PR_HISP_BABS.astype('float64'), PR_HISP_HS.astype('float64'), PR_HISP_HSINC.astype('float64'))

            # plot 1
            FB_ALL = pd.DataFrame(FB_HISP_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])
            NB_ALL = pd.DataFrame(NB_HISP_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])

            plot_clustered_stacked([FB_ALL, NB_ALL], ["Puerto Rican Hispanic", "Native born Mainland"],
                                   title='% of Puerto Rican Hispanic '

                                         'and Native born Mainland- ' + str(year), save_name=str(year) + '/img1',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
            # plot 2
            FB_POC = pd.DataFrame(FB_HISP_POC_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])
            NB_WNH = pd.DataFrame(FB_WNH_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])

            plot_clustered_stacked([FB_POC, NB_WNH], ["FB Hispanic", "Puerto Rican hispanic"],
                                   title='% of Foreign born Hispanic(English Proficient) '
                                         'and Puerto Rican hispanic - ' + str(year),
                                   save_name=str(year) + '/img2',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college",
                                         "Without High school diploma"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college",
                                         "Without High school diploma"])

            plot_clustered_stacked([FB_M, FB_F], ["Native Born Mainland", "Puerto Rican Hispanic"],
                                   title='% of Native Born Mainland and Puerto Rican Hispanic (English Proficient) - ' + str(
                year), save_name=str(year) + '/img3',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_new':
            # fetch data
            FB_HISP = pd.read_csv('data/edu_att/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_Edu_percent.csv')
            NB_HISP = pd.read_csv('data/edu_att/PR_2012_2016/NB_Mainland_PR/step_2/NB_Mainland_PR_Edu_percent.csv')

            FB_HISP_POC = pd.read_csv('data/edu_att/PR_new/PR_POC/step_2/PR_POC_Edu_percent.csv')
            FB_WNH = pd.read_csv('data/edu_att/PR_new/PR_White/step_2/PR_White_Edu_percent.csv')

            # clean data
            FB_BABS = FB_HISP[146:]['BABS_mf_t']
            FB_HS = FB_HISP[146:]['HS_mf_t']
            FB_HSINC = FB_HISP[146:]['HSINC_mf_t']
            NB_BABS = NB_HISP[146:]['BABS_mf_t']
            NB_HS = NB_HISP[146:]['HS_mf_t']
            NB_HSINC = NB_HISP[146:]['HSINC_mf_t']

            FB_HISP_POC_BABS = FB_HISP_POC[146:]['BABS_mf_t']
            FB_HISP_POC_HS = FB_HISP_POC[146:]['HS_mf_t']
            FB_HISP_POC_HSINC = FB_HISP_POC[146:]['HSINC_mf_t']
            FB_WNH_BABS = FB_WNH[146:]['BABS_mf_t']
            FB_WNH_HS = FB_WNH[146:]['HS_mf_t']
            FB_WNH_HSINC = FB_WNH[146:]['HSINC_mf_t']

            NB_Mainland_PR_BABS = FB_HISP[146:]['BABS_m']
            NB_Mainland_PR_HS = FB_HISP[146:]['HS_m']
            NB_Mainland_PR_HSINC = FB_HISP[146:]['HSINC_m']
            PR_HISP_BABS = FB_HISP[146:]['BABS_f']
            PR_HISP_HS = FB_HISP[146:]['HS_f']
            PR_HISP_HSINC = FB_HISP[146:]['HSINC_f']

            # zip data
            FB_HISP_data = zip(FB_BABS, FB_HS, FB_HSINC)
            NB_HISP_data = zip(NB_BABS, NB_HS, NB_HSINC)

            FB_HISP_POC_data = zip(FB_HISP_POC_BABS.astype('float64'), FB_HISP_POC_HS.astype('float64'),
                                   FB_HISP_POC_HSINC.astype('float64'))
            FB_WNH_data = zip(FB_WNH_BABS.astype('float64'), FB_WNH_HS.astype('float64'),
                              FB_WNH_HSINC.astype('float64'))

            FB_M_data = zip(NB_Mainland_PR_BABS.astype('float64'), NB_Mainland_PR_HS.astype('float64'),
                            NB_Mainland_PR_HSINC.astype('float64'))
            FB_F_data = zip(PR_HISP_BABS.astype('float64'), PR_HISP_HS.astype('float64'),
                            PR_HISP_HSINC.astype('float64'))

            # plot 1
            FB_ALL = pd.DataFrame(FB_HISP_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])
            NB_ALL = pd.DataFrame(NB_HISP_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])

            plot_clustered_stacked([FB_ALL, NB_ALL], ["Puerto Rican Hispanic", "Native born Mainland Puerto Rican"],
                                   title='% of Puerto Rican Hispanic '

                                         'and Native born Mainland Puerto Rican- ' + str(year), save_name=str(year) + '/img1',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
            # plot 2
            FB_POC = pd.DataFrame(FB_HISP_POC_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])
            NB_WNH = pd.DataFrame(FB_WNH_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college",
                                           "Without High school diploma"])

            plot_clustered_stacked([FB_POC, NB_WNH], ["Puerto Rican People of color", "Puerto Rican White"],
                                   title='% of Puerto Rican People of color(English Proficient) '
                                         'and Puerto Rican White - ' + str(year),
                                   save_name=str(year) + '/img2',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college",
                                         "Without High school diploma"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college",
                                         "Without High school diploma"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='% of Puerto Rican Hispanic (English Proficient) Male and Female- ' + str(
                                       year), save_name=str(year) + '/img3',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        #'''

        # Unemployment

        #'''
        print 'unemp'
        if year in [2014, 2016, 2017]:
            # plot 1
            FB_unemp = pd.read_csv('data/unemp/'+ str(year) +'_UnEmp/FB_ALL_M_F/UnEmp.csv')
            NB_unemp = pd.read_csv('data/unemp/'+ str(year) +'_UnEmp/NB_ALL/UnEmp.csv')

            FB_unemp_BABS = FB_unemp[146:]['BABS_UnEmp_Total']
            FB_unemp_HS = FB_unemp[146:]['HS_UnEmp_Total']

            NB_unemp_BABS = NB_unemp[146:]['BABS_UnEmp_Total']
            NB_unemp_HS = NB_unemp[146:]['HS_UnEmp_Total']


            FB_unemp_data = zip(FB_unemp_BABS.astype('float64'),FB_unemp_HS.astype('float64'))
            NB_unemp_data = zip(NB_unemp_BABS.astype('float64'),NB_unemp_HS.astype('float64'))



            FB_unemp = pd.DataFrame(FB_unemp_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college"])
            NB_unemp = pd.DataFrame(NB_unemp_data,
                                  index=names,
                                  columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp, NB_unemp], ["Foreign born", "Native born"],
                                   title='Unemployment rate (%) of Foreign(English Proficient) and Native born  - ' + str(year),save_name=str(year)+'/img4',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])


            # plot 2
            FB_unemp_POC = pd.read_csv('data/unemp/' + str(year) + '_UnEmp/FB_POC/UnEmp.csv')
            NB_unemp_WNH = pd.read_csv('data/unemp/' + str(year) + '_UnEmp/NB_WNH/UnEmp.csv')

            FB_unemp_POC_BABS = FB_unemp_POC[146:]['BABS_UnEmp_Total']
            FB_unemp_POC_HS = FB_unemp_POC[146:]['HS_UnEmp_Total']

            NB_unemp_WNH_BABS = NB_unemp_WNH[146:]['BABS_UnEmp_Total']
            NB_unemp_WNH_HS = NB_unemp_WNH[146:]['HS_UnEmp_Total']

            FB_unemp_POC_data = zip(FB_unemp_POC_BABS.astype('float64'), FB_unemp_POC_HS.astype('float64'))
            NB_unemp_WNH_data = zip(NB_unemp_WNH_BABS.astype('float64'), NB_unemp_WNH_HS.astype('float64'))

            FB_unemp_POC = pd.DataFrame(FB_unemp_POC_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])
            NB_unemp_WNH = pd.DataFrame(NB_unemp_WNH_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp_POC, NB_unemp_WNH], ["FB-people of color", "NB-white non hispanic"],
                                   title='Unemployment rate (%) of Foreign born prople of color(English Proficient) and Native '
                                         'born white non hispanic - ' + str(year),save_name=str(year)+'/img5',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])


            # plot 3
            FB_unemp = pd.read_csv('data/unemp/' + str(year) + '_UnEmp/FB_ALL_M_F/UnEmp.csv')

            FB_M_BABS = FB_unemp[146:]['BABS_UnEmp_M']
            FB_M_HS = FB_unemp[146:]['HS_UnEmp_M']

            FB_F_BABS = FB_unemp[146:]['BABS_UnEmp_F']
            FB_F_HS = FB_unemp[146:]['HS_UnEmp_F']

            FB_M_data = zip(FB_M_BABS.astype('float64'), FB_M_HS.astype('float64'))
            FB_F_data = zip(FB_F_BABS.astype('float64'), FB_F_HS.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Unemployment rate (%) of Foreign(English Proficient) born Male and Female  - ' + str(year),
            save_name = str(year) + '/img6',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'HISP_2016':
            # plot 1
            FB_unemp = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/UnEmp.csv')
            NB_unemp = pd.read_csv('data/PR_HISP_DATA/NB_Hispanic/step_2/UnEmp.csv')

            FB_unemp_BABS = FB_unemp[146:]['BABS_UnEmp_Total']
            FB_unemp_HS = FB_unemp[146:]['HS_UnEmp_Total']

            NB_unemp_BABS = NB_unemp[146:]['BABS_UnEmp_Total']
            NB_unemp_HS = NB_unemp[146:]['HS_UnEmp_Total']

            FB_unemp_data = zip(FB_unemp_BABS.astype('float64').multiply(100), FB_unemp_HS.astype('float64').multiply(100))
            NB_unemp_data = zip(NB_unemp_BABS.astype('float64').multiply(100), NB_unemp_HS.astype('float64').multiply(100))

            FB_unemp = pd.DataFrame(FB_unemp_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])
            NB_unemp = pd.DataFrame(NB_unemp_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp, NB_unemp], ["Foreign born Hispanic", "Native born Hispanic"],
                                   title='Unemployment rate (%) of Foreign(English Proficient) and Native born Hispanic - ' + str(
                                       year), save_name=str(year) + '/img7',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_unemp_POC = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic_POC/step_2/UnEmp.csv')
            NB_unemp_WNH = pd.read_csv('data/PR_HISP_DATA/FB_WNH/step_2/UnEmp.csv')

            FB_unemp_POC_BABS = FB_unemp_POC[146:]['BABS_UnEmp_Total']
            FB_unemp_POC_HS = FB_unemp_POC[146:]['HS_UnEmp_Total']

            NB_unemp_WNH_BABS = NB_unemp_WNH[146:]['BABS_UnEmp_Total']
            NB_unemp_WNH_HS = NB_unemp_WNH[146:]['HS_UnEmp_Total']

            FB_unemp_POC_data = zip(FB_unemp_POC_BABS.astype('float64').multiply(100), FB_unemp_POC_HS.astype('float64').multiply(100))
            NB_unemp_WNH_data = zip(NB_unemp_WNH_BABS.astype('float64').multiply(100), NB_unemp_WNH_HS.astype('float64').multiply(100))

            FB_unemp_POC = pd.DataFrame(FB_unemp_POC_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])
            NB_unemp_WNH = pd.DataFrame(NB_unemp_WNH_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp_POC, NB_unemp_WNH], ["FB Hispanic people of color", "FB-white non hispanic"],
                                   title='Unemployment rate (%) of Foreign born Hispanic prople of color(English Proficient) and Foreign '
                                         'born white non hispanic - ' + str(year), save_name=str(year) + '/img8',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_unemp = pd.read_csv('data/PR_HISP_DATA/FB_WNH/step_2/UnEmp.csv')

            FB_M_BABS = FB_unemp[146:]['BABS_UnEmp_M']
            FB_M_HS = FB_unemp[146:]['HS_UnEmp_M']

            FB_F_BABS = FB_unemp[146:]['BABS_UnEmp_F']
            FB_F_HS = FB_unemp[146:]['HS_UnEmp_F']

            FB_M_data = zip(FB_M_BABS.astype('float64').multiply(100), FB_M_HS.astype('float64').multiply(100))
            FB_F_data = zip(FB_F_BABS.astype('float64').multiply(100), FB_F_HS.astype('float64').multiply(100))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Unemployment rate (%) of Foreign(English Proficient) born Hispanic Male and Female  - ' + str(
                                       year),
                                   save_name=str(year) + '/img9',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_2012_2016':
            # plot 1
            FB_unemp = pd.read_csv('data/PR_HISP_DATA/NB_Mainland/step_2/UnEmp.csv')
            NB_unemp = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/UnEmp.csv')

            FB_unemp_BABS = FB_unemp[146:]['BABS_UnEmp_Total']
            FB_unemp_HS = FB_unemp[146:]['HS_UnEmp_Total']

            NB_unemp_BABS = NB_unemp[146:]['BABS_UnEmp_Total']
            NB_unemp_HS = NB_unemp[146:]['HS_UnEmp_Total']

            FB_unemp_data = zip(FB_unemp_BABS.astype('float64').multiply(100), FB_unemp_HS.astype('float64').multiply(100))
            NB_unemp_data = zip(NB_unemp_BABS.astype('float64').multiply(100), NB_unemp_HS.astype('float64').multiply(100))

            FB_unemp = pd.DataFrame(FB_unemp_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])
            NB_unemp = pd.DataFrame(NB_unemp_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp, NB_unemp], ["Native born Mainland","Puerto Rican Hispanic"],
                                   title='Unemployment rate (%) of Puerto Rican Hispanic and Native born Mainland - ' + str(
                                       year), save_name=str(year) + '/img7',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_unemp_POC = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/UnEmp.csv')
            NB_unemp_WNH = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/UnEmp.csv')

            FB_unemp_POC_BABS = FB_unemp_POC[146:]['BABS_UnEmp_Total']
            FB_unemp_POC_HS = FB_unemp_POC[146:]['HS_UnEmp_Total']

            NB_unemp_WNH_BABS = NB_unemp_WNH[146:]['BABS_UnEmp_Total']
            NB_unemp_WNH_HS = NB_unemp_WNH[146:]['HS_UnEmp_Total']

            FB_unemp_POC_data = zip(FB_unemp_POC_BABS.astype('float64').multiply(100), FB_unemp_POC_HS.astype('float64').multiply(100))
            NB_unemp_WNH_data = zip(NB_unemp_WNH_BABS.astype('float64').multiply(100), NB_unemp_WNH_HS.astype('float64').multiply(100))

            FB_unemp_POC = pd.DataFrame(FB_unemp_POC_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])
            NB_unemp_WNH = pd.DataFrame(NB_unemp_WNH_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp_POC, NB_unemp_WNH], ["FB Hispanic", "Puerto Rican hispanic"],
                                   title='Unemployment rate (%) of Foreign born(English Proficient) and Puerto Rican'
                                         ' hispanic - ' + str(year), save_name=str(year) + '/img8',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_unemp_POC = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/UnEmp.csv')
            NB_unemp_WNH = pd.read_csv('data/PR_HISP_DATA/NB_Mainland_PR/step_2/UnEmp.csv')

            FB_unemp_POC_BABS = FB_unemp_POC[146:]['BABS_UnEmp_Total']
            FB_unemp_POC_HS = FB_unemp_POC[146:]['HS_UnEmp_Total']

            NB_unemp_WNH_BABS = NB_unemp_WNH[146:]['BABS_UnEmp_Total']
            NB_unemp_WNH_HS = NB_unemp_WNH[146:]['HS_UnEmp_Total']

            FB_unemp_POC_data = zip(FB_unemp_POC_BABS.astype('float64').multiply(100), FB_unemp_POC_HS.astype('float64').multiply(100))
            NB_unemp_WNH_data = zip(NB_unemp_WNH_BABS.astype('float64').multiply(100), NB_unemp_WNH_HS.astype('float64').multiply(100))

            FB_unemp_POC = pd.DataFrame(FB_unemp_POC_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])
            NB_unemp_WNH = pd.DataFrame(NB_unemp_WNH_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp_POC, NB_unemp_WNH], ["Puerto Rican hispanic", "Native born Mainland"],
                                   title='Unemployment rate (%) of Puerto Rican hispanic and NAtive born Mainland - '
                                         + str(year), save_name=str(year) + '/img9',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_new':
            # plot 1
            FB_unemp = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/UnEmp.csv')
            NB_unemp = pd.read_csv('data/PR_HISP_DATA/NB_Mainland_PR/step_2/UnEmp.csv')

            FB_unemp_BABS = FB_unemp[146:]['BABS_UnEmp_Total']
            FB_unemp_HS = FB_unemp[146:]['HS_UnEmp_Total']

            NB_unemp_BABS = NB_unemp[146:]['BABS_UnEmp_Total']
            NB_unemp_HS = NB_unemp[146:]['HS_UnEmp_Total']

            FB_unemp_data = zip(FB_unemp_BABS.astype('float64').multiply(100), FB_unemp_HS.astype('float64').multiply(100))
            NB_unemp_data = zip(NB_unemp_BABS.astype('float64').multiply(100), NB_unemp_HS.astype('float64').multiply(100))

            FB_unemp = pd.DataFrame(FB_unemp_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])
            NB_unemp = pd.DataFrame(NB_unemp_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp, NB_unemp], ["Puerto Rican Hispanic", "Native born Mainland Puerto Rican"],
                                   title='Unemployment rate (%) of Puerto Rican Hispanic and Native born Mainland Puerto Rican- ' + str(
                                       year), save_name=str(year) + '/img7',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_unemp_POC = pd.read_csv('data/PR_HISP_DATA/PR_POC/step_2/UnEmp.csv')
            NB_unemp_WNH = pd.read_csv('data/PR_HISP_DATA/PR_White/step_2/UnEmp.csv')

            FB_unemp_POC_BABS = FB_unemp_POC[146:]['BABS_UnEmp_Total'].replace('#DIV/0!' , np.nan)
            FB_unemp_POC_HS = FB_unemp_POC[146:]['HS_UnEmp_Total'].replace('#DIV/0!' , np.nan)

            NB_unemp_WNH_BABS = NB_unemp_WNH[146:]['BABS_UnEmp_Total'].replace('#DIV/0!' , np.nan)
            NB_unemp_WNH_HS = NB_unemp_WNH[146:]['HS_UnEmp_Total'].replace('#DIV/0!' , np.nan)

            FB_unemp_POC_data = zip(FB_unemp_POC_BABS.astype('float64').multiply(100), FB_unemp_POC_HS.astype('float64').multiply(100))
            NB_unemp_WNH_data = zip(NB_unemp_WNH_BABS.astype('float64').multiply(100), NB_unemp_WNH_HS.astype('float64').multiply(100))

            FB_unemp_POC = pd.DataFrame(FB_unemp_POC_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])
            NB_unemp_WNH = pd.DataFrame(NB_unemp_WNH_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp_POC, NB_unemp_WNH], ["Puerto Rican people of color", "Puerto Rican White"],
                                   title='Unemployment rate (%) of Puerto Rican people of color(English Proficient) and Puerto Rican White'
                                         ' - ' + str(year), save_name=str(year) + '/img8',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_unemp_POC = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/UnEmp.csv')

            FB_unemp_POC_BABS = FB_unemp_POC[146:]['BABS_UnEmp_M']
            FB_unemp_POC_HS = FB_unemp_POC[146:]['HS_UnEmp_M']

            NB_unemp_WNH_BABS = FB_unemp_POC[146:]['BABS_UnEmp_F']
            NB_unemp_WNH_HS = FB_unemp_POC[146:]['HS_UnEmp_F']

            FB_unemp_POC_data = zip(FB_unemp_POC_BABS.astype('float64').multiply(100), FB_unemp_POC_HS.astype('float64').multiply(100))
            NB_unemp_WNH_data = zip(NB_unemp_WNH_BABS.astype('float64').multiply(100), NB_unemp_WNH_HS.astype('float64').multiply(100))

            FB_unemp_POC = pd.DataFrame(FB_unemp_POC_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])
            NB_unemp_WNH = pd.DataFrame(NB_unemp_WNH_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_unemp_POC, NB_unemp_WNH], ["Male", "Female"],
                                   title='Unemployment rate (%) of Puerto Rican hispanic Male and Female - '
                                         + str(year), save_name=str(year) + '/img9',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

        #'''

        # income

        #'''
        print 'income'
        if year in [2014,2016,2017]:
            # plot 1
            FB_income = pd.read_csv('data/income/' + str(year) + '_income/FB_All/step_1/Income_level_FT_Workers.csv')
            NB_income = pd.read_csv('data/income/' + str(year) + '_income/NB_All/step_1/Income_level_FT_Workers.csv')

            FB_income_BABS = FB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)
            FB_income_HS = FB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)

            NB_income_BABS = NB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)
            NB_income_HS = NB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)

            FB_income_data = zip(FB_income_BABS.astype('float64',errors='ignore'), FB_income_HS.astype('float64',errors='ignore'))
            NB_income_data = zip(NB_income_BABS.astype('float64',errors='ignore'), NB_income_HS.astype('float64',errors='ignore'))

            FB_income = pd.DataFrame(FB_income_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])
            NB_income = pd.DataFrame(NB_income_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income, NB_income], ["Foreign born", "Native born"],
                                   title='Income level of Foreign(English Proficient) and Native born  - ' + str(year)
                                   , save_name=str(year) + '/img7',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_income_POC = pd.read_csv('data/income/' + str(year) + '_income/FB_POC/step_1/Income_level_FT_Workers.csv')
            NB_income_WNH = pd.read_csv('data/income/' + str(year) + '_income/NB_WNH/step_1/Income_level_FT_Workers.csv')

            FB_income_POC_BABS = FB_income_POC[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)
            FB_income_POC_HS = FB_income_POC[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)

            NB_income_WNH_BABS = NB_income_WNH[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)
            NB_income_WNH_HS = NB_income_WNH[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!' , np.nan)

            FB_income_POC_data = zip(FB_income_POC_BABS.astype('float64'), FB_income_POC_HS.astype('float64'))
            NB_income_WNH_data = zip(NB_income_WNH_BABS.astype('float64'), NB_income_WNH_HS.astype('float64'))

            FB_income_POC = pd.DataFrame(FB_income_POC_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])
            NB_income_WNH = pd.DataFrame(NB_income_WNH_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income_POC, NB_income_WNH], ["FB-people of color", "NB-white non hispanic"],
                                   title='Income level of Foreign born prople of color(English Proficient) and Native born white non hispanic - ' + str(
                                       year),save_name=str(year)+'/img8',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_income = pd.read_csv('data/income/' + str(year) + '_income/FB_All/step_1/Income_level_FT_Workers.csv')

            FB_M_BABS = FB_income[146:]['BABS_Avg_PINCP_m'].replace('#DIV/0!' , np.nan)
            FB_M_HS = FB_income[146:]['HS_Avg_PINCP_m'].replace('#DIV/0!' , np.nan)

            FB_F_BABS = FB_income[146:]['BABS_Avg_PINCP_f'].replace('#DIV/0!' , np.nan)
            FB_F_HS = FB_income[146:]['HS_Avg_PINCP_f'].replace('#DIV/0!' , np.nan)

            FB_M_data = zip(FB_M_BABS.astype('float64'), FB_M_HS.astype('float64'))
            FB_F_data = zip(FB_F_BABS.astype('float64'), FB_F_HS.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Income level of Foreign(English Proficient) born Male and Female  - ' + str(
                                       year),save_name=str(year)+'/img9',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'HISP_2016':
            # plot 1
            FB_income = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/Income_level_FT_Workers.csv')
            NB_income = pd.read_csv('data/PR_HISP_DATA/NB_Hispanic/step_2/Income_level_FT_Workers.csv')

            FB_income_BABS = FB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            FB_income_HS = FB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            NB_income_BABS = NB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            NB_income_HS = NB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            FB_income_data = zip(FB_income_BABS.astype('float64', errors='ignore'),
                                 FB_income_HS.astype('float64', errors='ignore'))
            NB_income_data = zip(NB_income_BABS.astype('float64', errors='ignore'),
                                 NB_income_HS.astype('float64', errors='ignore'))

            FB_income = pd.DataFrame(FB_income_data,
                                     index=names,
                                     columns=["College degree or better", "High School diploma / some college"])
            NB_income = pd.DataFrame(NB_income_data,
                                     index=names,
                                     columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income, NB_income], ["Foreign born Hispanic", "Native born Hispanic"],
                                   title='Income level of Foreign(English Proficient) and Native born Hispanic - ' + str(year)
                                   , save_name=str(year) + '/img10',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_income_POC = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic_POC/step_2/Income_level_FT_Workers.csv')
            NB_income_WNH = pd.read_csv('data/PR_HISP_DATA/FB_WNH/step_2/Income_level_FT_Workers.csv')

            FB_income_POC_BABS = FB_income_POC[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            FB_income_POC_HS = FB_income_POC[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            NB_income_WNH_BABS = NB_income_WNH[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            NB_income_WNH_HS = NB_income_WNH[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            FB_income_POC_data = zip(FB_income_POC_BABS.astype('float64'), FB_income_POC_HS.astype('float64'))
            NB_income_WNH_data = zip(NB_income_WNH_BABS.astype('float64'), NB_income_WNH_HS.astype('float64'))

            FB_income_POC = pd.DataFrame(FB_income_POC_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])
            NB_income_WNH = pd.DataFrame(NB_income_WNH_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income_POC, NB_income_WNH], ["FB Hispanic people of color", "FB-white non hispanic"],
                                   title='Income level of Foreign born Hispanic prople of color(English Proficient) and Foreign born white non hispanic - ' + str(
                                       year), save_name=str(year) + '/img11',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_income = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/Income_level_FT_Workers.csv')

            FB_M_BABS = FB_income[146:]['BABS_Avg_PINCP_m'].replace('#DIV/0!', np.nan)
            FB_M_HS = FB_income[146:]['HS_Avg_PINCP_m'].replace('#DIV/0!', np.nan)

            FB_F_BABS = FB_income[146:]['BABS_Avg_PINCP_f'].replace('#DIV/0!', np.nan)
            FB_F_HS = FB_income[146:]['HS_Avg_PINCP_f'].replace('#DIV/0!', np.nan)

            FB_M_data = zip(FB_M_BABS.astype('float64'), FB_M_HS.astype('float64'))
            FB_F_data = zip(FB_F_BABS.astype('float64'), FB_F_HS.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Income level of Foreign(English Proficient) born Hispanic Male and Female  - ' + str(
                                       year), save_name=str(year) + '/img12',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_2012_2016':
            # plot 1
            FB_income = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Income_level_FT_Workers.csv')
            NB_income = pd.read_csv('data/PR_HISP_DATA/NB_Mainland/step_2/Income_level_FT_Workers.csv')

            FB_income_BABS = FB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            FB_income_HS = FB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            NB_income_BABS = NB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            NB_income_HS = NB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            FB_income_data = zip(FB_income_BABS.astype('float64', errors='ignore'),
                                 FB_income_HS.astype('float64', errors='ignore'))
            NB_income_data = zip(NB_income_BABS.astype('float64', errors='ignore'),
                                 NB_income_HS.astype('float64', errors='ignore'))

            FB_income = pd.DataFrame(FB_income_data,
                                     index=names,
                                     columns=["College degree or better", "High School diploma / some college"])
            NB_income = pd.DataFrame(NB_income_data,
                                     index=names,
                                     columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income, NB_income], ["Pierto Rican Hispanic", "Native born Mainland"],
                                   title='Income level of Pierto Rican Hispanic and Native born Mainland - ' + str(year)
                                   , save_name=str(year) + '/img10',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_income_POC = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/Income_level_FT_Workers.csv')
            NB_income_WNH = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Income_level_FT_Workers.csv')

            FB_income_POC_BABS = FB_income_POC[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            FB_income_POC_HS = FB_income_POC[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            NB_income_WNH_BABS = NB_income_WNH[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            NB_income_WNH_HS = NB_income_WNH[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            FB_income_POC_data = zip(FB_income_POC_BABS.astype('float64'), FB_income_POC_HS.astype('float64'))
            NB_income_WNH_data = zip(NB_income_WNH_BABS.astype('float64'), NB_income_WNH_HS.astype('float64'))

            FB_income_POC = pd.DataFrame(FB_income_POC_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])
            NB_income_WNH = pd.DataFrame(NB_income_WNH_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income_POC, NB_income_WNH], ["FB Hispanic ", "Puerto Rican hispanic"],
                                   title='Income level of Foreign born Hispanic (English Proficient) and Puerto Rican hispanic - ' + str(
                                       year), save_name=str(year) + '/img11',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_income_POC = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Income_level_FT_Workers.csv')
            NB_income_WNH = pd.read_csv('data/PR_HISP_DATA/NB_Mainland_PR/step_2/Income_level_FT_Workers.csv')

            FB_income_POC_BABS = FB_income_POC[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            FB_income_POC_HS = FB_income_POC[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            NB_income_WNH_BABS = NB_income_WNH[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            NB_income_WNH_HS = NB_income_WNH[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            FB_income_POC_data = zip(FB_income_POC_BABS.astype('float64'), FB_income_POC_HS.astype('float64'))
            NB_income_WNH_data = zip(NB_income_WNH_BABS.astype('float64'), NB_income_WNH_HS.astype('float64'))

            FB_income_POC = pd.DataFrame(FB_income_POC_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])
            NB_income_WNH = pd.DataFrame(NB_income_WNH_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income_POC, NB_income_WNH], ["Puerto Rican Hispanic ", "Native born Mainland Puerto Rican"],
                                   title='Income level of Puerto Rican hispanic and Native born Mainland Puerto Rican - ' + str(
                                       year), save_name=str(year) + '/img12',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_new':
            # plot 1
            FB_income = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Income_level_FT_Workers.csv')
            NB_income = pd.read_csv('data/PR_HISP_DATA/NB_Mainland_PR/step_2/Income_level_FT_Workers.csv')

            FB_income_BABS = FB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            FB_income_HS = FB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            NB_income_BABS = NB_income[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            NB_income_HS = NB_income[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            FB_income_data = zip(FB_income_BABS.astype('float64', errors='ignore'),
                                 FB_income_HS.astype('float64', errors='ignore'))
            NB_income_data = zip(NB_income_BABS.astype('float64', errors='ignore'),
                                 NB_income_HS.astype('float64', errors='ignore'))

            FB_income = pd.DataFrame(FB_income_data,
                                     index=names,
                                     columns=["College degree or better", "High School diploma / some college"])
            NB_income = pd.DataFrame(NB_income_data,
                                     index=names,
                                     columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income, NB_income], ["Puerto Rican Hispanic", "Native born Mainland Puerto Rican"],
                                   title='Income level of Puerto Rican Hispanic and Native born Mainland Puerto Rican - ' + str(year)
                                   , save_name=str(year) + '/img10',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_income_POC = pd.read_csv('data/PR_HISP_DATA/PR_POC/step_2/Income_level_FT_Workers.csv')
            NB_income_WNH = pd.read_csv('data/PR_HISP_DATA/PR_White/step_2/Income_level_FT_Workers.csv')

            FB_income_POC_BABS = FB_income_POC[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            FB_income_POC_HS = FB_income_POC[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            NB_income_WNH_BABS = NB_income_WNH[146:]['BABS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)
            NB_income_WNH_HS = NB_income_WNH[146:]['HS_Avg_PINCP_mf_t'].replace('#DIV/0!', np.nan)

            FB_income_POC_data = zip(FB_income_POC_BABS.astype('float64'), FB_income_POC_HS.astype('float64'))
            NB_income_WNH_data = zip(NB_income_WNH_BABS.astype('float64'), NB_income_WNH_HS.astype('float64'))

            FB_income_POC = pd.DataFrame(FB_income_POC_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])
            NB_income_WNH = pd.DataFrame(NB_income_WNH_data,
                                         index=names,
                                         columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_income_POC, NB_income_WNH], ["Puerto Rican people of color", "Puerto Rican-white"],
                                   title='Income level of Puerto Rican people of color and Puerto Rican-white - ' + str(
                                       year), save_name=str(year) + '/img11',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_income = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Income_level_FT_Workers.csv')

            FB_M_BABS = FB_income[146:]['BABS_Avg_PINCP_m'].replace('#DIV/0!', np.nan)
            FB_M_HS = FB_income[146:]['HS_Avg_PINCP_m'].replace('#DIV/0!', np.nan)

            FB_F_BABS = FB_income[146:]['BABS_Avg_PINCP_f'].replace('#DIV/0!', np.nan)
            FB_F_HS = FB_income[146:]['HS_Avg_PINCP_f'].replace('#DIV/0!', np.nan)

            FB_M_data = zip(FB_M_BABS.astype('float64'), FB_M_HS.astype('float64'))
            FB_F_data = zip(FB_F_BABS.astype('float64'), FB_F_HS.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Income level of Puerto Rican Hispanic Male and Female  - ' + str(
                                       year), save_name=str(year) + '/img12',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

        #'''

        # poverty

        #'''
        print 'poverty'
        if year in [2014, 2016, 2017]:
            # plot 1
            FB_poverty = pd.read_csv('data/poverty/' + str(year) + '_poverty/FB_ALL_M_F/Poverty.csv')
            NB_poverty = pd.read_csv('data/poverty/' + str(year) + '_poverty/NB_ALL/Poverty.csv')

            FB_poverty_BABS = FB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!' , np.nan)
            FB_poverty_HS = FB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!' , np.nan)

            NB_poverty_BABS = NB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!' , np.nan)
            NB_poverty_HS = NB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!' , np.nan)

            FB_poverty_data = zip(FB_poverty_BABS.astype('float64',errors='ignore'), FB_poverty_HS.astype('float64',errors='ignore'))
            NB_poverty_data = zip(NB_poverty_BABS.astype('float64',errors='ignore'), NB_poverty_HS.astype('float64',errors='ignore'))

            FB_poverty = pd.DataFrame(FB_poverty_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])
            NB_poverty = pd.DataFrame(NB_poverty_data,
                                    index=names,
                                    columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty, NB_poverty], ["Foreign born", "Native born"],
                                   title='Poverty rate (%) of Foreign(English Proficient) and Native born  - ' + str(year),
                                   save_name=str(year) + '/img10',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_poverty_POC = pd.read_csv('data/poverty/' + str(year) + '_poverty/FB_POC/Poverty.csv')
            NB_poverty_WNH = pd.read_csv('data/poverty/' + str(year) + '_poverty/NB_WNH/Poverty.csv')

            FB_poverty_POC_BABS = FB_poverty_POC[146:]['BABS_Poverty_Total'].replace('#DIV/0!' , np.nan)
            FB_poverty_POC_HS = FB_poverty_POC[146:]['HS_Poverty_Total'].replace('#DIV/0!' , np.nan)

            NB_poverty_WNH_BABS = NB_poverty_WNH[146:]['BABS_Poverty_Total'].replace('#DIV/0!' , np.nan)
            NB_poverty_WNH_HS = NB_poverty_WNH[146:]['HS_Poverty_Total'].replace('#DIV/0!' , np.nan)

            FB_poverty_POC_data = zip(FB_poverty_POC_BABS.astype('float64'), FB_poverty_POC_HS.astype('float64'))
            NB_poverty_WNH_data = zip(NB_poverty_WNH_BABS.astype('float64'), NB_poverty_WNH_HS.astype('float64'))

            FB_poverty_POC = pd.DataFrame(FB_poverty_POC_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])
            NB_poverty_WNH = pd.DataFrame(NB_poverty_WNH_data,
                                        index=names,
                                        columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty_POC, NB_poverty_WNH], ["FB-people of color", "NB-white non hispanic"],
                                   title='Poverty rate (%) of Foreign born people of color(English Proficient) and Native born white non hispanic - ' + str(
                                       year),save_name=str(year)+'/img11',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_poverty = pd.read_csv('data/poverty/' + str(year) + '_poverty/FB_ALL_M_F/Poverty.csv')

            FB_M_BABS = FB_poverty[146:]['BABS_Poverty_M'].replace('#DIV/0!' , np.nan)
            FB_M_HS = FB_poverty[146:]['HS_Poverty_M'].replace('#DIV/0!' , np.nan)

            FB_F_BABS = FB_poverty[146:]['BABS_Poverty_F'].replace('#DIV/0!' , np.nan)
            FB_F_HS = FB_poverty[146:]['HS_Poverty_F'].replace('#DIV/0!' , np.nan)

            FB_M_data = zip(FB_M_BABS.astype('float64'), FB_M_HS.astype('float64'))
            FB_F_data = zip(FB_F_BABS.astype('float64'), FB_F_HS.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Poverty rate (%) of Foreign(English Proficient) born Male and Female  - ' + str(
                                       year),save_name=str(year)+'/img12',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'HISP_2016':
            # plot 1
            FB_poverty = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/Poverty.csv')
            NB_poverty = pd.read_csv('data/PR_HISP_DATA/NB_Hispanic/step_2/Poverty.csv')

            FB_poverty_BABS = FB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            FB_poverty_HS = FB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            NB_poverty_BABS = NB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            NB_poverty_HS = NB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            FB_poverty_data = zip(FB_poverty_BABS.astype('float64').multiply(100),
                                  FB_poverty_HS.astype('float64').multiply(100))
            NB_poverty_data = zip(NB_poverty_BABS.astype('float64').multiply(100),
                                  NB_poverty_HS.astype('float64').multiply(100))

            FB_poverty = pd.DataFrame(FB_poverty_data,
                                      index=names,
                                      columns=["College degree or better", "High School diploma / some college"])
            NB_poverty = pd.DataFrame(NB_poverty_data,
                                      index=names,
                                      columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty, NB_poverty], ["Foreign born Hispanic", "Native born Hispanic"],
                                   title='Poverty rate (%) of Foreign(English Proficient) and Native born Hispanic - ' + str(
                                       year),
                                   save_name=str(year) + '/img13',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_poverty_POC = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic_POC/step_2/Poverty.csv')
            NB_poverty_WNH = pd.read_csv('data/PR_HISP_DATA/FB_WNH/step_2/Poverty.csv')

            FB_poverty_POC_BABS = FB_poverty_POC[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            FB_poverty_POC_HS = FB_poverty_POC[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            NB_poverty_WNH_BABS = NB_poverty_WNH[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            NB_poverty_WNH_HS = NB_poverty_WNH[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            FB_poverty_POC_data = zip(FB_poverty_POC_BABS.astype('float64').multiply(100), FB_poverty_POC_HS.astype('float64').multiply(100))
            NB_poverty_WNH_data = zip(NB_poverty_WNH_BABS.astype('float64').multiply(100), NB_poverty_WNH_HS.astype('float64').multiply(100))

            FB_poverty_POC = pd.DataFrame(FB_poverty_POC_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])
            NB_poverty_WNH = pd.DataFrame(NB_poverty_WNH_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty_POC, NB_poverty_WNH], ["FB Hispanic people of color", "FB-white non hispanic"],
                                   title='Poverty rate (%) of Foreign born Hispanic people of color(English Proficient) and Foreign born white non hispanic - ' + str(
                                       year), save_name=str(year) + '/img14',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_poverty = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/Poverty.csv')

            FB_M_BABS = FB_poverty[146:]['BABS_Poverty_M'].replace('#DIV/0!', np.nan)
            FB_M_HS = FB_poverty[146:]['HS_Poverty_M'].replace('#DIV/0!', np.nan)

            FB_F_BABS = FB_poverty[146:]['BABS_Poverty_F'].replace('#DIV/0!', np.nan)
            FB_F_HS = FB_poverty[146:]['HS_Poverty_F'].replace('#DIV/0!', np.nan)

            FB_M_data = zip(FB_M_BABS.astype('float64').multiply(100), FB_M_HS.astype('float64').multiply(100))
            FB_F_data = zip(FB_F_BABS.astype('float64').multiply(100), FB_F_HS.astype('float64').multiply(100))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Poverty rate (%) of Foreign(English Proficient) born Hispanic Male and Female  - ' + str(
                                       year), save_name=str(year) + '/img15',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_2012_2016':
            # plot 1
            FB_poverty = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Poverty.csv')
            NB_poverty = pd.read_csv('data/PR_HISP_DATA/NB_Mainland/step_2/Poverty.csv')

            FB_poverty_BABS = FB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            FB_poverty_HS = FB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            NB_poverty_BABS = NB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            NB_poverty_HS = NB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)


            FB_poverty_data = zip(FB_poverty_BABS.astype('float64').multiply(100),
                                  FB_poverty_HS.astype('float64').multiply(100))
            NB_poverty_data = zip(NB_poverty_BABS.astype('float64').multiply(100),
                                  NB_poverty_HS.astype('float64').multiply(100))

            FB_poverty = pd.DataFrame(FB_poverty_data,
                                      index=names,
                                      columns=["College degree or better", "High School diploma / some college"])
            NB_poverty = pd.DataFrame(NB_poverty_data,
                                      index=names,
                                      columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty, NB_poverty], ["Puerto Rican Hispanic", "Native born Mainaland"],
                                   title='Poverty rate (%) of Puerto Rican Hispanic and Native born Mainland - ' + str(
                                       year),
                                   save_name=str(year) + '/img13',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_poverty_POC = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Poverty.csv')
            NB_poverty_WNH = pd.read_csv('data/PR_HISP_DATA/FB_Hispanic/step_2/Poverty.csv')

            FB_poverty_POC_BABS = FB_poverty_POC[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            FB_poverty_POC_HS = FB_poverty_POC[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            NB_poverty_WNH_BABS = NB_poverty_WNH[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            NB_poverty_WNH_HS = NB_poverty_WNH[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            FB_poverty_POC_data = zip(FB_poverty_POC_BABS.astype('float64').multiply(100),
                                      FB_poverty_POC_HS.astype('float64').multiply(100))
            NB_poverty_WNH_data = zip(NB_poverty_WNH_BABS.astype('float64').multiply(100),
                                      NB_poverty_WNH_HS.astype('float64').multiply(100))

            FB_poverty_POC = pd.DataFrame(FB_poverty_POC_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])
            NB_poverty_WNH = pd.DataFrame(NB_poverty_WNH_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty_POC, NB_poverty_WNH], ["Puerto Rican Hispanic", "Foreign Born hispanic"],
                                   title='Poverty rate (%) of Puerto Rican Hispanic and Foreign Born hispanic - ' + str(
                                       year), save_name=str(year) + '/img14',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_poverty_POC = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Poverty.csv')
            NB_poverty_WNH = pd.read_csv('data/PR_HISP_DATA/NB_Mainland_PR/step_2/Poverty.csv')

            FB_poverty_POC_BABS = FB_poverty_POC[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            FB_poverty_POC_HS = FB_poverty_POC[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            NB_poverty_WNH_BABS = NB_poverty_WNH[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            NB_poverty_WNH_HS = NB_poverty_WNH[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            FB_poverty_POC_data = zip(FB_poverty_POC_BABS.astype('float64').multiply(100),
                                      FB_poverty_POC_HS.astype('float64').multiply(100))
            NB_poverty_WNH_data = zip(NB_poverty_WNH_BABS.astype('float64').multiply(100),
                                      NB_poverty_WNH_HS.astype('float64').multiply(100))

            FB_poverty_POC = pd.DataFrame(FB_poverty_POC_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])
            NB_poverty_WNH = pd.DataFrame(NB_poverty_WNH_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty_POC, NB_poverty_WNH], ["Puerto Rican Hispanic", "Native Born mainland Puerto Rican"],
                                   title='Poverty rate (%) of Puerto Rican Hispanic and Native Born mainland Puerto Rican - ' + str(
                                       year), save_name=str(year) + '/img15',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_new':
            # plot 1
            FB_poverty = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Poverty.csv')
            NB_poverty = pd.read_csv('data/PR_HISP_DATA/NB_Mainland_PR/step_2/Poverty.csv')

            FB_poverty_BABS = FB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            FB_poverty_HS = FB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            NB_poverty_BABS = NB_poverty[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            NB_poverty_HS = NB_poverty[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            FB_poverty_data = zip(FB_poverty_BABS.astype('float64').multiply(100),
                                  FB_poverty_HS.astype('float64').multiply(100))
            NB_poverty_data = zip(NB_poverty_BABS.astype('float64').multiply(100),
                                  NB_poverty_HS.astype('float64').multiply(100))

            FB_poverty = pd.DataFrame(FB_poverty_data,
                                      index=names,
                                      columns=["College degree or better", "High School diploma / some college"])
            NB_poverty = pd.DataFrame(NB_poverty_data,
                                      index=names,
                                      columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty, NB_poverty], ["Puerto Rican Hispanic", "Native born Mainland Puerto Rican"],
                                   title='Poverty rate (%) of Puerto Rican Hispanic and Native born Mainland Puerto Rican - ' + str(
                                       year),
                                   save_name=str(year) + '/img13',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_poverty_POC = pd.read_csv('data/PR_HISP_DATA/PR_POC/step_2/Poverty.csv')
            NB_poverty_WNH = pd.read_csv('data/PR_HISP_DATA/PR_White/step_2/Poverty.csv')

            FB_poverty_POC_BABS = FB_poverty_POC[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            FB_poverty_POC_HS = FB_poverty_POC[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            NB_poverty_WNH_BABS = NB_poverty_WNH[146:]['BABS_Poverty_Total'].replace('#DIV/0!', np.nan)
            NB_poverty_WNH_HS = NB_poverty_WNH[146:]['HS_Poverty_Total'].replace('#DIV/0!', np.nan)

            FB_poverty_POC_data = zip(FB_poverty_POC_BABS.astype('float64').multiply(100),
                                      FB_poverty_POC_HS.astype('float64').multiply(100))
            NB_poverty_WNH_data = zip(NB_poverty_WNH_BABS.astype('float64').multiply(100),
                                      NB_poverty_WNH_HS.astype('float64').multiply(100))

            FB_poverty_POC = pd.DataFrame(FB_poverty_POC_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])
            NB_poverty_WNH = pd.DataFrame(NB_poverty_WNH_data,
                                          index=names,
                                          columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_poverty_POC, NB_poverty_WNH],
                                   ["Puerto Rican people of color", "Puerto Rican white"],
                                   title='Poverty rate (%) of Puerto Rican people of color and Puerto Rican white - ' + str(
                                       year), save_name=str(year) + '/img14',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_poverty = pd.read_csv('data/PR_HISP_DATA/PR_Hispanic/step_2/Poverty.csv')

            FB_M_BABS = FB_poverty[146:]['BABS_Poverty_M'].replace('#DIV/0!', np.nan)
            FB_M_HS = FB_poverty[146:]['HS_Poverty_M'].replace('#DIV/0!', np.nan)

            FB_F_BABS = FB_poverty[146:]['BABS_Poverty_F'].replace('#DIV/0!', np.nan)
            FB_F_HS = FB_poverty[146:]['HS_Poverty_F'].replace('#DIV/0!', np.nan)

            FB_M_data = zip(FB_M_BABS.astype('float64').multiply(100), FB_M_HS.astype('float64').multiply(100))
            FB_F_data = zip(FB_F_BABS.astype('float64').multiply(100), FB_F_HS.astype('float64').multiply(100))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])
            FB_F = pd.DataFrame(FB_F_data,
                                index=names,
                                columns=["College degree or better", "High School diploma / some college"])

            plot_clustered_stacked([FB_M, FB_F], ["Male", "Female"],
                                   title='Poverty rate (%) of Puerto Rican Hispanic Male and Female  - ' + str(
                                       year), save_name=str(year) + '/img15',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

        #'''

        #'''

        # Eng prof
        print 'Eng prof '
        # plot 1
        if year in [2014,2016,2017]:
            FB_edu_all = pd.read_csv('data/eng_prof/' + str(year) + '/FB_All/step_2/FB_ALL_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/' + str(year) + '/FB_POC/step_2/FB_POC_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'),FB_edu_POC_BABS.astype('float64'))
            #FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                          index=names,
                                          columns=["FB ALL", "FB POC" ])


            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among Foreign born residents across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img13',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_edu = pd.read_csv('data/eng_prof/' + str(year) + '/FB_All/step_2/FB_ALL_eng_prof_percent.csv')

            FB_M = FB_edu[146:]['ENG_prof_m'].replace('#DIV/0!', np.nan)

            FB_F = FB_edu[146:]['ENG_prof_f'].replace('#DIV/0!', np.nan)

            FB_M_data = zip(FB_M.astype('float64'),FB_F.astype('float64'))
            #FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Male","Female"])


            plot_clustered_stacked([FB_M],
                                   title='English Proficiency (%) among Foreign born Men and Women of New York state - ' + str(
                                       year),save_name=str(year)+'/img14',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'HISP_2016':
            # plot 1
            FB_edu_all = pd.read_csv('data/eng_prof/' + str(year) + '/FB_Hispanic/step_2/FB_Hispanic_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/' + str(year) + '/NB_Hispanic/step_2/NB_Hispanic_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'), FB_edu_POC_BABS.astype('float64'))
            # FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                      index=names,
                                      columns=["FB Hispanic", "NB Hispanic"])

            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among Foreign born residents across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img4',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
            # plot 2
            FB_edu_all = pd.read_csv('data/eng_prof/' + str(year) + '/FB_Hispanic_POC/step_2/FB_Hispanic_POC_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/' + str(year) + '/FB_WNH/step_2/FB_WNH_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'), FB_edu_POC_BABS.astype('float64'))
            # FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                      index=names,
                                      columns=["FB Hispanic POC", "FB WNH"])

            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among Foreign born residents across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img5',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_edu = pd.read_csv('data/eng_prof/' + str(year) + '/FB_Hispanic/step_2/FB_Hispanic_eng_prof_percent.csv')

            FB_M = FB_edu[146:]['ENG_prof_m'].replace('#DIV/0!', np.nan)

            FB_F = FB_edu[146:]['ENG_prof_f'].replace('#DIV/0!', np.nan)

            FB_M_data = zip(FB_M.astype('float64'), FB_F.astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Male", "Female"])

            plot_clustered_stacked([FB_M],
                                   title='English Proficiency (%) among Foreign born Men and Women of New York state - ' + str(
                                       year), save_name=str(year) + '/img6',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_2012_2016':
            # plot 1
            FB_edu_all = pd.read_csv('data/eng_prof/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/PR_2012_2016/NB_Mainland/step_2/NB_Mainland_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'), FB_edu_POC_BABS.astype('float64'))
            # FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                      index=names,
                                      columns=["PR Hispanic", "NB Mainland"])

            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among PR Hispanic and Native born mainland people across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img4',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
            # plot 2
            FB_edu_all = pd.read_csv('data/eng_prof/HISP_2016/FB_Hispanic/step_2/FB_Hispanic_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'), FB_edu_POC_BABS.astype('float64'))
            # FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                      index=names,
                                      columns=["FB Hispanic ", "PR Hispanic"])

            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among Foreign born Hispanic and PR Hispanic residents across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img5',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_edu_all = pd.read_csv('data/eng_prof/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/PR_2012_2016/NB_Mainland_PR/step_2/NB_Mainland_PR_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'), FB_edu_POC_BABS.astype('float64'))
            # FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                      index=names,
                                      columns=["PR Hispanic", "NB Mainland"])

            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among Puerto Rican Hispanic and Native born  Mainland residents across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img6',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_new':
            # plot 1
            FB_edu_all = pd.read_csv('data/eng_prof/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/PR_2012_2016/NB_Mainland_PR/step_2/NB_Mainland_PR_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'), FB_edu_POC_BABS.astype('float64'))
            # FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                      index=names,
                                      columns=["Puerto Rican Hispanic", "NB Mainland Puerto Rican"])

            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among Puerto Rican born residents across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img4',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
            # plot 2
            FB_edu_all = pd.read_csv('data/eng_prof/PR_new/PR_POC/step_2/PR_POC_eng_prof_percent.csv')
            FB_edu_POC = pd.read_csv('data/eng_prof/PR_new/PR_White/step_2/PR_White_eng_prof_percent.csv')

            FB_edu_all_BABS = FB_edu_all[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_POC_BABS = FB_edu_POC[146:]['ENG_prof_mf_t'].replace('#DIV/0!', np.nan)

            FB_edu_all_data = zip(FB_edu_all_BABS.astype('float64'), FB_edu_POC_BABS.astype('float64'))
            # FB_edu_POC_data = zip(FB_edu_POC_BABS.astype('float64'))

            FB_edu_all = pd.DataFrame(FB_edu_all_data,
                                      index=names,
                                      columns=["Puerto Rican POC", "Puerto Rican White"])

            plot_clustered_stacked([FB_edu_all],
                                   title='English Proficiency (%) among Puerto Rican born residents across regions of New York state - ' + str(
                                       year),
                                   save_name=str(year) + '/img5',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 3
            FB_edu = pd.read_csv('data/eng_prof/PR_2012_2016/PR_Hispanic/step_2/PR_Hispanic_eng_prof_percent.csv')

            FB_M = FB_edu[146:]['ENG_prof_m'].replace('#DIV/0!', np.nan)

            FB_F = FB_edu[146:]['ENG_prof_f'].replace('#DIV/0!', np.nan)

            FB_M_data = zip(FB_M.astype('float64'), FB_F.astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Male", "Female"])

            plot_clustered_stacked([FB_M],
                                   title='English Proficiency (%) among Puerto Rican born Men and Women of New York state - ' + str(
                                       year), save_name=str(year) + '/img6',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

        #'''

        #'''

        # Demographics Pic
        print 'Demographics Pie'
        if year in [2014,2016,2017]:
            # plot 1
            data = pd.read_csv('data/Demographics/' + str(year) + '/FB_ALL_total_population_percent.csv')
            pie_plot(names=names,sizes=data['Percentage_mf_t'],save_name=str(year)+'/img15', title='Foreign born population across New York state region - '+ str(year))
        elif year == 'HISP_2016':
            # plot 1
            FB_edu = pd.read_csv('data/Demographics/' + str(year) + '/FB_Hispanic_total_population_percent.csv')

            FB_M_data = zip(FB_edu['Percentage_mf_t'].astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Regions"])

            plot_clustered_stacked([FB_M],
                                   title='Foreign born Hispanic population (%) across New York state region from FB people - ' + str(
                                       year), save_name=str(year) + '/img16',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_edu = pd.read_csv('data/Demographics/' + str(year) + '/FB_Hispanic_POC_total_population_percent.csv')

            FB_M_data = zip(FB_edu['Percentage_mf_t'].astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Regions"])

            plot_clustered_stacked([FB_M],
                                   title='Foreign born Hispanic People of Color population (%) across New York state region from FB people- ' + str(
                                       year), save_name=str(year) + '/img17',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_2012_2016':
            # plot 1
            FB_edu = pd.read_csv('data/Demographics/' + str(year) + '/PR_Hispanic_total_population_percent.csv')

            FB_M_data = zip(FB_edu['Percentage_mf_t'].astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Regions"])

            plot_clustered_stacked([FB_M],
                                   title='Puerto Rico Hispanic population (%) across New York state region from Native born people - ' + str(
                                       year), save_name=str(year) + '/img16',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_edu = pd.read_csv('data/Demographics/' + str(year) + '/NB_Mainland_PR_total_population_percent.csv')

            FB_M_data = zip(FB_edu['Percentage_mf_t'].astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Regions"])

            plot_clustered_stacked([FB_M],
                                   title='Native born mainland Puerto Rican People population (%) across New York state region from Native born people- ' + str(
                                       year), save_name=str(year) + '/img17',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])
        elif year == 'PR_new':
            # plot 1
            FB_edu = pd.read_csv('data/Demographics/PR_new/PR_White_total_population_percent.csv')

            FB_M_data = zip(FB_edu['Percentage_mf_t'].astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Regions"])

            plot_clustered_stacked([FB_M],
                                   title='Puerto Rican White population (%) across New York state region from FB people - ' + str(
                                       year), save_name=str(year) + '/img16',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            # plot 2
            FB_edu = pd.read_csv('data/Demographics/PR_new/PR_POC_total_population_percent.csv')

            FB_M_data = zip(FB_edu['Percentage_mf_t'].astype('float64'))
            # FB_F_data = zip(FB_F.astype('float64'))

            FB_M = pd.DataFrame(FB_M_data,
                                index=names,
                                columns=["Regions"])

            plot_clustered_stacked([FB_M],
                                   title='Puerto Rican Hispanic People of Color population (%) across New York state region from FB people- ' + str(
                                       year), save_name=str(year) + '/img17',
                                   legend1_loc=[0.3, 0.85], legend2_loc=[0.6, 0.85])

            #'''