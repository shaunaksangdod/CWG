import json
import csv
import pandas

def csv_to_json_HISP_PR():
    # Open the CSV

    # For Group 1 and 2: Effect of Nativity

    result_group1=pandas.DataFrame()
    data_disparity_BABS=pandas.read_csv('data_PR_HISP/2016/Disparities/FB_Hispanic_NB_Hispanic_disparity.csv',usecols=['puma','Unemployment_BABS','FT_Work_BABS','Poverty_BABS','Working_Poor_BABS','Income_level_BABS','Rent_Burden_BABS','Home_ownership_BABS'])
    data_scores_BABS=pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_NB_HISP.csv',usecols=['Unemployment_BABS_score','FT_Work_BABS_score','Poverty_BABS_score','Working_Poor_BABS_score','Income_level_BABS_score','Rent_Burden_BABS_score','Home_ownership_BABS_score','Overall_BABS_score'])
    data_grades_BABS=pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_NB_HISP.csv',usecols=['Unemployment_BABS_grade','FT_Work_BABS_grade','Poverty_BABS_grade','Working_Poor_BABS_grade','Income_level_BABS_grade','Rent_Burden_BABS_grade','Home_ownership_BABS_grade','Overall_BABS_grade'])
    result_group1=pandas.concat([data_disparity_BABS,data_scores_BABS,data_grades_BABS],axis=1)
    result_group1.drop([0])
    result_group1 = result_group1[result_group1['puma'] != "Total Geo"]
    result_group1.to_json(path_or_buf='data_PR_HISP/json_2016_HISP/parsed_group1.json',orient='records')

    result_group2 = pandas.DataFrame()
    data_disparity_HS = pandas.read_csv('data_PR_HISP/2016/Disparities/FB_Hispanic_NB_Hispanic_disparity.csv',
                                          usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
                                                   'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
                                                   'Home_ownership_HS'])
    data_scores_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_NB_HISP.csv',
                                       usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
                                                'Working_Poor_HS_score', 'Income_level_HS_score',
                                                'Rent_Burden_HS_score', 'Home_ownership_HS_score','Overall_HS_score'])
    data_grades_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_NB_HISP.csv',
                                       usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
                                                'Working_Poor_HS_grade', 'Income_level_HS_grade',
                                                'Rent_Burden_HS_grade', 'Home_ownership_HS_grade','Overall_HS_grade'])
    result_group2 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    result_group2.drop(0)
    result_group2 = result_group2[result_group2['puma'] != "Total Geo"]
    result_group2.to_json(path_or_buf='data_PR_HISP/json_2016_HISP/parsed_group2.json', orient='records')

    #For group1

    children={}
    with open('data_PR_HISP/json_2016_HISP/parsed_group1.json') as input_file:
        raw_data = json.load(input_file)
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']:{
                  "Overall": {
                  "Score": str(item["Overall_BABS_score"]),
                  "Grade": item["Overall_BABS_grade"]
                },
                "Full Time": {
                  "Ratio": str(item["FT_Work_BABS"]),
                  "Score": str(item["FT_Work_BABS_score"]),
                  "Grade": item["FT_Work_BABS_grade"]
                },
                "Poverty": {
                  "Ratio": str(item["Poverty_BABS"]),
                  "Score": str(item["Poverty_BABS_score"]),
                  "Grade": item["Poverty_BABS_grade"]
                },
                "Working Poor": {
                  "Ratio": str(item["Working_Poor_BABS"]),
                  "Score": str(item["Working_Poor_BABS_score"]),
                  "Grade": item["Working_Poor_BABS_grade"]
                },
                "Homeownership": {
                  "Ratio": str(item["Home_ownership_BABS"]),
                  "Score": str(item["Home_ownership_BABS_score"]),
                  "Grade": item["Home_ownership_BABS_grade"]
                },
                "Rent Burden": {
                  "Ratio": str(item["Rent_Burden_BABS"]),
                  "Score": str(item["Rent_Burden_BABS_score"]),
                  "Grade": item["Rent_Burden_BABS_grade"]
                },
                "Unemployment": {
                  "Ratio": str(item["Unemployment_BABS"]),
                  "Score": str(item["Unemployment_BABS_score"]),
                  "Grade": item["Unemployment_BABS_grade"]
                },
                "Income": {
                  "Ratio": str(item["Income_level_BABS"]),
                  "Score": str(item["Income_level_BABS_score"]),
                  "Grade": item["Income_level_BABS_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    #container['name'] = 'name'
    #container= children

    with open('data_PR_HISP/json_2016_HISP/group1.json','w') as out_file:
        json.dump(children,out_file)

    # For group2
    with open('data_PR_HISP/json_2016_HISP/parsed_group2.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HS_score"]),
                    "Grade": item["Overall_HS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_HS"]),
                    "Score": str(item["FT_Work_HS_score"]),
                    "Grade": item["FT_Work_HS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_HS"]),
                    "Score": str(item["Poverty_HS_score"]),
                    "Grade": item["Poverty_HS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_HS"]),
                    "Score": str(item["Working_Poor_HS_score"]),
                    "Grade": item["Working_Poor_HS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_HS"]),
                    "Score": str(item["Home_ownership_HS_score"]),
                    "Grade": item["Home_ownership_HS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_HS"]),
                    "Score": str(item["Rent_Burden_HS_score"]),
                    "Grade": item["Rent_Burden_HS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_HS"]),
                    "Score": str(item["Unemployment_HS_score"]),
                    "Grade": item["Unemployment_HS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_HS"]),
                    "Score": str(item["Income_level_HS_score"]),
                    "Grade": item["Income_level_HS_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    # container['name'] = 'name'
    #container = children

    with open('data_PR_HISP/json_2016_HISP/group2.json', 'w') as out_file:
        json.dump(children, out_file)

    #For Group 3 and Group 4: Effect of Race

    result_group3 = pandas.DataFrame()
    data_disparity_BABS = pandas.read_csv('data_PR_HISP/2016/Disparities/FB_Hispanic_POC_FB_White_Hispanic_disparity.csv',
                                          usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS', 'Poverty_BABS',
                                                   'Working_Poor_BABS', 'Income_level_BABS','Rent_Burden_BABS',
                                                   'Home_ownership_BABS'])
    data_scores_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_POC_FB_WH.csv',
                                       usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
                                                'Working_Poor_BABS_score', 'Income_level_BABS_score',
                                                'Rent_Burden_BABS_score', 'Home_ownership_BABS_score','Overall_BABS_score'])
    data_grades_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_POC_FB_WH.csv',
                                       usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade', 'Poverty_BABS_grade',
                                                'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
                                                'Rent_Burden_BABS_grade', 'Home_ownership_BABS_grade','Overall_BABS_grade'])
    result_group3 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    result_group3.drop(0)
    result_group3 = result_group3[result_group3['puma'] != "Total Geo"]
    result_group3.to_json(path_or_buf='data_PR_HISP/json_2016_HISP/parsed_group3.json', orient='records')

    result_group4 = pandas.DataFrame()
    data_disparity_HS = pandas.read_csv('data_PR_HISP/2016/Disparities/FB_Hispanic_POC_FB_White_Hispanic_disparity.csv',
                                        usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
                                                 'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
                                                 'Home_ownership_HS'])
    data_scores_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_POC_FB_WH.csv',
                                     usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
                                              'Working_Poor_HS_score', 'Income_level_HS_score',
                                              'Rent_Burden_HS_score', 'Home_ownership_HS_score','Overall_HS_score'])
    data_grades_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_POC_FB_WH.csv',
                                     usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
                                              'Working_Poor_HS_grade', 'Income_level_HS_grade',
                                              'Rent_Burden_HS_grade', 'Home_ownership_HS_grade','Overall_HS_grade'])
    result_group4 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    result_group4.drop(0)
    result_group4 = result_group4[result_group4['puma'] != "Total Geo"]
    result_group4.to_json(path_or_buf='data_PR_HISP/json_2016_HISP/parsed_group4.json', orient='records')

    # For group3
    with open('data_PR_HISP/json_2016_HISP/parsed_group3.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_BABS_score"]),
                    "Grade": item["Overall_BABS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_BABS"]),
                    "Score": str(item["FT_Work_BABS_score"]),
                    "Grade": item["FT_Work_BABS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_BABS"]),
                    "Score": str(item["Poverty_BABS_score"]),
                    "Grade": item["Poverty_BABS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_BABS"]),
                    "Score": str(item["Working_Poor_BABS_score"]),
                    "Grade": item["Working_Poor_BABS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_BABS"]),
                    "Score": str(item["Home_ownership_BABS_score"]),
                    "Grade": item["Home_ownership_BABS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_BABS"]),
                    "Score": str(item["Rent_Burden_BABS_score"]),
                    "Grade": item["Rent_Burden_BABS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_BABS"]),
                    "Score": str(item["Unemployment_BABS_score"]),
                    "Grade": item["Unemployment_BABS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_BABS"]),
                    "Score": str(item["Income_level_BABS_score"]),
                    "Grade": item["Income_level_BABS_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    # container['name'] = 'name'
    #container = children

    with open('data_PR_HISP/json_2016_HISP/group3.json', 'w') as out_file:
        json.dump(children, out_file)



    # For group4
    with open('data_PR_HISP/json_2016_HISP/parsed_group4.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HS_score"]),
                    "Grade": item["Overall_HS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_HS"]),
                    "Score": str(item["FT_Work_HS_score"]),
                    "Grade": item["FT_Work_HS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_HS"]),
                    "Score": str(item["Poverty_HS_score"]),
                    "Grade": item["Poverty_HS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_HS"]),
                    "Score": str(item["Working_Poor_HS_score"]),
                    "Grade": item["Working_Poor_HS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_HS"]),
                    "Score": str(item["Home_ownership_HS_score"]),
                    "Grade": item["Home_ownership_HS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_HS"]),
                    "Score": str(item["Rent_Burden_HS_score"]),
                    "Grade": item["Rent_Burden_HS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_HS"]),
                    "Score": str(item["Unemployment_HS_score"]),
                    "Grade": item["Unemployment_HS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_HS"]),
                    "Score": str(item["Income_level_HS_score"]),
                    "Grade": item["Income_level_HS_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    # container['name'] = 'name'
    #container = children

    with open('data_PR_HISP/json_2016_HISP/group4.json', 'w') as out_file:
        json.dump(children, out_file)


    # For Group 9 and Group 10: Effect of Gender

    result_group5 = pandas.DataFrame()
    data_disparity_BABS = pandas.read_csv('data_PR_HISP/2016/Disparities/FB_Hispanic_F_FB_Hispanic_M_disparity.csv',
                                          usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS', 'Poverty_BABS',
                                                   'Working_Poor_BABS', 'Income_level_BABS', 'Rent_Burden_BABS',
                                                   'Home_ownership_BABS'])
    data_scores_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_Hispanic_F_FB_Hispanic_M.csv',
                                       usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score',
                                                'Poverty_BABS_score',
                                                'Working_Poor_BABS_score', 'Income_level_BABS_score',
                                                'Rent_Burden_BABS_score', 'Home_ownership_BABS_score',
                                                'Overall_BABS_score'])
    data_grades_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_Hispanic_F_FB_Hispanic_M.csv',
                                       usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade',
                                                'Poverty_BABS_grade',
                                                'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
                                                'Rent_Burden_BABS_grade', 'Home_ownership_BABS_grade',
                                                'Overall_BABS_grade'])
    result_group5 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    result_group5.drop(0)
    result_group5 = result_group5[result_group5['puma'] != "Total Geo"]
    result_group5.to_json(path_or_buf='data_PR_HISP/json_2016_HISP/parsed_group9.json', orient='records')

    result_group6 = pandas.DataFrame()
    data_disparity_HS = pandas.read_csv('data_PR_HISP/2016/Disparities/FB_Hispanic_F_FB_Hispanic_M_disparity.csv',
                                        usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
                                                 'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
                                                 'Home_ownership_HS'])
    data_scores_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_Hispanic_F_FB_Hispanic_M.csv',
                                     usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
                                              'Working_Poor_HS_score', 'Income_level_HS_score',
                                              'Rent_Burden_HS_score', 'Home_ownership_HS_score',
                                              'Overall_HS_score'])
    data_grades_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_Hispanic_F_FB_Hispanic_M.csv',
                                     usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
                                              'Working_Poor_HS_grade', 'Income_level_HS_grade',
                                              'Rent_Burden_HS_grade', 'Home_ownership_HS_grade',
                                              'Overall_HS_grade'])
    result_group6 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    result_group6.drop(0)
    result_group6 = result_group6[result_group6['puma'] != "Total Geo"]
    result_group6.to_json(path_or_buf='data_PR_HISP/json_2016_HISP/parsed_group10.json', orient='records')

    # For group5
    with open('data_PR_HISP/json_2016_HISP/parsed_group9.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_BABS_score"]),
                    "Grade": item["Overall_BABS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_BABS"]),
                    "Score": str(item["FT_Work_BABS_score"]),
                    "Grade": item["FT_Work_BABS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_BABS"]),
                    "Score": str(item["Poverty_BABS_score"]),
                    "Grade": item["Poverty_BABS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_BABS"]),
                    "Score": str(item["Working_Poor_BABS_score"]),
                    "Grade": item["Working_Poor_BABS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_BABS"]),
                    "Score": str(item["Home_ownership_BABS_score"]),
                    "Grade": item["Home_ownership_BABS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_BABS"]),
                    "Score": str(item["Rent_Burden_BABS_score"]),
                    "Grade": item["Rent_Burden_BABS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_BABS"]),
                    "Score": str(item["Unemployment_BABS_score"]),
                    "Grade": item["Unemployment_BABS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_BABS"]),
                    "Score": str(item["Income_level_BABS_score"]),
                    "Grade": item["Income_level_BABS_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    # container['name'] = 'name'
    #container = children

    with open('data_PR_HISP/json_2016_HISP/group9.json', 'w') as out_file:
        json.dump(children, out_file)


    # For group6
    with open('data_PR_HISP/json_2016_HISP/parsed_group10.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HS_score"]),
                    "Grade": item["Overall_HS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_HS"]),
                    "Score": str(item["FT_Work_HS_score"]),
                    "Grade": item["FT_Work_HS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_HS"]),
                    "Score": str(item["Poverty_HS_score"]),
                    "Grade": item["Poverty_HS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_HS"]),
                    "Score": str(item["Working_Poor_HS_score"]),
                    "Grade": item["Working_Poor_HS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_HS"]),
                    "Score": str(item["Home_ownership_HS_score"]),
                    "Grade": item["Home_ownership_HS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_HS"]),
                    "Score": str(item["Rent_Burden_HS_score"]),
                    "Grade": item["Rent_Burden_HS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_HS"]),
                    "Score": str(item["Unemployment_HS_score"]),
                    "Grade": item["Unemployment_HS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_HS"]),
                    "Score": str(item["Income_level_HS_score"]),
                    "Grade": item["Income_level_HS_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    # container['name'] = 'name'
    #container = children

    with open('data_PR_HISP/json_2016_HISP/group10.json', 'w') as out_file:
        json.dump(children, out_file)

    """ # For Puerto Rican Data

    # For Group 1 and 2: Effect of Ethinicty

    result_group1 = pandas.DataFrame()
    data_disparity_BABS = pandas.read_csv('data_PR_HISP/2016/Disparities/PR_Hispanic_NB_Mainland_disparity.csv',
                                          usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS', 'Poverty_BABS',
                                                   'Working_Poor_BABS', 'Income_level_BABS', 'Rent_Burden_BABS',
                                                   'Home_ownership_BABS'])
    data_scores_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_Mainland.csv',
                                       usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score',
                                                'Poverty_BABS_score', 'Working_Poor_BABS_score',
                                                'Income_level_BABS_score', 'Rent_Burden_BABS_score',
                                                'Home_ownership_BABS_score', 'Overall_BABS_score'])
    data_grades_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_Mainland.csv',
                                       usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade',
                                                'Poverty_BABS_grade', 'Working_Poor_BABS_grade',
                                                'Income_level_BABS_grade', 'Rent_Burden_BABS_grade',
                                                'Home_ownership_BABS_grade', 'Overall_BABS_grade'])
    result_group1 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    result_group1.drop([0])
    result_group1 = result_group1[result_group1['puma'] != "Total Geo"]
    result_group1.to_json(path_or_buf='data_PR_HISP/json_2016_PR/parsed_group1.json', orient='records')

    result_group2 = pandas.DataFrame()
    data_disparity_HS = pandas.read_csv('data_PR_HISP/2016/Disparities/PR_Hispanic_NB_Mainland_disparity.csv',
                                        usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
                                                 'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
                                                 'Home_ownership_HS'])
    data_scores_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_Mainland.csv',
                                     usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
                                              'Working_Poor_HS_score', 'Income_level_HS_score',
                                              'Rent_Burden_HS_score', 'Home_ownership_HS_score',
                                              'Overall_HS_score'])
    data_grades_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_Mainland.csv',
                                     usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
                                              'Working_Poor_HS_grade', 'Income_level_HS_grade',
                                              'Rent_Burden_HS_grade', 'Home_ownership_HS_grade',
                                              'Overall_HS_grade'])
    result_group2 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    result_group2.drop(0)
    result_group2 = result_group2[result_group2['puma'] != "Total Geo"]
    result_group2.to_json(path_or_buf='data_PR_HISP/json_2016_PR/parsed_group2.json', orient='records')

    # For group1

    children = {}
    with open('data_PR_HISP/json_2016_PR/parsed_group1.json') as input_file:
        raw_data = json.load(input_file)
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_BABS_score"]),
                    "Grade": item["Overall_BABS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_BABS"]),
                    "Score": str(item["FT_Work_BABS_score"]),
                    "Grade": item["FT_Work_BABS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_BABS"]),
                    "Score": str(item["Poverty_BABS_score"]),
                    "Grade": item["Poverty_BABS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_BABS"]),
                    "Score": str(item["Working_Poor_BABS_score"]),
                    "Grade": item["Working_Poor_BABS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_BABS"]),
                    "Score": str(item["Home_ownership_BABS_score"]),
                    "Grade": item["Home_ownership_BABS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_BABS"]),
                    "Score": str(item["Rent_Burden_BABS_score"]),
                    "Grade": item["Rent_Burden_BABS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_BABS"]),
                    "Score": str(item["Unemployment_BABS_score"]),
                    "Grade": item["Unemployment_BABS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_BABS"]),
                    "Score": str(item["Income_level_BABS_score"]),
                    "Grade": item["Income_level_BABS_grade"]
                },
                "Naturalization": {}
            }

        })
    # container = {}
    # container['name'] = 'name'
    # container= children

    with open('data_PR_HISP/json_2016_PR/group1.json', 'w') as out_file:
        json.dump(children, out_file)

    # For group2
    with open('data_PR_HISP/json_2016_PR/parsed_group2.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HS_score"]),
                    "Grade": item["Overall_HS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_HS"]),
                    "Score": str(item["FT_Work_HS_score"]),
                    "Grade": item["FT_Work_HS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_HS"]),
                    "Score": str(item["Poverty_HS_score"]),
                    "Grade": item["Poverty_HS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_HS"]),
                    "Score": str(item["Working_Poor_HS_score"]),
                    "Grade": item["Working_Poor_HS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_HS"]),
                    "Score": str(item["Home_ownership_HS_score"]),
                    "Grade": item["Home_ownership_HS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_HS"]),
                    "Score": str(item["Rent_Burden_HS_score"]),
                    "Grade": item["Rent_Burden_HS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_HS"]),
                    "Score": str(item["Unemployment_HS_score"]),
                    "Grade": item["Unemployment_HS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_HS"]),
                    "Score": str(item["Income_level_HS_score"]),
                    "Grade": item["Income_level_HS_grade"]
                },
                "Naturalization": {}
            }

        })
    # container = {}
    # container['name'] = 'name'
    # container = children

    with open('data_PR_HISP/json_2016_PR/group2.json', 'w') as out_file:
        json.dump(children, out_file)

    # For Group 3 and Group 4: Effect of Race

    result_group3 = pandas.DataFrame()
    data_disparity_BABS = pandas.read_csv(
        'data_PR_HISP/2016/Disparities/FB_Hispanic_PR_Hispanic_disparity.csv',
        usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS', 'Poverty_BABS',
                 'Working_Poor_BABS', 'Income_level_BABS', 'Rent_Burden_BABS',
                 'Home_ownership_BABS'])
    data_scores_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_PR_HISP.csv',
                                       usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score',
                                                'Poverty_BABS_score',
                                                'Working_Poor_BABS_score', 'Income_level_BABS_score',
                                                'Rent_Burden_BABS_score', 'Home_ownership_BABS_score',
                                                'Overall_BABS_score'])
    data_grades_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_PR_HISP.csv',
                                       usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade',
                                                'Poverty_BABS_grade',
                                                'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
                                                'Rent_Burden_BABS_grade', 'Home_ownership_BABS_grade',
                                                'Overall_BABS_grade'])
    result_group3 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    result_group3.drop(0)
    result_group3 = result_group3[result_group3['puma'] != "Total Geo"]
    result_group3.to_json(path_or_buf='data_PR_HISP/json_2016_PR/parsed_group3.json', orient='records')

    result_group4 = pandas.DataFrame()
    data_disparity_HS = pandas.read_csv(
        'data_PR_HISP/2016/Disparities/FB_Hispanic_PR_Hispanic_disparity.csv',
        usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
                 'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
                 'Home_ownership_HS'])
    data_scores_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_NB_HISP.csv',
                                     usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
                                              'Working_Poor_HS_score', 'Income_level_HS_score',
                                              'Rent_Burden_HS_score', 'Home_ownership_HS_score',
                                              'Overall_HS_score'])
    data_grades_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/FB_HISP_NB_HISP.csv',
                                     usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
                                              'Working_Poor_HS_grade', 'Income_level_HS_grade',
                                              'Rent_Burden_HS_grade', 'Home_ownership_HS_grade',
                                              'Overall_HS_grade'])
    result_group4 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    result_group4.drop(0)
    result_group4 = result_group4[result_group4['puma'] != "Total Geo"]
    result_group4.to_json(path_or_buf='data_PR_HISP/json_2016_PR/parsed_group4.json', orient='records')

    # For group3
    with open('data_PR_HISP/json_2016_PR/parsed_group3.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_BABS_score"]),
                    "Grade": item["Overall_BABS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_BABS"]),
                    "Score": str(item["FT_Work_BABS_score"]),
                    "Grade": item["FT_Work_BABS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_BABS"]),
                    "Score": str(item["Poverty_BABS_score"]),
                    "Grade": item["Poverty_BABS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_BABS"]),
                    "Score": str(item["Working_Poor_BABS_score"]),
                    "Grade": item["Working_Poor_BABS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_BABS"]),
                    "Score": str(item["Home_ownership_BABS_score"]),
                    "Grade": item["Home_ownership_BABS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_BABS"]),
                    "Score": str(item["Rent_Burden_BABS_score"]),
                    "Grade": item["Rent_Burden_BABS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_BABS"]),
                    "Score": str(item["Unemployment_BABS_score"]),
                    "Grade": item["Unemployment_BABS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_BABS"]),
                    "Score": str(item["Income_level_BABS_score"]),
                    "Grade": item["Income_level_BABS_grade"]
                },
                "Naturalization": {}
            }

        })
    # container = {}
    # container['name'] = 'name'
    # container = children

    with open('data_PR_HISP/json_2016_PR/group3.json', 'w') as out_file:
        json.dump(children, out_file)

    # For group4
    with open('data_PR_HISP/json_2016_PR/parsed_group4.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HS_score"]),
                    "Grade": item["Overall_HS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_HS"]),
                    "Score": str(item["FT_Work_HS_score"]),
                    "Grade": item["FT_Work_HS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_HS"]),
                    "Score": str(item["Poverty_HS_score"]),
                    "Grade": item["Poverty_HS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_HS"]),
                    "Score": str(item["Working_Poor_HS_score"]),
                    "Grade": item["Working_Poor_HS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_HS"]),
                    "Score": str(item["Home_ownership_HS_score"]),
                    "Grade": item["Home_ownership_HS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_HS"]),
                    "Score": str(item["Rent_Burden_HS_score"]),
                    "Grade": item["Rent_Burden_HS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_HS"]),
                    "Score": str(item["Unemployment_HS_score"]),
                    "Grade": item["Unemployment_HS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_HS"]),
                    "Score": str(item["Income_level_HS_score"]),
                    "Grade": item["Income_level_HS_grade"]
                },
                "Naturalization": {}
            }

        })
    # container = {}
    # container['name'] = 'name'
    # container = children

    with open('data_PR_HISP/json_2016_PR/group4.json', 'w') as out_file:
        json.dump(children, out_file)

    # For Group 9 and Group 10: Effect of Place of Birth

    result_group5 = pandas.DataFrame()
    data_disparity_BABS = pandas.read_csv('data_PR_HISP/2016/Disparities/PR_Hispanic_NB_Mainland_PR_disparity.csv',
                                          usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS', 'Poverty_BABS',
                                                   'Working_Poor_BABS', 'Income_level_BABS', 'Rent_Burden_BABS',
                                                   'Home_ownership_BABS'])
    data_scores_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_mainland_PR.csv',
                                       usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score',
                                                'Poverty_BABS_score',
                                                'Working_Poor_BABS_score', 'Income_level_BABS_score',
                                                'Rent_Burden_BABS_score', 'Home_ownership_BABS_score',
                                                'Overall_BABS_score'])
    data_grades_BABS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_mainland_PR.csv',
                                       usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade',
                                                'Poverty_BABS_grade',
                                                'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
                                                'Rent_Burden_BABS_grade', 'Home_ownership_BABS_grade',
                                                'Overall_BABS_grade'])
    result_group5 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    result_group5.drop(0)
    result_group5 = result_group5[result_group5['puma'] != "Total Geo"]
    result_group5.to_json(path_or_buf='data_PR_HISP/json_2016_PR/parsed_group9.json', orient='records')

    result_group6 = pandas.DataFrame()
    data_disparity_HS = pandas.read_csv('data_PR_HISP/2016/Disparities/PR_Hispanic_NB_Mainland_PR_disparity.csv',
                                        usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
                                                 'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
                                                 'Home_ownership_HS'])
    data_scores_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_mainland_PR.csv',
                                     usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
                                              'Working_Poor_HS_score', 'Income_level_HS_score',
                                              'Rent_Burden_HS_score', 'Home_ownership_HS_score',
                                              'Overall_HS_score'])
    data_grades_HS = pandas.read_csv('data_PR_HISP/2016/Score_Grades/PR_Hispanic_NB_mainland_PR.csv',
                                     usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
                                              'Working_Poor_HS_grade', 'Income_level_HS_grade',
                                              'Rent_Burden_HS_grade', 'Home_ownership_HS_grade',
                                              'Overall_HS_grade'])
    result_group6 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    result_group6.drop(0)
    result_group6 = result_group6[result_group6['puma'] != "Total Geo"]
    result_group6.to_json(path_or_buf='data_PR_HISP/json_2016_PR/parsed_group10.json', orient='records')

    # For group5
    with open('data_PR_HISP/json_2016_PR/parsed_group9.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_BABS_score"]),
                    "Grade": item["Overall_BABS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_BABS"]),
                    "Score": str(item["FT_Work_BABS_score"]),
                    "Grade": item["FT_Work_BABS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_BABS"]),
                    "Score": str(item["Poverty_BABS_score"]),
                    "Grade": item["Poverty_BABS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_BABS"]),
                    "Score": str(item["Working_Poor_BABS_score"]),
                    "Grade": item["Working_Poor_BABS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_BABS"]),
                    "Score": str(item["Home_ownership_BABS_score"]),
                    "Grade": item["Home_ownership_BABS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_BABS"]),
                    "Score": str(item["Rent_Burden_BABS_score"]),
                    "Grade": item["Rent_Burden_BABS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_BABS"]),
                    "Score": str(item["Unemployment_BABS_score"]),
                    "Grade": item["Unemployment_BABS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_BABS"]),
                    "Score": str(item["Income_level_BABS_score"]),
                    "Grade": item["Income_level_BABS_grade"]
                },
                "Naturalization": {}
            }

        })
    # container = {}
    # container['name'] = 'name'
    # container = children

    with open('data_PR_HISP/json_2016_PR/group9.json', 'w') as out_file:
        json.dump(children, out_file)

    # For group6
    with open('data_PR_HISP/json_2016_PR/parsed_group10.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HS_score"]),
                    "Grade": item["Overall_HS_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["FT_Work_HS"]),
                    "Score": str(item["FT_Work_HS_score"]),
                    "Grade": item["FT_Work_HS_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["Poverty_HS"]),
                    "Score": str(item["Poverty_HS_score"]),
                    "Grade": item["Poverty_HS_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["Working_Poor_HS"]),
                    "Score": str(item["Working_Poor_HS_score"]),
                    "Grade": item["Working_Poor_HS_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["Home_ownership_HS"]),
                    "Score": str(item["Home_ownership_HS_score"]),
                    "Grade": item["Home_ownership_HS_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["Rent_Burden_HS"]),
                    "Score": str(item["Rent_Burden_HS_score"]),
                    "Grade": item["Rent_Burden_HS_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["Unemployment_HS"]),
                    "Score": str(item["Unemployment_HS_score"]),
                    "Grade": item["Unemployment_HS_grade"]
                },
                "Income": {
                    "Ratio": str(item["Income_level_HS"]),
                    "Score": str(item["Income_level_HS_score"]),
                    "Grade": item["Income_level_HS_grade"]
                },
                "Naturalization": {}
            }

        })
    # container = {}
    # container['name'] = 'name'
    # container = children

    with open('data_PR_HISP/json_2016_PR/group10.json', 'w') as out_file:
        json.dump(children, out_file) """


