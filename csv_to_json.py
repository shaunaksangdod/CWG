import json
import csv
import pandas


#TODO add group_11
def csv_to_json():
    # Open the CSV

    # #For group1 and group2
    # result_group1=pandas.DataFrame()
    # data_disparity_BABS=pandas.read_csv('data/2018/Disparities/NB_ALL_FB_ALL_Disparity.csv',usecols=['puma','Unemployment_BABS','FT_Work_BABS','Poverty_BABS','Working_Poor_BABS','Income_level_BABS','Rent_Burden_BABS','Home_Ownership_BABS'])
    # data_scores_BABS=pandas.read_csv('data/2018/Scores_Grades/NB_ALL_FB_ALL_Scores_Grades.csv',usecols=['Unemployment_BABS_score','FT_Work_BABS_score','Poverty_BABS_score','Working_Poor_BABS_score','Income_level_BABS_score','Rent_Burden_BABS_score','Home_Ownership_BABS_score','Overall_BABS_score'])
    # data_grades_BABS=pandas.read_csv('data/2018/Scores_Grades/NB_ALL_FB_ALL_Scores_Grades.csv',usecols=['Unemployment_BABS_grade','FT_Work_BABS_grade','Poverty_BABS_grade','Working_Poor_BABS_grade','Income_level_BABS_grade','Rent_Burden_BABS_grade','Home_Ownership_BABS_grade','Overall_BABS_grade'])
    # result_group1=pandas.concat([data_disparity_BABS,data_scores_BABS,data_grades_BABS],axis=1)
    # result_group1.drop([0])
    # result_group1 = result_group1[result_group1['puma'] != "Total Geo"]
    # result_group1.to_json(path_or_buf='data/json_2018/parsed_group1.json',orient='records')
    #
    # result_group2 = pandas.DataFrame()
    # data_disparity_HS = pandas.read_csv('data/2018/Disparities/NB_ALL_FB_ALL_Disparity.csv',
    #                                       usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
    #                                                'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
    #                                                'Home_Ownership_HS'])
    # data_scores_HS = pandas.read_csv('data/2018/Scores_Grades/NB_ALL_FB_ALL_Scores_Grades.csv',
    #                                    usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
    #                                             'Working_Poor_HS_score', 'Income_level_HS_score',
    #                                             'Rent_Burden_HS_score', 'Home_Ownership_HS_score','Overall_HS_score'])
    # data_grades_HS = pandas.read_csv('data/2018/Scores_Grades/NB_ALL_FB_ALL_Scores_Grades.csv',
    #                                    usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
    #                                             'Working_Poor_HS_grade', 'Income_level_HS_grade',
    #                                             'Rent_Burden_HS_grade', 'Home_Ownership_HS_grade','Overall_HS_grade'])
    # result_group2 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    # result_group2.drop(0)
    # result_group2 = result_group2[result_group2['puma'] != "Total Geo"]
    # result_group2.to_json(path_or_buf='data/json_2018/parsed_group2.json', orient='records')
    #
    # #For group1
    #
    # children={}
    # with open('data/json_2018/parsed_group1.json') as input_file:
    #     raw_data = json.load(input_file)
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']:{
    #               "Overall": {
    #               "Score": str(item["Overall_BABS_score"]),
    #               "Grade": item["Overall_BABS_grade"]
    #             },
    #             "Full Time": {
    #               "Ratio": str(item["FT_Work_BABS"]),
    #               "Score": str(item["FT_Work_BABS_score"]),
    #               "Grade": item["FT_Work_BABS_grade"]
    #             },
    #             "Poverty": {
    #               "Ratio": str(item["Poverty_BABS"]),
    #               "Score": str(item["Poverty_BABS_score"]),
    #               "Grade": item["Poverty_BABS_grade"]
    #             },
    #             "Working Poor": {
    #               "Ratio": str(item["Working_Poor_BABS"]),
    #               "Score": str(item["Working_Poor_BABS_score"]),
    #               "Grade": item["Working_Poor_BABS_grade"]
    #             },
    #             "Homeownership": {
    #               "Ratio": str(item["Home_Ownership_BABS"]),
    #               "Score": str(item["Home_Ownership_BABS_score"]),
    #               "Grade": item["Home_Ownership_BABS_grade"]
    #             },
    #             "Rent Burden": {
    #               "Ratio": str(item["Rent_Burden_BABS"]),
    #               "Score": str(item["Rent_Burden_BABS_score"]),
    #               "Grade": item["Rent_Burden_BABS_grade"]
    #             },
    #             "Unemployment": {
    #               "Ratio": str(item["Unemployment_BABS"]),
    #               "Score": str(item["Unemployment_BABS_score"]),
    #               "Grade": item["Unemployment_BABS_grade"]
    #             },
    #             "Income": {
    #               "Ratio": str(item["Income_level_BABS"]),
    #               "Score": str(item["Income_level_BABS_score"]),
    #               "Grade": item["Income_level_BABS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # #container['name'] = 'name'
    # #container= children
    #
    # with open('data/json_2018/group1.json','w') as out_file:
    #     json.dump(children,out_file)
    #
    # # For group2
    # with open('data/json_2018/parsed_group2.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_HS_score"]),
    #                 "Grade": item["Overall_HS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_HS"]),
    #                 "Score": str(item["FT_Work_HS_score"]),
    #                 "Grade": item["FT_Work_HS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_HS"]),
    #                 "Score": str(item["Poverty_HS_score"]),
    #                 "Grade": item["Poverty_HS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_HS"]),
    #                 "Score": str(item["Working_Poor_HS_score"]),
    #                 "Grade": item["Working_Poor_HS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_HS"]),
    #                 "Score": str(item["Home_Ownership_HS_score"]),
    #                 "Grade": item["Home_Ownership_HS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_HS"]),
    #                 "Score": str(item["Rent_Burden_HS_score"]),
    #                 "Grade": item["Rent_Burden_HS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_HS"]),
    #                 "Score": str(item["Unemployment_HS_score"]),
    #                 "Grade": item["Unemployment_HS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_HS"]),
    #                 "Score": str(item["Income_level_HS_score"]),
    #                 "Grade": item["Income_level_HS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group2.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    # #For Group 3 and Group 4
    #
    # result_group3 = pandas.DataFrame()
    # data_disparity_BABS = pandas.read_csv('data/2018/Disparities/NB_WNH_FB_POC_Disparity.csv',
    #                                       usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS', 'Poverty_BABS',
    #                                                'Working_Poor_BABS', 'Income_level_BABS','Rent_Burden_BABS',
    #                                                'Home_Ownership_BABS'])
    # data_scores_BABS = pandas.read_csv('data/2018/Scores_Grades/NB_WNH_FB_POC_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
    #                                             'Working_Poor_BABS_score', 'Income_level_BABS_score',
    #                                             'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score','Overall_BABS_score'])
    # data_grades_BABS = pandas.read_csv('data/2018/Scores_Grades/NB_WNH_FB_POC_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade', 'Poverty_BABS_grade',
    #                                             'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
    #                                             'Rent_Burden_BABS_grade', 'Home_Ownership_BABS_grade','Overall_BABS_grade'])
    # result_group3 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    # result_group3.drop(0)
    # result_group3 = result_group3[result_group3['puma'] != "Total Geo"]
    # result_group3.to_json(path_or_buf='data/json_2018/parsed_group3.json', orient='records')
    #
    # result_group4 = pandas.DataFrame()
    # data_disparity_HS = pandas.read_csv('data/2018/Disparities/NB_WNH_FB_POC_Disparity.csv',
    #                                     usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
    #                                              'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
    #                                              'Home_Ownership_HS'])
    # data_scores_HS = pandas.read_csv('data/2018/Scores_Grades/NB_WNH_FB_POC_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
    #                                           'Working_Poor_HS_score', 'Income_level_HS_score',
    #                                           'Rent_Burden_HS_score', 'Home_Ownership_HS_score','Overall_HS_score'])
    # data_grades_HS = pandas.read_csv('data/2018/Scores_Grades/NB_WNH_FB_POC_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
    #                                           'Working_Poor_HS_grade', 'Income_level_HS_grade',
    #                                           'Rent_Burden_HS_grade', 'Home_Ownership_HS_grade','Overall_HS_grade'])
    # result_group4 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    # result_group4.drop(0)
    # result_group4 = result_group4[result_group4['puma'] != "Total Geo"]
    # result_group4.to_json(path_or_buf='data/json_2018/parsed_group4.json', orient='records')
    #
    # # For group3
    # with open('data/json_2018/parsed_group3.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_BABS_score"]),
    #                 "Grade": item["Overall_BABS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_BABS"]),
    #                 "Score": str(item["FT_Work_BABS_score"]),
    #                 "Grade": item["FT_Work_BABS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_BABS"]),
    #                 "Score": str(item["Poverty_BABS_score"]),
    #                 "Grade": item["Poverty_BABS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_BABS"]),
    #                 "Score": str(item["Working_Poor_BABS_score"]),
    #                 "Grade": item["Working_Poor_BABS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_BABS"]),
    #                 "Score": str(item["Home_Ownership_BABS_score"]),
    #                 "Grade": item["Home_Ownership_BABS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_BABS"]),
    #                 "Score": str(item["Rent_Burden_BABS_score"]),
    #                 "Grade": item["Rent_Burden_BABS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_BABS"]),
    #                 "Score": str(item["Unemployment_BABS_score"]),
    #                 "Grade": item["Unemployment_BABS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_BABS"]),
    #                 "Score": str(item["Income_level_BABS_score"]),
    #                 "Grade": item["Income_level_BABS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group3.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    #
    #
    # # For group4
    # with open('data/json_2018/parsed_group4.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_HS_score"]),
    #                 "Grade": item["Overall_HS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_HS"]),
    #                 "Score": str(item["FT_Work_HS_score"]),
    #                 "Grade": item["FT_Work_HS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_HS"]),
    #                 "Score": str(item["Poverty_HS_score"]),
    #                 "Grade": item["Poverty_HS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_HS"]),
    #                 "Score": str(item["Working_Poor_HS_score"]),
    #                 "Grade": item["Working_Poor_HS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_HS"]),
    #                 "Score": str(item["Home_Ownership_HS_score"]),
    #                 "Grade": item["Home_Ownership_HS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_HS"]),
    #                 "Score": str(item["Rent_Burden_HS_score"]),
    #                 "Grade": item["Rent_Burden_HS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_HS"]),
    #                 "Score": str(item["Unemployment_HS_score"]),
    #                 "Grade": item["Unemployment_HS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_HS"]),
    #                 "Score": str(item["Income_level_HS_score"]),
    #                 "Grade": item["Income_level_HS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group4.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    #
    # #For Group 12 and Group 13
    #
    # result_group12 = pandas.DataFrame()
    # data_disparity_BABS = pandas.read_csv('data/2018/Disparities/FB_WNH_FB_POC_Disparity.csv',
    #                                       usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS', 'Poverty_BABS',
    #                                                'Working_Poor_BABS', 'Income_level_BABS','Rent_Burden_BABS',
    #                                                'Home_Ownership_BABS'])
    # data_scores_BABS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score', 'Poverty_BABS_score',
    #                                             'Working_Poor_BABS_score', 'Income_level_BABS_score',
    #                                             'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score','Overall_BABS_score'])
    # data_grades_BABS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade', 'Poverty_BABS_grade',
    #                                             'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
    #                                             'Rent_Burden_BABS_grade', 'Home_Ownership_BABS_grade','Overall_BABS_grade'])
    # result_group12 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    # result_group12.drop(0)
    # result_group12 = result_group12[result_group12['puma'] != "Total Geo"]
    # result_group12.to_json(path_or_buf='data/json_2018/parsed_group12.json', orient='records')
    #
    # result_group13 = pandas.DataFrame()
    # data_disparity_HS = pandas.read_csv('data/2018/Disparities/FB_WNH_FB_POC_Disparity.csv',
    #                                     usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
    #                                              'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
    #                                              'Home_Ownership_HS'])
    # data_scores_HS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
    #                                           'Working_Poor_HS_score', 'Income_level_HS_score',
    #                                           'Rent_Burden_HS_score', 'Home_Ownership_HS_score','Overall_HS_score'])
    # data_grades_HS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
    #                                           'Working_Poor_HS_grade', 'Income_level_HS_grade',
    #                                           'Rent_Burden_HS_grade', 'Home_Ownership_HS_grade','Overall_HS_grade'])
    # result_group13 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    # result_group13.drop(0)
    # result_group13 = result_group13[result_group13['puma'] != "Total Geo"]
    # result_group13.to_json(path_or_buf='data/json_2018/parsed_group13.json', orient='records')
    #
    # # For group12
    # with open('data/json_2018/parsed_group12.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_BABS_score"]),
    #                 "Grade": item["Overall_BABS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_BABS"]),
    #                 "Score": str(item["FT_Work_BABS_score"]),
    #                 "Grade": item["FT_Work_BABS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_BABS"]),
    #                 "Score": str(item["Poverty_BABS_score"]),
    #                 "Grade": item["Poverty_BABS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_BABS"]),
    #                 "Score": str(item["Working_Poor_BABS_score"]),
    #                 "Grade": item["Working_Poor_BABS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_BABS"]),
    #                 "Score": str(item["Home_Ownership_BABS_score"]),
    #                 "Grade": item["Home_Ownership_BABS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_BABS"]),
    #                 "Score": str(item["Rent_Burden_BABS_score"]),
    #                 "Grade": item["Rent_Burden_BABS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_BABS"]),
    #                 "Score": str(item["Unemployment_BABS_score"]),
    #                 "Grade": item["Unemployment_BABS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_BABS"]),
    #                 "Score": str(item["Income_level_BABS_score"]),
    #                 "Grade": item["Income_level_BABS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group12.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    #
    #
    # # For group13
    # with open('data/json_2018/parsed_group13.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_HS_score"]),
    #                 "Grade": item["Overall_HS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_HS"]),
    #                 "Score": str(item["FT_Work_HS_score"]),
    #                 "Grade": item["FT_Work_HS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_HS"]),
    #                 "Score": str(item["Poverty_HS_score"]),
    #                 "Grade": item["Poverty_HS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_HS"]),
    #                 "Score": str(item["Working_Poor_HS_score"]),
    #                 "Grade": item["Working_Poor_HS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_HS"]),
    #                 "Score": str(item["Home_Ownership_HS_score"]),
    #                 "Grade": item["Home_Ownership_HS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_HS"]),
    #                 "Score": str(item["Rent_Burden_HS_score"]),
    #                 "Grade": item["Rent_Burden_HS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_HS"]),
    #                 "Score": str(item["Unemployment_HS_score"]),
    #                 "Grade": item["Unemployment_HS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_HS"]),
    #                 "Score": str(item["Income_level_HS_score"]),
    #                 "Grade": item["Income_level_HS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group13.json', 'w') as out_file:
    #     json.dump(children, out_file)


    #for Group 11

    result_group11=pandas.DataFrame()
    data_disparity_HSINC= pandas.read_csv('data/2018/GRP11/Scores_Grades/FB_HSINC_Disparity_percentage.csv',usecols=['puma','HSINC_UnEmp_Total',
                                                                                                    'HSINC_FT_Work_Total','HSINC_Poverty_Total',
                                                                                                    'HSINC_Working_Poor_Total','HSINC_Rent_Burden_Total',
                                                                                                    'HSINC_Home_Ownership_Total','HSINC_Avg_PINCP_mf_t'])
    data_scores_HSINC = pandas.read_csv('data/2018/GRP11/Scores_Grades/FB_HSINC_Scores_Grades.csv',usecols=['Unemployment_HSINC_score','FT_Work_HSINC_score',
                                                                                                            'Poverty_HSINC_score','Working_Poor_HSINC_score',
                                                                                                            'Rent_Burden_HSINC_score','Home_Ownership_HSINC_score',
                                                                                                            'Income_level_HSINC_score','Overall_HSINC_score'])
    data_grades_HSINC= pandas.read_csv('data/2018/GRP11/Scores_Grades/FB_HSINC_Scores_Grades.csv',usecols=['Unemployment_HSINC_grade','FT_Work_HSINC_grade',
                                                                                                            'Poverty_HSINC_grade','Working_Poor_HSINC_grade',
                                                                                                            'Rent_Burden_HSINC_grade','Home_Ownership_HSINC_grade',
                                                                                                            'Income_level_HSINC_grade','Overall_HSINC_grade'])
    result_group11 = pandas.concat([data_disparity_HSINC, data_scores_HSINC, data_grades_HSINC], axis=1)
    result_group11.drop(0)
    result_group11 = result_group11[result_group11['puma'] != "Total Geo"]
    result_group11.to_json(path_or_buf='data/json_2018/parsed_group11.json', orient='records')

    with open('data/json_2018/parsed_group11.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HSINC_score"]),
                    "Grade": item["Overall_HSINC_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["HSINC_FT_Work_Total"]),
                    "Score": str(item["FT_Work_HSINC_score"]),
                    "Grade": item["FT_Work_HSINC_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["HSINC_Poverty_Total"]),
                    "Score": str(item["Poverty_HSINC_score"]),
                    "Grade": item["Poverty_HSINC_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["HSINC_Working_Poor_Total"]),
                    "Score": str(item["Working_Poor_HSINC_score"]),
                    "Grade": item["Working_Poor_HSINC_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["HSINC_Home_Ownership_Total"]),
                    "Score": str(item["Home_Ownership_HSINC_score"]),
                    "Grade": item["Home_Ownership_HSINC_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["HSINC_Rent_Burden_Total"]),
                    "Score": str(item["Rent_Burden_HSINC_score"]),
                    "Grade": item["Rent_Burden_HSINC_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["HSINC_UnEmp_Total"]),
                    "Score": str(item["Unemployment_HSINC_score"]),
                    "Grade": item["Unemployment_HSINC_grade"]
                },
                "Income": {
                    "Ratio": str(item["HSINC_Avg_PINCP_mf_t"]),
                    "Score": str(item["Income_level_HSINC_score"]),
                    "Grade": item["Income_level_HSINC_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    # container['name'] = 'name'
    #container = children

    with open('data/json_2018/group11.json', 'w') as out_file:
        json.dump(children, out_file)

    # # For Group 5 and Group 6
    #
    # result_group5 = pandas.DataFrame()
    # data_disparity_BABS = pandas.read_csv('data/2018/Disparities/NB_ALL_F_FB_ALL_F_Disparity.csv',
    #                                       usecols=['puma', 'Unemployment_BABS_F', 'FT_Work_BABS_F', 'Poverty_BABS_F',
    #                                                'Working_Poor_BABS_F', 'Income_level_BABS_F', 'Rent_Burden_BABS_F',
    #                                                'Home_Ownership_BABS_F'])
    # data_scores_BABS = pandas.read_csv('data/2018/Scores_Grades/NB_ALL_F_FB_ALL_F_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score',
    #                                             'Poverty_BABS_score',
    #                                             'Working_Poor_BABS_score', 'Income_level_BABS_score',
    #                                             'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
    #                                             'Overall_BABS_score'])
    # data_grades_BABS = pandas.read_csv('data/2018/Scores_Grades/NB_ALL_F_FB_ALL_F_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade',
    #                                             'Poverty_BABS_grade',
    #                                             'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
    #                                             'Rent_Burden_BABS_grade', 'Home_Ownership_BABS_grade',
    #                                             'Overall_BABS_grade'])
    # result_group5 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    # result_group5.drop(0)
    # result_group5 = result_group5[result_group5['puma'] != "Total Geo"]
    # result_group5.to_json(path_or_buf='data/json_2018/parsed_group5.json', orient='records')
    #
    # result_group6 = pandas.DataFrame()
    # data_disparity_HS = pandas.read_csv('data/2018/Disparities/NB_ALL_F_FB_ALL_F_Disparity.csv',
    #                                     usecols=['puma', 'Unemployment_HS_F', 'FT_Work_HS_F', 'Poverty_HS_F',
    #                                              'Working_Poor_HS_F', 'Income_level_HS_F', 'Rent_Burden_HS_F',
    #                                              'Home_Ownership_HS_F'])
    # data_scores_HS = pandas.read_csv('data/2018/Scores_Grades/NB_ALL_F_FB_ALL_F_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
    #                                           'Working_Poor_HS_score', 'Income_level_HS_score',
    #                                           'Rent_Burden_HS_score', 'Home_Ownership_HS_score',
    #                                           'Overall_HS_score'])
    # data_grades_HS = pandas.read_csv('data/2018/Scores_Grades/NB_ALL_F_FB_ALL_F_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
    #                                           'Working_Poor_HS_grade', 'Income_level_HS_grade',
    #                                           'Rent_Burden_HS_grade', 'Home_Ownership_HS_grade',
    #                                           'Overall_HS_grade'])
    # result_group6 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    # result_group6.drop(0)
    # result_group6 = result_group6[result_group6['puma'] != "Total Geo"]
    # result_group6.to_json(path_or_buf='data/json_2018/parsed_group6.json', orient='records')
    #
    # # For group5
    # with open('data/json_2018/parsed_group5.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_BABS_score"]),
    #                 "Grade": item["Overall_BABS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_BABS_F"]),
    #                 "Score": str(item["FT_Work_BABS_score"]),
    #                 "Grade": item["FT_Work_BABS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_BABS_F"]),
    #                 "Score": str(item["Poverty_BABS_score"]),
    #                 "Grade": item["Poverty_BABS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_BABS_F"]),
    #                 "Score": str(item["Working_Poor_BABS_score"]),
    #                 "Grade": item["Working_Poor_BABS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_BABS_F"]),
    #                 "Score": str(item["Home_Ownership_BABS_score"]),
    #                 "Grade": item["Home_Ownership_BABS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_BABS_F"]),
    #                 "Score": str(item["Rent_Burden_BABS_score"]),
    #                 "Grade": item["Rent_Burden_BABS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_BABS_F"]),
    #                 "Score": str(item["Unemployment_BABS_score"]),
    #                 "Grade": item["Unemployment_BABS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_BABS_F"]),
    #                 "Score": str(item["Income_level_BABS_score"]),
    #                 "Grade": item["Income_level_BABS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group5.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    #
    # # For group6
    # with open('data/json_2018/parsed_group6.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_HS_score"]),
    #                 "Grade": item["Overall_HS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_HS_F"]),
    #                 "Score": str(item["FT_Work_HS_score"]),
    #                 "Grade": item["FT_Work_HS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_HS_F"]),
    #                 "Score": str(item["Poverty_HS_score"]),
    #                 "Grade": item["Poverty_HS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_HS_F"]),
    #                 "Score": str(item["Working_Poor_HS_score"]),
    #                 "Grade": item["Working_Poor_HS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_HS_F"]),
    #                 "Score": str(item["Home_Ownership_HS_score"]),
    #                 "Grade": item["Home_Ownership_HS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_HS_F"]),
    #                 "Score": str(item["Rent_Burden_HS_score"]),
    #                 "Grade": item["Rent_Burden_HS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_HS_F"]),
    #                 "Score": str(item["Unemployment_HS_score"]),
    #                 "Grade": item["Unemployment_HS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_HS_F"]),
    #                 "Score": str(item["Income_level_HS_score"]),
    #                 "Grade": item["Income_level_HS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group6.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    # # For Group 7 and Group 8
    #
    # result_group7 = pandas.DataFrame()
    # data_disparity_BABS = pandas.read_csv('data/2018/Disparities/FB_WNH_FB_POC_F_Disparity.csv',
    #                                       usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS',
    #                                                'Poverty_BABS',
    #                                                'Working_Poor_BABS', 'Income_level_BABS',
    #                                                'Rent_Burden_BABS',
    #                                                'Home_Ownership_BABS'])
    # data_scores_BABS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_F_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score',
    #                                             'Poverty_BABS_score',
    #                                             'Working_Poor_BABS_score', 'Income_level_BABS_score',
    #                                             'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
    #                                             'Overall_BABS_score'])
    # data_grades_BABS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_F_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade',
    #                                             'Poverty_BABS_grade',
    #                                             'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
    #                                             'Rent_Burden_BABS_grade', 'Home_Ownership_BABS_grade',
    #                                             'Overall_BABS_grade'])
    # result_group7 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    # result_group7.drop(0)
    # result_group7 = result_group7[result_group7['puma'] != "Total Geo"]
    # result_group7.to_json(path_or_buf='data/json_2018/parsed_group7.json', orient='records')
    #
    # result_group8 = pandas.DataFrame()
    # data_disparity_HS = pandas.read_csv('data/2018/Disparities/FB_WNH_FB_POC_F_Disparity.csv',
    #                                     usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
    #                                              'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
    #                                              'Home_Ownership_HS'])
    # data_scores_HS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_F_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
    #                                           'Working_Poor_HS_score', 'Income_level_HS_score',
    #                                           'Rent_Burden_HS_score', 'Home_Ownership_HS_score',
    #                                           'Overall_HS_score'])
    # data_grades_HS = pandas.read_csv('data/2018/Scores_Grades/FB_WNH_FB_POC_F_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
    #                                           'Working_Poor_HS_grade', 'Income_level_HS_grade',
    #                                           'Rent_Burden_HS_grade', 'Home_Ownership_HS_grade',
    #                                           'Overall_HS_grade'])
    # result_group8 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    # result_group8.drop(0)
    # result_group8 = result_group8[result_group8['puma'] != "Total Geo"]
    # result_group8.to_json(path_or_buf='data/json_2018/parsed_group8.json', orient='records')
    #
    # # For group7
    # with open('data/json_2018/parsed_group7.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_BABS_score"]),
    #                 "Grade": item["Overall_BABS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_BABS"]),
    #                 "Score": str(item["FT_Work_BABS_score"]),
    #                 "Grade": item["FT_Work_BABS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_BABS"]),
    #                 "Score": str(item["Poverty_BABS_score"]),
    #                 "Grade": item["Poverty_BABS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_BABS"]),
    #                 "Score": str(item["Working_Poor_BABS_score"]),
    #                 "Grade": item["Working_Poor_BABS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_BABS"]),
    #                 "Score": str(item["Home_Ownership_BABS_score"]),
    #                 "Grade": item["Home_Ownership_BABS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_BABS"]),
    #                 "Score": str(item["Rent_Burden_BABS_score"]),
    #                 "Grade": item["Rent_Burden_BABS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_BABS"]),
    #                 "Score": str(item["Unemployment_BABS_score"]),
    #                 "Grade": item["Unemployment_BABS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_BABS"]),
    #                 "Score": str(item["Income_level_BABS_score"]),
    #                 "Grade": item["Income_level_BABS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group7.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    # # For group8
    # with open('data/json_2018/parsed_group8.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_HS_score"]),
    #                 "Grade": item["Overall_HS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_HS"]),
    #                 "Score": str(item["FT_Work_HS_score"]),
    #                 "Grade": item["FT_Work_HS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_HS"]),
    #                 "Score": str(item["Poverty_HS_score"]),
    #                 "Grade": item["Poverty_HS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_HS"]),
    #                 "Score": str(item["Working_Poor_HS_score"]),
    #                 "Grade": item["Working_Poor_HS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_HS"]),
    #                 "Score": str(item["Home_Ownership_HS_score"]),
    #                 "Grade": item["Home_Ownership_HS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_HS"]),
    #                 "Score": str(item["Rent_Burden_HS_score"]),
    #                 "Grade": item["Rent_Burden_HS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_HS"]),
    #                 "Score": str(item["Unemployment_HS_score"]),
    #                 "Grade": item["Unemployment_HS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_HS"]),
    #                 "Score": str(item["Income_level_HS_score"]),
    #                 "Grade": item["Income_level_HS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group8.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    # # For Group 9 and Group 10
    #
    # result_group9 = pandas.DataFrame()
    # data_disparity_BABS = pandas.read_csv('data/2018/Disparities/FB_ALL_F_M_Disparity.csv',
    #                                       usecols=['puma', 'Unemployment_BABS', 'FT_Work_BABS',
    #                                                'Poverty_BABS',
    #                                                'Working_Poor_BABS', 'Income_level_BABS',
    #                                                'Rent_Burden_BABS',
    #                                                'Home_Ownership_BABS'])
    # data_scores_BABS = pandas.read_csv('data/2018/Scores_Grades/FB_ALL_F_M_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_score', 'FT_Work_BABS_score',
    #                                             'Poverty_BABS_score',
    #                                             'Working_Poor_BABS_score', 'Income_level_BABS_score',
    #                                             'Rent_Burden_BABS_score', 'Home_Ownership_BABS_score',
    #                                             'Overall_BABS_score'])
    # data_grades_BABS = pandas.read_csv('data/2018/Scores_Grades/FB_ALL_F_M_Scores_Grades.csv',
    #                                    usecols=['Unemployment_BABS_grade', 'FT_Work_BABS_grade',
    #                                             'Poverty_BABS_grade',
    #                                             'Working_Poor_BABS_grade', 'Income_level_BABS_grade',
    #                                             'Rent_Burden_BABS_grade', 'Home_Ownership_BABS_grade',
    #                                             'Overall_BABS_grade'])
    # result_group9 = pandas.concat([data_disparity_BABS, data_scores_BABS, data_grades_BABS], axis=1)
    # result_group9.drop(0)
    # result_group9 = result_group9[result_group9['puma'] != "Total Geo"]
    # result_group9.to_json(path_or_buf='data/json_2018/parsed_group9.json', orient='records')
    #
    # result_group10 = pandas.DataFrame()
    # data_disparity_HS = pandas.read_csv('data/2018/Disparities/FB_ALL_F_M_Disparity.csv',
    #                                     usecols=['puma', 'Unemployment_HS', 'FT_Work_HS', 'Poverty_HS',
    #                                              'Working_Poor_HS', 'Income_level_HS', 'Rent_Burden_HS',
    #                                              'Home_Ownership_HS'])
    # data_scores_HS = pandas.read_csv('data/2018/Scores_Grades/FB_ALL_F_M_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_score', 'FT_Work_HS_score', 'Poverty_HS_score',
    #                                           'Working_Poor_HS_score', 'Income_level_HS_score',
    #                                           'Rent_Burden_HS_score', 'Home_Ownership_HS_score',
    #                                           'Overall_HS_score'])
    # data_grades_HS = pandas.read_csv('data/2018/Scores_Grades/FB_ALL_F_M_Scores_Grades.csv',
    #                                  usecols=['Unemployment_HS_grade', 'FT_Work_HS_grade', 'Poverty_HS_grade',
    #                                           'Working_Poor_HS_grade', 'Income_level_HS_grade',
    #                                           'Rent_Burden_HS_grade', 'Home_Ownership_HS_grade',
    #                                           'Overall_HS_grade'])
    # result_group10 = pandas.concat([data_disparity_HS, data_scores_HS, data_grades_HS], axis=1)
    # result_group10.drop(0)
    # result_group10 = result_group10[result_group10['puma'] != "Total Geo"]
    # result_group10.to_json(path_or_buf='data/json_2018/parsed_group10.json', orient='records')
    #
    # # For group9
    # with open('data/json_2018/parsed_group9.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_BABS_score"]),
    #                 "Grade": item["Overall_BABS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_BABS"]),
    #                 "Score": str(item["FT_Work_BABS_score"]),
    #                 "Grade": item["FT_Work_BABS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_BABS"]),
    #                 "Score": str(item["Poverty_BABS_score"]),
    #                 "Grade": item["Poverty_BABS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_BABS"]),
    #                 "Score": str(item["Working_Poor_BABS_score"]),
    #                 "Grade": item["Working_Poor_BABS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_BABS"]),
    #                 "Score": str(item["Home_Ownership_BABS_score"]),
    #                 "Grade": item["Home_Ownership_BABS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_BABS"]),
    #                 "Score": str(item["Rent_Burden_BABS_score"]),
    #                 "Grade": item["Rent_Burden_BABS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_BABS"]),
    #                 "Score": str(item["Unemployment_BABS_score"]),
    #                 "Grade": item["Unemployment_BABS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_BABS"]),
    #                 "Score": str(item["Income_level_BABS_score"]),
    #                 "Grade": item["Income_level_BABS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group9.json', 'w') as out_file:
    #     json.dump(children, out_file)
    #
    # # For group10
    # with open('data/json_2018/parsed_group10.json') as input_file:
    #     raw_data = json.load(input_file)
    # children = {}
    # for item in raw_data:
    #     if item['puma'] is 'Total Geo':
    #         continue
    #     children.update({
    #         item['puma']: {
    #             "Overall": {
    #                 "Score": str(item["Overall_HS_score"]),
    #                 "Grade": item["Overall_HS_grade"]
    #             },
    #             "Full Time": {
    #                 "Ratio": str(item["FT_Work_HS"]),
    #                 "Score": str(item["FT_Work_HS_score"]),
    #                 "Grade": item["FT_Work_HS_grade"]
    #             },
    #             "Poverty": {
    #                 "Ratio": str(item["Poverty_HS"]),
    #                 "Score": str(item["Poverty_HS_score"]),
    #                 "Grade": item["Poverty_HS_grade"]
    #             },
    #             "Working Poor": {
    #                 "Ratio": str(item["Working_Poor_HS"]),
    #                 "Score": str(item["Working_Poor_HS_score"]),
    #                 "Grade": item["Working_Poor_HS_grade"]
    #             },
    #             "Homeownership": {
    #                 "Ratio": str(item["Home_Ownership_HS"]),
    #                 "Score": str(item["Home_Ownership_HS_score"]),
    #                 "Grade": item["Home_Ownership_HS_grade"]
    #             },
    #             "Rent Burden": {
    #                 "Ratio": str(item["Rent_Burden_HS"]),
    #                 "Score": str(item["Rent_Burden_HS_score"]),
    #                 "Grade": item["Rent_Burden_HS_grade"]
    #             },
    #             "Unemployment": {
    #                 "Ratio": str(item["Unemployment_HS"]),
    #                 "Score": str(item["Unemployment_HS_score"]),
    #                 "Grade": item["Unemployment_HS_grade"]
    #             },
    #             "Income": {
    #                 "Ratio": str(item["Income_level_HS"]),
    #                 "Score": str(item["Income_level_HS_score"]),
    #                 "Grade": item["Income_level_HS_grade"]
    #             },
    #             "Naturalization": {}
    #         }
    #
    #     })
    # #container = {}
    # # container['name'] = 'name'
    # #container = children
    #
    # with open('data/json_2018/group10.json', 'w') as out_file:
    #     json.dump(children, out_file)


    #for Group 11

    result_group11=pandas.DataFrame()
    data_disparity_HSINC= pandas.read_csv('data/2018/GRP11/Scores_Grades/FB_HSINC_Disparity_percentage.csv',usecols=['puma','HSINC_UnEmp_Total',
                                                                                                    'HSINC_FT_Work_Total','HSINC_Poverty_Total',
                                                                                                    'HSINC_Working_Poor_Total','HSINC_Rent_Burden_Total',
                                                                                                    'HSINC_Home_Ownership_Total','HSINC_Avg_PINCP_mf_t'])
    data_scores_HSINC = pandas.read_csv('data/2018/GRP11/Scores_Grades/FB_HSINC_Scores_Grades.csv',usecols=['Unemployment_HSINC_score','FT_Work_HSINC_score',
                                                                                                            'Poverty_HSINC_score','Working_Poor_HSINC_score',
                                                                                                            'Rent_Burden_HSINC_score','Home_Ownership_HSINC_score',
                                                                                                            'Income_level_HSINC_score','Overall_HSINC_score'])
    data_grades_HSINC= pandas.read_csv('data/2018/GRP11/Scores_Grades/FB_HSINC_Scores_Grades.csv',usecols=['Unemployment_HSINC_grade','FT_Work_HSINC_grade',
                                                                                                            'Poverty_HSINC_grade','Working_Poor_HSINC_grade',
                                                                                                            'Rent_Burden_HSINC_grade','Home_Ownership_HSINC_grade',
                                                                                                            'Income_level_HSINC_grade','Overall_HSINC_grade'])
    result_group11 = pandas.concat([data_disparity_HSINC, data_scores_HSINC, data_grades_HSINC], axis=1)
    result_group11.drop(0)
    result_group11 = result_group11[result_group11['puma'] != "Total Geo"]
    result_group11.to_json(path_or_buf='data/json_2018/parsed_group11.json', orient='records')

    with open('data/json_2018/parsed_group11.json') as input_file:
        raw_data = json.load(input_file)
    children = {}
    for item in raw_data:
        if item['puma'] is 'Total Geo':
            continue
        children.update({
            item['puma']: {
                "Overall": {
                    "Score": str(item["Overall_HSINC_score"]),
                    "Grade": item["Overall_HSINC_grade"]
                },
                "Full Time": {
                    "Ratio": str(item["HSINC_FT_Work_Total"]),
                    "Score": str(item["FT_Work_HSINC_score"]),
                    "Grade": item["FT_Work_HSINC_grade"]
                },
                "Poverty": {
                    "Ratio": str(item["HSINC_Poverty_Total"]),
                    "Score": str(item["Poverty_HSINC_score"]),
                    "Grade": item["Poverty_HSINC_grade"]
                },
                "Working Poor": {
                    "Ratio": str(item["HSINC_Working_Poor_Total"]),
                    "Score": str(item["Working_Poor_HSINC_score"]),
                    "Grade": item["Working_Poor_HSINC_grade"]
                },
                "Homeownership": {
                    "Ratio": str(item["HSINC_Home_Ownership_Total"]),
                    "Score": str(item["Home_Ownership_HSINC_score"]),
                    "Grade": item["Home_Ownership_HSINC_grade"]
                },
                "Rent Burden": {
                    "Ratio": str(item["HSINC_Rent_Burden_Total"]),
                    "Score": str(item["Rent_Burden_HSINC_score"]),
                    "Grade": item["Rent_Burden_HSINC_grade"]
                },
                "Unemployment": {
                    "Ratio": str(item["HSINC_UnEmp_Total"]),
                    "Score": str(item["Unemployment_HSINC_score"]),
                    "Grade": item["Unemployment_HSINC_grade"]
                },
                "Income": {
                    "Ratio": str(item["HSINC_Avg_PINCP_mf_t"]),
                    "Score": str(item["Income_level_HSINC_score"]),
                    "Grade": item["Income_level_HSINC_grade"]
                },
                "Naturalization": {}
            }

        })
    #container = {}
    # container['name'] = 'name'
    #container = children

    with open('data/json_2018/group11.json', 'w') as out_file:
        json.dump(children, out_file)
