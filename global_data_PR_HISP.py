import pandas
from scrap_edu_att_PR_HISP import get_PUMA_from_CSV

PUMA_Counties = get_PUMA_from_CSV()

NB_ALL_UnEmp_p,NB_ALL_FT_Work_p,NB_ALL_Poverty_p,NB_ALL_Working_Poor_p,NB_ALL_Income_level_p,NB_ALL_Rent_burden_p,NB_ALL_Home_ownership_p = None,None,None,None,None,None,None
FB_Hispanic_UnEmp_p,FB_Hispanic_FT_Work_p,FB_Hispanic_Poverty_p,FB_Hispanic_Working_Poor_p,FB_Hispanic_Income_level_p,FB_Hispanic_Rent_burden_p,FB_Hispanic_Home_ownership_p = None,None,None,None,None,None,None
FB_WNH_UnEmp_p,FB_WNH_FT_Work_p,FB_WNH_Poverty_p,FB_WNH_Working_Poor_p,FB_WNH_Income_level_p,FB_WNH_Rent_burden_p,FB_WNH_Home_ownership_p = None,None,None,None,None,None,None
NB_Mainland_UnEmp_p,NB_Mainland_FT_Work_p,NB_Mainland_Poverty_p,NB_Mainland_Working_Poor_p,NB_Mainland_Income_level_p,NB_Mainland_Rent_burden_p,NB_Mainland_Home_ownership_p = None,None,None,None,None,None,None
PR_Hispanic_UnEmp_p,PR_Hispanic_FT_Work_p,PR_Hispanic_Poverty_p,PR_Hispanic_Working_Poor_p,PR_Hispanic_Income_level_p,PR_Hispanic_Rent_burden_p,PR_Hispanic_Home_ownership_p = None,None,None,None,None,None,None
NB_Mainland_PR_UnEmp_p,NB_Mainland_PR_FT_Work_p,NB_Mainland_PR_Poverty_p,NB_Mainland_PR_Working_Poor_p,NB_Mainland_PR_Income_level_p,NB_Mainland_PR_Rent_burden_p,NB_Mainland_PR_Home_ownership_p = None,None,None,None,None,None,None
FB_Hispanic_POC_UnEmp_p,FB_Hispanic_POC_FT_Work_p,FB_Hispanic_POC_Poverty_p,FB_Hispanic_POC_Working_Poor_p,FB_Hispanic_POC_Income_level_p,FB_Hispanic_POC_Rent_burden_p,FB_Hispanic_POC_Home_ownership_p = None,None,None,None,None,None,None
NB_WNH_UnEmp_p,NB_WNH_FT_Work_p,NB_WNH_Poverty_p,NB_WNH_Working_Poor_p,NB_WNH_Income_level_p,NB_WNH_Rent_burden_p,NB_WNH_Home_ownership_p = None,None,None,None,None,None,None
PR_POC_UnEmp_p,PR_POC_FT_Work_p,PR_POC_Poverty_p,PR_POC_Working_Poor_p,PR_POC_Income_level_p,PR_POC_Rent_burden_p,PR_POC_Home_ownership_p = None,None,None,None,None,None,None
PR_White_UnEmp_p,PR_White_FT_Work_p,PR_White_Poverty_p,PR_White_Working_Poor_p,PR_White_Income_level_p,PR_White_Rent_burden_p,PR_White_Home_ownership_p = None,None,None,None,None,None,None


NB_ALL_FB_ALL_d = pandas.DataFrame()
NB_ALL_F_FB_ALL_F_d = pandas.DataFrame()
FB_ALL_F_M_d = pandas.DataFrame()
NB_WNH_FB_POC_d = pandas.DataFrame()


