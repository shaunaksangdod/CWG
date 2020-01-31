import pandas
from scrap import get_PUMA_from_CSV

PUMA_Counties = get_PUMA_from_CSV()

NB_ALL_UnEmp_p,NB_ALL_FT_Work_p,NB_ALL_Poverty_p,NB_ALL_Working_Poor_p,NB_ALL_Income_level_p,NB_ALL_Rent_burden_p,NB_ALL_Home_ownership_p = None,None,None,None,None,None,None
NB_WNH_UnEmp_p,NB_WNH_FT_Work_p,NB_WNH_Poverty_p,NB_WNH_Working_Poor_p,NB_WNH_Income_level_p,NB_WNH_Rent_burden_p,NB_WNH_Home_ownership_p = None,None,None,None,None,None,None
FB_WNH_UnEmp_p,FB_WNH_FT_Work_p,FB_WNH_Poverty_p,FB_WNH_Working_Poor_p,FB_WNH_Income_level_p,FB_WNH_Rent_burden_p,FB_WNH_Home_ownership_p = None,None,None,None,None,None,None
FB_ALL_UnEmp_p,FB_ALL_FT_Work_p,FB_ALL_Poverty_p,FB_ALL_Working_Poor_p,FB_ALL_Income_level_p,FB_ALL_Rent_burden_p,FB_ALL_Home_ownership_p = None,None,None,None,None,None,None
FB_POC_UnEmp_p,FB_POC_FT_Work_p,FB_POC_Poverty_p,FB_POC_Working_Poor_p,FB_POC_Income_level_p,FB_POC_Rent_burden_p,FB_POC_Home_ownership_p = None,None,None,None,None,None,None
FB_ALL_GRP11_UnEmp_p,FB_ALL_GRP11_FT_Work_p,FB_ALL_GRP11_Poverty_p,FB_ALL_GRP11_Working_Poor_p,FB_ALL_GRP11_Income_level_p,FB_ALL_GRP11_Rent_burden_p,FB_ALL_GRP11_Home_ownership_p = None,None,None,None,None,None,None
FB_White_UnEmp_p,FB_White_FT_Work_p,FB_White_Poverty_p,FB_White_Working_Poor_p,FB_White_Income_level_p,FB_White_Rent_burden_p,FB_White_Home_ownership_p = None,None,None,None,None,None,None

NB_ALL_FB_ALL_d = pandas.DataFrame()
NB_ALL_F_FB_ALL_F_d = pandas.DataFrame()
FB_ALL_F_M_d = pandas.DataFrame()
NB_WNH_FB_POC_d = pandas.DataFrame()
FB_WNH_FB_POC_d = pandas.DataFrame()
FB_POC_FB_White_d = pandas.DataFrame()

