import pandas as pd

calendars_la=pd.read_csv('./calendar_losangeles.csv')
calendars_la['region']=['los_angeles']*len(calendars_la)

calendars_portland=pd.read_csv('./calendar_portland.csv')
calendars_portland['region']=['portland']*len(calendars_portland)

calendars_sd=pd.read_csv('./calendar_sandiego.csv')
calendars_sd['region']=['san_diego']*len(calendars_sd)

calendars_salem = pd.read_csv('./calendar_salem.csv')
calendars_salem['region']=['salem']*len(calendars_salem)

final_calendars=pd.concat([calendars_la, calendars_portland, calendars_salem, calendars_sd])
final_calendars.to_csv('final_calendars.csv')

final_calendars['Date']= pd.to_datetime(final_calendars['date'])


final_calendars['year'] = final_calendars['Date'].dt.year
final_calendars['month'] = final_calendars['Date'].dt.month

final_calendars.to_csv('./final_calendars_final.csv')

