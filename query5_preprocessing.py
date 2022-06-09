import pandas as pd

reviews_la=pd.read_csv('./reviews_losangeles.csv')
reviews_la['region']=['los_angeles']*len(reviews_la)

reviews_portland=pd.read_csv('./reviews_portland.csv')
reviews_portland['region']=['portland']*len(reviews_portland)

reviews_sd=pd.read_csv('./reviews_sandiego.csv')
reviews_sd['region']=['san_diego']*len(reviews_sd)

reviews_salem = pd.read_csv('./reviews_salem.csv')
reviews_salem['region']=['salem']*len(reviews_salem)

final_reviews=pd.concat([reviews_la, reviews_portland, reviews_salem, reviews_sd])
final_reviews.to_csv('final_reviews.csv')


final_reviews['Date']= pd.to_datetime(final_reviews['date'])

final_reviews['year'] = final_reviews['Date'].dt.year
final_reviews['month'] = final_reviews['Date'].dt.month

final_reviews= final_reviews.to_csv('./final_reviews_final.csv')
