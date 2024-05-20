import os

from constant import watch_brands, bag_brands
from firebase import FireBaseService
from prediction import PredictionManager, PricePredict

if __name__ == '__main__':
    db = FireBaseService().get_db()

    for target_brand in bag_brands:
        output_file = './prediction/bag_' + target_brand.lower() + '.csv'
        if not os.path.exists(output_file):
            PricePredict(db=db, brand=target_brand.lower().replace(' ', '_'),
                         col='BagPriceTrend').main_predict(n=3, output_file=output_file)

    for target_brand in watch_brands:
        output_file = './prediction/watch_' + target_brand.lower() + '.csv'
        if not os.path.exists(output_file):
            PricePredict(db=db, brand=target_brand.lower(),
                         col='WatchPriceTrend').main_predict(n=3, output_file=output_file)

    output_file = './prediction/diamond.csv'
    if not os.path.exists(output_file):
        PricePredict(db=db, col='DiamondPriceTrend').main_predict(n=3, output_file=output_file)

    manager = PredictionManager(db=db)
    manager.insert_prediction(category='bag', brands=bag_brands)
    manager.insert_prediction(category='watch', brands=watch_brands)
    manager.insert_prediction(category='diamond')
