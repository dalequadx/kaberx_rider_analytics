import pandas as pd

from sklearn import ensemble
from sklearn.model_selection import GridSearchCV

import datetime
SEED = 20202020  # seed to maintain reproducibility


def main(url):
    _df = load_data(url)
    df = clean_data(_df)

    pickup_rows, delivery_rows = df['job_category'] == "Pickup", df['job_category'] == "Delivery"
    pickup_df, delivery_df = df.loc[pickup_rows].reset_index(drop=True), df.loc[delivery_rows].reset_index(drop=True)
    # We do this but actually delivery_df is useless

    pickup_model_base = build_model(pickup_df)
    pickup_model = pickup_model_base.best_estimator_

    # This part is unnecessary, clean this on refactor
    X_train, y_train, X_test, y_test = train_test_partition(pickup_df)

    # TODO tomorrow
    # tomorrow = X_test['schedule'] == (datetime.datetime.today() + datetime.timedelta(1))
    return pickup_model.predict(X_test)[0]


def load_data(url):
    return pd.read_csv(url)


def clean_data(_df, **kwargs):
    # clean the data
    df = _df.copy()
    df['schedule'] = pd.to_datetime(df['schedule'])

    remove_vars = ['hub_code', 'parcels', 'rider_lastyear', 'rider_52week']
    name_vars = ['job_category']
    dummy_vars = ['quarter', 'day_of_week', 'is_weekend', 'is_holiday', 'is_after_sale']
    value_vars = [x for x in list(df.columns) if x not in remove_vars + name_vars + dummy_vars]

    final_df = df[name_vars + value_vars]
    final_df = final_df.sort_values(['job_category', 'schedule']
                                    , ascending=[True, False])

    def _dummify(var):
        return pd.get_dummies(df[var]
                              , prefix="is")

    dummy_df = [_dummify(x) for x in dummy_vars]

    final_df = pd.concat([final_df] + dummy_df
                         , axis=1)

    # subset the data to only include the last 365 days
    final_df = final_df.loc[final_df['schedule'] >= '2019-01-01']

    return final_df


def build_model(_df, **kwargs):
    df = _df.sort_values('schedule', ascending=False).reset_index(drop=True)

    X_train, y_train, X_test, y_test = train_test_partition(df)

    params = {'n_estimators': [100, 500, 1000]
        , 'learning_rate': [0.01, 0.05, 0.1]}
    clf = GridSearchCV(ensemble.GradientBoostingRegressor(min_samples_split=2
                                                          , loss='ls'
                                                          , random_state=SEED)
                       , params
                       , cv=6)

    _model = clf

    _model.fit(X_train, y_train.values.ravel())

    return _model


def train_test_partition(_df, **kwargs):
    df = _df
    row_count = df.shape[0]
    test_partition = round(row_count * 0.2)

    test_idx = range(test_partition + 1)
    train_idx = range(test_partition + 1, row_count)

    X_vars = list(df.columns)[3:]
    y_vars = ['riders']

    X_train, X_test = df.loc[train_idx, X_vars], df.loc[test_idx, X_vars]
    y_train, y_test = df.loc[train_idx, y_vars], df.loc[test_idx, y_vars]

    return X_train, y_train, X_test, y_test


if __name__ == "__main__":
    print(main("../Data/makati_data.csv"))
