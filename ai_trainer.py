import os
import time
import threading
from datetime import datetime
import pandas as pd
import joblib
from dotenv import load_dotenv
from xgboost import XGBClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt

# Загружаем переменные окружения из .env
load_dotenv()
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")

SIGNALS_LOG = 'signals_log.csv'
MODEL_PATH = 'ai_model_xgb.pkl'
ENCODER_PATH = 'encoder_xgb.pkl'
NEW_FEATURES_LOG = 'feature_search_log.csv'
RETRAIN_INTERVAL_MINUTES = 60  # Переобучение каждый час

######################
# BYBIT API ИНТЕГРАЦИЯ
######################
from pybit.unified_trading import HTTP

class BybitAPI:
    def __init__(self, api_key, api_secret, testnet=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.session = HTTP(
            api_key=self.api_key,
            api_secret=self.api_secret,
            testnet=self.testnet
        )

    def send_order(self, symbol, side, volume, price=None, order_type='Market', leverage=None, tp=None, sl=None, category="linear"):
        params = {
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": volume,
            "category": category,
        }
        if order_type == 'Limit' and price is not None:
            params['price'] = price
            params['timeInForce'] = 'GTC'
        if tp is not None:
            params["takeProfit"] = tp
        if sl is not None:
            params["stopLoss"] = sl
        try:
            result = self.session.place_order(**params)
            print(f"[BYBIT] Order result: {result}")
            return result
        except Exception as e:
            print(f"[BYBIT] Ошибка при отправке ордера: {e}")

######################
# DATA & FEATURES
######################
def load_data():
    df = pd.read_csv(SIGNALS_LOG)
    # Проверка и назначение правильных заголовков
    if 'result' not in df.columns:
        if 'ai_result' in df.columns:
            df.rename(columns={'ai_result': 'result'}, inplace=True)
        else:
            # Назначаем названия столбцов по количеству столбцов в файле
            if len(df.columns) == 11:
                df.columns = ['timestamp','symbol','price','direction','strategy','rsi','ema','volume','result','target_price','target_range']
            elif len(df.columns) == 18:
                df.columns = ['timestamp','symbol','price','direction','strategy','rsi','ema','volume','result','target_price','target_range','weekday','hour','price_diff','strategy_symbol','strategy_weekday','hour_range','prediction']
            else:
                raise ValueError(f"signals_log.csv: Неожиданное количество столбцов ({len(df.columns)}).")
    df['result'] = df['result'].apply(lambda x: 1 if str(x).strip() == '1' else 0)
    df = df.fillna(-1)
    for col in ['rsi', 'ema']:
        if (df[col] == -1).any():
            df[col] = df[col].replace(-1, df[col].mean())
    df['weekday'] = pd.to_datetime(df['timestamp']).dt.weekday
    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    # --- Безопасно парсим price_diff по target_price ---
    def parse_tp(tp, price):
        if pd.isnull(tp):
            return 0
        if isinstance(tp, str):
            tp_str = tp.strip().replace('"','').replace("'","")
            if ',' in tp_str:
                try:
                    tp_val = float(tp_str.split(',')[0])
                except Exception:
                    tp_val = float(price)
            else:
                try:
                    tp_val = float(tp_str)
                except Exception:
                    tp_val = float(price)
        else:
            try:
                tp_val = float(tp)
            except Exception:
                tp_val = float(price)
        return tp_val - float(price)
    df['price_diff'] = df.apply(lambda row: parse_tp(row['target_price'], row['price']), axis=1)
    df = generate_strategy_combinations(df)
    df = generate_hour_range_feature(df)
    # --- Используем strategy и symbol в признаках! ---
    cat_features = ['direction', 'strategy', 'symbol', 'weekday', 'hour', 'strategy_symbol', 'strategy_weekday', 'hour_range']
    enc = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    cat_data = enc.fit_transform(df[cat_features])
    cat_cols = enc.get_feature_names_out(cat_features)
    num_features = ['price', 'volume', 'rsi', 'ema', 'price_diff']
    num_data = df[num_features].astype(float)
    features = pd.concat([num_data, pd.DataFrame(cat_data, columns=cat_cols)], axis=1)
    target = df['result']
    return features, target, enc, num_features, cat_features, features.columns.tolist()

def generate_strategy_combinations(df):
    df['strategy_symbol'] = df['strategy'].astype(str) + '_' + df['symbol'].astype(str)
    df['strategy_weekday'] = df['strategy'].astype(str) + '_' + df['weekday'].astype(str)
    return df

def generate_hour_range_feature(df):
    bins = [0, 7, 12, 16, 21, 24]
    labels = ['night', 'morning', 'day', 'evening', 'late']
    df['hour_range'] = pd.cut(df['hour'], bins=bins, labels=labels, include_lowest=True)
    return df

def plot_feature_importance(model, feature_names, out_path='feature_importance.png'):
    importance = model.feature_importances_
    sorted_idx = importance.argsort()
    plt.figure(figsize=(12, 8))
    plt.barh(range(len(feature_names)), importance[sorted_idx], align='center')
    plt.yticks(range(len(feature_names)), [feature_names[i] for i in sorted_idx])
    plt.xlabel('Важность')
    plt.title('Важность признаков (XGBoost)')
    plt.tight_layout()
    plt.savefig(out_path)
    print(f'График важности сохранён: {out_path}')

def log_feature_search(results):
    df = pd.DataFrame(results)
    if os.path.exists(NEW_FEATURES_LOG):
        df_old = pd.read_csv(NEW_FEATURES_LOG)
        df = pd.concat([df_old, df], ignore_index=True)
    df.to_csv(NEW_FEATURES_LOG, index=False)
    print(f'Лог новых признаков сохранён: {NEW_FEATURES_LOG}')

######################
# ОБУЧЕНИЕ/РЕТРАЙНИНГ
######################
def train_model(auto_retrain=True):
    X, y, enc, num_features, cat_features, all_feature_names = load_data()
    from collections import Counter
    print("=== Баланс классов:", Counter(y))
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [3, 5, 10],
        'learning_rate': [0.01, 0.05, 0.1],
        'subsample': [0.7, 0.8, 1.0],
        'colsample_bytree': [0.8, 1.0],
        'gamma': [0, 0.1, 0.3],
        'scale_pos_weight': [float(sum(y_train == 0)) / max(1, sum(y_train == 1))]
    }
    model = XGBClassifier(
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss'
    )
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        scoring='f1',
        cv=StratifiedKFold(n_splits=3, shuffle=True, random_state=42),
        verbose=2,
        n_jobs=-1
    )
    grid_search.fit(X_train, y_train)
    best_model = grid_search.best_estimator_
    joblib.dump(best_model, MODEL_PATH)
    joblib.dump(enc, ENCODER_PATH)
    print("XGBoost модель (GridSearch) и энкодер сохранены!")
    print(f"Лучшие параметры: {grid_search.best_params_}")
    y_pred = best_model.predict(X_test)
    print("=== Классификационный отчёт (XGBoost+GridSearch) ===")
    print(classification_report(y_test, y_pred))
    print("=== Матрица ошибок ===")
    print(confusion_matrix(y_test, y_pred))
    plot_feature_importance(best_model, X_train.columns)
    results = [{
        'timestamp': str(datetime.utcnow()),
        'features_used': '|'.join(all_feature_names),
        'f1_score': grid_search.best_score_
    }]
    log_feature_search(results)
    if auto_retrain:
        monitor_and_retrain()

def monitor_and_retrain():
    pass

def auto_retrain_loop():
    while True:
        print(f"[RETRAIN] {datetime.now()} Запуск переобучения...")
        train_model(auto_retrain=False)
        time.sleep(RETRAIN_INTERVAL_MINUTES * 60)

######################
# ПРЕДСКАЗАНИЯ
######################
def make_features_for_prediction(price, volume, rsi, ema, direction, strategy, symbol, timestamp, encoder, num_features, cat_features, tp=None):
    dt = pd.to_datetime(timestamp)
    weekday = dt.weekday()
    hour = dt.hour
    if tp is not None:
        if isinstance(tp, str):
            tp_val_str = tp.strip().replace('"','').replace("'","")
            if ',' in tp_val_str:
                try:
                    tp_val = float(tp_val_str.split(',')[0])
                except Exception:
                    tp_val = float(price)
            else:
                try:
                    tp_val = float(tp_val_str)
                except Exception:
                    tp_val = float(price)
        else:
            try:
                tp_val = float(tp)
            except Exception:
                tp_val = float(price)
    else:
        tp_val = float(price)
    price_diff = tp_val - float(price)
    strategy_symbol = str(strategy) + '_' + str(symbol)
    strategy_weekday = str(strategy) + '_' + str(weekday)
    bins = [0, 7, 12, 16, 21, 24]
    labels = ['night', 'morning', 'day', 'evening', 'late']
    hour_range = pd.cut([hour], bins=bins, labels=labels, include_lowest=True)[0]
    num_vals = [price, volume, rsi, ema, price_diff]
    cat_dict = dict(zip(cat_features, [direction, strategy, symbol, weekday, hour, strategy_symbol, strategy_weekday, hour_range]))
    num_dict = dict(zip(num_features, num_vals))
    df_signal = pd.DataFrame([{**num_dict, **cat_dict}])
    cat_data = encoder.transform(df_signal[cat_features])
    cat_cols = encoder.get_feature_names_out(cat_features)
    final_df = pd.concat([df_signal[num_features].astype(float), pd.DataFrame(cat_data, columns=cat_cols)], axis=1)
    return final_df

def predict_signal(price, volume, rsi, ema, direction, strategy, symbol, timestamp, tp=None):
    print("[AI DEBUG] INPUTS:", price, volume, rsi, ema, direction, strategy, symbol, timestamp, tp)
    # Проверяем корректность даты/времени
    try:
        pd.to_datetime(timestamp)
    except Exception as e:
        print(f"[AI] Некорректный формат даты: {timestamp} ({e})")
        return f"AI ошибка: Unknown datetime string format, unable to parse: {timestamp}, at position 0"
    try:
        encoder = joblib.load(ENCODER_PATH)
        best_model = joblib.load(MODEL_PATH)
    except Exception as e:
        print(f"[AI] Модель или энкодер не найдены: {e}")
        return 1
    num_features = ['price', 'volume', 'rsi', 'ema', 'price_diff']
    cat_features = ['direction', 'strategy', 'symbol', 'weekday', 'hour', 'strategy_symbol', 'strategy_weekday', 'hour_range']
    features = make_features_for_prediction(
        price, volume, rsi, ema, direction, strategy, symbol, timestamp, encoder, num_features, cat_features, tp=tp
    )
    prediction = best_model.predict(features)[0]
    return prediction

######################
# ОСНОВНОЙ AI-ТРЕЙДИНГ ЦИКЛ
######################
def ai_trading_loop(bybit, signal_file=SIGNALS_LOG, interval=300):
    print("[AI] Запуск торгового цикла...")
    while not (os.path.exists(MODEL_PATH) and os.path.exists(ENCODER_PATH)):
        print("[AI] Ожидаю появление файлов модели и энкодера...")
        time.sleep(5)
    num_features = ['price', 'volume', 'rsi', 'ema', 'price_diff']
    cat_features = ['direction', 'strategy', 'symbol', 'weekday', 'hour', 'strategy_symbol', 'strategy_weekday', 'hour_range']
    try:
        encoder = joblib.load(ENCODER_PATH)
        best_model = joblib.load(MODEL_PATH)
    except Exception as e:
        print(f"[AI] Модель или энкодер не найдены: {e}")
        return
    traded_ids = set()
    while True:
        if not os.path.exists(signal_file):
            print(f"[AI] Нет файла сигналов {signal_file}")
            time.sleep(interval)
            continue
        df = pd.read_csv(signal_file)
        if 'id' not in df.columns:
            df['id'] = df.index
        for i, row in df.iterrows():
            signal_id = row['id']
            if signal_id in traded_ids:
                continue
            features = make_features_for_prediction(
                row['price'], row['volume'], row['rsi'], row['ema'],
                row['direction'], row['strategy'], row['symbol'], row['timestamp'],
                encoder, num_features, cat_features, tp=row.get('target_price', None)
            )
            prediction = best_model.predict(features)[0]
            if prediction == 1:
                print(f"[AI] Сигнал {signal_id}: модель считает успешным, отправляем ордер!")
                side = 'Buy' if str(row['direction']).lower() == 'up' else 'Sell'
                symbol = row['symbol']
                volume = row.get('volume', 0.01)
                leverage = row.get('leverage', 5)
                price = row.get('price', None)
                tp = row.get('target_price', None)
                sl = row.get('sl', None)
                order_type = row.get('order_type', 'Market')
                category = "linear" if str(symbol).endswith("USDT") else "spot"
                try:
                    bybit.send_order(
                        symbol=symbol,
                        side=side,
                        volume=volume,
                        price=price,
                        order_type=order_type,
                        leverage=leverage if category == "linear" else None,
                        tp=tp,
                        sl=sl,
                        category=category
                    )
                except Exception as e:
                    print(f"[BYBIT] Ошибка при отправке ордера: {e}")
                traded_ids.add(signal_id)
            else:
                print(f"[AI] Сигнал {signal_id}: модель считает НЕуспешным, пропускаем")
        time.sleep(interval)

######################
# ГЛАВНАЯ ФУНКЦИЯ
######################
def main():
    train_model(auto_retrain=False)
    threading.Thread(target=auto_retrain_loop, daemon=True).start()
    bybit = BybitAPI(BYBIT_API_KEY, BYBIT_API_SECRET, testnet=False)
    ai_trading_loop(bybit, interval=300)

if __name__ == '__main__':
    main()