#UTILS
import pandas as pd
import numpy as np
import os
from utils.data_access import prices_df
from utils.nombres import get_activo_label

from functools import lru_cache
from django.conf import settings
from django.utils import timezone
from django.db import OperationalError



BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKERS_CSV = os.path.join(BASE_DIR, "datasets", "tickers_nombres.csv")



@lru_cache
def _nombres_map() -> dict[str, str]:
    try:
        df = pd.read_csv(TICKERS_CSV)
        tick = df["Ticker"].astype(str).str.upper().str.strip()
        name = df["Nombre"].astype(str).str.strip()
        return dict(zip(tick, name))
    except Exception:
        return {}

def _safe_nombre(ticker: str) -> str:
    return _nombres_map().get(str(ticker).upper().strip(), str(ticker).upper().strip())

def calcular_rsi(series, period= 14):
        
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def calcular_macd_crossover(df, col='close', short=12, long=26, signal=9) -> str:
    """
    Calcula el MACD y su línea de señal a partir de una serie de precios.

    Parámetros:
    - df: DataFrame que contiene una columna con los precios (ej: 'close').
    - col: nombre de la columna a usar para el cálculo (default: 'close').
    - short: periodo para la EMA rápida (default: 12).
    - long: periodo para la EMA lenta (default: 26).
    - signal: periodo para la EMA de la línea de señal (default: 9).

    Retorna:
    - El mismo DataFrame con columnas adicionales: 'macd', 'signal'.
    """

    df = df.copy()

    # Cálculo de las EMAs
    ema_short = df[col].ewm(span=short, adjust=False).mean()
    ema_long = df[col].ewm(span=long, adjust=False).mean()

    # MACD
    df['macd'] = ema_short - ema_long

    # Línea de señal
    df['signal'] = df['macd'].ewm(span=signal, adjust=False).mean()

    if df['macd'].iloc[-1] > df['signal'].iloc[-1]:
          return "BULL"
    else:
          return "BEAR"
    

def calcular_dmi_adx(df, periodo: int = 14) -> list:
    """
    Calcula los indicadores +DI, -DI y ADX a partir de columnas high, low y close.

    Parámetros:
    - df: DataFrame con columnas 'high', 'low', 'close'.
    - periodo: número de períodos para el suavizado (por defecto: 14).

    Retorna:
    - Una lista con: 1) string: Tendencia según último valor de DM_SMOOTH+ Y DM_SMOOTH-; 2) string: fortaleza de tendencia
    """
    
    df = df.copy()

    # Cálculo de diferencias
    df['upMove'] = df['high'].diff()
    df['downMove'] = -df['low'].diff()

    df['+DM'] = ((df['upMove'] > df['downMove']) & (df['upMove'] > 0)) * df['upMove']
    df['-DM'] = ((df['downMove'] > df['upMove']) & (df['downMove'] > 0)) * df['downMove']

    # True Range
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['close'].shift()).abs()
    df['tr3'] = (df['low'] - df['close'].shift()).abs()
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)

    # Suavizado tipo Wilder (EMA especial)
    df['ATR'] = df['TR'].rolling(window=periodo).mean()
    df['+DM_smooth'] = df['+DM'].rolling(window=periodo).mean()
    df['-DM_smooth'] = df['-DM'].rolling(window=periodo).mean()

    # DI+
    df['+DI'] = 100 * (df['+DM_smooth'] / df['ATR'])
    # DI–
    df['-DI'] = 100 * (df['-DM_smooth'] / df['ATR'])

    # DX
    df['DX'] = 100 * (abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI']))
    # ADX (promedio de DX)
    df['ADX'] = df['DX'].rolling(window=periodo).mean()
    tendencia = "STRONG" if df['ADX'].iloc[-1] >= 25 else "WEAK"

    # Limpiar columnas intermedias si querés
    #df = df.drop(columns=['upMove', 'downMove', '+DM', '-DM', 'tr1', 'tr2', 'tr3', 'TR', 'ATR', '+DM_smooth', '-DM_smooth', 'DX'])

    if  df['+DM_smooth'].iloc[-1] > df['-DM_smooth'].iloc[-1]:
         return ["BULL", tendencia]
    else:
         return ["BEAR", tendencia]
    



def obtenerSenalyGrabar(df: pd.DataFrame, ticker: str,
                        estrategia: str = "EMA Crossover",
                        short: int = 5, long: int = 10):
    from senales.models import SenalTecnica  # import local para evitar ciclos

    if df is None or df.empty:
        return

    # --- Normalización mínima y orden temporal ---
    df = df.copy()

    # Aceptar índice datetime o columna date
    if isinstance(df.index, pd.DatetimeIndex) and "date" not in df.columns:
        df = df.reset_index().rename(columns={df.index.name or "index": "date"})

    # Mapear fecha/cierre
    for cand in ["date","timestamp","Timestamp","time","Date","Datetime","INDEX","Index"]:
        if cand in df.columns:
            df = df.rename(columns={cand:"date"}); break

    price_col = "close"
    for cand in ["adj_close","Adj Close","AdjClose","PRICE","Price","close","Close"]:
        if cand in df.columns:
            price_col = "adj_close" if "adj" in cand.lower() or cand in ("PRICE","Price") else "close"
            df = df.rename(columns={cand: price_col})
            break

    if "date" not in df.columns or price_col not in df.columns:
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=["date", price_col]).sort_values("date")
    if df.empty:
        return
    df = df.set_index("date")

    # --- EMAs y cruce sobre la MISMA serie de precio que usarás para rendimiento ---
    s = df[price_col]
    df[f"EMA{short}"] = s.ewm(span=short, adjust=False).mean()
    df[f"EMA{long}"]  = s.ewm(span=long,  adjust=False).mean()
    df["posicion"] = (df[f"EMA{short}"] > df[f"EMA{long}"]).astype(int)
    df["cambio"] = df["posicion"].diff().fillna(0)

    cruces = df[df["cambio"] != 0]
    if cruces.empty:
        # si querés grabar HOLD en vez de salir, descomenta el bloque siguiente:
        # SenalTecnica.objects.update_or_create(
        #     activo=format_activo(ticker),
        #     indicador=estrategia,
        #     defaults={
        #         "fecha": timezone.make_aware(df.index[-1], timezone.get_current_timezone()),
        #         "tipo": "HOLD",
        #         "precio_senal": round(float(s.iloc[-1]), 4),
        #         "precio_actual": round(float(s.iloc[-1]), 4),
        #         "detalle": f"Sin cruce EMA{short}/{long}",
        #         "rendimiento": 0.0,
        #     },
        # )
        return

    ultima = cruces.iloc[-1]
    tipo = "BUY" if float(ultima["cambio"]) > 0 else "SELL"
    detalle = f"Cruce {'alcista' if tipo=='BUY' else 'bajista'} entre EMA{short} y EMA{long}"

    # --- Fecha robusta (evita NaT -> epoch 1969) ---
    fecha_senal = ultima.name
    fecha_senal = pd.to_datetime(fecha_senal, errors="coerce")
    if pd.isna(fecha_senal):
        fecha_senal = df.index[-1]  # fallback sano
    if fecha_senal.tzinfo is None:
        fecha_senal = timezone.make_aware(fecha_senal, timezone.get_current_timezone())

    # --- Precios y rendimiento en la misma escala (preferir adj_close) ---
    precio_senal = float(ultima[price_col])
    precio_actual = float(s.iloc[-1])
    if tipo == "BUY":
        rendimiento = (precio_actual / precio_senal - 1.0) * 100.0
    else:
        rendimiento = (precio_senal / precio_actual - 1.0) * 100.0

    activo_display = get_activo_label(ticker)
    SenalTecnica.objects.update_or_create(
        activo=activo_display,
        indicador=estrategia,
        defaults={
            "fecha": fecha_senal,
            "tipo": tipo,
            "precio_senal": round(precio_senal, 4),
            "precio_actual": round(precio_actual, 4),
            "detalle": detalle,
            "rendimiento": round(float(rendimiento), 2),
        },
    )




def cargar_csv_local(symbol: str, folder: str) -> pd.DataFrame:
    """
    Lee datasets/<folder>/<symbol>.csv y retorna un DF con columnas ['Timestamp','Close'].
    """
    path = os.path.join(BASE_DIR, 'datasets', folder, f'{symbol}.csv')
    df = pd.read_csv(path, usecols=['Timestamp', 'Close'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.sort_values('Timestamp')
    return df



def matriz_precios(activos: list[str], timeframe: str = "diario") -> pd.DataFrame:
    """
    Retorna un DF ancho: index=Date, columnas=tickers, valores=Close.
    Realiza INNER JOIN de fechas (solo intersección).
    """
    tf = "daily" if timeframe == "diario" else "weekly"
    wide = None
    cols = []

    for sym in activos:
        try:
            df = prices_df(symbol = sym, tf = tf)
            s = df.set_index('date')['close'].rename(sym)
            cols.append(s)
        except Exception as e:
            # Podés loguear e imprimir si querés
            # print(f"[WARN] No pude cargar {sym} ({folder}): {e}")
            continue

    if not cols:
        raise ValueError("No se pudo cargar ningún activo. Verifique tickers y CSVs.")

    # INNER JOIN de todas las series por índice (fechas)
    wide = pd.concat(cols, axis=1, join='inner').dropna(how='any')
    if wide.shape[1] < 2:
        raise ValueError("Se necesita al menos 2 activos con fechas coincidentes.")

    return wide


