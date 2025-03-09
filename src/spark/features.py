from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, stddev, lag, lead, when, sum, lit, row_number, expr
import pyspark.sql.functions as F

def calculate_sma(df, column, window_size):
    """
    Calculate average price over a specific time period
    
    Args:
        df: DataFrame with time series data
        column: Column to calculate SMA for
        window_size: Window size for the moving average
        
    Returns:
        DataFrame with SMA column added
    """
    window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-(window_size-1), 0)
    
    return df.withColumn(f"sma_{window_size}", avg(col(column)).over(window_spec))

def calculate_ema(df, column, window_size):
    """
    Calculate average prices but gives more weight to recent prices
    
    Args:
        df: DataFrame with time series data
        column: Column to calculate EMA for
        window_size: Window size for the moving average
        
    Returns:
        DataFrame with EMA column added
    """
    # Calculate the multiplier
    multiplier = 2.0 / (window_size + 1)
    
    # Define a window specification for ordering
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    # Calculate initial SMA for the first window_size records
    sma_df = calculate_sma(df, column, window_size)
    
    # Initialize EMA with SMA for the first window_size rows
    ema_df = sma_df.withColumn(
        f"ema_{window_size}_temp",
        when(
            row_number().over(window_spec) <= window_size,
            col(f"sma_{window_size}")
        ).otherwise(None)
    )
    
    # Recursively calculate EMA for the rest of the rows
    ema_df = ema_df.withColumn(
        f"ema_{window_size}",
        when(
            col(f"ema_{window_size}_temp").isNotNull(),
            col(f"ema_{window_size}_temp")
        ).otherwise(
            (col(column) * multiplier) + (lag(f"ema_{window_size}", 1).over(window_spec) * (1 - multiplier))
        )
    )
    
    # Drop the temporary column
    return ema_df.drop(f"ema_{window_size}_temp")

def calculate_bollinger_bands(df, column, window_size=20, num_std=2):
    """
    Measure the volatility of a time series (how much price moves up/down in given period)
    
    Args:
        df: DataFrame with time series data
        column: Column to calculate Bollinger Bands for
        window_size: Window size (default: 20)
        num_std: Number of standard deviations (default: 2)
        
    Returns:
        DataFrame with Bollinger Bands columns added
    """
    window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-(window_size-1), 0)
    
    # Calculate middle band (SMA)
    result_df = df.withColumn(f"bb_middle_{window_size}", avg(col(column)).over(window_spec))
    
    # Calculate standard deviation
    result_df = result_df.withColumn(f"bb_std_{window_size}", stddev(col(column)).over(window_spec))
    
    # Calculate upper and lower bands
    result_df = result_df.withColumn(
        f"bb_upper_{window_size}",
        col(f"bb_middle_{window_size}") + (col(f"bb_std_{window_size}") * num_std)
    )
    
    result_df = result_df.withColumn(
        f"bb_lower_{window_size}",
        col(f"bb_middle_{window_size}") - (col(f"bb_std_{window_size}") * num_std)
    )
    
    # Drop the temporary standard deviation column
    return result_df.drop(f"bb_std_{window_size}")

def calculate_rsi(df, column, window_size=14):
    """
    Calculate Relative Strength Index (RSI). Measures momentum
    
    Args:
        df: DataFrame with time series data
        column: Column to calculate RSI for
        window_size: Window size (default: 14)
        
    Returns:
        DataFrame with RSI column added
    """
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    # Calculate price changes
    result_df = df.withColumn("price_change", col(column) - lag(col(column), 1).over(window_spec))
    
    # Calculate gains and losses
    result_df = result_df.withColumn(
        "gain",
        when(col("price_change") > 0, col("price_change")).otherwise(0)
    )
    
    result_df = result_df.withColumn(
        "loss",
        when(col("price_change") < 0, -col("price_change")).otherwise(0)
    )
    
    # Calculate average gains and losses
    window_spec_agg = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-(window_size-1), 0)
    
    result_df = result_df.withColumn("avg_gain", avg(col("gain")).over(window_spec_agg))
    result_df = result_df.withColumn("avg_loss", avg(col("loss")).over(window_spec_agg))
    
    # Calculate RS and RSI
    result_df = result_df.withColumn(
        "rs",
        when(col("avg_loss") == 0, lit(100)).otherwise(col("avg_gain") / col("avg_loss"))
    )
    
    result_df = result_df.withColumn(
        f"rsi_{window_size}",
        when(col("avg_loss") == 0, lit(100)).otherwise(100 - (100 / (1 + col("rs"))))
    )
    
    # Drop temporary columns
    return result_df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs")

def calculate_macd(df, column, fast_period=12, slow_period=26, signal_period=9):
    """
    Calculate Moving Average Convergence Divergence. Tracks trend strength 
    
    Args:
        df: DataFrame with time series data
        column: Column to calculate MACD for
        fast_period: Fast EMA period (default: 12)
        slow_period: Slow EMA period (default: 26)
        signal_period: Signal line period (default: 9)
        
    Returns:
        DataFrame with MACD columns added
    """
    # Calculate fast and slow EMAs
    result_df = calculate_ema(df, column, fast_period)
    result_df = calculate_ema(result_df, column, slow_period)
    
    # Calculate MACD line
    result_df = result_df.withColumn(
        "macd_line",
        col(f"ema_{fast_period}") - col(f"ema_{slow_period}")
    )
    
    # Calculate signal line (EMA of MACD line)
    window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-(signal_period-1), 0)
    
    result_df = result_df.withColumn(
        "macd_signal",
        avg(col("macd_line")).over(window_spec)
    )
    
    # Calculate MACD histogram
    result_df = result_df.withColumn(
        "macd_histogram",
        col("macd_line") - col("macd_signal")
    )
    
    return result_df

def calculate_volatility(df, column, window_size=5):
    """
    Calculate price volatility as standard deviation over specified period
    
    Args:
        df: DataFrame with time series data
        column: Column to calculate volatility for
        window_size: Window size (default: 5)
        
    Returns:
        DataFrame with volatility column added
    """
    window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-(window_size-1), 0)
    
    # Calculate standard deviation of prices
    result_df = df.withColumn(
        f"volatility_{window_size}",
        stddev(col(column)).over(window_spec)
    )
    
    # Also calculate as percentage of price
    result_df = result_df.withColumn(
        f"volatility_pct_{window_size}",
        (col(f"volatility_{window_size}") / col(column)) * 100
    )
    
    return result_df

def calculate_price_momentum(df, column, periods=[1, 3, 6]):
    """
    Calculate price momentum (percentage change) over various periods
    
    Args:
        df: DataFrame with time series data
        column: Column to calculate momentum for
        periods: List of periods to calculate momentum for
        
    Returns:
        DataFrame with momentum columns added
    """
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    result_df = df
    
    for period in periods:
        result_df = result_df.withColumn(
            f"momentum_{period}",
            (col(column) / lag(col(column), period).over(window_spec) - 1) * 100
        )
    
    return result_df

def add_prediction_targets(df, price_column="price", horizon=1):
    """
    Add future price as prediction target (for 4-hour forecasting)
    
    Args:
        df: DataFrame with time series data
        price_column: Column name for price data
        horizon: Number of periods to look ahead
        
    Returns:
        DataFrame with prediction targets
    """
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    # Add future price as target
    result_df = df.withColumn(
        f"target_price_{horizon}",
        lead(col(price_column), horizon).over(window_spec)
    )
    
    # Add price change percentage as alternative target
    result_df = result_df.withColumn(
        f"target_pct_change_{horizon}",
        when(col(f"target_price_{horizon}").isNotNull(),
             (col(f"target_price_{horizon}") - col(price_column)) / col(price_column) * 100
        ).otherwise(None)
    )
    
    # Add target direction (up/down) - useful for classification
    result_df = result_df.withColumn(
        f"target_direction_{horizon}",
        when(col(f"target_pct_change_{horizon}") > 0, 1).otherwise(0)
    )
    
    return result_df

def create_time_features(df, timestamp_column="timestamp"):
    """
    Create time-based features from timestamp
    
    Args:
        df: DataFrame with time series data
        timestamp_column: Column name with timestamp data
        
    Returns:
        DataFrame with time features added
    """
    result_df = df \
        .withColumn("hour_of_day", F.hour(col(timestamp_column))) \
        .withColumn("day_of_week", F.dayofweek(col(timestamp_column))) \
        .withColumn("day_of_month", F.dayofmonth(col(timestamp_column))) \
        .withColumn("month", F.month(col(timestamp_column))) \
        .withColumn("is_weekend", when(F.dayofweek(col(timestamp_column)).isin([1, 7]), 1).otherwise(0))
    
    return result_df

def add_all_features(df, price_column="price"):
    """
    Add all technical indicators as features - optimized for 4-hour predictions
    
    Args:
        df: DataFrame with time series data
        price_column: Column name for price data (default: "price")
        
    Returns:
        DataFrame with all technical indicators added
    """
    result_df = df
    
    # Add time-based features
    result_df = create_time_features(result_df)
    
    # Add SMAs with different periods - adjusted for 4-hour timeframe
    for period in [3, 6, 12, 24, 72]:  # 12h, 24h, 48h, 4 days, 12 days
        result_df = calculate_sma(result_df, price_column, period)
    
    # Add EMAs with different periods
    for period in [3, 6, 12, 24, 72]:
        result_df = calculate_ema(result_df, price_column, period)
    
    # Add Bollinger Bands
    result_df = calculate_bollinger_bands(result_df, price_column, 24, 2)  # 4 days
    
    # Add RSI
    result_df = calculate_rsi(result_df, price_column, 14)  # 56 hours
    
    # Add MACD - optimized for 4-hour candles
    result_df = calculate_macd(result_df, price_column, 3, 9, 4)  # 12h, 36h, 16h
    
    # Add volatility
    result_df = calculate_volatility(result_df, price_column, 6)  # 24 hours
    
    # Add price momentum
    result_df = calculate_price_momentum(result_df, price_column, [1, 3, 6, 12])
    
    # Add prediction targets
    result_df = add_prediction_targets(result_df, price_column, 1)  # 1 period ahead (4 hours)
    
    return result_df