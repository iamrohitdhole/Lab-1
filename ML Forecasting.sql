-- SQL query code using ML Forecasting and Tasks

-- 1. Training Task: Creating a View and ML Model

-- Creating or replacing a view for the training input table
CREATE OR REPLACE VIEW stock.adhoc.market_data_view AS
SELECT
    CAST(DATE AS TIMESTAMP_NTZ) AS DATE, 
    CLOSE, 
    SYMBOL
FROM stock.stocks.market_data;

-- Creating or replacing the ML forecasting model using Snowflake's ML capabilities
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST stock.analytics.predict_stock_price (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'stock.adhoc.market_data_view'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Evaluating the trained model by displaying metrics
CALL stock.analytics.predict_stock_price!SHOW_EVALUATION_METRICS();


-- 2. Prediction Task: Generating Predictions

-- Starting a transaction to make predictions using the trained model
BEGIN
    -- Calling the forecast function to predict stock prices for 7 days
    CALL stock.analytics.predict_stock_price!FORECAST(
        FORECASTING_PERIODS => 7,
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );

    -- Capturing the results of the forecast into a new table using the SQL ID from the function
    LET x := SQLID;
    CREATE OR REPLACE TABLE stock.adhoc.market_data_forecast AS
    SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- Creating the final table combining actual and forecasted data
CREATE TABLE IF NOT EXISTS stock.analytics.market_data AS
SELECT 
    SYMBOL, 
    DATE, 
    CLOSE AS actual, 
    NULL AS forecast, 
    NULL AS lower_bound, 
    NULL AS upper_bound
FROM stock.stocks.market_data

UNION ALL

SELECT 
    REPLACE(series, '"', '') AS SYMBOL, 
    ts AS DATE, 
    NULL AS actual, 
    forecast, 
    lower_bound, 
    upper_bound
FROM stock.adhoc.market_data_forecast;