1. Market data ingestion
   WebSockets / REST for prices & funding.
   Normalisation across venues (symbols, units, timeframes).
   Persistence (DB) for analysis + monitoring.
2. Arb engine
   Look at latest state → detect opportunities (with fees, skew limits, caps).
   Decide how much to trade and on which side (risk limits).
3. Execution
   Place/cancel orders on venues.
   Handle errors, retries, partial fills, rate limits.
   Track open positions & PnL.
4. Monitoring & control
   Logs, metrics, health checks.
   Kill-switch, max drawdown, “stop trading” switch.
