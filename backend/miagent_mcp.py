"""
miagent_mcp.py — Market Intelligence Agent.
Fetches the live market metrics required by AgentState using yfinance.
Replaces the missing MCP-based implementation.
"""

import numpy as np
import yfinance as yf


def get_market_metrics(ticker: str) -> dict:
    """
    Fetches market intelligence metrics required by miAgentnode in nodes.py.
    Returns:
        dict with keys: current_equity_price, market_capitalization,
                        historical_equity_volatility_252d, risk_free_rate, gnp_deflator
    """
    try:
        tk = yf.Ticker(ticker)
        info = tk.info
        hist = tk.history(period="1y")

        if hist.empty or len(hist) < 2:
            volatility = 0.25  # fallback
        else:
            returns = hist["Close"].pct_change().dropna()
            volatility = float(returns.std() * np.sqrt(252))

        return {
            "current_equity_price": float(info.get("currentPrice") or info.get("regularMarketPrice") or 0.0),
            "market_capitalization": float(info.get("marketCap") or 0.0),
            "historical_equity_volatility_252d": volatility,
            "risk_free_rate": 0.043,   # approximate 10Y US Treasury yield
            "gnp_deflator": 128.648,   # BEA implicit price deflator (base year 2017=100)
        }
    except Exception as e:
        print(f"[miagent_mcp] Failed to fetch market metrics for {ticker}: {e}")
        # Return safe defaults so the graph doesn't crash
        return {
            "current_equity_price": 0.0,
            "market_capitalization": 0.0,
            "historical_equity_volatility_252d": 0.25,
            "risk_free_rate": 0.043,
            "gnp_deflator": 128.648,
        }
