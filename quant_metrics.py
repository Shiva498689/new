from dataclasses import dataclass
from typing import Optional
import pandas as pd
import yfinance as yf
@dataclass
class FinancialMetrics:
    revenue_latest: Optional[float] = None
    revenue_previous: Optional[float] = None
    cogs_latest: Optional[float] = None
    cogs_previous: Optional[float] = None
    gross_profit_latest: Optional[float] = None
    gross_profit_previous: Optional[float] = None
    sga_expenses_latest: Optional[float] = None
    sga_expenses_previous: Optional[float] = None
    depreciation_amortization_latest: Optional[float] = None
    depreciation_amortization_previous: Optional[float] = None
    net_income_latest: Optional[float] = None
    net_income_previous: Optional[float] = None
    net_income_continuing_ops_latest: Optional[float] = None
    net_income_continuing_ops_previous: Optional[float] = None
    operating_cash_flow_latest: Optional[float] = None
    operating_cash_flow_previous: Optional[float] = None
    capex_latest: Optional[float] = None
    capex_previous: Optional[float] = None
    current_assets_latest: Optional[float] = None
    current_assets_previous: Optional[float] = None
    current_liabilities_latest: Optional[float] = None
    current_liabilities_previous: Optional[float] = None
    cash_and_equivalents_latest: Optional[float] = None
    cash_and_equivalents_previous: Optional[float] = None
    receivables_latest: Optional[float] = None
    receivables_previous: Optional[float] = None
    gross_ppe_latest: Optional[float] = None
    gross_ppe_previous: Optional[float] = None
    total_assets_latest: Optional[float] = None
    total_assets_previous: Optional[float] = None
    total_liabilities_latest: Optional[float] = None
    total_liabilities_previous: Optional[float] = None
    long_term_debt_latest: Optional[float] = None
    long_term_debt_previous: Optional[float] = None
    short_term_debt_latest: Optional[float] = None
    short_term_debt_previous: Optional[float] = None

class YahooFinanceExtractor:
    def __init__(self, ticker: str):
        self.ticker = yf.Ticker(ticker)
    def _extract_two_years(self, df, labels):
        if df.empty:
            return None, None
        for label in labels:

            if label in df.index:

                row = df.loc[label]

                latest = None
                previous = None

                if len(row) >= 1 and pd.notna(row.iloc[0]):
                    latest = float(row.iloc[0])

                if len(row) >= 2 and pd.notna(row.iloc[1]):
                    previous = float(row.iloc[1])

                return latest, previous

        return None, None

    def extract(self) -> FinancialMetrics:
        income = self.ticker.financials
        balance = self.ticker.balance_sheet
        cashflow = self.ticker.cashflow
        metrics = FinancialMetrics()
        metrics.revenue_latest, metrics.revenue_previous = self._extract_two_years(
            income,
            ["Total Revenue"]
        )

        metrics.cogs_latest, metrics.cogs_previous = self._extract_two_years(
            income,
            ["Cost Of Revenue"]
        )

        metrics.gross_profit_latest, metrics.gross_profit_previous = self._extract_two_years(
            income,
            ["Gross Profit"]
        )

        metrics.sga_expenses_latest, metrics.sga_expenses_previous = self._extract_two_years(
            income,
            ["Selling General And Administration"]
        )

        metrics.depreciation_amortization_latest, metrics.depreciation_amortization_previous = self._extract_two_years(
            cashflow,
            ["Depreciation And Amortization"]
        )

        metrics.net_income_latest, metrics.net_income_previous = self._extract_two_years(
            income,
            ["Net Income"]
        )

        metrics.net_income_continuing_ops_latest, metrics.net_income_continuing_ops_previous = self._extract_two_years(
            income,
            ["Net Income Continuous Operations"]
        )

        metrics.operating_cash_flow_latest, metrics.operating_cash_flow_previous = self._extract_two_years(
            cashflow,
            ["Operating Cash Flow"]
        )

        metrics.capex_latest, metrics.capex_previous = self._extract_two_years(
            cashflow,
            ["Capital Expenditure"]
        )

        metrics.current_assets_latest, metrics.current_assets_previous = self._extract_two_years(
            balance,
            ["Current Assets"]
        )

        metrics.current_liabilities_latest, metrics.current_liabilities_previous = self._extract_two_years(
            balance,
            ["Current Liabilities"]
        )

        metrics.cash_and_equivalents_latest, metrics.cash_and_equivalents_previous = self._extract_two_years(
            balance,
            ["Cash And Cash Equivalents"]
        )

        metrics.receivables_latest, metrics.receivables_previous = self._extract_two_years(
            balance,
            ["Accounts Receivable"]
        )

        metrics.gross_ppe_latest, metrics.gross_ppe_previous = self._extract_two_years(
            balance,
            ["Gross PPE"]
        )

        metrics.total_assets_latest, metrics.total_assets_previous = self._extract_two_years(
            balance,
            ["Total Assets"]
        )

        metrics.total_liabilities_latest, metrics.total_liabilities_previous = self._extract_two_years(
            balance,
            ["Total Liabilities Net Minority Interest"]
        )

        metrics.long_term_debt_latest, metrics.long_term_debt_previous = self._extract_two_years(
            balance,
            ["Long Term Debt"]
        )

        metrics.short_term_debt_latest, metrics.short_term_debt_previous = self._extract_two_years(
            balance,
            ["Current Debt"]
        )

        return metrics