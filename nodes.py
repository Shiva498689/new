from langgraph.graph import START, END , StateGraph
from typing import TypedDict , Literal , Optional , List  , Any , Dict
from ingestion_agent import run_ingestion_pipeline
from quant_metrics import YahooFinanceExtractor
from error_handlers import handle_node_errors
from typing import Dict, Any
from analysis_agent import calculate_all_scores
from quant_web import run_pipeline
import json
from consumer_analysis import analysis_genrator
import os
from dotenv import load_dotenv
load_dotenv()
from miagent_mcp import get_market_metrics
from typing import Dict, Any
from google.genai.types import GenerateContentConfig
from google import genai
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


class _StateProxy:
    """Bridges AgentState (TypedDict/dict) to attribute-style access.
    Required by analysis_agent.py which uses state.field_name notation.
    Math code is locked — this adapter is the correct integration boundary."""
    def __init__(self, d: dict):
        self.__dict__.update(d)
# the structure is 
#     risk_report = {
#     "piotroski": {...},
#     "beneish": {...},
#     "ohlson": {...},
#     "merton": {...},
#     "dcf_risk": {...},
#     "overall_assessment": {...}
# }
    # return {

    #     "piotroski_f_score":
    #         Tier1FormulaRegistry.calculate_piotroski_f_score(state),

    #     "beneish_m_score":
    #         Tier1FormulaRegistry.calculate_beneish_m_score(state),

    #     "ohlson_o_score_probability":
    #         Tier1FormulaRegistry.calculate_ohlson_o_score(state),

    #     "merton_distance_to_default":
    #         Tier1FormulaRegistry.calculate_merton_distance_to_default(state),
            
    #     "monte_carlo_dcf ": Tier1FormulaRegistry.calculate_monte_carlo_dcf(state)
    # }
    
        # return {
        #     "deterministic_dcf_value": float(deterministic_value),
        #     "monte_carlo_p10_floor": float(np.percentile(simulations, 10)),
        #     "monte_carlo_p50_median": float(np.percentile(simulations, 50)),
        #     "monte_carlo_p90_ceiling": float(np.percentile(simulations, 90)),
        # }
class AgentState(TypedDict):
    groq_api_key  :str
    ticker: str
    revenue_latest: Optional[float]
    revenue_previous: Optional[float]
    cogs_latest: Optional[float]
    cogs_previous: Optional[float]
    gross_profit_latest: Optional[float]
    gross_profit_previous: Optional[float]
    sga_expenses_latest: Optional[float]
    sga_expenses_previous: Optional[float]
    depreciation_amortization_latest: Optional[float]
    depreciation_amortization_previous: Optional[float]
    net_income_latest: Optional[float]
    net_income_previous: Optional[float]
    net_income_continuing_ops_latest: Optional[float]
    net_income_continuing_ops_previous: Optional[float]
    operating_cash_flow_latest: Optional[float]
    operating_cash_flow_previous: Optional[float]
    capex_latest: Optional[float]
    capex_previous: Optional[float]
    current_assets_latest: Optional[float]
    current_assets_previous: Optional[float]
    current_liabilities_latest: Optional[float]
    current_liabilities_previous: Optional[float]
    cash_and_equivalents_latest: Optional[float]
    cash_and_equivalents_previous: Optional[float]
    receivables_latest: Optional[float]
    receivables_previous: Optional[float]
    gross_ppe_latest: Optional[float]
    gross_ppe_previous: Optional[float]
    total_assets_latest: Optional[float]
    total_assets_previous: Optional[float]
    total_liabilities_latest: Optional[float]
    total_liabilities_previous: Optional[float]
    long_term_debt_latest: Optional[float]
    long_term_debt_previous: Optional[float]
    short_term_debt_latest: Optional[float]
    short_term_debt_previous: Optional[float]
    shareholders_equity: Optional[float] = None
    common_shares_outstanding: Optional[float] = None
    #mi agent 
    current_equity_price: float
    market_capitalization: float
    historical_equity_volatility_252d: float
    risk_free_rate: float
    gnp_deflator: float
    piotroski_f_score: int
    beneish_m_score: float
    ohlson_o_score_probability: float
    merton_distance_to_default: Optional[float]
    deterministic_dcf_value: float
    monte_carlo_p10_floor: float 
    monte_carlo_p50_median: float
    monte_carlo_p90_ceiling: float
    # execution_tier: 1# Tier 1 (Hardcoded), Tier 2 (Cache), Tier 3 (LLM String Fallback)
    # The output of the RI agent,
    metric_name: str
    computed_value: Any
    formula_expression: str
    inputs_referenced: List[str]
    #ri agent
    risk_report: Dict[str, Any]
    quant_analysis: Optional[Dict[Any, Any]]
    narrative_analysis: List[Dict[Any , Any]]
    analysis_ri_agent : Optional[Dict[Any , Any]]
    error: Optional[str] = None

@handle_node_errors("quant_web")
def quant_web_node(state:AgentState) -> AgentState:
    audit_ledger = run_pipeline(state["ticker"]  , state["groq_api_key"])
    state["quant_analysis"] = audit_ledger
    return state

@handle_node_errors("narrative")
def narrative_analysis_node(state :AgentState) -> AgentState:
    answer  = analysis_genrator(state["ticker"])
    state["narrative_analysis"] = answer
    return state

@handle_node_errors("risk_flagging")
def Risk_flagging_node(state: AgentState) -> AgentState:
    risk_report = {}
    f = state["piotroski_f_score"]

    if f >= 7:
        f_flag = "LOW_RISK"
    elif f >= 4:
        f_flag = "MEDIUM_RISK"
    else:
        f_flag = "HIGH_RISK"
    risk_report["piotroski"] = {
        "score": f,
        "risk": f_flag,
        "interpretation": "Higher score indicates strong financial strength and improving fundamentals"
    }
    m = state["beneish_m_score"]
    if m < -2.5:
        m_flag = "LOW_RISK"
    elif m < -1.5:
        m_flag = "MEDIUM_RISK"
    else:
        m_flag = "HIGH_RISK (possible earnings manipulation)"
    risk_report["beneish"] = {
        "score": m,
        "risk": m_flag,
        "interpretation": "Detects probability of earnings manipulation"
    }

    o = state["ohlson_o_score_probability"]
    if o < 0.3:
        o_flag = "LOW_RISK"
    elif o < 0.6:
        o_flag = "MEDIUM_RISK"
    else:
        o_flag = "HIGH_RISK (distress probability elevated)"

    risk_report["ohlson"] = {
        "score": o,
        "risk": o_flag,
        "interpretation": "Probability of financial distress"
    }
    d = state["merton_distance_to_default"]

    if d > 2.5:
        d_flag = "LOW_RISK"
    elif d > 1.0:
        d_flag = "MEDIUM_RISK"
    else:
        d_flag = "HIGH_RISK (default proximity)"

    risk_report["merton"] = {
        "score": d,
        "risk": d_flag,
        "interpretation": "Distance from default boundary (structural credit risk)"
    }

    p10 = state["monte_carlo_p10_floor"]
    p50 = state["monte_carlo_p50_median"]
    p90 = state["monte_carlo_p90_ceiling"]
    upside = (p90 - p50) / (p50 + 1e-9)
    downside = (p50 - p10) / (p50 + 1e-9)
    if upside < 0.2 and downside < 0.2:
        dcf_flag = "LOW_RISK"
    elif upside < 0.5:
        dcf_flag = "MEDIUM_RISK"
    else:
        dcf_flag = "HIGH_RISK (valuation uncertainty high)"

    risk_report["dcf_risk"] = {
        "p10": p10,
        "p50": p50,
        "p90": p90,
        "risk": dcf_flag,
        "interpretation": "Measures uncertainty in valuation distribution"
    }
    score_map = {
        "LOW_RISK": 1,
        "MEDIUM_RISK": 2,
        "HIGH_RISK": 3
    }
    total = 0
    count = 0

    for k in ["piotroski", "beneish", "ohlson", "merton", "dcf_risk"]:
        r = risk_report[k]["risk"]
        if "HIGH" in r:
            r_val = 3
        elif "MEDIUM" in r:
            r_val = 2
        else:
            r_val = 1

        total += r_val
        count += 1

    avg_risk = total / count
    if avg_risk <= 1.4:
        overall = "HEALTHY (Low Risk Company)"
    elif avg_risk <= 2.2:
        overall = "MODERATE RISK"
    else:
        overall = "STRESSED / HIGH RISK COMPANY"
    risk_report["overall_assessment"] = {
        "average_risk_score": avg_risk,
        "final_verdict": overall,
        "components_analyzed": list(score_map.keys())
    }
    state["risk_report"] = risk_report
    return state

@handle_node_errors("market_intelligence")
def miAgentnode(state :AgentState) -> AgentState:
    metrics  = get_market_metrics(state["ticker"])
    state["gnp_deflator"] = metrics["gnp_deflator"]
    state["current_equity_price"] = metrics["current_equity_price"]
    state["market_capitalization"]  =metrics["market_capitalization"]
    state["historical_equity_volatility_252d"] = metrics["historical_equity_volatility_252d"]
    state["risk_free_rate"] = metrics["risk_free_rate"]
    return state 

@handle_node_errors("analysis")
def Analysis_agent_node(state: AgentState) -> AgentState:
   proxy = _StateProxy(state)
   a = calculate_all_scores(proxy)
   # Handle trailing-space key variation from analysis_agent.py
   dcf = a.get("monte_carlo_dcf") or a.get("monte_carlo_dcf ")
   state["piotroski_f_score"] = a["piotroski_f_score"]
   state["ohlson_o_score_probability"] = a["ohlson_o_score_probability"]
   state["beneish_m_score"] = a["beneish_m_score"]
   state["merton_distance_to_default"] = a["merton_distance_to_default"]
   state["deterministic_dcf_value"] = dcf["deterministic_dcf_value"]
   state["monte_carlo_p10_floor"] = dcf["monte_carlo_p10_floor"]
   state["monte_carlo_p50_median"] = dcf["monte_carlo_p50_median"]
   state["monte_carlo_p90_ceiling"] = dcf["monte_carlo_p90_ceiling"]
   return state 



@handle_node_errors("report_generation")
def report_generation_node(state: Dict[str, Any]) -> Dict[str, Any]:
 

    report_payload = {
        "risk_report": state.get("risk_report"),
        "quant_analysis": state.get("quant_analysis"),
        "narrative_analysis": state.get("narrative_analysis"),
        "analysis_ri_agent": state.get("analysis_ri_agent"),

        "market_metrics": {
            "current_equity_price": state.get("current_equity_price"),
            "market_capitalization": state.get("market_capitalization"),
            "historical_equity_volatility_252d": state.get("historical_equity_volatility_252d"),
            "risk_free_rate": state.get("risk_free_rate"),
            "gnp_deflator": state.get("gnp_deflator"),
        },

        "risk_models": {
            "piotroski_f_score": state.get("piotroski_f_score"),
            "beneish_m_score": state.get("beneish_m_score"),
            "ohlson_o_score_probability": state.get("ohlson_o_score_probability"),
            "merton_distance_to_default": state.get("merton_distance_to_default"),
        },

        "valuation": {
            "deterministic_dcf_value": state.get("deterministic_dcf_value"),
            "monte_carlo_p10_floor": state.get("monte_carlo_p10_floor"),
            "monte_carlo_p50_median": state.get("monte_carlo_p50_median"),
            "monte_carlo_p90_ceiling": state.get("monte_carlo_p90_ceiling"),
        },
    }

    system_prompt = """
You are the presentation engine of an institutional financial due-diligence platform.
Be proffesional and do not use those emojis anywhere .
IMPORTANT

Do NOT perform any new financial analysis.

Do NOT invent numbers.

Do NOT estimate anything.

Use ONLY the supplied JSON.

Your task is ONLY to convert the supplied information into an executive-quality markdown report.

The markdown should render beautifully inside Markdown / ReactMarkdown.

Requirements

# Executive Summary

# Overall Risk Rating

# Investment Recommendation

# Business Overview

# Quantitative Financial Analysis

- tables
- callouts
- KPI cards

# Narrative Analysis

Summarize every qualitative finding.

# Risk Assessment

Highlight

- Severe Risks
- Moderate Risks
- Low Risks

Use markdown blockquotes and warning emojis.

# Fraud Detection

Present

- Piotrosski F Score
- Beneish M Score
- Ohlson O Score
- Merton Distance to Default

Explain what every metric means.

# Valuation

Display

- Deterministic DCF
- Monte Carlo P10
- Monte Carlo Median
- Monte Carlo P90

Create comparison tables.

# Missing Data

Every unavailable value must appear under

## Data Unavailable

Never hide missing information.

# Risk Flags

Highlight every

- anomaly
- manipulation signal
- accounting concern
- liquidity concern
- leverage concern

# Visualizations

Do NOT generate images.

Instead generate Mermaid diagrams.

Generate

1. pie charts

2. flowcharts

3. quadrant diagrams

4. gantt if useful

5. sequence diagrams if useful

6. xychart-beta

7. Sankey

8. timeline

9. mindmap

10. architecture diagram

11. gitgraph if useful

Use Mermaid syntax.

Use markdown tables wherever appropriate.

Output ONLY markdown.

No JSON.
"""

    user_prompt = json.dumps(report_payload, indent=2)

    response = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=user_prompt,
        config=GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=0.15,
        ),
    )

    state["markdown_report"] = response.text

    return state
@handle_node_errors("ingestion")
def ingestion_node(state : AgentState) -> AgentState:
    run_ingestion_pipeline(state["ticker"])
    ticker = state["ticker"]
    extractor = YahooFinanceExtractor(state["ticker"])
    metrics = extractor.extract()
    state["revenue_latest"] = metrics.revenue_latest
    state["revenue_previous"] = metrics.revenue_previous
    state["cogs_latest"] = metrics.cogs_latest
    state["cogs_previous"] = metrics.cogs_previous
    state["gross_profit_latest"] = metrics.gross_profit_latest
    state["gross_profit_previous"] = metrics.gross_profit_previous
    state["sga_expenses_latest"] = metrics.sga_expenses_latest
    state["sga_expenses_previous"] = metrics.sga_expenses_previous
    state["depreciation_amortization_latest"] = metrics.depreciation_amortization_latest
    state["depreciation_amortization_previous"] = metrics.depreciation_amortization_previous
    state["net_income_latest"] = metrics.net_income_latest
    state["net_income_previous"] = metrics.net_income_previous
    state["net_income_continuing_ops_latest"] = metrics.net_income_continuing_ops_latest
    state["net_income_continuing_ops_previous"] = metrics.net_income_continuing_ops_previous
    state["operating_cash_flow_latest"] = metrics.operating_cash_flow_latest
    state["operating_cash_flow_previous"] = metrics.operating_cash_flow_previous
    state["capex_latest"] = metrics.capex_latest
    state["capex_previous"] = metrics.capex_previous
    state["current_assets_latest"] = metrics.current_assets_latest
    state["current_assets_previous"] = metrics.current_assets_previous
    state["current_liabilities_latest"] = metrics.current_liabilities_latest
    state["current_liabilities_previous"] = metrics.current_liabilities_previous
    state["cash_and_equivalents_latest"] = metrics.cash_and_equivalents_latest
    state["cash_and_equivalents_previous"] = metrics.cash_and_equivalents_previous
    state["receivables_latest"] = metrics.receivables_latest
    state["receivables_previous"] = metrics.receivables_previous
    state["gross_ppe_latest"] = metrics.gross_ppe_latest
    state["gross_ppe_previous"] = metrics.gross_ppe_previous
    state["total_assets_latest"] = metrics.total_assets_latest
    state["total_assets_previous"] = metrics.total_assets_previous
    state["total_liabilities_latest"] = metrics.total_liabilities_latest
    state["total_liabilities_previous"] = metrics.total_liabilities_previous
    state["long_term_debt_latest"] = metrics.long_term_debt_latest
    state["long_term_debt_previous"] = metrics.long_term_debt_previous
    state["short_term_debt_latest"] = metrics.short_term_debt_latest
    state["short_term_debt_previous"] = metrics.short_term_debt_previous
    return state


# class FinancialDueDiligence():
#     def __init__ (self , ticker):
#         self.builder  = StateGraph(AgentState)
#         self.builder.ticker = ticker 

#         self.buider.add_node()
#         pass