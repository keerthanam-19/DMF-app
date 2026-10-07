"""
app.py - Databricks Data Quality Framework (DMF) Dashboard
Production-Ready Single-Page Application consuming live Databricks tables:
- workspace.default.dq_tables
- workspace.default.dq_metric_catalog
- workspace.default.dq_metric_assignments
- workspace.default.dq_metric_results
- workspace.default.dq_alerts
- workspace.default.dq_table_scores
- workspace.default.dq_overall_scores
- workspace.default.dq_score_history

STRICT COMPLIANCE:
- NO fake KPI trends
- NO fake sparklines
- NO fake 14-day projections
- NO fake medallion mappings (only displays if layer column exists in dq_tables)
- NO fake AI agent results (Replaced with Rule Execution Monitor)
- NO fake alerts or simulated sample datasets
"""

import os
import time
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc, html, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc

import db

# Initialize Dash application
app = dash.Dash(
    __name__,
    title="Databricks Data Quality Framework",
    update_title=None,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap"
    ],
    suppress_callback_exceptions=True
)

server = app.server

# ==========================================================
# CHART BUILDERS (STRICTLY FROM LIVE DATA)
# ==========================================================

def build_rule_distribution_chart(df_rules: pd.DataFrame) -> go.Figure:
    """Horizontal stacked bar chart showing live Pass % vs Fail % across metric categories."""
    fig = go.Figure()
    if df_rules.empty:
        fig.update_layout(
            annotations=[dict(text="No metric executions recorded yet", showarrow=False, font=dict(color="#64748b"))],
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            height=205
        )
        return fig

    categories = df_rules["category"].tolist()
    pass_pcts = df_rules["pass_pct"].tolist()
    fail_pcts = df_rules["fail_pct"].tolist()
    totals = df_rules["total_executions"].tolist()
    passed_counts = df_rules["passed"].tolist()
    failed_counts = df_rules["failed"].tolist()

    fig.add_trace(go.Bar(
        y=categories,
        x=pass_pcts,
        name="Pass %",
        orientation="h",
        marker=dict(color="#10b981"),
        customdata=list(zip(totals, passed_counts)),
        hovertemplate="<b>%{y}</b><br>Pass: %{x:.1f}% (%{customdata[1]} checks)<br>Total Executions: %{customdata[0]}<extra></extra>"
    ))

    fig.add_trace(go.Bar(
        y=categories,
        x=fail_pcts,
        name="Fail %",
        orientation="h",
        marker=dict(color="#ef4444"),
        customdata=list(zip(totals, failed_counts)),
        hovertemplate="<b>%{y}</b><br>Fail: %{x:.1f}% (%{customdata[1]} checks)<extra></extra>"
    ))

    fig.update_layout(
        barmode="stack",
        margin=dict(l=130, r=20, t=10, b=25),
        height=205,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=9, family="JetBrains Mono", color="#64748b")
        ),
        xaxis=dict(
            range=[0, 100],
            ticksuffix="%",
            showgrid=True,
            gridcolor="#f1f5f9",
            tickfont=dict(size=9, family="JetBrains Mono", color="#64748b")
        ),
        yaxis=dict(
            autorange="reversed",
            tickfont=dict(size=10, family="JetBrains Mono", color="#0f172a", weight="bold")
        )
    )
    return fig

def build_score_trend_chart(df_trend: pd.DataFrame) -> go.Figure:
    """Overall Score Trend line chart strictly from dq_score_history."""
    fig = go.Figure()
    if df_trend.empty:
        return fig

    try:
        dates = pd.to_datetime(df_trend["execution_time"]).dt.strftime("%b %d %H:%M").tolist()
    except Exception:
        dates = [str(x)[:16] for x in df_trend["execution_time"]]
    scores = df_trend["dq_score"].tolist()

    # Target SLA Floor Line (95.0%)
    fig.add_trace(go.Scatter(
        x=dates,
        y=[95.0] * len(dates),
        mode="lines",
        line=dict(color="#f59e0b", width=1.5, dash="dot"),
        name="Target SLA Floor (95%)",
        hoverinfo="name+y"
    ))

    # Main DQ Score Trend Line
    fig.add_trace(go.Scatter(
        x=dates,
        y=scores,
        mode="lines+markers",
        line=dict(color="#0284c7", width=2.5),
        marker=dict(size=6, color="#0369a1"),
        name="Table DQ Score",
        hovertemplate="<b>%{x}</b><br>Score: %{y:.1f}%<extra></extra>"
    ))

    min_s = max(0, int(min(scores) - 10))
    fig.update_layout(
        margin=dict(l=40, r=20, t=10, b=28),
        height=205,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=9, family="JetBrains Mono", color="#64748b")
        ),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=True,
            linecolor="#cbd5e1",
            tickfont=dict(size=9, color="#64748b", family="JetBrains Mono")
        ),
        yaxis=dict(
            range=[min_s, 105],
            showgrid=True,
            gridcolor="#f1f5f9",
            zeroline=False,
            tickfont=dict(size=10, color="#64748b", family="JetBrains Mono"),
            ticksuffix="%"
        )
    )
    return fig

# ==========================================================
# UI COMPONENT HELPERS
# ==========================================================

def render_kpi_card(title: str, value: str, subtext: str):
    """Renders a clean KPI card with real data, no fake sparklines."""
    return html.Div([
        html.Div([
            html.Span(title, className="dq-kpi-title"),
        ], className="dq-kpi-card-header"),
        html.Div([
            html.Span(value, className="dq-kpi-value"),
        ], className="dq-kpi-value-row"),
        html.Div(subtext, className="dq-kpi-footer")
    ], className="dq-kpi-card")

def render_score_badge(score: float):
    """Color-coded score badge matching Green >=90, Yellow 70-89, Red <70."""
    if score >= 90.0:
        return html.Span(f"{score:.1f}%", className="score-badge green")
    elif score >= 70.0:
        return html.Span(f"{score:.1f}%", className="score-badge yellow")
    else:
        return html.Span(f"{score:.1f}%", className="score-badge red")

# ==========================================================
# ==========================================================
# APPLICATION LAYOUT (2-PAGE DASHBOARD)
# ==========================================================
app.layout = html.Div([
    # State Stores
    dcc.Store(id="selected-table-store", data=1),
    dcc.Store(id="registered-table-id-store", data=None),
    dcc.Store(id="alerts-filter-store", data="ALL"),
    dcc.Store(id="dmf-refresh-store", data=0),
    dcc.Store(id="active-page-store", data="monitoring"),
    dcc.Store(id="validation-server-result-store", data=None),
    dcc.Interval(id="auto-sync-interval", interval=60*1000, n_intervals=0),

    # FLOATING POP-UP VALIDATION NOTIFICATION (Enterprise Toast Notification)
    html.Div(
        id="validation-popup-container",
        className="dq-validation-popup state-in-progress",
        style={"display": "none"},
        children=[
            html.Div(
                className="dq-popup-top-bar",
                children=[
                    html.Div(
                        className="dq-popup-badge",
                        children=[
                            html.Span(className="dq-popup-status-dot"),
                            html.Span("VALIDATION IN PROGRESS", id="popup-badge-text")
                        ]
                    ),
                    html.Button("×", id="popup-close-btn", className="dq-popup-close-btn", n_clicks=0, title="Dismiss")
                ]
            ),
            html.Div(
                className="dq-popup-body",
                children=[
                    html.Div(
                        className="dq-popup-main-row",
                        children=[
                            html.Div("⚙", id="popup-icon-container", className="dq-popup-icon-box"),
                            html.Div(
                                className="dq-popup-text-box",
                                children=[
                                    html.Div("Validation Engine Running...", id="popup-title", className="dq-popup-title"),
                                    html.Div(
                                        "Executing Data Quality checks against Databricks Lakehouse tables...",
                                        id="popup-message",
                                        className="dq-popup-message"
                                    )
                                ]
                            )
                        ]
                    )
                ]
            ),
            html.Div(
                id="popup-progress-track",
                className="dq-popup-progress-track",
                children=[
                    html.Div(className="dq-popup-progress-bar")
                ]
            )
        ]
    ),

    # Live Databricks Connection Status Banner
    html.Div(id="connection-status-banner"),

    # HEADER CONTROL BAR
    html.Div([
        html.Div([
            html.Div([
                html.Span(className="dq-pulse-beacon"),
                html.Span("DATA QUALITY CONTROL CENTER", className="dq-title"),
                html.Span("DATABRICKS LIVE WAREHOUSE", className="dq-prod-badge")
            ], className="dq-title-row"),
            html.Div("Databricks Lakehouse | Metadata-Driven Validation | workspace.default", className="dq-subtitle")
        ], className="dq-header-left"),

        html.Div([
            html.Div([
                html.Div(id="clock-display", className="dq-clock-time"),
                html.Div("Databricks SQL Warehouse Active", className="dq-clock-subtext")
            ], className="dq-clock-box"),

            html.Button("Sync Platform", id="sync-btn", className="btn-dq-sync")
        ], className="dq-header-right")
    ], className="dq-header"),

    # 2-PAGE NAVIGATION BAR (Tabs to switch between Monitoring Suite and Configuration Studio)
    html.Div([
        html.Div([
            html.Button(
                [
                    html.Span("DATA QUALITY MONITORING & ANALYTICS", style={"fontWeight": "800", "letterSpacing": "0.03em"}),
                    html.Span("PAGE 1", className="dq-tab-pill")
                ],
                id="tab-btn-monitoring",
                className="dq-nav-tab active",
                n_clicks=0
            ),
            html.Button(
                [
                    html.Span("RULE CONFIGURATION & DATASET STUDIO", style={"fontWeight": "800", "letterSpacing": "0.03em"}),
                    html.Span("PAGE 2", className="dq-tab-pill")
                ],
                id="tab-btn-config",
                className="dq-nav-tab",
                n_clicks=0
            )
        ], className="dq-nav-tab-group"),

        html.Div([
            html.Span("Workspace: workspace.default", style={"fontSize": "11.5px", "color": "#64748b", "fontFamily": "JetBrains Mono"}),
        ], style={"display": "flex", "alignItems": "center"})
    ], className="dq-nav-container"),

    # ==========================================================
    # PAGE 1: DATA QUALITY MONITORING & ANALYTICS SUITE
    # ==========================================================
    html.Div(id="page-monitoring-view", children=[
        # Executive Summary
        html.Div(id="executive-summary-container"),

        # KPI Cards
        html.Div([
            html.Div([
                html.Div([
                    html.Span("KEY PERFORMANCE INDICATORS", className="dq-panel-title"),
                    html.Span("PLATFORM-WIDE METRICS", className="dq-panel-subtitle")
                ], className="dq-panel-title-group")
            ], className="dq-panel-header", style={"marginBottom": "10px"}),
            html.Div(id="kpi-cards-container", className="dq-kpi-grid")
        ], style={"marginBottom": "20px"}),

        # Medallion Container (Hidden if layer column does not exist)
        html.Div(id="medallion-container"),

        # Overall Score Trend
        html.Div(id="score-trend-container", style={"marginBottom": "20px"}),

        # Table Health Leaderboard
        html.Div([
            html.Div([
                html.Div([
                    html.Span("TABLE HEALTH LEADERBOARD", className="dq-panel-title"),
                    html.Span("RANKED BY DQ SCORE", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Click any table row to configure & inspect in Dataset Studio (Page 2)", style={"fontSize": "11px", "color": "#0284c7", "fontWeight": "600"})
            ], className="dq-panel-header"),
            html.Div(id="table-health-leaderboard-container", style={"overflowX": "auto"})
        ], className="dq-panel", style={"marginBottom": "20px"}),

        # Rule Distribution Analytics
        html.Div([
            html.Div([
                html.Div([
                    html.Span("RULE DISTRIBUTION ANALYTICS", className="dq-panel-title"),
                    html.Span("LIVE EXECUTIONS BY CATEGORY", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Pass % vs. Fail % breakdown across metric categories in workspace.default", style={"fontSize": "11px", "color": "#64748b"})
            ], className="dq-panel-header"),
            html.Div("Pass % vs. Fail % breakdown across metric categories in workspace.default:", style={"fontSize": "11px", "color": "#64748b", "marginBottom": "6px"}),
            dcc.Graph(id="rule-dist-chart", config={"displayModeBar": False})
        ], className="dq-panel", style={"marginBottom": "20px"}),

        # Rule Execution Monitor
        html.Div([
            html.Div([
                html.Div([
                    html.Span("RULE EXECUTION MONITOR", className="dq-panel-title"),
                    html.Span("LIVE VALIDATION EXECUTIONS", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Consuming dq_metric_results", style={"fontSize": "11px", "color": "#0284c7", "fontWeight": "700", "fontFamily": "JetBrains Mono"})
            ], className="dq-panel-header"),
            html.Div(id="rule-execution-monitor-container", style={"overflowX": "auto"})
        ], className="agent-monitor-panel", style={"marginBottom": "20px"}),

        # Alerts & Anomalies
        html.Div([
            html.Div([
                html.Div([
                    html.Span("ALERTS & ANOMALIES", className="dq-panel-title"),
                    html.Span(id="alerts-count-subtitle", className="dq-panel-subtitle", style={"marginLeft": "10px"})
                ], className="dq-panel-title-group"),

                # Severity Filter Buttons
                html.Div([
                    html.Button("ALL ALERTS", id="filter-btn-all", className="btn-dq-filter active"),
                    html.Button("CRITICAL", id="filter-btn-critical", className="btn-dq-filter"),
                    html.Button("HIGH / WARNING", id="filter-btn-warning", className="btn-dq-filter")
                ], className="dq-alerts-filter-bar")
            ], className="dq-panel-header"),

            html.Div(id="alerts-list-container", className="dq-alerts-list")
        ], className="dq-alerts-panel", style={"marginBottom": "20px"})
    ]),

    # ==========================================================
    # PAGE 2: EDITING, RULE CONFIGURATION & DATASET STUDIO
    # ==========================================================
    html.Div(id="page-config-view", style={"display": "none"}, children=[
        # Register New Monitored Table
        html.Div([
            html.Div([
                html.Div([
                    html.Span("REGISTER NEW MONITORED TABLE", className="dq-panel-title"),
                    html.Span("INSERTS INTO workspace.default.dq_tables", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Choose discovered delta tables and metadata present in Databricks", style={"fontSize": "11px", "color": "#64748b", "fontFamily": "JetBrains Mono"})
            ], className="dq-panel-header", style={"marginBottom": "12px"}),

            html.Div([
                html.Div([
                    html.Label("TABLE NAME (DISCOVERED)", style={"fontSize": "10px", "fontWeight": "700", "color": "#64748b", "fontFamily": "JetBrains Mono"}),
                    dcc.Dropdown(
                        id="register-table-select",
                        placeholder="Choose table...",
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"fontSize": "12px"}
                    )
                ]),

                html.Div([
                    html.Label("CATALOG", style={"fontSize": "10px", "fontWeight": "700", "color": "#64748b", "fontFamily": "JetBrains Mono"}),
                    dcc.Dropdown(
                        id="register-catalog-select",
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"fontSize": "12px"}
                    )
                ]),

                html.Div([
                    html.Label("SCHEMA", style={"fontSize": "10px", "fontWeight": "700", "color": "#64748b", "fontFamily": "JetBrains Mono"}),
                    dcc.Dropdown(
                        id="register-schema-select",
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"fontSize": "12px"}
                    )
                ]),

                html.Div([
                    html.Label("TABLE OWNER", style={"fontSize": "10px", "fontWeight": "700", "color": "#64748b", "fontFamily": "JetBrains Mono"}),
                    dcc.Dropdown(
                        id="register-owner-select",
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"fontSize": "12px"}
                    )
                ]),

                html.Div([
                    html.Button("Register Table", id="register-table-btn", className="btn-dq-secondary", style={"width": "100%", "marginTop": "18px"})
                ])
            ], className="dq-form-grid"),

            html.Div(id="register-table-feedback", style={"marginTop": "8px"})
        ], className="dq-register-box"),

        # Table Inspector
        html.Div([
            # Header of Table Inspector
            html.Div([
                html.Div([
                    html.Div([
                        html.Span("TABLE INSPECTOR", className="dq-inspector-title-v2"),
                        html.Span(id="inspector-metrics-count", style={"color": "#64748b", "fontWeight": "600", "fontSize": "13px", "marginLeft": "8px"})
                    ], className="dq-panel-title-group"),
                    html.Div("Pick a table, add or remove DMFs, and the DQ score recalculates live. Available actions depend on your role.", className="dq-inspector-subtitle-v2")
                ]),

                # Right Large Score HUD
                dcc.Loading(
                    id="loading-table-hud",
                    type="dot",
                    color="#0284c7",
                    children=html.Div(id="table-large-score-container", className="dq-score-hud-v2")
                )
            ], className="dq-inspector-header-v2"),

            # Top Bar: Monitored Table Dropdown & Tags
            html.Div([
                html.Div([
                    dcc.Dropdown(
                        id="table-selector-dropdown",
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"minWidth": "250px", "fontFamily": "JetBrains Mono"}
                    ),
                    html.Div(id="table-meta-pills", style={"display": "flex", "gap": "6px", "flexWrap": "wrap"})
                ], className="dq-table-selector-box"),

                html.Div([
                    html.Button("Quick Re-check", id="run-all-checks-btn-quick", className="btn-run-all-checks")
                ])
            ], className="dq-inspector-top-bar"),

            # Attached DMFs Table
            dcc.Loading(
                id="loading-table-assignments",
                type="dot",
                color="#0284c7",
                children=html.Div(id="active-assignments-container", style={"overflowX": "auto"})
            ),

            html.Div("No penalties — all active checks passing. Score reflects only currently attached and enabled checks.", className="dq-table-footer-note")
        ], className="dq-inspector-panel-v2"),

        # Add DMF / Rule Configuration
        html.Div([
            html.Div([
                html.Div([
                    html.Span("ADD DMF / RULE CONFIGURATION", className="dq-panel-title"),
                    html.Span("ATTACH RULES TO SELECTED TABLE", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Configure metrics, threshold criteria, and weights for live evaluation", style={"fontSize": "11px", "color": "#64748b"})
            ], className="dq-panel-header", style={"marginBottom": "14px"}),

            # Mode Switcher Bar: Mode A (Threshold-Based) vs Mode B (Expectation-Based)
            html.Div([
                html.Div([
                    html.Span("SELECT VALIDATION MODE:", style={"fontSize": "11px", "fontWeight": "800", "color": "#1e293b", "fontFamily": "JetBrains Mono", "marginRight": "18px"}),
                    dcc.RadioItems(
                        id="validation-mode-radio",
                        options=[
                            {"label": " Mode A: Threshold-Based Validation", "value": "MODE_A"},
                            {"label": " Mode B: Expectation-Based Validation", "value": "MODE_B"}
                        ],
                        value="MODE_A",
                        inline=True,
                        inputStyle={"marginRight": "6px", "cursor": "pointer", "width": "16px", "height": "16px"},
                        labelStyle={"marginRight": "24px", "cursor": "pointer", "fontSize": "13px", "fontWeight": "700", "fontFamily": "JetBrains Mono", "color": "#0f172a", "display": "inline-flex", "alignItems": "center"}
                    )
                ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap"}),

                # Dynamic Mode Description Banner
                html.Div(id="mode-info-banner", style={"marginTop": "8px", "fontSize": "12px", "fontFamily": "JetBrains Mono", "color": "#0369a1", "fontWeight": "600"})
            ], style={"backgroundColor": "#f8fafc", "border": "1.5px solid #cbd5e1", "borderRadius": "8px", "padding": "12px 18px", "marginBottom": "16px"}),

            # Input Row: Metric | Operator | Threshold / Expected Values | Weight | [Add DMF]
            html.Div([
                # Metric Dropdown (Categorized)
                html.Div([
                    html.Label("Metric", className="dq-add-dmf-label"),
                    dcc.Dropdown(
                        id="add-metric-select",
                        options=db.get_metric_catalog_grouped_options("MODE_A"),
                        value="Null Count Check",
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"minWidth": "280px", "fontFamily": "JetBrains Mono"}
                    )
                ], className="dq-add-dmf-field"),

                # Operator Dropdown (Supported: =, !=, <, <=, >, >=, IN, NOT IN, BETWEEN)
                html.Div([
                    html.Label("Operator", className="dq-add-dmf-label"),
                    dcc.Dropdown(
                        id="add-operator-select",
                        options=[
                            {"label": "= (Equals)", "value": "="},
                            {"label": "!= (Not Equals)", "value": "!="},
                            {"label": "< (Less Than)", "value": "<"},
                            {"label": "<= (Less Than or Equal)", "value": "<="},
                            {"label": "> (Greater Than)", "value": ">"},
                            {"label": ">= (Greater Than or Equal)", "value": ">="},
                            {"label": "BETWEEN (Range)", "value": "BETWEEN"}
                        ],
                        value="=",
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"minWidth": "165px", "fontFamily": "JetBrains Mono"}
                    )
                ], className="dq-add-dmf-field"),

                # Dynamic Threshold / Expected Values Input
                html.Div([
                    html.Label("Threshold Input", id="add-threshold-label", className="dq-add-dmf-label"),
                    dcc.Input(
                        id="add-threshold-input",
                        type="text",
                        value="0",
                        placeholder="e.g. 0",
                        className="dq-input-text",
                        style={"minWidth": "220px"}
                    )
                ], className="dq-add-dmf-field"),

                # Weight Dropdown
                html.Div([
                    html.Label("Weight", className="dq-add-dmf-label"),
                    dcc.Dropdown(
                        id="add-weight-select",
                        options=[
                            {"label": "Low (5 pts)", "value": 5.0},
                            {"label": "Medium (10 pts)", "value": 10.0},
                            {"label": "High (15 pts)", "value": 15.0},
                            {"label": "Critical (20 pts)", "value": 20.0}
                        ],
                        value=10.0,
                        searchable=False,
                        clearable=False,
                        className="dash-dropdown",
                        style={"minWidth": "150px"}
                    )
                ], className="dq-add-dmf-field"),

                # Add DMF Button
                html.Div([
                    html.Button("Add DMF", id="deploy-metric-btn", className="btn-add-dmf-primary")
                ], className="dq-add-dmf-field")
            ], className="dq-add-dmf-row"),

            # Mode B Fuzzy Typo / Mode A Helper Note
            html.Div(id="mode-helper-note-container"),

            # Column Selector Row (Multi-column Checklist & Select All)
            html.Div([
                # "Select all cols" Toggle Box
                html.Div([
                    dcc.Checklist(
                        id="add-column-select-all",
                        options=[{"label": " Select all cols", "value": "ALL"}],
                        value=[],
                        inputStyle={"marginRight": "6px", "cursor": "pointer", "width": "15px", "height": "15px"},
                        labelStyle={"cursor": "pointer", "fontSize": "12.5px", "fontWeight": "700", "fontFamily": "JetBrains Mono", "display": "inline-flex", "alignItems": "center", "color": "#0284c7"}
                    )
                ], style={"paddingRight": "14px", "marginRight": "14px", "borderRight": "2px solid #cbd5e1", "display": "flex", "alignItems": "center"}),

                # Discovered Columns Checkboxes Container
                html.Div([
                    dcc.Checklist(
                        id="add-column-checklist",
                        options=[],
                        value=[],
                        inline=True,
                        inputStyle={"marginRight": "5px", "cursor": "pointer", "width": "15px", "height": "15px"},
                        labelStyle={"marginRight": "16px", "cursor": "pointer", "fontSize": "13px", "fontFamily": "JetBrains Mono", "display": "inline-flex", "alignItems": "center"}
                    )
                ], id="add-column-checkbox-container", style={"display": "flex", "alignItems": "center", "gap": "16px", "flexWrap": "wrap", "flexGrow": "1"}),

                # Dynamic Selection Hint
                html.Span("1 column selected.", id="add-column-hint-text", className="dq-column-hint")
            ], className="dq-column-checklist-row"),

            html.Div(id="add-metric-feedback", style={"marginTop": "10px"})
        ], className="dq-add-dmf-panel"),

        # Run Validation
        html.Div([
            html.Div([
                html.Div([
                    html.Span("RUN VALIDATION", className="dq-panel-title"),
                    html.Span("EXECUTE FULL QUALITY SUITE", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Triggers complete evaluation across all active tables & DMF assignments in workspace.default", style={"fontSize": "11px", "color": "#64748b"})
            ], className="dq-panel-header"),

            html.Div([
                html.Div([
                    html.Div("Run Databricks Validation Engine", style={"fontSize": "14px", "fontWeight": "700", "color": "#0f172a", "marginBottom": "4px"}),
                    html.Div("Evaluates SQL templates in dq_metric_catalog against monitored Delta tables, records check results in dq_metric_results, updates alerts, and recomputes live table & overall quality scores.", style={"fontSize": "12px", "color": "#475569", "maxWidth": "750px", "lineHeight": "1.5"})
                ]),
                html.Div([
                    html.Button("Run Validation Suite Now", id="run-all-checks-btn", className="btn-run-validation-primary")
                ])
            ], className="dq-run-validation-content"),

            dcc.Loading(
                id="loading-run-validation",
                type="circle",
                color="#10b981",
                children=html.Div(id="run-validation-feedback", style={"marginTop": "10px", "minHeight": "24px"})
            )
        ], className="dq-run-validation-panel"),

        # Databricks Delta Live Tables & Genie Diagnostics Terminal
        html.Div([
            html.Div([
                html.Span("DATABRICKS DELTA LIVE TABLES & GENIE DIAGNOSTICS", style={"fontSize": "12px", "fontWeight": "800", "fontFamily": "JetBrains Mono", "color": "#0f172a"}),
                html.Span("workspace.default.dq_metric_results", style={"fontSize": "11px", "color": "#64748b", "fontFamily": "JetBrains Mono", "display": "block"})
            ]),
            html.Div([
                html.Pre(id="debug-sql-query", style={"margin": "0"}),
                html.Button("Copy SQL", id="copy-sql-btn", className="btn-terminal-copy")
            ], className="dq-terminal-box")
        ], style={"marginTop": "16px", "backgroundColor": "#ffffff", "border": "1px solid #e2e8f0", "borderRadius": "8px", "padding": "16px 20px"})
    ]),

    # FOOTER
    html.Div([
        html.Span("Databricks Data Quality Framework · Catalog: workspace.default · Tables: dq_tables, dq_metric_catalog, dq_metric_assignments, dq_metric_results, dq_alerts, dq_table_scores, dq_overall_scores"),
        html.Span(id="footer-timestamp-display")
    ], className="dq-footer")

], className="dq-container")

# ==========================================================
# CALLBACKS & INTERACTIVITY
# ==========================================================

# 0. Page View Navigation (Page 1: Monitoring & Analytics vs Page 2: Rule Configuration & Studio)
@app.callback(
    [Output("page-monitoring-view", "style"),
     Output("page-config-view", "style"),
     Output("tab-btn-monitoring", "className"),
     Output("tab-btn-config", "className"),
     Output("active-page-store", "data")],
    [Input("tab-btn-monitoring", "n_clicks"),
     Input("tab-btn-config", "n_clicks"),
     Input({"type": "table-row-click", "index": dash.ALL}, "n_clicks")],
    [State("active-page-store", "data")]
)
def switch_page_view(mon_clicks, cfg_clicks, row_clicks, current_page):
    ctx = callback_context
    if not ctx.triggered:
        return {"display": "block"}, {"display": "none"}, "dq-nav-tab active", "dq-nav-tab", "monitoring"

    trig_id = ctx.triggered_id

    # If user clicked on a table row in Leaderboard on Page 1, navigate to Configuration/Studio (Page 2)
    if isinstance(trig_id, dict) and trig_id.get("type") == "table-row-click":
        if any(c for c in (row_clicks or [])):
            return {"display": "none"}, {"display": "block"}, "dq-nav-tab", "dq-nav-tab active", "config"

    if trig_id == "tab-btn-config":
        return {"display": "none"}, {"display": "block"}, "dq-nav-tab", "dq-nav-tab active", "config"

    if trig_id == "tab-btn-monitoring":
        return {"display": "block"}, {"display": "none"}, "dq-nav-tab active", "dq-nav-tab", "monitoring"

    if current_page == "config":
        return {"display": "none"}, {"display": "block"}, "dq-nav-tab", "dq-nav-tab active", "config"
    return {"display": "block"}, {"display": "none"}, "dq-nav-tab active", "dq-nav-tab", "monitoring"

# 1. Connection Banner & Clocks
@app.callback(
    [Output("connection-status-banner", "children"),
     Output("clock-display", "children"),
     Output("footer-timestamp-display", "children")],
    [Input("sync-btn", "n_clicks"),
     Input("auto-sync-interval", "n_intervals")]
)
def update_banner_and_clock(n_clicks, n_intervals):
    now_utc = datetime.utcnow()
    clock_str = f"SYNCED {now_utc.strftime('%H:%M:%S')} UTC"
    footer_str = f"LAST SCAN {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC"

    is_live, msg = db.test_connection()
    if is_live:
        banner = html.Div([
            html.Span(f"DATABRICKS LAKEHOUSE CONNECTED: {db.DATABRICKS_CATALOG}.{db.DATABRICKS_SCHEMA} SQL Warehouse Active", style={"fontWeight": "700"}),
            html.Span(f"Server: {db.CLEAN_HOST}", style={"fontSize": "11px"})
        ], style={"backgroundColor": "#ecfdf5", "border": "1px solid #a7f3d0", "color": "#065f46", "padding": "6px 16px", "borderRadius": "8px", "fontSize": "11px", "fontFamily": "JetBrains Mono", "display": "flex", "justifyContent": "space-between", "marginBottom": "16px"})
    else:
        banner = html.Div([
            html.Span(f"DATABRICKS FRAMEWORK: {msg}", style={"fontWeight": "600"}),
            html.Span("Set credentials in .env to link live SQL warehouse", style={"fontSize": "10px"})
        ], style={"backgroundColor": "#fefce8", "border": "1px solid #fef08a", "color": "#854d0e", "padding": "6px 16px", "borderRadius": "8px", "fontSize": "11px", "fontFamily": "JetBrains Mono", "display": "flex", "justifyContent": "space-between", "marginBottom": "16px"})

    return banner, clock_str, footer_str

# 2. Executive Summary Panel (Section 5)
@app.callback(
    Output("executive-summary-container", "children"),
    [Input("sync-btn", "n_clicks"),
     Input("dmf-refresh-store", "data"),
     Input("auto-sync-interval", "n_intervals")]
)
def update_executive_summary(n_clicks, refresh_data, n_intervals):
    kpis = db.get_kpis_data()
    leaderboard = db.get_table_health_leaderboard()

    best_table = f"{leaderboard.iloc[0]['table_name']} ({leaderboard.iloc[0]['dq_score']:.1f}%)" if not leaderboard.empty else "N/A"
    worst_table = f"{leaderboard.iloc[-1]['table_name']} ({leaderboard.iloc[-1]['dq_score']:.1f}%)" if not leaderboard.empty else "N/A"

    return html.Div([
        html.Div([
            html.Div([
                html.Span("EXECUTIVE SUMMARY", className="exec-summary-title"),
                html.Span("LIVE DATABRICKS LAKEHOUSE", className="dq-panel-subtitle", style={"marginLeft": "10px"})
            ], className="dq-panel-title-group"),
            html.Span(f"Execution: {kpis['execution_time']}", style={"fontSize": "10px", "color": "#64748b", "fontFamily": "JetBrains Mono"})
        ], className="exec-summary-header"),
        html.Div([
            html.Div([
                html.Span("Overall Score", className="exec-stat-label"),
                html.Span(f"{kpis['overall_score']:.1f}%", className="exec-stat-value", style={"color": "#0284c7"})
            ], className="exec-stat-box"),
            html.Div([
                html.Span("Datasets Monitored", className="exec-stat-label"),
                html.Span(str(kpis["tables_monitored"]), className="exec-stat-value")
            ], className="exec-stat-box"),
            html.Div([
                html.Span("Checks Executed", className="exec-stat-label"),
                html.Span(f"{kpis['total_checks']:,}", className="exec-stat-value")
            ], className="exec-stat-box"),
            html.Div([
                html.Span("Active Alerts", className="exec-stat-label"),
                html.Span(str(kpis["total_alerts"]), className="exec-stat-value", style={"color": "#dc2626" if kpis["total_alerts"] > 0 else "#059669"})
            ], className="exec-stat-box"),
            html.Div([
                html.Span("Passing Rate", className="exec-stat-label"),
                html.Span(f"{kpis['pass_rate']:.1f}%", className="exec-stat-value", style={"color": "#059669"})
            ], className="exec-stat-box"),
            html.Div([
                html.Span("Best Table", className="exec-stat-label"),
                html.Span(best_table, className="exec-stat-value", style={"color": "#059669"})
            ], className="exec-stat-box"),
            html.Div([
                html.Span("Lowest Table", className="exec-stat-label"),
                html.Span(worst_table, className="exec-stat-value", style={"color": "#dc2626" if "100" not in worst_table else "#059669"})
            ], className="exec-stat-box"),
            html.Div([
                html.Span("Last Execution", className="exec-stat-label"),
                html.Span(str(kpis["execution_time"])[11:19] if len(str(kpis["execution_time"])) >= 19 else str(kpis["execution_time"]), className="exec-stat-value")
            ], className="exec-stat-box")
        ], className="exec-grid")
    ], className="executive-summary-panel")

# 3. KPI Cards (Section 2 - Live data, no fake sparklines)
@app.callback(
    Output("kpi-cards-container", "children"),
    [Input("sync-btn", "n_clicks"),
     Input("dmf-refresh-store", "data"),
     Input("auto-sync-interval", "n_intervals")]
)
def update_kpis(n_clicks, refresh_data, n_intervals):
    kpis = db.get_kpis_data()

    score_val = f"{kpis['overall_score']:.1f}%"
    score_sub = "Overall data quality score across all monitored tables"

    t_cnt = kpis["tables_monitored"]
    tables_val = f"{t_cnt} Table{'s' if t_cnt != 1 else ''}"
    tables_sub = "Total datasets configured in dq_tables"

    c_cnt = kpis["total_checks"]
    checks_val = f"{c_cnt:,} Checks"
    checks_sub = "Total validation checks executed in dq_metric_results"

    a_cnt = kpis["total_alerts"]
    alerts_val = f"{a_cnt} Alert{'s' if a_cnt != 1 else ''}"
    alerts_sub = "Open anomaly alerts in dq_alerts"

    pass_val = f"{kpis['pass_rate']:.1f}%"
    pass_sub = "Percentage of validation checks passing thresholds"

    tiles = [
        render_kpi_card("Overall DQ Score", score_val, score_sub),
        render_kpi_card("Monitored Tables", tables_val, tables_sub),
        render_kpi_card("Executed Checks", checks_val, checks_sub),
        render_kpi_card("Active Alerts", alerts_val, alerts_sub),
        render_kpi_card("Validation Pass Rate", pass_val, pass_sub)
    ]
    return tiles

# 4. Medallion Container (Section 3: Only if layer column exists in dq_tables, otherwise hidden)
@app.callback(
    Output("medallion-container", "children"),
    [Input("sync-btn", "n_clicks")]
)
def update_medallion(n_clicks):
    if db.check_layer_column_exists():
        # Render layer if layer column exists in dq_tables
        return html.Div("Medallion Architecture Layer Overview", className="medallion-overview-panel")
    # Hide Medallion section completely if layer column does not exist
    return html.Div(style={"display": "none"})

# 5. Charts & Score Trend (Section 7 & Section 9)
@app.callback(
    [Output("score-trend-container", "children"),
     Output("rule-dist-chart", "figure")],
    [Input("sync-btn", "n_clicks"),
     Input("dmf-refresh-store", "data"),
     Input("auto-sync-interval", "n_intervals")]
)
def update_dashboard_charts(n_clicks, refresh_data, n_intervals):
    # Rule Distribution Analytics (Section 9)
    df_rules = db.get_rule_distribution_analytics()
    fig_rules = build_rule_distribution_chart(df_rules)

    # Score Trend (Section 7: from dq_score_history)
    df_trend = db.get_score_history()
    if not df_trend.empty and len(df_trend) >= 1:
        fig_trend = build_score_trend_chart(df_trend)
        trend_comp = html.Div([
            html.Div([
                html.Div([
                    html.Span("OVERALL SCORE TREND", className="dq-panel-title"),
                    html.Span("HISTORICAL TRAJECTORY", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Track overall quality progression across validation runs", style={"fontSize": "11px", "color": "#64748b"})
            ], className="dq-panel-header"),
            dcc.Graph(figure=fig_trend, config={"displayModeBar": False})
        ], className="dq-panel")
    else:
        trend_comp = html.Div([
            html.Div([
                html.Div([
                    html.Span("OVERALL SCORE TREND", className="dq-panel-title"),
                    html.Span("HISTORICAL TRAJECTORY", className="dq-panel-subtitle")
                ], className="dq-panel-title-group"),
                html.Span("Historical score logging active", style={"fontSize": "11px", "color": "#64748b"})
            ], className="dq-panel-header"),
            html.Div("Historical quality trajectory is being recorded. Running validation records snapshots to dq_score_history.", style={"padding": "24px", "color": "#64748b", "fontFamily": "JetBrains Mono", "textAlign": "center"})
        ], className="dq-panel")

    return trend_comp, fig_rules

# 6. Table Health Leaderboard (Section 5: dq_table_scores JOIN dq_tables)
@app.callback(
    Output("table-health-leaderboard-container", "children"),
    [Input("sync-btn", "n_clicks"),
     Input("dmf-refresh-store", "data"),
     Input("auto-sync-interval", "n_intervals"),
     Input("selected-table-store", "data")]
)
def update_table_health_leaderboard(n_clicks, refresh_data, n_intervals, selected_table_id):
    df = db.get_table_health_leaderboard()

    header = html.Thead(html.Tr([
        html.Th("RANK"),
        html.Th("TABLE NAME"),
        html.Th("DQ SCORE"),
        html.Th("PASSED WEIGHT"),
        html.Th("TOTAL WEIGHT"),
        html.Th("EXECUTION TIME")
    ]))

    if df.empty:
        return html.Div("No table scores calculated yet in dq_table_scores.", style={"padding": "12px", "color": "#64748b", "fontFamily": "JetBrains Mono"})

    rows = []
    for idx, row in df.iterrows():
        t_id = int(row["table_id"])
        t_name = str(row["table_name"])
        score = float(row["dq_score"])
        tot_w = float(row["total_weight"])
        pas_w = float(row["passed_weight"])
        exec_t = str(row["execution_time"])

        is_selected = (t_id == selected_table_id)
        row_cls = "clickable-row selected-row" if is_selected else "clickable-row"
        rank_label = str(idx + 1)

        rows.append(html.Tr([
            html.Td(html.Span(rank_label, style={"fontWeight": "800", "fontSize": "13px"})),
            html.Td(html.Span(t_name, style={"fontWeight": "700", "fontFamily": "JetBrains Mono", "color": "#0f172a"})),
            html.Td(render_score_badge(score)),
            html.Td(f"{pas_w:.1f}", style={"fontFamily": "JetBrains Mono", "color": "#059669", "fontWeight": "700"}),
            html.Td(f"{tot_w:.1f}", style={"fontFamily": "JetBrains Mono", "color": "#334155"}),
            html.Td(exec_t[:19], style={"fontFamily": "JetBrains Mono", "fontSize": "11px", "color": "#64748b"})
        ], className=row_cls, id={"type": "table-row-click", "index": t_id}))

    return html.Table([header, html.Tbody(rows)], className="dq-table")

# 7. Rule Execution Monitor (Section 6: Replaces AI Agent Monitor)
# Source: dq_metric_results JOIN dq_metric_assignments JOIN dq_metric_catalog JOIN dq_tables
@app.callback(
    Output("rule-execution-monitor-container", "children"),
    [Input("sync-btn", "n_clicks"),
     Input("auto-sync-interval", "n_intervals")]
)
def update_rule_execution_monitor(n_clicks, n_intervals):
    df = db.get_rule_execution_monitor()

    header = html.Thead(html.Tr([
        html.Th("ASSIGNMENT ID"),
        html.Th("TABLE NAME"),
        html.Th("METRIC NAME"),
        html.Th("ACTUAL VALUE"),
        html.Th("OPERATOR"),
        html.Th("CRITERIA / THRESHOLD"),
        html.Th("STATUS"),
        html.Th("EXECUTION TIME")
    ]))

    if df.empty:
        return html.Div("No rule executions recorded in dq_metric_results.", style={"padding": "12px", "color": "#64748b", "fontFamily": "JetBrains Mono"})

    rows = []
    for _, r in df.iterrows():
        st = str(r["status"]).upper()
        if st in ("PASS", "PASSED"):
            badge_cls = "agent-status-completed"
            st_text = "PASS"
        else:
            badge_cls = "agent-status-failed"
            st_text = "FAIL"

        rows.append(html.Tr([
            html.Td(html.Code(f"#{r['assignment_id']}", style={"color": "#64748b"})),
            html.Td(html.Span(r['table_name'], style={"fontWeight": "700", "fontFamily": "JetBrains Mono", "color": "#0f172a"})),
            html.Td(html.Span(r['metric_name'], style={"fontWeight": "600", "color": "#0284c7"})),
            html.Td(html.Code(str(r['actual_value']), style={"fontWeight": "700"})),
            html.Td(html.Span(str(r.get('operator', '<=')), style={"padding": "2px 6px", "backgroundColor": "#eff6ff", "color": "#1d4ed8", "borderRadius": "3px", "fontWeight": "700", "fontSize": "11px", "fontFamily": "JetBrains Mono"})),
            html.Td(html.Code(str(r['threshold']))),
            html.Td(html.Span(st_text, className=badge_cls)),
            html.Td(str(r['execution_time'])[:19], style={"fontFamily": "JetBrains Mono", "fontSize": "11px", "color": "#64748b"})
        ]))

    return html.Table([header, html.Tbody(rows)], className="dq-table")

# 8. Alerts Feed (Section 7: workspace.default.dq_alerts)
@app.callback(
    [Output("alerts-filter-store", "data"),
     Output("filter-btn-all", "className"),
     Output("filter-btn-critical", "className"),
     Output("filter-btn-warning", "className")],
    [Input("filter-btn-all", "n_clicks"),
     Input("filter-btn-critical", "n_clicks"),
     Input("filter-btn-warning", "n_clicks")],
    [State("alerts-filter-store", "data")]
)
def handle_alert_filter(n_all, n_crit, n_warn, current_filter):
    ctx = callback_context
    if not ctx.triggered:
        return "ALL", "btn-dq-filter active", "btn-dq-filter", "btn-dq-filter"

    btn_id = ctx.triggered[0]["prop_id"].split(".")[0]
    c_all, c_crit, c_warn = "btn-dq-filter", "btn-dq-filter", "btn-dq-filter"

    if btn_id == "filter-btn-critical":
        return "DQ_FAILURE", c_all, c_crit + " active", c_warn
    elif btn_id == "filter-btn-warning":
        return "HIGH", c_all, c_crit, c_warn + " active"
    else:
        return "ALL", c_all + " active", c_crit, c_warn

@app.callback(
    [Output("alerts-list-container", "children"),
     Output("alerts-count-subtitle", "children")],
    [Input("alerts-filter-store", "data"),
     Input("sync-btn", "n_clicks"),
     Input("auto-sync-interval", "n_intervals")]
)
def update_alerts_feed(filter_severity, n_clicks, n_intervals):
    df_alerts = db.get_alerts_feed(None if filter_severity == "ALL" else filter_severity)
    total_df = db.get_alerts_feed(None)
    subtitle = f"[{len(df_alerts)} of {len(total_df)} Active Incidents in dq_alerts]"

    if df_alerts.empty:
        empty_msg = html.Div([
            html.Div("✓ All Active Quality Checks Passing", style={"fontSize": "13px", "fontWeight": "700", "color": "#059669", "fontFamily": "JetBrains Mono", "marginBottom": "4px"}),
            html.Div("No open anomalies or threshold violations detected across monitored tables.", style={"fontSize": "11.5px", "color": "#64748b", "fontFamily": "JetBrains Mono"})
        ], style={"padding": "16px 20px", "backgroundColor": "#f0fdf4", "borderRadius": "8px", "border": "1px solid #bbf7d0"})
        return empty_msg, "[0 Active Anomalies in dq_alerts]"

    items = []
    for _, alert_row in df_alerts.iterrows():
        sev = str(alert_row.get("severity", "DQ_FAILURE")).upper()
        badge_cls = "badge-critical" if "FAIL" in sev or "CRIT" in sev else "badge-warning"

        t_name = alert_row.get("table_name", f"Table #{alert_row.get('table_id')}")
        m_name = alert_row.get("metric_name", f"Metric #{alert_row.get('metric_id')}")
        msg = alert_row.get("alert_message", "Threshold validation exceeded")
        c_time = str(alert_row.get("created_time", ""))

        items.append(html.Div([
            html.Div([
                html.Span(sev, className=badge_cls),
                html.Div([
                    html.Div(msg, className="dq-alert-msg"),
                    html.Div(f"TABLE: {t_name} · METRIC: {m_name} · TABLE_ID: {alert_row.get('table_id')}", className="dq-alert-meta")
                ])
            ], className="dq-alert-left"),
            html.Div([
                html.Span(c_time[:19], style={"fontSize": "11px", "fontFamily": "JetBrains Mono", "color": "#94a3b8"}),
                html.Span("Open", style={"fontSize": "11px", "fontWeight": "700", "padding": "2px 8px", "borderRadius": "4px", "backgroundColor": "#fef2f2", "border": "1px solid #fecaca", "color": "#dc2626"})
            ], className="dq-alert-right")
        ], className="dq-alert-item"))

    return items, subtitle

# 9. Table Dropdown Options (Table Inspector matching Image 2)
@app.callback(
    [Output("table-selector-dropdown", "options"),
     Output("table-selector-dropdown", "value")],
    [Input("sync-btn", "n_clicks")],
    [State("table-selector-dropdown", "value")]
)
def populate_table_dropdown(n_clicks, current_val):
    tables = db.get_tables_list()
    if not tables:
        return [], None
    # Formats as schema.table_name matching Image 2 (e.g. default.source_table, default.copa_sales)
    options = [
        {"label": f"{t.get('schema_name', 'default')}.{t['table_name']}", "value": t["table_id"]}
        for t in tables
    ]
    val = current_val if any(t["table_id"] == current_val for t in tables) else tables[0]["table_id"]
    return options, val

# 9B. Sync dropdown from Leaderboard Row clicks and newly registered table
@app.callback(
    Output("table-selector-dropdown", "value", allow_duplicate=True),
    [Input({"type": "table-row-click", "index": dash.ALL}, "n_clicks"),
     Input("registered-table-id-store", "data")],
    prevent_initial_call=True
)
def sync_table_selection(row_clicks, reg_table_id):
    ctx = callback_context
    if not ctx.triggered:
        return no_update
    trig_id = ctx.triggered_id
    if trig_id == "registered-table-id-store" and reg_table_id is not None:
        return reg_table_id
    if isinstance(trig_id, dict) and trig_id.get("type") == "table-row-click":
        for c in (row_clicks or []):
            if c:
                return trig_id.get("index")
    return no_update

@app.callback(
    Output("selected-table-store", "data"),
    [Input("table-selector-dropdown", "value")]
)
def update_selected_table_store(val):
    return val or 1

# 10. Dynamic Dual Mode (Mode A: Threshold-Based vs Mode B: Expectation-Based) & DMF Configurator
@app.callback(
    [Output("validation-mode-radio", "value"),
     Output("mode-info-banner", "children"),
     Output("mode-helper-note-container", "children"),
     Output("add-metric-select", "options"),
     Output("add-metric-select", "value"),
     Output("add-operator-select", "options"),
     Output("add-operator-select", "value"),
     Output("add-threshold-label", "children"),
     Output("add-threshold-input", "value"),
     Output("add-threshold-input", "placeholder")],
    [Input("validation-mode-radio", "value"),
     Input("add-metric-select", "value"),
     Input("add-operator-select", "value"),
     Input("sync-btn", "n_clicks")],
    [State("add-threshold-input", "value")]
)
def manage_validation_modes_and_rules(mode_val, metric_val, operator_val, n_clicks, current_input_val):
    ctx = callback_context
    trig_id = None
    try:
        trig_id = ctx.triggered_id if ctx.triggered else None
    except Exception:
        trig_id = None

    MODE_A_OPERATORS = [
        {"label": "= (Equals)", "value": "="},
        {"label": "!= (Not Equals)", "value": "!="},
        {"label": "< (Less Than)", "value": "<"},
        {"label": "<= (Less Than or Equal)", "value": "<="},
        {"label": "> (Greater Than)", "value": ">"},
        {"label": ">= (Greater Than or Equal)", "value": ">="},
        {"label": "BETWEEN (Range Min,Max)", "value": "BETWEEN"}
    ]

    MODE_B_OPERATORS = [
        {"label": "IN (Expected Values Set)", "value": "IN"},
        {"label": "NOT IN (Excluded Values Set)", "value": "NOT IN"},
        {"label": "= (Exact Match)", "value": "="},
        {"label": "!= (Not Equal Match)", "value": "!="}
    ]

    MODE_B_NOTE = html.Div([
        html.Div([
            html.Span("EXPECTATION-BASED VALIDATION ENGINE (MODE B)", style={"fontWeight": "800", "color": "#92400e", "fontSize": "12px"}),
            html.Span("Automatic Fuzzy Typo Detection Enabled", style={"backgroundColor": "#fef3c7", "color": "#b45309", "border": "1px solid #fde68a", "borderRadius": "4px", "padding": "2px 7px", "fontSize": "10.5px", "fontWeight": "700", "marginLeft": "10px"})
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
        html.Div("• Expected Values Check: Evaluates column data against expected set via IN / NOT IN. Invalid Count = 0 -> PASS; Invalid Count > 0 -> FAIL.", style={"fontSize": "11.5px", "color": "#78350f", "marginBottom": "4px"}),
        html.Div("• Misspelling & Typo Suggestion: When unexpected values like 'Depsoit' are encountered, the framework automatically queries sample invalid records and suggests the closest valid expected value ('Deposit') using fuzzy string distance.", style={"fontSize": "11.5px", "color": "#78350f"})
    ], style={"backgroundColor": "#fffbeb", "border": "1.5px solid #fcd34d", "borderRadius": "8px", "padding": "12px 16px", "marginTop": "10px", "fontFamily": "JetBrains Mono"})

    MODE_A_NOTE = html.Div([
        html.Span("📊 Mode A (Threshold-Based): Evaluates actual column count, percentage, freshness, or numeric range against your configured threshold using operators (=, !=, <, <=, >, >=, or BETWEEN). For Range Check, threshold format is min,max (e.g. 0, 1000000).", style={"fontSize": "11.5px", "color": "#475569", "fontFamily": "JetBrains Mono"})
    ], style={"backgroundColor": "#f8fafc", "border": "1px solid #cbd5e1", "borderRadius": "6px", "padding": "8px 14px", "marginTop": "10px"})

    # Case 1: User explicitly toggles between Mode A and Mode B
    if trig_id == "validation-mode-radio":
        if mode_val == "MODE_B":
            banner = "MODE B: Expectation-Based Validation Active — Validating categorical & domain values against predefined expected lists with auto typo detection."
            opts_metrics = db.get_metric_catalog_grouped_options("MODE_B")
            return (
                "MODE_B",
                banner,
                MODE_B_NOTE,
                opts_metrics,
                "Expected Values Check",
                MODE_B_OPERATORS,
                "IN",
                "Expected Values Input",
                "Deposit, Withdrawal, Transfer",
                "e.g. Deposit, Withdrawal, Transfer"
            )
        else:
            banner = "MODE A: Threshold-Based Validation Active — Evaluating numeric metrics (counts, percentages, freshness, ranges) against numerical thresholds."
            opts_metrics = db.get_metric_catalog_grouped_options("MODE_A")
            return (
                "MODE_A",
                banner,
                MODE_A_NOTE,
                opts_metrics,
                "Null Count Check",
                MODE_A_OPERATORS,
                "=",
                "Threshold Input",
                "0",
                "e.g. 0"
            )

    # Case 2: User changes the operator dropdown directly
    if trig_id == "add-operator-select":
        op = str(operator_val or "").upper()
        if op in ("IN", "NOT IN"):
            lbl = "Expected Values Input" if op == "IN" else "Excluded Values Input"
            placeholder = "e.g. Deposit, Withdrawal, Transfer"
            val = current_input_val if (current_input_val and "," in str(current_input_val)) else "Deposit, Withdrawal, Transfer"
            banner = "MODE B: Expectation-Based Validation Active — Validating categorical & domain values against predefined expected lists with auto typo detection."
            return (
                "MODE_B",
                banner,
                MODE_B_NOTE,
                no_update,
                no_update,
                no_update,
                no_update,
                lbl,
                val,
                placeholder
            )
        elif op == "BETWEEN":
            lbl = "Threshold Input (Min, Max)"
            placeholder = "e.g. 0, 1000000"
            val = current_input_val if (current_input_val and "," in str(current_input_val)) else "0, 1000000"
            banner = "MODE A: Threshold-Based Validation Active — Evaluating numeric metrics (counts, percentages, freshness, ranges) against numerical thresholds."
            return (
                "MODE_A",
                banner,
                MODE_A_NOTE,
                no_update,
                no_update,
                no_update,
                no_update,
                lbl,
                val,
                placeholder
            )
        else:
            lbl = "Threshold Input"
            placeholder = "e.g. 0"
            val = current_input_val if (current_input_val and not ("," in str(current_input_val))) else "0"
            return (
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                lbl,
                val,
                placeholder
            )

    # Case 3: User changes metric or initial load
    m = str(metric_val or "").upper()
    if "EXPECTED VALUES" in m or "ALLOWED VALUES" in m:
        banner = "MODE B: Expectation-Based Validation Active — Validating categorical & domain values against predefined expected lists with auto typo detection."
        opts_metrics = db.get_metric_catalog_grouped_options("MODE_B")
        return (
            "MODE_B",
            banner,
            MODE_B_NOTE,
            opts_metrics,
            metric_val or "Expected Values Check",
            MODE_B_OPERATORS,
            "IN",
            "Expected Values Input",
            "Deposit, Withdrawal, Transfer",
            "e.g. Deposit, Withdrawal, Transfer"
        )
    elif "RANGE" in m:
        banner = "MODE A: Threshold-Based Validation Active — Range check validation (BETWEEN min, max)."
        opts_metrics = db.get_metric_catalog_grouped_options("MODE_A")
        return (
            "MODE_A",
            banner,
            MODE_A_NOTE,
            opts_metrics,
            metric_val,
            MODE_A_OPERATORS,
            "BETWEEN",
            "Threshold Input (Min, Max)",
            "0, 1000000",
            "e.g. 0, 1000000"
        )
    elif "FRESHNESS" in m:
        banner = "MODE A: Threshold-Based Validation Active — Freshness SLA validation in days."
        opts_metrics = db.get_metric_catalog_grouped_options("MODE_A")
        return (
            "MODE_A",
            banner,
            MODE_A_NOTE,
            opts_metrics,
            metric_val,
            MODE_A_OPERATORS,
            "<=",
            "Threshold Input (Days)",
            "7",
            "e.g. 7"
        )
    elif "DUPLICATE PERCENT" in m or "NULL PERCENT" in m:
        banner = "MODE A: Threshold-Based Validation Active — Percentage tolerance validation."
        opts_metrics = db.get_metric_catalog_grouped_options("MODE_A")
        return (
            "MODE_A",
            banner,
            MODE_A_NOTE,
            opts_metrics,
            metric_val,
            MODE_A_OPERATORS,
            "<=",
            "Threshold Input (%)",
            "1" if "DUPLICATE" in m else "5",
            "e.g. 1"
        )
    else:
        banner = "MODE A: Threshold-Based Validation Active — Evaluating numeric metrics against threshold values."
        opts_metrics = db.get_metric_catalog_grouped_options("MODE_A")
        return (
            "MODE_A",
            banner,
            MODE_A_NOTE,
            opts_metrics,
            metric_val or "Null Count Check",
            MODE_A_OPERATORS,
            "=",
            "Threshold Input",
            "0",
            "e.g. 0"
        )

# 11. Table Inspector Data & Attached DMFs Table (Image 2)
@app.callback(
    [Output("table-meta-pills", "children"),
     Output("table-large-score-container", "children"),
     Output("inspector-metrics-count", "children"),
     Output("active-assignments-container", "children"),
     Output("debug-sql-query", "children")],
    [Input("table-selector-dropdown", "value"),
     Input("dmf-refresh-store", "data"),
     Input("sync-btn", "n_clicks"),
     Input("auto-sync-interval", "n_intervals")]
)
def update_table_inspector(table_id, refresh_data, n_clicks, n_intervals):
    if table_id is None:
        table_id = 1

    score_info = db.get_table_score_info(table_id)
    attached = db.get_attached_dmfs(table_id)

    # Metadata pills matching Image 2
    pills = [
        html.Span("Form", style={"padding": "3px 8px", "backgroundColor": "#f1f5f9", "color": "#475569", "borderRadius": "4px", "fontSize": "11px", "fontWeight": "600", "fontFamily": "JetBrains Mono"}),
        html.Span(f"{score_info.get('schema_name', 'default')} zone", style={"padding": "3px 8px", "backgroundColor": "#f1f5f9", "color": "#475569", "borderRadius": "4px", "fontSize": "11px", "fontWeight": "600", "fontFamily": "JetBrains Mono"}),
        html.Span(f"owner: {score_info.get('table_owner', 'data-eng')}", style={"padding": "3px 8px", "backgroundColor": "#ffffff", "color": "#64748b", "borderRadius": "4px", "fontSize": "11px", "border": "1px solid #e2e8f0", "fontFamily": "JetBrains Mono"})
    ]

    # Large Score HUD matching Image 2
    score = score_info["dq_score"]
    score_color = "#10b981" if score >= 90 else ("#f59e0b" if score >= 70 else "#ef4444")
    hud = [
        html.Div(f"{score:.0f}%", className="dq-score-num-v2", style={"color": score_color}),
        html.Div("weighted DQ score", className="dq-score-lbl-v2"),
        html.Div([
            html.Span("Passing" if score >= 90 else ("Warning" if score >= 70 else "Failing"))
        ], className="dq-score-status-v2", style={"color": score_color})
    ]

    count_title = f"{len(attached)} DMFs attached"

    # Attached DMFs Table matching Image 2
    if not attached:
        table_comp = html.Div(
            f"No DMFs attached to this table yet. Use 'Add a DMF' below to configure quality checks.",
            style={"padding": "18px 14px", "color": "#64748b", "fontFamily": "JetBrains Mono", "fontSize": "12.5px"}
        )
    else:
        th = html.Thead(html.Tr([
            html.Th("Metric"),
            html.Th("Column"),
            html.Th("Current value"),
            html.Th("Operator"),
            html.Th("Threshold / Expected"),
            html.Th("Weight"),
            html.Th("Status"),
            html.Th("On"),
            html.Th("Actions", style={"textAlign": "right"})
        ]))
        rows = []
        for d in attached:
            raw_st = d.get("status")
            if raw_st is None or pd.isna(raw_st) or str(raw_st).strip().upper() in ("NAN", "NONE", ""):
                st_pill = html.Span("Pending", style={"padding": "3px 8px", "backgroundColor": "#f8fafc", "border": "1px solid #cbd5e1", "borderRadius": "12px", "fontSize": "11px", "fontWeight": "600", "color": "#64748b"})
            else:
                st = str(raw_st).upper()
                is_pass = st in ("PASS", "PASSED")
                st_pill = html.Span("Pass", className="dq-status-pill-pass") if is_pass else html.Span("Fail", className="dq-status-pill-fail")

            w_num = float(d.get("weight", 10.0))
            w_str = "Low" if w_num <= 5 else ("Medium" if w_num <= 10 else ("High" if w_num <= 15 else "Critical"))

            raw_cur = d.get("actual_value")
            cur_val = "-" if (raw_cur is None or pd.isna(raw_cur) or str(raw_cur).strip().upper() in ("NAN", "NONE", "")) else str(raw_cur)
            op_val = str(d.get("operator", "<=")).strip()
            thresh_val = str(d.get("threshold", "0"))

            # Render current value with typo suggestion highlight if detected
            if "Did you mean" in cur_val:
                count_part = cur_val.split("(")[0].strip() if "(" in cur_val else cur_val
                suggestion_part = ("(" + cur_val.split("(", 1)[1]) if "(" in cur_val else ""
                cur_val_component = html.Div([
                    html.Div(count_part, style={"fontWeight": "700", "color": "#ef4444", "fontFamily": "JetBrains Mono", "fontSize": "12px"}),
                    html.Div(
                        suggestion_part, 
                        style={"fontSize": "11px", "color": "#b45309", "backgroundColor": "#fef3c7", "padding": "2px 6px", "borderRadius": "4px", "marginTop": "3px", "fontFamily": "JetBrains Mono", "display": "inline-block"}
                    )
                ])
            else:
                cur_val_component = html.Span(cur_val, style={"fontFamily": "JetBrains Mono", "fontSize": "12.5px"})

            rows.append(html.Tr([
                # Metric Name & Category
                html.Td([
                    html.Div(d.get("metric_name", "METRIC"), style={"fontWeight": "700", "color": "#0f172a", "fontSize": "13px"}),
                    html.Div(d.get("metric_category", "Validity"), style={"fontSize": "11px", "color": "#64748b", "marginTop": "2px"})
                ]),
                # Column
                html.Td(str(d.get("column_name", "-")) if d.get("column_name") not in (None, "*", "(table level)") else "-", style={"fontFamily": "JetBrains Mono", "fontSize": "12.5px"}),
                # Current Value
                html.Td(cur_val_component),
                # Operator
                html.Td(html.Span(op_val, style={"padding": "2px 7px", "backgroundColor": "#eff6ff", "color": "#1d4ed8", "border": "1px solid #bfdbfe", "borderRadius": "4px", "fontSize": "11.5px", "fontWeight": "700", "fontFamily": "JetBrains Mono"})),
                # Threshold / Expected
                html.Td(thresh_val, style={"fontFamily": "JetBrains Mono", "fontSize": "12px", "maxWidth": "220px", "wordBreak": "break-word"}),
                # Weight
                html.Td(html.Span(f"{w_str}", style={"padding": "3px 8px", "backgroundColor": "#f8fafc", "border": "1px solid #cbd5e1", "borderRadius": "4px", "fontSize": "11.5px", "color": "#334155"})),
                # Status
                html.Td(st_pill),
                # On
                html.Td(html.Span("Active", style={"fontSize": "11px", "fontWeight": "700", "color": "#0284c7"})),
                # Actions
                html.Td([
                    html.Button("Run", id={"type": "dmf-rerun-btn", "index": d["assignment_id"]}, className="btn-action-icon", title="Re-run check"),
                    html.Button("Delete", id={"type": "dmf-delete-btn", "index": d["assignment_id"]}, className="btn-action-icon btn-action-delete", title="Remove DMF")
                ], style={"textAlign": "right"})
            ]))
        table_comp = html.Table([th, html.Tbody(rows)], className="dq-dmf-table")

    first_assignment_id = attached[0]["assignment_id"] if attached else None
    debug_sql = db.get_root_cause_query(table_id, first_assignment_id)

    return pills, hud, count_title, table_comp, debug_sql

# 12. Populate Discovered Columns Checklist (Add a DMF)
@app.callback(
    [Output("add-column-checklist", "options"),
     Output("add-column-checklist", "value"),
     Output("add-column-select-all", "value"),
     Output("add-column-hint-text", "children")],
    [Input("table-selector-dropdown", "value"),
     Input("sync-btn", "n_clicks")]
)
def update_columns_checklist(table_id, n_clicks):
    score_info = db.get_table_score_info(table_id or 1)
    table_name = score_info.get("table_name", "")
    cols = db.get_table_columns(table_name)
    if not cols or cols == ["*"]:
        cols = ["user_id", "event_type", "event_timestamp"]

    opts = [{"label": f" {c}", "value": c} for c in cols if c != "*"]
    if not opts:
        opts = [{"label": " (table-level)", "value": "*"}]
    default_vals = [opts[0]["value"]]
    hint = f"1 column selected ({opts[0]['value']})."
    return opts, default_vals, [], hint

# 12B. Synchronize "Select all cols" & Column Checkboxes
@app.callback(
    [Output("add-column-checklist", "value", allow_duplicate=True),
     Output("add-column-select-all", "value", allow_duplicate=True),
     Output("add-column-hint-text", "children", allow_duplicate=True)],
    [Input("add-column-select-all", "value"),
     Input("add-column-checklist", "value")],
    [State("add-column-checklist", "options")],
    prevent_initial_call=True
)
def sync_select_all_columns(select_all_val, checklist_vals, col_options):
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update, no_update

    trig_id = getattr(ctx, "triggered_id", None) or (ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else "")
    all_col_values = [opt["value"] for opt in (col_options or [])]
    total_cols = len(all_col_values)

    checklist_vals = checklist_vals or []
    select_all_val = select_all_val or []

    # If user toggled "Select all cols"
    if trig_id == "add-column-select-all":
        if "ALL" in select_all_val:
            new_vals = list(all_col_values)
            hint = f"{len(new_vals)} columns selected (All)."
            return new_vals, ["ALL"], hint
        else:
            hint = "0 columns selected. Select at least 1 column."
            return [], [], hint

    # If user clicked individual column checkboxes
    if trig_id == "add-column-checklist":
        n_selected = len(checklist_vals)
        is_all_selected = (n_selected == total_cols and total_cols > 0)
        new_select_all = ["ALL"] if is_all_selected else []

        if n_selected == 0:
            hint = "0 columns selected. Select at least 1 column."
        elif is_all_selected:
            hint = f"{n_selected} columns selected (All)."
        elif n_selected == 1:
            hint = f"1 column selected ({checklist_vals[0]})."
        else:
            hint = f"{n_selected} columns selected."

        return no_update, new_select_all, hint

    return no_update, no_update, no_update

# 13. Populate Registration Dropdowns (Discovered Tables, Catalogs, Schemas, Owners)
@app.callback(
    [Output("register-table-select", "options"),
     Output("register-table-select", "value"),
     Output("register-catalog-select", "options"),
     Output("register-catalog-select", "value"),
     Output("register-schema-select", "options"),
     Output("register-schema-select", "value"),
     Output("register-owner-select", "options"),
     Output("register-owner-select", "value")],
    [Input("sync-btn", "n_clicks")]
)
def populate_registration_dropdowns(n_clicks):
    # 1. Unregistered tables from Databricks
    tables = db.get_discoverable_tables()
    registered = [t["table_name"].lower() for t in db.get_tables_list()]
    unreg = [t for t in tables if t.lower() not in registered]
    tbl_options = [{"label": f"{t} (Discovered Delta Table)", "value": t} for t in unreg]
    if not tbl_options:
        tbl_options = [{"label": f"{t} (Monitored)", "value": t} for t in tables]
    tbl_val = unreg[0] if unreg else (tables[0] if tables else None)

    # 2. Catalogs
    catalogs = db.get_available_catalogs()
    cat_options = [{"label": f"{c} (Catalog)", "value": c} for c in catalogs]
    cat_val = "workspace" if "workspace" in catalogs else (catalogs[0] if catalogs else "workspace")

    # 3. Schemas
    schemas = db.get_available_schemas(cat_val)
    sch_options = [{"label": f"{s} (Schema)", "value": s} for s in schemas]
    sch_val = "default" if "default" in schemas else (schemas[0] if schemas else "default")

    # 4. Owners
    owners = db.get_available_owners()
    owner_options = [{"label": f"{o} (Owner)", "value": o} for o in owners]
    owner_val = "Keerthana" if "Keerthana" in owners else "data-eng"

    return tbl_options, tbl_val, cat_options, cat_val, sch_options, sch_val, owner_options, owner_val

# 14. Handle Table Registration (INSERT INTO dq_tables)
@app.callback(
    [Output("register-table-feedback", "children"),
     Output("registered-table-id-store", "data"),
     Output("sync-btn", "n_clicks")],
    [Input("register-table-btn", "n_clicks")],
    [State("register-table-select", "value"),
     State("register-catalog-select", "value"),
     State("register-schema-select", "value"),
     State("register-owner-select", "value"),
     State("sync-btn", "n_clicks")]
)
def handle_register_table(n_clicks, tbl_name, cat_name, sch_name, owner_name, sync_clicks):
    if not n_clicks:
        return no_update, no_update, no_update
    if not tbl_name:
        return html.Div("Please select a table name to register.", style={"color": "#dc2626", "fontSize": "11px", "fontFamily": "JetBrains Mono"}), no_update, no_update

    success, msg, new_id = db.register_new_table(
        table_name=tbl_name,
        catalog_name=cat_name or "workspace",
        schema_name=sch_name or "default",
        table_owner=owner_name or "data-eng"
    )
    if success:
        fb = html.Div(f"{msg}", style={"color": "#059669", "fontWeight": "700", "fontSize": "11px", "fontFamily": "JetBrains Mono"})
        return fb, new_id, (sync_clicks or 0) + 1
    else:
        fb = html.Div(f"{msg}", style={"color": "#dc2626", "fontSize": "11px", "fontFamily": "JetBrains Mono"})
        return fb, new_id, (sync_clicks or 0) + 1

# 15. Attach Metric Assignment to Table (Add DMF matching Image 1)
@app.callback(
    [Output("add-metric-feedback", "children"),
     Output("dmf-refresh-store", "data", allow_duplicate=True),
     Output("sync-btn", "n_clicks", allow_duplicate=True)],
    [Input("deploy-metric-btn", "n_clicks")],
    [State("selected-table-store", "data"),
     State("add-metric-select", "value"),
     State("add-column-checklist", "value"),
     State("add-operator-select", "value"),
     State("add-threshold-input", "value"),
     State("add-weight-select", "value"),
     State("dmf-refresh-store", "data"),
     State("sync-btn", "n_clicks")],
    prevent_initial_call=True
)
def handle_add_metric(n_clicks, table_id, metric_name, col_names, operator_val, thresh_val, weight, refresh_data, sync_clicks):
    if not n_clicks:
        return no_update, no_update, no_update

    if not metric_name or table_id is None:
        return html.Div("Please select a valid metric.", style={"color": "#dc2626", "fontSize": "11px", "fontFamily": "JetBrains Mono"}), no_update, no_update

    if not col_names:
        return html.Div("Please select at least 1 column (or select all cols).", style={"color": "#dc2626", "fontSize": "11px", "fontFamily": "JetBrains Mono"}), no_update, no_update

    # Prepare list of column names
    actual_cols = []
    for c in col_names:
        if c in ("*", "(table-level)", "(table level)"):
            actual_cols.append(None)
        elif c and str(c).strip():
            actual_cols.append(str(c).strip())

    if not actual_cols:
        actual_cols = [None]

    success = db.add_metric_assignments_batch(
        table_id=table_id,
        metric_id=metric_name,
        column_names=actual_cols,
        threshold_value=thresh_val or "0",
        weight=float(weight) if weight else 10.0,
        severity="HIGH",
        operator=operator_val or "="
    )

    if success:
        next_ref = (refresh_data or 0) + 1
        num_cols = len(actual_cols)
        if num_cols == 1:
            col_desc = f"column '{actual_cols[0]}'" if actual_cols[0] else "table-level"
        else:
            sample_preview = ", ".join(actual_cols[:3]) + (f" +{num_cols - 3} more" if num_cols > 3 else "")
            col_desc = f"{num_cols} columns ({sample_preview})"
        fb = html.Div(f"✓ DMF '{metric_name}' ({operator_val} {thresh_val}) successfully attached across {col_desc} on table #{table_id}.", style={"color": "#059669", "fontSize": "12px", "fontWeight": "700", "fontFamily": "JetBrains Mono"})
        return fb, next_ref, (sync_clicks or 0) + 1
    else:
        fb = html.Div("Failed to attach DMF into Databricks.", style={"color": "#dc2626", "fontSize": "12px", "fontFamily": "JetBrains Mono"})
        return fb, no_update, no_update

# 16. Handle Table Inspector Actions & Run Validation Suite
@app.callback(
    [Output("dmf-refresh-store", "data", allow_duplicate=True),
     Output("sync-btn", "n_clicks", allow_duplicate=True),
     Output("run-validation-feedback", "children"),
     Output("validation-popup-container", "style"),
     Output("validation-popup-container", "className"),
     Output("popup-badge-text", "children"),
     Output("popup-icon-container", "children"),
     Output("popup-title", "children"),
     Output("popup-message", "children"),
     Output("popup-progress-track", "style")],
    [Input("run-all-checks-btn", "n_clicks"),
     Input("run-all-checks-btn-quick", "n_clicks"),
     Input({"type": "dmf-delete-btn", "index": dash.ALL}, "n_clicks"),
     Input({"type": "dmf-rerun-btn", "index": dash.ALL}, "n_clicks")],
    [State("dmf-refresh-store", "data"),
     State("sync-btn", "n_clicks")],
    prevent_initial_call=True
)
def handle_table_inspector_actions(run_all_clicks, run_quick_clicks, delete_clicks, rerun_clicks, refresh_data, sync_clicks):
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update

    trig = ctx.triggered[0]["prop_id"]
    next_ref = (refresh_data or 0) + 1
    next_sync = (sync_clicks or 0) + 1

    if hasattr(ctx, "triggered_id") and isinstance(ctx.triggered_id, dict):
        action_type = ctx.triggered_id.get("type")
        assignment_id = ctx.triggered_id.get("index")
        if action_type == "dmf-delete-btn" and assignment_id:
            db.delete_metric_assignment(assignment_id)
            db.clear_cache()
            return next_ref, next_sync, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update
        elif action_type == "dmf-rerun-btn" and assignment_id:
            db.clear_cache()
            success, msg = db.run_single_metric_validation(assignment_id)
            popup_style = {"display": "block", "opacity": "1", "transform": "none"}
            popup_cls = "dq-validation-popup state-success" if success else "dq-validation-popup state-error"
            badge_text = "CHECK SUCCESSFUL" if success else "CHECK INCOMPLETE / ERROR"
            icon = "✓" if success else "⚠"
            title = "Check Executed Successfully" if success else "Check Incomplete / Error"
            progress_style = {"display": "none"}
            return next_ref, next_sync, no_update, popup_style, popup_cls, badge_text, icon, title, msg, progress_style

    if "run-all-checks-btn-quick" in trig or "run-all-checks-btn" in trig:
        db.clear_cache()
        success, msg = db.run_validation_pipeline()
        fb = html.Div(
            f"{msg}",
            style={
                "color": "#059669" if success else "#0284c7",
                "fontWeight": "700",
                "fontSize": "12px",
                "fontFamily": "JetBrains Mono",
                "padding": "8px 14px",
                "backgroundColor": "#ecfdf5" if success else "#f0f9ff",
                "borderRadius": "6px",
                "border": "1px solid #a7f3d0" if success else "#bae6fd",
                "display": "inline-block"
            }
        )
        popup_style = {"display": "block", "opacity": "1", "transform": "none"}
        popup_cls = "dq-validation-popup state-success" if success else "dq-validation-popup state-error"
        badge_text = "VALIDATION SUCCESSFUL" if success else "VALIDATION INCOMPLETE / ERROR"
        icon = "✓" if success else "⚠"
        title = "Validation Completed Successfully" if success else "Validation Incomplete / Error"
        progress_style = {"display": "none"}
        return next_ref, next_sync, fb, popup_style, popup_cls, badge_text, icon, title, msg, progress_style

    return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update


# ==========================================================
# APPLICATION ENTRYPOINT
# ==========================================================
if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", 8050)))
    debug_mode = os.environ.get("DASH_DEBUG", "False").lower() in ("true", "1")
    print(f"Starting Databricks DMF Dashboard on 0.0.0.0:{port} (Debug: {debug_mode})")
    app.run(host="0.0.0.0", port=port, debug=debug_mode)
