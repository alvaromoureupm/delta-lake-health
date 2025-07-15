import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

def visualize_skew_analysis(metrics):
    if not hasattr(metrics, 'skew_metrics') or not metrics.skew_metrics:
        return go.Figure().update_layout(
            title="No skew metrics available",
            annotations=[dict(
                text="No partition skew data available for analysis",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.5, y=0.5
            )]
        )
    
    skew_metrics = metrics.skew_metrics
    
    records_per_partition = skew_metrics.get('records_per_partition_dict', {})
    
    if not records_per_partition:
        return go.Figure().update_layout(
            title="No partition count data available",
            annotations=[dict(
                text="Partition counts are not available",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.5, y=0.5
            )]
        )
    
    fig = make_subplots(
        rows=2, cols=2,
        specs=[[{"type": "bar"}, {"type": "pie"}],
               [{"type": "indicator", "colspan": 2}, {}]],
        subplot_titles=("Partition Record Distribution", "Partition Size Breakdown", 
                        ""),
        column_widths=[0.6, 0.4],
        row_heights=[0.7, 0.3]
    )
    
    partition_cols = skew_metrics.get('partition_columns', ['partition'])
    
    df = pd.DataFrame({
        'partition': list(records_per_partition.keys()),
        'record_count': list(records_per_partition.values())
    }).sort_values('record_count', ascending=False)
    
    top_n = min(15, len(df))
    top_df = df.head(top_n)
    
    # Bar chart for partition distribution
    fig.add_trace(
        go.Bar(
            x=top_df['partition'],
            y=top_df['record_count'],
            marker_color='royalblue',
            text=top_df['record_count'],
            textposition='auto',
            hovertemplate="<b>%{x}</b><br>Records: %{y}<extra></extra>"
        ),
        row=1, col=1
    )
    
    # Add a line for average records per partition
    avg_records = df['record_count'].mean()
    fig.add_shape(
        type="line",
        x0=-0.5, y0=avg_records, x1=top_n-0.5, y1=avg_records,
        line=dict(color="red", width=2, dash="dash"),
        row=1, col=1
    )
    
    fig.add_annotation(
        x=top_n/2, y=avg_records,
        text=f"Average: {avg_records:.1f}",
        showarrow=False,
        yshift=10,
        font=dict(color="red"),
        row=1, col=1
    )
    
    # Pie chart for partition breakdown
    if len(df) > 10:
        top_10 = df.head(10).copy()
        others = pd.DataFrame({
            'partition': ['Others'],
            'record_count': [df.iloc[10:]['record_count'].sum()]
        })
        pie_df = pd.concat([top_10, others])
    else:
        pie_df = df
    
    fig.add_trace(
        go.Pie(
            labels=pie_df['partition'],
            values=pie_df['record_count'],
            hole=0.4,
            textinfo='percent+label',
            insidetextorientation='radial',
            marker=dict(colors=px.colors.qualitative.Pastel)
        ),
        row=1, col=2
    )
    
    # Skewness gauge
    skewness_max = skew_metrics.get('skewness_max', 0)
    skew_color = "green" if skewness_max < 0.3 else "orange" if skewness_max < 0.7 else "red"
    
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=skewness_max,
            title={"text": "Partition Skewness"},
            gauge={
                "axis": {"range": [0, 1]},
                "bar": {"color": skew_color},
                "steps": [
                    {"range": [0, 0.3], "color": "rgba(0, 250, 0, 0.2)"},
                    {"range": [0.3, 0.7], "color": "rgba(255, 165, 0, 0.2)"},
                    {"range": [0.7, 1], "color": "rgba(255, 0, 0, 0.2)"}
                ],
                "threshold": {
                    "line": {"color": "black", "width": 2},
                    "thickness": 0.75,
                    "value": skewness_max
                }
            },
            delta={
                "reference": skew_metrics.get('threshold', 0.1),
                "increasing": {"color": "red"},
                "decreasing": {"color": "green"}
            }
        ),
        row=2, col=1
    )
    
    count_values = list(records_per_partition.values())
    max_count = max(count_values) if count_values else 0
    min_count = min(count_values) if count_values else 0
    count_mean = sum(count_values) / len(count_values) if count_values else 0
    count_stddev = (sum((c - count_mean)**2 for c in count_values) / len(count_values))**0.5 if count_values else 0
    
    metrics_to_show = [
        ("Max Records", max_count),
        ("Min Records", min_count),
        ("StdDev", count_stddev),
        ("Total Partitions", len(records_per_partition))
    ]
    
    for i, (label, value) in enumerate(metrics_to_show):
        fig.add_annotation(
            x=0.85, y=0.15 - i*0.03,
            xref="paper", yref="paper",
            text=f"{label}: <b>{value:,.1f}</b>",
            showarrow=False,
            font=dict(size=12),
            align="left"
        )
    
    fig.update_layout(
        title_text=f"Partition Skew Analysis ({', '.join(partition_cols)})",
        height=700,
        width=1000,
        showlegend=False
    )
    
    fig.update_xaxes(title_text="Partition", tickangle=45, row=1, col=1)
    fig.update_yaxes(title_text="Record Count", row=1, col=1)
    
    return fig