from plotly.subplots import make_subplots
import plotly.graph_objects as go
from delta_lake_health.health_analyzers.base_analyzer import DeltaAnalyzerMetrics, HealthStatus

def create_health_dashboard(metrics: DeltaAnalyzerMetrics):
    health_score = metrics.health_score
    health_status = metrics.health_status

    fig = make_subplots(
        rows=2, cols=2,
        specs=[[{"type": "indicator"}, {"type": "indicator"}],
               [{"type": "table"}, {"type": "bar"}]],
        subplot_titles=("", "Table Metrics", 
                       "Recommended Actions", "Operation Counts"),
        column_widths=[0.5, 0.5],
        row_heights=[0.5, 0.5]
    )
    
    health_colors = {
        HealthStatus.HEALTHY: "green",
        HealthStatus.UNHEALTHY: "orange",
        HealthStatus.VERY_UNHEALTHY: "red"
    }
    
    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=health_score,
            title={"text": f"Health Score: {health_status.value}"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": health_colors.get(health_status, "gray")},
                "steps": [
                    {"range": [0, 50], "color": "rgba(255, 0, 0, 0.2)"},
                    {"range": [50, 80], "color": "rgba(255, 165, 0, 0.2)"},
                    {"range": [80, 100], "color": "rgba(0, 128, 0, 0.2)"}
                ],
                "threshold": {
                    "line": {"color": "black", "width": 2},
                    "thickness": 0.75,
                    "value": health_score
                }
            }
        ),
        row=1, col=1
    )
    
    metrics_values = [
        metrics.record_count,
        metrics.total_file_count,
        metrics.data_file_count,
        metrics.table_size_bytes / (1024**2) if metrics.table_size_bytes is not None else None,
        metrics.folder_size_bytes / (1024**2) if metrics.folder_size_bytes is not None else None
    ]
    metrics_labels = [
        "Record Count",
        "Total Files",
        "Data Files",
        "Table Size (MB)",
        "Folder Size (MB)"
    ]
    display_metrics = [(label, value) for label, value in zip(metrics_labels, metrics_values) if value is not None]
    
    for i, (label, value) in enumerate(display_metrics):
        if isinstance(value, float):
            value_str = f"{value:,.2f}"
        elif isinstance(value, int):
            value_str = f"{value:,}"
        else:
            value_str = str(value)
        
        fig.add_annotation(
            x=0.75, y=0.95 - i*0.08,
            xref="paper", yref="paper",
            text=f"{label}: <b>{value_str}</b>",
            showarrow=False,
            font=dict(size=11),
            align="left"
        )
    
    recommendations = []
    
    if metrics.needs_vacuum or metrics.files_needing_vacuum > 0:
        recommendations.append(["RUN VACUUM", "Vacuum the table to remove orphan files and reclaim storage space"])
    
    if metrics.needs_optimize or metrics.small_files_count > 10:
        recommendations.append(["RUN OPTIMIZE", "Optimize the table to combine small files and improve query performance"])
    
    if metrics.is_skewed:
        recommendations.append(["REBALANCE PARTITIONS", "Address data skew to ensure better query performance"])
    
    if metrics.has_orphan_files:
        recommendations.append(["CLEAN ORPHAN FILES", "Remove orphan files to reclaim storage space"])

    
    if recommendations:
        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Action", "Description"],
                    fill_color='royalblue',
                    align='left',
                    font=dict(color='white', size=12)
                ),
                cells=dict(
                    values=list(zip(*recommendations)),
                    fill_color='lavender',
                    align='left'
                )
            ),
            row=2, col=1
        )
    else:
        fig.add_annotation(
            x=0.25, y=0.25,
            xref="paper", yref="paper",
            text="No maintenance actions required",
            showarrow=False,
            font=dict(size=14, color="green"),
            align="center"
        )
    
    operation_types = ["Writes", "Deletes", "Optimizes"]
    operation_counts = [metrics.number_of_writes, metrics.number_of_deletes, metrics.number_of_optimizes]
    fig.add_trace(
        go.Bar(
            x=operation_types,
            y=operation_counts,
            marker_color=["royalblue", "crimson", "green"],
            text=operation_counts,
            textposition="auto",
            hovertemplate="<b>%{x}</b><br>Count: %{y}<extra></extra>"
        ),
        row=2, col=2
    )
    fig.update_layout(
        height=800,
        width=1000,
        title_text="Delta Table Health Dashboard",
        showlegend=False
    )
    return fig