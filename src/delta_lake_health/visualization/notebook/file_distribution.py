import os
import numpy as np
import pandas as pd
from deltalake import DeltaTable
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from delta_lake_health.health_analyzers.base_analyzer import HealthStatus


def analyze_file_distribution(table_path, analyzer=None):
    dt = DeltaTable(table_path)
    file_uris = dt.file_uris()
    
    file_sizes = []
    file_names = []
    
    for uri in file_uris:
        if uri.startswith("file://"):
            local_path = uri[7:]
        else:
            local_path = uri
            
        if "_delta_log" in local_path:
            continue
            
        try:
            file_size = os.path.getsize(local_path)
            file_sizes.append(file_size / (1024 * 1024))
            file_names.append(os.path.basename(local_path))
        except (FileNotFoundError, OSError):
            continue
    
    files_df = pd.DataFrame({
        "file_name": file_names,
        "size_mb": file_sizes
    })
    
    files_df = files_df.sort_values("size_mb", ascending=False)
    
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "histogram"}, {"type": "bar"}]],
        subplot_titles=("File Size Histogram", "Partition Record Count")
    )
    
    fig.add_trace(
        go.Histogram(
            x=files_df["size_mb"],
            nbinsx=20,
            marker_color="green",
            hovertemplate="Size range: %{x} MB<br>Count: %{y}<extra></extra>"
        ),
        row=1, col=1
    )
    
    # Use the skew_metrics from the analyzer for partition distribution
    try:

        partition_data_available = False
        if analyzer is not None and hasattr(analyzer, 'result') and hasattr(analyzer.result, 'metrics'):
            metrics = analyzer.result.metrics
            if hasattr(metrics, 'skew_metrics') and 'records_per_partition_dict' in metrics.skew_metrics:
                partition_data_available = True
                
                records_per_partition = metrics.skew_metrics['records_per_partition_dict']
                partition_cols = metrics.skew_metrics.get('partition_columns', ['partition'])
                
                partitions_df = pd.DataFrame({
                    'partition': list(records_per_partition.keys()),
                    'record_count': list(records_per_partition.values())
                })
                partitions_df = partitions_df.sort_values('record_count', ascending=False)
                
                top_n = min(20, len(partitions_df))
                top_partitions = partitions_df.head(top_n)
                
                fig.add_trace(
                    go.Bar(
                        x=top_partitions["partition"],
                        y=top_partitions["record_count"],
                        marker_color="royalblue",
                        text=top_partitions["record_count"],
                        textposition="auto",
                        hovertemplate="<b>%{x}</b><br>Records: %{y}<extra></extra>"
                    ),
                    row=1, col=2
                )
                
                fig.update_xaxes(
                    title_text=f"Partition: {', '.join(partition_cols)}", 
                    tickangle=45, 
                    row=1, col=2
                )

    except Exception as e:
        fig.add_annotation(
            x=0.75, y=0.5,
            xref="paper", yref="paper",
            text=f"Could not analyze partitions: {str(e)}",
            showarrow=False,
            font=dict(size=12),
            row=1, col=2
        )

    fig.update_layout(
        title_text="Delta Table File and Partition Analysis",
        showlegend=False,
        height=500,
        width=1000
    )
    
    fig.update_xaxes(title_text="File Size (MB)", row=1, col=1)
    fig.update_yaxes(title_text="Count", row=1, col=1)
    fig.update_yaxes(title_text="Record Count", row=1, col=2)
    
    return fig, files_df