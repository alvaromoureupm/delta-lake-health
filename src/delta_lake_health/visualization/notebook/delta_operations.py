import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from deltalake import DeltaTable


def visualize_delta_operations(table_path):
    dt = DeltaTable(table_path)
    history = dt.history()
    
    operations = []
    
    for entry in history:
        op_type = entry.get("operation", "")
        timestamp = entry.get("timestamp", "")
        version = entry.get("version", 0)
        
        op_metrics = entry.get("operationMetrics", {})
        op_params = entry.get("operationParameters", {})
        
        hover_text = f"Version: {version}<br>Operation: {op_type}<br>"
        
        if op_metrics:
            hover_text += "<br><b>Operation Metrics:</b><br>"
            for k, v in op_metrics.items():
                k_formatted = k.replace('_', ' ').title()
                if isinstance(v, (int, float)) and 'time' in k and v > 1000:
                    v_formatted = f"{v/1000:.2f} seconds"
                elif isinstance(v, (int, float)) and ('size' in k or 'bytes' in k) and v > 1024:
                    if v > 1024 * 1024 * 1024:
                        v_formatted = f"{v/(1024**3):.2f} GB"
                    elif v > 1024 * 1024:
                        v_formatted = f"{v/(1024**2):.2f} MB"
                    else:
                        v_formatted = f"{v/1024:.2f} KB"
                else:
                    v_formatted = str(v)
                hover_text += f"{k_formatted}: {v_formatted}<br>"
        
        if op_params:
            important_params = {k: v for k, v in op_params.items() 
                               if k in ['predicate', 'partitionBy', 'dataChange', 'description']}
            if important_params:
                hover_text += "<br><b>Parameters:</b><br>"
                for k, v in important_params.items():
                    k_formatted = k.replace('_', ' ').title()
                    if isinstance(v, str) and len(v) > 50:
                        v_formatted = v[:47] + "..."
                    else:
                        v_formatted = str(v)
                    hover_text += f"{k_formatted}: {v_formatted}<br>"
        
        operations.append({
            'operation': op_type,
            'timestamp': timestamp,
            'version': version,
            'hover_text': hover_text,
            'num_files_added': int(op_metrics.get('num_added_files', 0)),
            'num_files_removed': int(op_metrics.get('num_removed_files', 0)),
            'num_rows_added': int(op_metrics.get('num_added_rows', 0)),
            'num_rows_removed': int(op_metrics.get('num_removed_rows', 0)),
            'execution_time_ms': int(op_metrics.get('execution_time_ms', 0))
        })
    
    df = pd.DataFrame(operations)
    
    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            title="No operation history available",
            annotations=[dict(
                text="No Delta table operations found in history",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.5, y=0.5
            )]
        )
        return fig
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    color_map = {
        'WRITE': 'blue',
        'MERGE': 'purple',
        'DELETE': 'red',
        'OPTIMIZE': 'green',
        'VACUUM': 'orange',
        'CREATE TABLE AS SELECT': 'teal',
        'CREATE TABLE': 'teal',
        'RESTORE': 'brown'
    }
    
    df['color'] = df['operation'].map(lambda x: color_map.get(x, 'gray'))
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Operation Timeline", "File and Row Changes"),
        specs=[[{"type": "scatter"}], [{"type": "bar"}]],
        row_heights=[0.6, 0.4],
        vertical_spacing=0.15
    )
    
    for op_type in df['operation'].unique():
        op_df = df[df['operation'] == op_type]
        
        marker_size = op_df.apply(
            lambda x: max(10, min(50, 
                            (x['num_files_added'] + x['num_files_removed'] + 5) * 2)), 
            axis=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=op_df['timestamp'],
                y=op_df['version'],
                mode='markers',
                name=op_type,
                marker=dict(
                    size=marker_size,
                    color=color_map.get(op_type, 'gray'),
                    line=dict(width=1, color='darkgray')
                ),
                text=op_df['hover_text'],
                hoverinfo='text',
                hoverlabel=dict(
                    bgcolor='white',
                    font_size=12,
                    font_family="Arial"
                )
            ),
            row=1, col=1
        )
    
    version_changes = df.set_index('version')
    
    fig.add_trace(
        go.Bar(
            x=version_changes.index,
            y=version_changes['num_files_added'],
            name='Files Added',
            marker_color='rgba(0, 128, 0, 0.7)'
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=version_changes.index,
            y=version_changes['num_files_removed'],
            name='Files Removed',
            marker_color='rgba(255, 0, 0, 0.7)'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        title="Delta Table Operation History",
        height=800,
        width=1000,
        hovermode='closest',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    fig.update_xaxes(title_text="Timestamp", row=1, col=1)
    fig.update_yaxes(title_text="Version", row=1, col=1)
    fig.update_xaxes(title_text="Version", row=2, col=1)
    fig.update_yaxes(title_text="Count", row=2, col=1)
    
    return fig
