import plotly.graph_objects as go
from plotly.subplots import make_subplots


def visualize_historical_trends(historical_df):
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Size Growth Over Time", "File Counts Over Time", 
                        "Efficiency Metrics Over Time", "Operations Over Time"),
        specs=[[{"secondary_y": True}, {"secondary_y": False}],
               [{"secondary_y": True}, {"secondary_y": False}]]
    )
    
    fig.add_trace(
        go.Scatter(
            x=historical_df["date"],
            y=historical_df["table_size_bytes"] / (1024**2),
            name="Table Size (MB)",
            line=dict(color="royalblue", width=3)
        ),
        row=1, col=1, secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=historical_df["date"],
            y=historical_df["folder_size_bytes"] / (1024**2),
            name="Folder Size (MB)",
            line=dict(color="red", width=3, dash="dot")
        ),
        row=1, col=1, secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=historical_df["date"],
            y=historical_df["record_count"],
            name="Record Count",
            line=dict(color="green", width=2)
        ),
        row=1, col=1, secondary_y=True
    )
    
    if "total_file_count" in historical_df.columns and not historical_df["total_file_count"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["total_file_count"],
                name="Total Files",
                line=dict(color="blue", width=3)
            ),
            row=1, col=2
        )
    
    if "data_file_count" in historical_df.columns and not historical_df["data_file_count"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["data_file_count"],
                name="Data Files",
                line=dict(color="purple", width=3)
            ),
            row=1, col=2
        )
    
    if "small_files_count" in historical_df.columns and not historical_df["small_files_count"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["small_files_count"],
                name="Small Files",
                line=dict(color="orange", width=3)
            ),
            row=1, col=2
        )
    
    if "orphan_files_count" in historical_df.columns and not historical_df["orphan_files_count"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["orphan_files_count"],
                name="Orphan Files",
                line=dict(color="red", width=3)
            ),
            row=1, col=2
        )
    
    if "file_size_efficiency" in historical_df.columns and not historical_df["file_size_efficiency"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["file_size_efficiency"],
                name="File Size Efficiency",
                line=dict(color="teal", width=3)
            ),
            row=2, col=1, secondary_y=False
        )
    
    if "storage_efficiency" in historical_df.columns and not historical_df["storage_efficiency"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["storage_efficiency"],
                name="Storage Efficiency",
                line=dict(color="darkorange", width=3)
            ),
            row=2, col=1, secondary_y=False
        )
    
    if "partition_skewness" in historical_df.columns and not historical_df["partition_skewness"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["partition_skewness"],
                name="Partition Skewness",
                line=dict(color="brown", width=3)
            ),
            row=2, col=1, secondary_y=True
        )
    
    if "number_of_writes" in historical_df.columns and not historical_df["number_of_writes"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["number_of_writes"],
                name="Writes",
                line=dict(color="blue", width=3)
            ),
            row=2, col=2
        )
    
    if "number_of_deletes" in historical_df.columns and not historical_df["number_of_deletes"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["number_of_deletes"],
                name="Deletes",
                line=dict(color="red", width=3)
            ),
            row=2, col=2
        )
    
    if "number_of_optimizes" in historical_df.columns and not historical_df["number_of_optimizes"].isna().all():
        fig.add_trace(
            go.Scatter(
                x=historical_df["date"],
                y=historical_df["number_of_optimizes"],
                name="Optimizes",
                line=dict(color="green", width=3)
            ),
            row=2, col=2
        )
    
    fig.update_xaxes(title_text="Date", row=1, col=1)
    fig.update_xaxes(title_text="Date", row=1, col=2)
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=2)
    
    fig.update_yaxes(title_text="Size (MB)", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Record Count", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="File Count", row=1, col=2)
    fig.update_yaxes(title_text="Efficiency", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Skewness", row=2, col=1, secondary_y=True)
    fig.update_yaxes(title_text="Operation Count", row=2, col=2)
    
    fig.update_layout(
        title_text="Historical Trends of Delta Table Health Metrics",
        height=800,
        width=1200,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig