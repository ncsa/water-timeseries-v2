import seaborn as sns
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def prepare_data_for_plot_dw(
    df: pd.DataFrame, group_vegetation: bool = True
) -> pd.DataFrame:
    """
    Restructure data for plotting.

    This function prepares a time-series DataFrame for visualization using seaborn by melting the DataFrame
    and optionally grouping specific vegetation types into a single category.

    Parameters:
    -----------
    df : pd.DataFrame
        The input DataFrame containing time series data with columns for various land cover types,
        including 'water', 'snow_and_ice', 'bare', and vegetation types.

    group_vegetation : bool, optional
        If True, groups the values of 'flooded_vegetation', 'grass', 'shrub_and_scrub', and 'trees'
        into a single 'vegetation' category. Defaults to True.

    Returns:
    --------
    pd.DataFrame
        A DataFrame formatted for plotting, containing the date and values for land cover categories.
        If `group_vegetation` is True, it includes 'water', 'bare', 'snow_and_ice', and a summed
        'vegetation' category. Otherwise, it includes individual vegetation types as well.

    Example:
    --------
    >>> df_plot = prepare_data_for_plot(data_frame, group_vegetation=True)

    Notes:
    ------
    Ensure that the input DataFrame contains the necessary columns before calling this function.
    Specifically, it should include columns for 'water', 'bare', 'snow_and_ice',
    and any specified vegetation types.
    """

    # Preprocess to correct fields
    data_plot = df.reset_index(drop=False)
    data_plot["date"] = pd.to_datetime(df.reset_index()["date"])

    # Melt for plotting with seaborn
    if group_vegetation:
        data_plot["vegetation"] = data_plot[
            ["flooded_vegetation", "grass", "shrub_and_scrub", "trees"]
        ].sum(axis=1)
        df_plot_prepared = data_plot.melt(
            id_vars="date",
            value_name="value",
            value_vars=["water", "bare", "snow_and_ice", "vegetation"],
        )
    else:
        df_plot_prepared = data_plot.melt(
            id_vars="date",
            value_name="value",
            value_vars=[
                "water",
                "bare",
                "snow_and_ice",
                "flooded_vegetation",
                "grass",
                "shrub_and_scrub",
                "trees",
            ],
        )

    return df_plot_prepared


def plot_water_time_series_dw(
    df: pd.DataFrame, first_break: pd.Timestamp | None, normalization_factor=None
) -> plt.figure:
    """
    Plots a time series of water area with a vertical line indicating a specified date.

    This function visualizes the time series data for water area alongside other land cover types.
    It highlights a significant date with a vertical line and provides an option for displaying
    normalized values on a secondary y-axis.

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame containing the time series data. It must include columns for 'date', 'value',
        and 'variable', where 'variable' indicates the type of land cover (e.g., 'water', 'bare', etc.).

    first_break : pd.Timestamp
        The date for the vertical line on the plot, indicating a significant event or change in the time series.

    normalization_factor : float, optional
        If provided, a secondary y-axis will be added showing normalized values as percentages.
        The normalization_factor is used to calculate these percentages. If None, only the primary
        y-axis (absolute values) will be shown. Defaults to None.

    Returns:
    --------
    plt.figure
        The matplotlib figure object containing the plotted time series with breaks indicated.

    Example:
    --------
    >>> fig = plot_water_time_series(df_plot, first_break=pd.Timestamp('2023-06-01'), normalization_factor=100)

    Notes:
    ------
    - Ensure that the input DataFrame contains the necessary columns before calling this function.
      Specifically, it should include 'date', 'value', and 'variable'.
    - The primary y-axis always shows absolute values in hectares.
    - If a normalization_factor is provided, a secondary y-axis will display relative area percentages.
    - Specific colors are used for 'water' (blue), 'bare' (brown), 'vegetation' (green), and 'snow_ice' (black).
    """

    fig, ax1 = plt.subplots(figsize=(10, 5))

    # Define color mapping
    color_map = {
        "water": "#4a90e2",  # Blue
        "bare": "#8B4513",  # Brown
        "vegetation": "#228B22",  # Green
        "snow_and_ice": "#000000",  # Black
    }

    # Plot each variable separately to control colors
    for variable in df["variable"].unique():
        data = df[df["variable"] == variable]
        color = color_map.get(variable, None)  # Use predefined color or None

        linewidth = 1.5 if variable == "water" else 0.3
        markersize = 6 if variable == "water" else 5

        sns.lineplot(
            data=data,
            x="date",
            y="value",
            color=color,
            linewidth=linewidth,
            marker="o",
            markersize=markersize,
            label=variable,
            ax=ax1,
        )

    if first_break is not None:
        # Add vertical line at first_break
        ax1.vlines(
            pd.to_datetime(first_break),
            ymin=0,
            ymax=df["value"].max(),
            color="k",
            ls="--",
        )

    # Set x-axis major locator and formatter for date formatting
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    # Rotate x-tick labels
    plt.setp(ax1.get_xticklabels(), rotation=90)

    # Set labels for left y-axis (absolute values)
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Area [ha]")
    ax1.grid(True)

    if normalization_factor:
        # Create right y-axis for normalized values
        ax2 = ax1.twinx()
        lower, upper = ax1.get_ylim()
        ax2.set_ylim(
            lower / normalization_factor * 100, upper / normalization_factor * 100
        )
        # Set labels for right y-axis (normalized values)
        ax2.set_ylabel("Relative Area [%]")

    plt.title("Lake area time-series")
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1))
    plt.tight_layout()  # Adjust layout to make room for rotated labels and legend

    return plt.gcf()


def plot_water_time_series_jrc(df: pd.DataFrame, first_break: pd.Timestamp | None,  plot_variables: list=['area_water_permanent', 'area_water_seasonal', 'area_land']
) -> plt.figure:
    
    df['area_total'] = df[['area_data', 'area_nodata']].sum(axis=1)
    fig, ax = plt.subplots(figsize=(10, 5))
    color_map = {
        "area_water_permanent": "#4a90e2",  # Blue
        "bare": "#8B4513",  # Brown
        "area_water_seasonal": "#a6bddb"
    }
    # pull annual data from xarray dataset
    # plot annual lake area
    df_melt = df.melt(id_vars=['date'], value_vars=plot_variables)
    sns.lineplot(df_melt, x='date', y='value', hue='variable', ax=ax, marker='o', markersize=6, linewidth=1.5)
    # create no data area
    ax.fill_between(x=df['date'], y1=df['area_total']- df['area_nodata'], y2=df['area_total'], fc=(0.7,0.7,0.7,0.5))
    
    # figure improvements
    # ax.set_xlim(start_date_dt_viz, date.fromisoformat('2022-01-01'))
    ax.set_ylabel('Total lake area [ha]')
    ax.grid(visible=True, which='major', lw=0.5)
    # Set minor gridlines
    ax.xaxis.set_minor_locator(mdates.YearLocator())
    ax.grid(visible=True, which='minor', lw=0.2)
    # ax.set_title('Lake: '+ str(lake_id))
    ax.legend().set_title('')
    
    fig = ax.get_figure()
    # return figure
    return fig