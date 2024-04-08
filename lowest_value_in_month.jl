"""
Finds the lowest value in a month of data.
"""

# %% Packages ----------------------------------------------------------------
using Dates
using DataFrames
using MarketData
using Statistics
using Plots


# %% Get market data ----------------------------------------------------------
asset = "SWPPX"

# Get the data
df0 = yahoo(:SWPPX)
# df0 = yahoo(:SWISX)
# df0 = yahoo(:SWSSX)

# Convert the date to a dataframe
df = DataFrame(df0);

# Add normalized Open column
df.NOpen = (df.Open .- minimum(df.Open)) ./ (maximum(df.Open) .- minimum(df.Open));

# Extract day from dates
df.date = Date.(df.timestamp);

# Extract month from dates
df.day = day.(df.date);

# Aggregate by day
df1 = combine(groupby(df, :day), :Open => mean);

# Plot the data
plot(
    df1.day,
    df1.Open_mean,
    xlabel = "Day",
    ylabel = "Normalized Open",
    title = "Normalized Open by Day",
    legend = :topleft,
)
