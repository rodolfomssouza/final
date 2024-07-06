"""
Finds the day with the highest probability of the lowest price
"""

# %% Packages ----------------------------------------------------------------
using Dates
using DataFrames
using MarketData
using Statistics
using Plots


# %% Functions ---------------------------------------------------------------
function prob_lowest_price(df::DataFrame)
    """
    Finds the day with the highest probability of the lowest price
    """
    # Extract day, month, and year from dates
    df.day = day.(df.date)
    df.month = month.(df.date)
    df.year = year.(df.date)

    # Add year-month column
    df.year_month = string.(df.year, "-", df.month)

    # Find the lowest price in each year-month
    df1 = combine(groupby(df, :year_month), :Low => minimum)

    # Find which day in each year-month has the lowest price
    df[!, :lowest_price] .= 0

    # Assign 1 to the day with the lowest price
    for i = 1:size(df1, 1)
        year_month = df1.year_month[i]
        lowest_price = df1.Low_minimum[i]
        idx = findall(x -> x == year_month, df.year_month)
        idx_lowest = findall(x -> x == lowest_price, df.Low)
        idx = intersect(idx, idx_lowest)
        df[idx, :lowest_price] .= 1
    end

    df_prob = combine(groupby(df, :day), :lowest_price => mean)

    # Rename columns
    DataFrames.rename!(df_prob, :day => :Day, :lowest_price_mean => :Probability)

    return df_prob
end


function get_market_data(asset::Symbol)
    """
    Get market data
    """
    # Get the data
    df0 = yahoo(asset)

    # Convert the date to a dataframe
    df = DataFrame(df0)

    # Extract day from dates
    df.date = Date.(df.timestamp)

    return df
end

function merge_data_with_continous_dates(df)
    """
    Create a continuous date range
    """
    d0 = first(df.date)
    d1 = last(df.date)
    dates = []
    for i = d0:d1
        push!(dates, i)
    end
    df_dates = DataFrame(date = dates)
    # Merge the dataframes
    df = outerjoin(df_dates, df, on = :date)
    # Sort by date
    df = sort(df, :date)
    return df
end


function fill_missing_data(df)
    """
    If data is missing, fill with next available data
    """
    for i = 1:size(df, 1)
        if ismissing(df.Low[i])
            df.Low[i] = df.Low[i + 1]
        end
        if ismissing(df.High[i])
            df.High[i] = df.High[i + 1]
        end
        if ismissing(df.Open[i])
            df.Open[i] = df.Open[i + 1]
        end
        if ismissing(df.Close[i])
            df.Close[i] = df.Close[i + 1]
        end
        if ismissing(df.Volume[i])
            df.Volume[i] = df.Volume[i + 1]
        end
    end
    return df
end


function main(asset::Symbol)
    """
    Main function
    """
    # Get market data
    df = get_market_data(asset)

    # Find the day with the highest probability of the lowest price
    df_prob = prob_lowest_price(df)

    return df_prob
end


function verify_price_month(df::DataFrame)
    """
    Verify the price range for each year-month
    """
    # Extract day, month, and year from dates
    df.day = day.(df.date)
    df.month = month.(df.date)
    df.year = year.(df.date)
    df.year_month = string.(df.year, "-", df.month)
    # Minimum and maximum prices each year-month
    df_min = combine(groupby(df, :year_month), :Low => minimum)
    df_max = combine(groupby(df, :year_month), :High => maximum)

    df_range = innerjoin(df_min, df_max, on = :year_month)

    df[!, :normalized_price] .= 0.0

    # Find the day with the lowest price
    for i = 1:size(df_range, 1)
        year_month = df_range.year_month[i]
        low_price = df_range.Low_minimum[i]
        high_price = df_range.High_maximum[i]
        idx_month = findall(x -> x == year_month, df.year_month)
        # Price normalized to the range
        price_range = (df.Low[idx_month] .- low_price) ./ (high_price .- low_price)
        df[idx_month, :normalized_price] .= price_range
    end

    df2 = combine(groupby(df, :day), :normalized_price => mean, :normalized_price => std)
    DataFrames.rename!(
        df2,
        :day => :Day,
        :normalized_price_mean => :Mean,
        :normalized_price_std => :Std,
    )

    return df, df2
end

# %% Calculate lowest price probability --------------------------------------
swppx = main(:SWPPX);
swisx = main(:SWISX);
swssx = main(:SWSSX);
swagx = main(:SWAGX);

# Plot the data
p1 = plot(
    swppx.Day,
    swppx.Probability,
    xlabel = "Day",
    ylabel = "Probability of Lowest Price",
    label = "SWPPX",
    legend = :topright,
)

p1 = plot!(swisx.Day, swisx.Probability, label = "SWISX")
p1 = plot!(swssx.Day, swssx.Probability, label = "SWSSX")
p1 = plot!(swagx.Day, swagx.Probability, label = "SWAGX")

display(p1)

# %% Tests -------------------------------------------------------------------
df = get_market_data(:SWPPX);
dfp = swppx;
# df = get_market_data(:SWISX);
# df = get_market_data(:SWSSX);
df, df1 = verify_price_month(df);

# Plot normalized price and standard deviation
p2 = plot(
    df1.Day,
    df1.Mean,
    linewidth = 2,
    ribbon = df1.Std,
    xlabel = "Day",
    ylabel = "Normalized Price",
    title = "Normalized Price by Day",
)

# Plot price and probability of lowest price
p3a = plot(
    df1.Day,
    df1.Mean,
    ribbon = df1.Std,
    linewidth = 2,
    xlabel = "Day",
    ylabel = "Normalized Price",
)

p3b = twinx()
plot!(
    p3b,
    dfp.Day,
    dfp.Probability,
    linewidth = 2,
    color = :red,
    label = "Probability",
    ylabel = "Probability of Lowest Price",
)
