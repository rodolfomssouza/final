"""
Module for financial analysis
"""

module module_fin

using DataFrames
using MarketData

export prob_lowest_price, get_market_data, merge_data_with_continous_dates

# %% Functions ----------------------------------------------------------------


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
    num_rows = size(df, 1)
    num_rows_check = 1
    while num_rows > num_rows_check
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
        dfr = dropmissing(df)
        num_rows_check = size(dfr, 1)
    end
    return df
end

end
