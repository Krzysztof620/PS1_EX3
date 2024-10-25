# %%
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import io
import requests

plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 3)
plt.rcParams["font.family"] = "sans-serif"

'''
# %%
# By the end of this chapter, we're going to have downloaded all of Canada's weather data for 2012, and saved it to a CSV. We'll do this by downloading it one month at a time, and then combining all the months together.
# Here's the temperature every hour for 2012!

weather_2012_final = pd.read_csv("data/weather_2012.csv", index_col="date_time")
weather_2012_final["temperature_c"].plot(figsize=(15, 6))
plt.show()
'''

# TODO: rewrite using Polars
weather_2012_final = pl.read_csv("data/weather_2012.csv")

weather_2012_final = weather_2012_final.with_columns([
    pl.col("date_time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
])

dates = weather_2012_final["date_time"].to_list()  
temperatures = weather_2012_final["temperature_c"].to_list()  

plt.figure(figsize=(15, 6))
plt.plot(dates, temperatures, label="Temperature (°C)")
plt.xlabel("Date")
plt.ylabel("Temperature (°C)")
plt.title("Hourly Temperature in 2012")
plt.legend()
plt.show()


'''
# %%
# Okay, let's start from the beginning.
# We're going to get the data for March 2012, and clean it up
# You can directly download a csv with a URL using Pandas!
# Note, the URL the repo provides is faulty but kindly, someone submitted a PR fixing it. Have a look
# here: https://github.com/jvns/pandas-cookbook/pull/74 and click on "Files changed" and then fix the url.


# This URL has to be fixed first!
url_template = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"

year = 2012
month = 3
url_march = url_template.format(month=3, year=2012)
weather_mar2012 = pd.read_csv(
    url_march,
    index_col="Date/Time (LST)",
    parse_dates=True,
    encoding="latin1",
    header=0,
)
weather_mar2012.head()
'''

# TODO: rewrite using Polars. Yes, Polars can handle URLs similarly.
url_template = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"

year = 2012
month = 3
url_march = url_template.format(month=month, year=year)

response = requests.get(url_march)

csv_data = io.StringIO(response.text)

pl_weather_mar2012 = pl.read_csv(
    csv_data,
    encoding="latin1",
)
pl_weather_mar2012 = pl_weather_mar2012.with_columns(
    pl.col("Date/Time (LST)").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")
)

print(pl_weather_mar2012.head())


'''
# %%
# Let's clean up the data a bit.
# You'll notice in the summary above that there are a few columns which are are either entirely empty or only have a few values in them. Let's get rid of all of those with `dropna`.
# The argument `axis=1` to `dropna` means "drop columns", not rows", and `how='any'` means "drop the column if any value is null".
weather_mar2012 = weather_mar2012.dropna(axis=1, how="any")
print(weather_mar2012[:5])

# This is much better now -- we only have columns with real data.
'''

# TODO: rewrite using Polars
non_empty_columns = []

for col in pl_weather_mar2012.columns:

    null_count = pl_weather_mar2012[col].null_count()

    if null_count == 0:
        if pl_weather_mar2012[col].dtype == pl.Utf8:
            empty_string_count = (pl_weather_mar2012[col] == "").sum() 
        else:
            empty_string_count = 0

        if empty_string_count == 0:
            non_empty_columns.append(col)  
pl_weather_mar2012_cleaned = pl_weather_mar2012.select(non_empty_columns)

print(pl_weather_mar2012_cleaned.head())


'''
# %%
# Let's get rid of columns that we do not need.
# For example, the year, month, day, time columns are redundant (we have Date/Time (LST) column).
# Let's get rid of those. The `axis=1` argument means "Drop columns", like before. The default for operations like `dropna` and `drop` is always to operate on rows.
weather_mar2012 = weather_mar2012.drop(["Year", "Month", "Day", "Time (LST)"], axis=1)
weather_mar2012[:5]
'''

# TODO: redo this using polars
pl_weather_mar2012_cleaned = pl_weather_mar2012_cleaned.drop(["Year", "Month", "Day", "Time (LST)"])

print(pl_weather_mar2012_cleaned.head())

'''
# %%
# When you look at the data frame, you see that some column names have some weird characters in them.
# Let's clean this up, too.
# Let's print the column names first:
weather_mar2012.columns

# And now rename the columns to make it easier to work with
weather_mar2012.columns = weather_mar2012.columns.str.replace(
    'ï»¿"', ""
)  # Remove the weird characters at the beginning
weather_mar2012.columns = weather_mar2012.columns.str.replace(
    "Â", ""
)  # Remove the weird characters at the
'''

# TODO: rewrite using Polars
print(pl_weather_mar2012_cleaned.columns)

cleaned_columns = [col.replace('ï»¿"', "").replace("Â", "") for col in pl_weather_mar2012_cleaned.columns]

pl_weather_mar2012_cleaned = pl_weather_mar2012_cleaned.rename({old: new for old, new in zip(pl_weather_mar2012_cleaned.columns, cleaned_columns)})

print(pl_weather_mar2012_cleaned.columns)


'''
# %%
# Optionally, you can also rename columns more manually for specific cases:
weather_mar2012 = weather_mar2012.rename(
    columns={
        'Longitude (x)"': "Longitude",
        "Latitude (y)": "Latitude",
        "Station Name": "Station_Name",
        "Climate ID": "Climate_ID",
        "Temp (°C)": "Temperature_C",
        "Dew Point Temp (Â°C)": "Dew_Point_Temp_C",
        "Rel Hum (%)": "Relative_Humidity",
        "Wind Spd (km/h)": "Wind_Speed_kmh",
        "Visibility (km)": "Visibility_km",
        "Stn Press (kPa)": "Station_Pressure_kPa",
        "Weather": "Weather",
    }
)
weather_mar2012.index.name = "date_time"

# Check the new column names
print(weather_mar2012.columns)

# Some people also prefer lower case column names.
weather_mar2012.columns = weather_mar2012.columns.str.lower()
print(weather_mar2012.columns)
'''


# TODO: redo this using polars
column_renames = {
    "Longitude (x)": "Longitude",
    "Latitude (y)": "Latitude",
    "Station Name": "Station_Name",
    "Climate ID": "Climate_ID",
    "Date/Time (LST)": "Date/Time (LST)",
    "Temp (°C)": "Temperature_C",
    "Dew Point Temp (°C)": "Dew_Point_Temp_C",
    "Rel Hum (%)": "Relative_Humidity",
    "Wind Spd (km/h)": "Wind_Speed_kmh",
    "Visibility (km)": "Visibility_km",
    "Stn Press (kPa)": "Station_Pressure_kPa",
    "Weather": "Weather",
}

pl_weather_mar2012_cleaned = pl_weather_mar2012_cleaned.rename(column_renames)

# Convert all column names to lowercase
pl_weather_mar2012_cleaned.columns = [col.lower() for col in pl_weather_mar2012_cleaned.columns]
print(pl_weather_mar2012_cleaned.columns)


'''
# %%
# Notice how it goes up to 25° C in the middle there? That was a big deal. It was March, and people were wearing shorts outside.
weather_mar2012["temperature_c"].plot(figsize=(15, 5))
plt.show()
'''

# TODO: redo this using polars
dates = pl_weather_mar2012_cleaned["date/time (lst)"].to_list()  
temperatures = pl_weather_mar2012_cleaned["temperature_c"].to_list()  

plt.figure(figsize=(15, 5))
plt.plot(dates, temperatures, label="Temperature (°C)")
plt.xlabel("Date")
plt.ylabel("Temperature (°C)")
plt.title("Hourly Temperature in March 2012")
plt.legend()
plt.show()


'''
# %%
# This one's just for fun -- we've already done this before, using groupby and aggregate! We will learn whether or not it gets colder at night. Well, obviously. But let's do it anyway.
temperatures = weather_mar2012[["temperature_c"]].copy()
print(temperatures.head)
temperatures.loc[:, "Hour"] = weather_mar2012.index.hour
temperatures.groupby("Hour").aggregate(np.median).plot()
plt.show()

# So it looks like the time with the highest median temperature is 2pm. Neat.
'''


# TODO: redo this using polars
temperatures = pl_weather_mar2012_cleaned.select(["date/time (lst)", "temperature_c"])

temperatures = temperatures.with_columns(
    pl.col("date/time (lst)").dt.hour().alias("Hour")
)

hourly_median_temps = temperatures.group_by("Hour").agg(
    pl.col("temperature_c").median().alias("median_temperature")
)

hourly_median_temps = hourly_median_temps.sort("Hour")

hours = hourly_median_temps["Hour"].to_list()
median_temps = hourly_median_temps["median_temperature"].to_list()

plt.figure(figsize=(15, 5))
plt.plot(hours, median_temps, marker="o", label="Median Temperature (°C)")
plt.xlabel("Hour of the Day")
plt.ylabel("Median Temperature (°C)")
plt.title("Median Hourly Temperature for March 2012")
plt.legend()
plt.grid(True)
plt.show()


'''
# %%
# Okay, so what if we want the data for the whole year? Ideally the API would just let us download that, but I couldn't figure out a way to do that.
# First, let's put our work from above into a function that gets the weather for a given month.


def clean_data(data):
    data = data.dropna(axis=1, how="any")
    data = data.drop(["Year", "Month", "Day", "Time (LST)"], axis=1)
    data.columns = data.columns.str.replace('ï»¿"', "")
    data.columns = data.columns.str.replace("Â", "")
    data = data.rename(
        columns={
            "Longitude (x)": "Longitude",
            "Latitude (y)": "Latitude",
            "Station Name": "Station_Name",
            "Climate ID": "Climate_ID",
            "Temp (°C)": "Temperature_C",
            "Dew Point Temp (°C)": "Dew_Point_Temp_C",
            "Rel Hum (%)": "Relative_Humidity",
            "Wind Spd (km/h)": "Wind_Speed_kmh",
            "Visibility (km)": "Visibility_km",
            "Stn Press (kPa)": "Station_Pressure_kPa",
            "Weather": "Weather",
        }
    )
    data.columns = data.columns.str.lower()
    data.index.name = "date_time"
    return data


def download_weather_month(year, month):
    url_template = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"
    url = url_template.format(year=year, month=month)
    weather_data = pd.read_csv(
        url, index_col="Date/Time (LST)", parse_dates=True, header=0
    )
    weather_data_clean = clean_data(weather_data)
    return weather_data_clean

download_weather_month(2012, 1)[:5]
'''


# TODO: redefine these functions using polars and your code above
def clean_data_polars(df):
    non_empty_columns = []

    for col in df.columns:
        null_count = df[col].null_count()

        if null_count == 0:
            # Check for empty strings only for string columns
            if df[col].dtype == pl.Utf8:  # Only check if it's a string column
                empty_string_count = (df[col] == "").sum()
            else:
                empty_string_count = 0

            if empty_string_count == 0:
                non_empty_columns.append(col)

    df = df.select(non_empty_columns)
    
    df = df.drop(["Year", "Month", "Day", "Time (LST)"])

    cleaned_columns = [col.replace('ï»¿"', "").replace("Â", "") for col in df.columns]
    df = df.rename({old: new for old, new in zip(df.columns, cleaned_columns)})

    column_renames = {
        "Longitude (x)": "longitude",
        "Latitude (y)": "latitude",
        "Station Name": "station_name",
        "Climate ID": "climate_id",
        "Date/Time (LST)": "date_time_lst",
        "Temp (°C)": "temperature_c",
        "Dew Point Temp (°C)": "dew_point_temp_c",
        "Rel Hum (%)": "relative_humidity",
        "Wind Spd (km/h)": "wind_speed_kmh",
        "Visibility (km)": "visibility_km",
        "Stn Press (kPa)": "station_pressure_kpa",
        "Weather": "weather",
    }
    df = df.rename(column_renames)

    return df

def download_weather_month_polars(year, month):
    url_template = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"
    url = url_template.format(year=year, month=month)

    # Fetch data from URL
    response = requests.get(url)
    csv_data = io.StringIO(response.text)

    # Read CSV using Polars
    df = pl.read_csv(csv_data, encoding="latin1")

    # Parse datetime column
    df = df.with_columns(
        pl.col("Date/Time (LST)").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")
    )

    # Clean data using the custom function
    df = clean_data_polars(df)

    return df

pl_download_month_1 = download_weather_month_polars(2012, 1)
print(pl_download_month_1.head(5))

'''
# %%
# Now, let's use a list comprehension to download all our data and then just concatenate these data frames
# This might take a while
data_by_month = [download_weather_month(2012, i) for i in range(1, 13)]
weather_2012 = pd.concat(data_by_month)
weather_2012.head()
'''


# TODO: do the same with polars
pl_data_by_month = [download_weather_month_polars(2012, i) for i in range(1, 13)]

# Concatenate the monthly DataFrames into one DataFrame
pl_weather_2012 = pl.concat(pl_data_by_month)

# Display the first few rows of the concatenated DataFrame
print(pl_weather_2012.head(5))


'''
# %%
# Now, let's save the data.
weather_2012.to_csv("../data/weather_2012.csv")
'''

# TODO: use polars to save the data.
pl_weather_2012.write_csv("data/pl_weather_2012.csv")