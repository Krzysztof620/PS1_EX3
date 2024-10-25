import pandas as pd
import polars as pl
import matplotlib.pyplot as plt

# Chapter2

""" 
complaints = pd.read_csv("./data/311-service-requests.csv", dtype="unicode")
complaints.head() 
"""

pl_complaints = pl.read_csv("./data/311-service-requests.csv", infer_schema_length=0)
pl_complaints.head()

""" 
complaints["Complaint Type"]
complaints[:5]
complaints["Complaint Type"][:5]
complaints[["Complaint Type", "Borough"]]
 """

pl_complaints.select("Complaint Type")
pl_complaints.head(5)
pl_complaints.select("Complaint Type").head(5)
pl_complaints.select(["Complaint Type", "Borough"])

""" 
complaint_counts = complaints["Complaint Type"].value_counts()
complaint_counts[:10]
"""

pl_complaint_counts = (
    pl_complaints.select(pl.col("Complaint Type").value_counts(sort=True, name="n"))
    .head(10)
    .unnest("Complaint Type")
)
pl_complaint_counts

""" 
complaint_counts[:10].plot(kind="bar")
plt.title("Top 10 Complaint Types")
plt.xlabel("Complaint Type")
plt.ylabel("Count")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()
"""

pl_complaint_counts.plot.bar(x="Complaint Type", y="n").properties(
    width=500, title="Top 10 Complaint Types"
)

# Chapter3

""" 
pd.set_option("display.max_columns", 60)
complaints = pd.read_csv("./data/311-service-requests.csv", dtype="unicode")
complaints
"""

# Specify 'Incident Zip' as a string column during CSV reading
pl_complaints = pl.read_csv(
    "./data/311-service-requests.csv",
    null_values="N/A",  # Handle 'N/A' as null values
    dtypes={"Incident Zip": pl.Utf8},  # Explicitly specify 'Incident Zip' as a string
)

pl_complaints

""" 
complaints[:5]
"""

pl_complaints.head()

""" 
noise_complaints = complaints[complaints["Complaint Type"] == "Noise - Street/Sidewalk"]
noise_complaints[:3]
"""

pl_noise_complaints = pl_complaints.filter(
    pl.col("Complaint Type") == "Noise - Street/Sidewalk"
)
pl_noise_complaints.head(3)

""" 
is_noise = complaints["Complaint Type"] == "Noise - Street/Sidewalk"
in_brooklyn = complaints["Borough"] == "BROOKLYN"
complaints[is_noise & in_brooklyn][:5]
"""

pl_is_noise = pl.col("Complaint Type") == "Noise - Street/Sidewalk"
pl_in_brooklyn = pl.col("Borough") == "BROOKLYN"
pl_complaints.filter(pl_is_noise & pl_in_brooklyn).head(5)

""" 
complaints[is_noise & in_brooklyn][
    ["Complaint Type", "Borough", "Created Date", "Descriptor"]
][:10]
"""

pl_complaints.filter(pl_is_noise & pl_in_brooklyn).select(
    ["Complaint Type", "Borough", "Created Date", "Descriptor"]
).head(10)

""" 
is_noise = complaints["Complaint Type"] == "Noise - Street/Sidewalk"
noise_complaints = complaints[is_noise]
noise_complaints["Borough"].value_counts()
"""

pl_noise_complaints = (
    pl_complaints.filter(pl.col("Complaint Type") == "Noise - Street/Sidewalk")
    .select(pl.col("Borough").value_counts(sort=True, name="n"))
    .unnest("Borough")
)
pl_noise_complaints

""" 
noise_complaint_counts = noise_complaints["Borough"].value_counts()
complaint_counts = complaints["Borough"].value_counts()
noise_complaint_counts / complaint_counts.astype(float)
"""

alphabetical_pl_noise_complaints = (
    pl_complaints.filter(pl.col("Complaint Type") == "Noise - Street/Sidewalk")
    .select(pl.col("Borough").value_counts())
    .unnest("Borough")
    .sort("Borough")
)
alphabetical_pl_complaint_counts = (
    pl_complaints.select(pl.col("Borough").value_counts())
    .unnest("Borough")
    .sort("Borough")
)
pl_noise_complaint_fraction = alphabetical_pl_noise_complaints.join(
    alphabetical_pl_complaint_counts, on="Borough"
)
pl_noise_complaint_fraction = pl_noise_complaint_fraction.with_columns(
    (pl.col("count") / pl.col("count_right")).alias("count_ratio")
).select(["Borough", "count_ratio"])
pl_noise_complaint_fraction

""" 
(noise_complaint_counts / complaint_counts.astype(float)).plot(kind="bar")
plt.title("Noise Complaints by Borough (Normalized)")
plt.xlabel("Borough")
plt.ylabel("Ratio of Noise Complaints to Total Complaints")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
"""

pl_noise_complaint_fraction.plot.bar(x="Borough", y="count_ratio").properties(
    width=500, title="Noise Complaints by Borough (Normalized)"
)
