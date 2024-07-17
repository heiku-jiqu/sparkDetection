from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(0)
g = (
    (
        fake.random_int(min=1,max=10000),
        fake.random_int(min=1,max=10000),
        x,
        fake.name(),
        fake.random_int(min=1656964539,max=1720151739)
    ) for x in range(1_000_000)
)
detection = pd.DataFrame(
    g,
    columns=[
        "geographical_location_oid", 
        "video_camera_oid", 
        "detection_oid", 
        "item_name", 
        "timestamp_detected"]
)
detection.to_parquet("detection.parquet")

g2 = (
    (
        x,
        fake.address()
    ) for x in range(10_000)
)

location = pd.DataFrame(
    g2,
    columns=["geographical_location_oid", "geographical_location"]
)
location.to_parquet("location.parquet")
