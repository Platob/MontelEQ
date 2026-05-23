from energyquantified.metadata import DataType, CurveType

from monteleq import APIClient

if __name__ == "__main__":
    client = APIClient(
        catalog_name="trading_tgp_prd",
        mode="databricks",
    )

    for batch in client.events.stream(
        data_types=DataType.FORECAST,
        batch_size=1
    ):
        print(batch)

    for df in client.curate_curves(
        client.events.requests(),
        insert=True,
    ):
        print(df)