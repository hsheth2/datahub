import pathlib

import requests


def get_dataset(dataset_urn):
    cookies = {
        "api_key": "Pkj-Xla9dQKqJBO9xeT8NgPBndhy4wQo1UokCMCSfwLEK1vy61fkl1wh--H4AonVSry5EJo5lQeA_1yxEt91jQ",
        "bid": "bd84fc3f-b817-4b17-b17b-6325846ea379",
        "PLAY_SESSION": "5001875762c5b72e22949cfeaac0bf9f8c21fda9-actor=urn%3Ali%3Acorpuser%3Adatahub&token=eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiU0VTU0lPTiIsInZlcnNpb24iOiIxIiwiZXhwIjoxNjUyMDM4MTI5LCJqdGkiOiIzYTE4YTZkMi05NjgwLTQzM2YtYTU4ZS0xNWYzODRiYjZmOGQiLCJzdWIiOiJkYXRhaHViIiwiaXNzIjoiZGF0YWh1Yi1tZXRhZGF0YS1zZXJ2aWNlIn0.J7EpO2pJy4ZmmN_yOXdLp4bLoHwO6O4ggRBrmOyA558",
        "actor": "urn:li:corpuser:datahub",
    }

    json_data = {
        "query": (pathlib.Path(__file__).parent / "get-dataset.gql").read_text(),
        "variables": {
            "urn": dataset_urn,
        },
    }

    response = requests.post(
        "http://localhost:9002/api/graphql",
        cookies=cookies,
        json=json_data,
    )

    return response.json()["data"]["dataset"]


if __name__ == "__main__":
    from pprint import pprint

    pprint(
        get_dataset(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)"
        )
    )
