from chalice import Chalice
from weather import Weather, Unit
import requests
import json
import os
import boto3
from concurrent.futures import ThreadPoolExecutor
from aws_xray_sdk.core import patch_all, xray_recorder

app = Chalice(app_name='xray-weather-sample')


@app.route('/')
def index():
    patch_all()
    try:
        data = fetch_weather_data()
        formatted_data = format_data(data)
        xray_recorder.begin_subsegment('mysub')
        update_database(json.loads(formatted_data)['data'])
        notify_sns(json.loads(formatted_data)['data'])
        xray_recorder.end_subsegment()
        return formatted_data
    except Exception as e:
        print(e)


@app.route('/trim', methods=['POST'], content_types=['application/x-www-form-urlencoded', 'application/json'])
def trim_data():
    try:
        request = app.current_request
        data = request.raw_body.decode("utf-8")
        forecast_data_map = json.loads(data)
        items_map = json.loads(forecast_data_map['data'])
        forecast_data = []
        for date, content in items_map.items():
            content = {**content, 'date': date}
            forecast_data.append(content)
        return {'data': forecast_data}
    except Exception as e:
        print(e)


def fetch_weather_data():
    weather = Weather(unit=Unit.CELSIUS)
    location = weather.lookup_by_location('sydney')
    forecasts = location.forecast
    output = {}
    for forecast in forecasts:
        forecast_data_map = {}
        forecast_data_map['forecast'] = forecast.text
        forecast_data_map['high'] = forecast.high
        forecast_data_map['low'] = forecast.low
        output[forecast.date] = forecast_data_map
    return output


def format_data(data):
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    response = requests.post('https://92qef2fmvh.execute-api.ap-southeast-2.amazonaws.com/api/trim',
                             json={"data": json.dumps(data)}, headers=headers)
    return response.text


def update_database(data):
    resource = boto3.resource('dynamodb')
    table = resource.Table(os.environ['XRAY_DYNAMO_TABLE'])
    if isinstance(data, list):
        # for d in data:
        #     table.put_item(
        #         Item=d
        #     )

        with ThreadPoolExecutor(max_workers=5) as insert_db_executor:
            for d in data:
                insert_db_executor.submit(table.put_item,  Item=d)


def notify_sns(data):
    sns_client = boto3.client('sns')
    sns_client.publish(
        TargetArn=os.environ['XRAY_SNS_TOPIC'],
        Message=json.dumps(
            {'default': json.dumps(data)}),
        MessageStructure='json'
    )
