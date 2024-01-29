from infrastructure.boto_session import get_boto_session


def save_data_to_s3(date_now, city, json):
    s3_session = get_boto_session()
    date = date_now.strftime('%d-%m-%Y')
    hour = date_now.hour
    file_name = 'weather_{hour}.json'.format(hour=hour)
    bucket = 'bronze_layer'
    key = '/open_weather/{city}/{date}/{file_name}'.format(city=city, date=date, file_name=file_name)
    s3_session.put_object(Bucket=bucket, Key=key, Body=json)
