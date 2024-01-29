from data_quality.data_quality_test import DataQualityTest


def main():
    data_quality = DataQualityTest()
    data_quality.validate_unique_ids()
    data_quality.validate_not_null_sale_value()
    data_quality.validate_sale_date()


if __name__ == '__main__':
    main()
