import pandas as pd
from data_extraction import DataExtractor
import numpy as np


class DataCleaning:

    # clean user data. removes null values, errors with dates,
    # incorrectly typed values and rows filled with the wrong information
    def clean_user_data(self):
        df = DataExtractor().read_rds_table("legacy_users")
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], infer_datetime_format=True,
                                             errors="coerce")
        df["join_date"] = pd.to_datetime(df["join_date"], infer_datetime_format=True,
                                         errors="coerce")
        df.dropna(subset=["date_of_birth", "join_date"],
                  inplace=True)

        return df

    # clean the data to remove any erroneous values, NULL values or errors with formatting
    def clean_card_data(self):
        df = DataExtractor().retrieve_pdf_data(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

        df["date_payment_confirmed"] = pd.to_datetime(df["date_payment_confirmed"],
                                                      infer_datetime_format=True,
                                                      errors="coerce")
        df["card_number"] = df["card_number"].astype(str)
        df["card_number"] = df["card_number"].str.replace('\W', '',
                                                          regex=True)
        df["card_number"] = df["card_number"].apply(
            lambda x: np.nan if x == 'NULL' else x)
        df.dropna(subset=["card_number", "date_payment_confirmed"],
                  inplace=True)

        card_details = pd.DataFrame(df)

        return card_details

    # cleans the data retrieve from the API and returns a pandas DataFrame.
    def clean_store_data(self):
        data = DataExtractor().retrieve_stores_data()
        data.replace({"continent": ["eeEurope", "eeAmerica"]}, {"continent": ["Europe", "America"]},
                     inplace=True)
        data.drop(columns="lat", inplace=True)
        data["opening_date"] = pd.to_datetime(data["opening_date"], infer_datetime_format=True,
                                              errors="coerce")
        data["store_type"] = data["store_type"].astype(str)
        data["store_type"] = data["store_type"].apply(
            lambda x: np.nan if x == "NULL" else x)
        data.dropna(subset=["opening_date", "store_type"], inplace=True)
        data["staff_numbers"] = data["staff_numbers"].str.replace(r"\D", "")

        return data

    # this will take the products DataFrame as an argument and return the products DataFrame.
    def convert_product_weights(self, products_df):
        products_df["weight"] = products_df["weight"].apply(str)
        products_df.replace({"weight": ["12 x 100g", "8 x 150g"]}, {"weight": ["1200g", "1200g"]}, inplace=True)
        filter_letters = lambda x: "".join(y for y in x if not y.isdigit())
        products_df["units"] = products_df["weight"].apply(filter_letters)
        products_df["weight"] = products_df["weight"].str.extract("([\d.]+)").astype(float)
        products_df["weight"] = products_df.apply(
            lambda x: x["weight"] / 1000 if x["units"] == "g" or x["units"] == "ml" else x["weight"], axis=1)
        products_df.drop(columns="units", inplace=True)
        return products_df

    # this method will clean the DataFrame of any additional erroneous values.
    def clean_products_data(self, products_dataframe):
        products_df = self.convert_product_weights(
            products_dataframe)
        products_df.dropna(subset=["uuid", "product_code"], inplace=True)
        products_df["date_added"] = pd.to_datetime(products_df["date_added"], format="%Y-%m-%d", errors="coerce")
        drop_prod_list = ["S1YB74MLMJ", "C3NCA2CL35", "WVPMHZP59U"]
        products_df.drop(products_df[products_df["category"].isin(drop_prod_list)].index, inplace=True)

        return products_df

    # will clean the orders table data. remove the columns, first_name, last_name and 1 to have the table in the
    # correct form before uploading to the database.
    def clean_orders_data(self):
        orders_df = DataExtractor().read_rds_table("orders_table")
        orders_df.drop(columns=["1", "first_name", "last_name", "level_0"], inplace=True)
        return orders_df

    def clean_date_times(self):
        date_time_df = DataExtractor().extract_from_s3_json()
        date_time_df["day"] = pd.to_numeric(date_time_df["day"], errors="coerce")
        date_time_df.dropna(subset=["day", "year", "month"], inplace=True)
        date_time_df["timestamp"] = pd.to_datetime(date_time_df["timestamp"], format="%H:%M:%S",
                                                   errors="coerce")
        return date_time_df
