import pandas as pd
import json
from datetime import datetime, timedelta
import psycopg2
import dlt
from typing import NoReturn


class OzonOrdersStruct:

  def __init__(self, date_extract: datetime):
    self.date_extract = date_extract.strftime("%Y-%m-%d")
    self.BASE_URL__ORDER_FBO = f"https://storage.yandexcloud.net/traidingbt/ozon_fbo_orders/ozon_fbo_orders_{self.date_extract}.csv"
    self.BASE_URL__ORDER_FBS = f"https://storage.yandexcloud.net/traidingbt/ozon_fbs_orders/ozon_fbs_orders_{self.date_extract}.csv"
    self.BASE_URL__MARKETING = f"https://storage.yandexcloud.net/traidingbt/ozon_marketing_expense/ozon_marketing_expense_{self.date_extract}.csv"
    self.BASE_URL__PRODUCT = f"https://storage.yandexcloud.net/traidingbt/ozon_product_goods/ozon_product_goods_{self.date_extract}.csv"
    self.BASE_URL__PRODUCT_PRICE = f"https://storage.yandexcloud.net/traidingbt/ozon_product_price/ozon_product_price_{self.date_extract}.csv"
    self.BASE_URL__PAID_STORAGE = f"https://storage.yandexcloud.net/traidingbt/ozon_paid_storage/ozon_paid_storage_{self.date_extract}.csv"
    self.BASE_URL__PRODUCT_COST = f"https://storage.yandexcloud.net/traidingbt/ozon_additional_comparisons/ozon_all_goods_info.csv"

    self.order_fbo = pd.read_csv(self.BASE_URL__ORDER_FBO, sep=";") # заказы по модели fbo
    self.order_fbs = pd.read_csv(self.BASE_URL__ORDER_FBS, sep=";") # заказы по модели fbs
    self.marketing = pd.read_csv(self.BASE_URL__MARKETING, sep=";") # расход по маркетингу
    self.product = pd.read_csv(self.BASE_URL__PRODUCT, sep=";") # текущий ассортимент по всем магазинам
    self.product_price = pd.read_csv(self.BASE_URL__PRODUCT_PRICE, sep=";") # цены на товары
    self.paid_storage = pd.read_csv(self.BASE_URL__PAID_STORAGE, sep=";") # платное хранение
    self.product_cost = pd.read_csv(self.BASE_URL__PRODUCT_COST, sep=";") # себестоимость товаров

  def load_db(self):
      
      @dlt.resource(
          table_name="ozon_marketing", 
          write_disposition={"disposition": "append"},
          schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
      )
      def get_ozon_marketing():
          for item in self.marketing.itertuples(index=False):
              yield item._asdict()
      
      @dlt.resource(
          table_name="ozon_product_goods", 
          primary_key="sku",
          write_disposition={"disposition": "merge"},
          schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
      )
      def get_ozon_product_goods():
          self.product = self.product.drop('index', axis=1)
          for item in self.product.itertuples(index=False):
              yield item._asdict()
              
      @dlt.resource(
          table_name="ozon_product_price",
          write_disposition={"disposition": "append"},
          schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
      )
      def get_ozon_product_price():
          for item in self.product_price.itertuples(index=False):
              yield item._asdict()
              
      @dlt.resource(
          table_name="ozon_paid_storage", 
          write_disposition={"disposition": "append"},
          schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
      )
      def get_ozon_paid_storage():
          for item in self.paid_storage.itertuples(index=False):
              yield item._asdict()
              
      @dlt.resource(
          table_name="ozon_fbo_orders", 
          write_disposition={"disposition": "append"},
          schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
      )
      def get_ozon_fbo_orders():
          for item in self.order_fbo.itertuples(index=False):
              yield item._asdict()
              
      @dlt.resource(
          table_name="ozon_fbs_orders", 
          write_disposition={"disposition": "append"},
          schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
      )
      def get_ozon_fbs_orders():
          for item in self.order_fbs.itertuples(index=False):
              yield item._asdict()
              
      @dlt.resource(
          table_name="ozon_product_cost",
          write_disposition={"disposition": "append"},
          schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
      )
      def get_product_cost():
          for item in self.product_cost.itertuples(index=False):
              yield item._asdict()
              
      @dlt.source
      def ozon_source():
          return get_ozon_marketing, get_ozon_product_goods, get_ozon_product_price, get_ozon_paid_storage, get_ozon_fbo_orders, get_ozon_fbs_orders, get_product_cost
              
      pipeline = dlt.pipeline(
          pipeline_name="ozon_info_from_s3",
          destination="postgres", 
           dataset_name="ozon", 
           dev_mode=False
      )
      
      load_info = pipeline.run(
          ozon_source(),
          credentials="postgresql://loader:dlt@localhost:5432/dlt_data"
      )

              
ozon_orders_struct = OzonOrdersStruct(date_extract=datetime.now()-timedelta(days=1))
ozon_orders_struct.load_db()