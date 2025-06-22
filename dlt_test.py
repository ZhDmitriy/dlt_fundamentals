import dlt 
from datetime import datetime

# Source -> Resource1 ... -> Resource2 и так далее

# создаем ресурс, который привязан к определенному источнику (откуда данные выгружаются)
@dlt.resource(
    table_name="users",
    primary_key="id", 
    write_disposition={"disposition": "merge", "strategy": "scd2"}, # реализуем стратегию медленно меняющихся измерений
    schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    )
def dlt_resource(): 
    # можем предобработать данные перед загрузкой
    data = [
        {"id": 1, 
         "name": "Alice", 
         "age": 17}, 
        {"id": 2, # когда добавляется новое поле, он просто прогружает его в базу данных 
         "name": "Bob"
         }, 
        {"id": 1, 
         "name": "Jane"
         },
        {"id": 4, 
         "name": "Kirill"
         }] 

    yield data # сохраняем в память по одному значению, в нашем случае целый список
    
@dlt.transformer(
    data_from=dlt_resource,
    primary_key="user_id",
    schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}, table_name="users_dop_info")
def dlt_transformer(items):
    for item in items: # строим связь, то есть получам для каждой строки доплнительную информацию, которую записываем в другую таблицу
        yield {
            "user_id": item["id"],  # Внешний ключ, ссылающийся на users.id (после загрузки всех таблиц, можем указать явно в базе данных)
            "info": f"This is {item['name']}",
            "name": item["name"]
        }
        
# Инкрементальная загрузка
# @dlt.resource(table_name="users_incremental", write_disposition={"disposition": "replace", "strategy": "upsert"}) # "strategy": "upsert" - вставляет запись, если ключ отсутствует в таблице
# def get_users(
#     user_id=dlt.sources.incremental("id", initial_value=0) # указываем поле, которое отслеживается, если появилось новое поле, то обязательно добавляем только его (обычно это время)
# ):
#     # если указывать время, то можем взять последнее время загрузки updated_at.last_value
#     data = [{"id": 1, "name": "Alice", "age": 17}, {"id": 2, "name": "Bob"}]
#     yield data
        
@dlt.source
def users_source(): # создаем источник для двух ресурсов
    return dlt_resource, dlt_transformer

pipeline = dlt.pipeline(
    pipeline_name="dlt_source", # наименование pipeline 
    destination="postgres", # пункт назначения
    dataset_name="dlt_test", # схема данных, которая создается с определенным номером
    dev_mode=False, # при запуске будет создана новая схема (если перезапускать pipeline, то будет добавляться в конец)
)

load_info = pipeline.run(
    users_source(), 
    #write_disposition="append", # если данные повторяются, то идет перезапись строк (по умолчанию append)
    credentials="postgresql://loader:dlt@localhost:5432/dlt_data")
 
# write_disposition
# replace - замена повторяющихся строк 
# merge - объединение повторяющихся строк
# append - инкрементальная загрузка
# skip - пропуск

# tables: 
# 	- evolve: разрешает создание новых таблиц, но не позволяет удалять существующие 
# 	- freeze: запрещает любые изменения в наборе таблиц 
# 	- discard_values: специальные случае
	
#  columns: 
#  	- evolve: разрешает создание новых столбцов
#  	- freeze: запрещает любые изменения в структуре столбцов 
#  	- discard_values: новые столбцы игнорируются, данные не загружаются
 	
#  data_type: 
#  	- freeze: строго запрещает изменения типов данных (вызовет ошибку при попытке)
#  	- evolve: разрешает некоторые безопасные преобразования типов
#  	- discard_values: данные с несоответствующими типами будут отброшены 
 	