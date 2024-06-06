from nbdev_squ.api import query_all

def model(dbt, session):
    return query_all(dbt.config.get('query'))
