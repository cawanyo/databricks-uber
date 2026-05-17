# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %sql
# MAGIC SELECT * FROM dev.bronze.stg_rides;

# COMMAND ----------

# DBTITLE 1,Cell 2
jinja_config = [
    {
        "table": "dev.bronze.stg_rides",
        "select": "dev.bronze.stg_rides.*",
        "where": "",
    },
    {
        "table": "dev.bronze.map_vehicle_types",
        "select": "dev.bronze.map_vehicle_types.*",
        "where": "",
        "on": "dev.bronze.stg_rides.vehicle_type_id = dev.bronze.map_vehicle_types.vehicle_type_id"
    },
    {
        "table": "dev.bronze.map_vehicle_makes",
        "select": "dev.bronze.map_vehicle_makes.*",
        "where": "",
        "on": "dev.bronze.stg_rides.vehicle_make_id = dev.bronze.map_vehicle_makes.vehicle_make_id"
    },
    {
        "table": "dev.bronze.map_cities",
        "select": "dev.bronze.map_cities.*",
        "where": "",
        "on": "dev.bronze.stg_rides.pickup_city_id = dev.bronze.map_cities.city_id"
    },
    {
        "table": "dev.bronze.map_cancellation_reasons",
        "select": "dev.bronze.map_cancellation_reasons.*",
        "where": "",
        "on": "dev.bronze.stg_rides.cancellation_reason_id = dev.bronze.map_cancellation_reasons.cancellation_reason_id"
    }
]


# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

# DBTITLE 1,Cell 4
jinja_str = """
SELECT 
    {% for config in jinja_config %}
        {{ config.select }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM 
    {% for config in jinja_config %}
        {% if loop.first %}
            {{ config.table }}
        {% else %}
            LEFT JOIN {{ config.table }} ON {{ config.on }}
        {% endif %}
    {% endfor %}
{% if where_clauses %}
WHERE 
    {% for clause in where_clauses %}
        {{ clause }}{% if not loop.last %} AND {% endif %}
    {% endfor %}
{% endif %}
"""

where_clauses = [config["where"] for config in jinja_config if config["where"]]

template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config, where_clauses=where_clauses)
print(rendered_template)


# COMMAND ----------

# DBTITLE 1,Cell 5
display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.bronze.dim_location

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.bronze.fact

# COMMAND ----------


