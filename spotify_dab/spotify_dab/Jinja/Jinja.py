# Databricks notebook source
parameters = [
    {
        "table":"spotify_catalog.silver.factstream",
        "alias":"factstream",
        "cols":"factstream.stream_id,factstream.listen_duration"
    },
    {
        "table":"spotify_catalog.silver.dimuser",
        "alias":"dimuser",
        "cols":"dimuser.user_id,dimuser.subscription_type,dimuser.user_name",
        "condition":"dimuser.user_id = factstream.user_id"
    },
    {
        "table":"spotify_catalog.silver.dimtrack",
        "alias":"dimtrack",
        "cols":"dimtrack.track_id,dimtrack.track_name",
        "condition":"dimtrack.track_id = factstream.track_id"
    }

]

# COMMAND ----------

pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

query_text = """
SELECT
    {% for param in parameters %}
        {{param.cols}}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
FROM 
   {% for param in parameters %}
      {% if loop.first %}
          {{param.table}} AS {{param.alias}}
      {% endif %}
    {% endfor %}
    {% for param in parameters %}
      {% if not loop.first %}
      LEFT JOIN 
           {{param.table}} AS {{param.alias}}
      ON 
           {{param.condition}}
        {% endif %}
    {% endfor %}       
     """

# COMMAND ----------

jinja_sql_str = Template(query_text)  # Ensure 'query_text' contains properly closed Jinja blocks, check for missing 'endif' or 'endfor'.
query = jinja_sql_str.render(parameters=parameters)
print(query)
 

# COMMAND ----------

spark.sql(query).createOrReplaceTempView("spotify_catalog.silver.business")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from business