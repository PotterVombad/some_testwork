from pyspark.sql.functions import col


# метод принимающий три датафрейма соответственно

def getpyspark(product, category, connection):
    category = category.selectExpr("id", "Name as cat")
    product.join(connection, product.id == connection.product_id, "left") \
        .join(category, category.id == connection.category_id, "left")
    df_rez = product.select(col("Name"), col("cat").alias("Category"))
    return df_rez
