from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def get_product_category_pairs(df_products, df_categoriesProducts, df_categories):
    joined_df = df_products.join(df_categoriesProducts, df_products["id"] == df_categoriesProducts["id продукта"], "left") \
                           .join(df_categories, df_categories["id"] == df_categoriesProducts["id категории"], "left")

    result_df = joined_df.select(col("Название продукта").alias("Имя продукта"),
                                 col("Название категории").alias("Имя категории"))

    products_without_category_df = df_products.join(df_categoriesProducts,
                                                    df_products["id"] == df_categoriesProducts["id продукта"],
                                                    "left") \
                                               .filter(df_categoriesProducts["id категории"].isNull()) \
                                               .select("Название продукта")

    final_result_df = result_df.union(products_without_category_df.withColumn("Имя категории", lit(None)))

    return final_result_df

spark = SparkSession.builder.getOrCreate()

df_products = spark.createDataFrame([
    (1,"Продукт 1",),
    (2,"Продукт 2",),
    (3,"Продукт 3",),
    (4,"Продукт 4",),
    (5,"Продукт 5",),
    (6,"Продукт 6",),
    (7,"Продукт 7",),
    (8,"Продукт 8",),
    (9,"Продукт 9",),
    (10, "Продукт 10",),
    (11, "Продукт 11",),
    (12, "Продукт 12",),
    (13, "Продукт 13",),
    (14, "Продукт 14",),
    (15, "Продукт 15",),
    (16, "Продукт 16",),
    (17, "Продукт 17",),
    (18, "Продукт 18",),
    (19, "Продукт 19",),
    (20, "Продукт 20",),
    (21, "Продукт 21",),
    (22, "Продукт 22",),
    (23, "Продукт 23",),
    (24, "Продукт 24",),
    (25, "Продукт 25",),
    (26, "Продукт 26",),
    (27, "Продукт 27",),
    (28, "Продукт 28",),
    (29, "Продукт 29",),
    (30, "Продукт 30",),
], ["id", "Название продукта"])

df_categoriesProducts = spark.createDataFrame([
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 1),
    (5, 2),
    (6, 3),
    (7, 1),
    (8, 2),
    (9, 3),
    (10, 1),
    (11, 2),
    (12, 3),
    (13, 1),
    (14, 2),
    (15, 3),
    (16, 1),
    (17, 2),
    (18, 3),
    (19, 1),
    (20, 2),
    (21, 3),
    (22, ),
    (23, ),
    (24, ),
    (25, ),
    (26, ),
    (27, ),
    (28, ),
    (29, ),
    (30, ),
], ["id продукта", "id категории"])

df_categories = spark.createDataFrame([
    (1, "Категория 2",),
    (2, "Категория 1",),
    (3, "Категория 3",),
], ["id", "Название категории"])

result = get_product_category_pairs(df_products, df_categoriesProducts, df_categories)
result.show(truncate=False)
