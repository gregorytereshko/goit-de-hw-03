from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

# Створюємо сесію Spark
spark = SparkSession.builder.appName("ProductPurchaseAnalysis").getOrCreate()

# 1. Завантажуємо датасети
products_df = spark.read.csv('./products.csv', header=True, inferSchema=True)  # Дані про продукти
purchases_df = spark.read.csv('./purchases.csv', header=True, inferSchema=True)  # Дані про покупки
users_df = spark.read.csv('./users.csv', header=True, inferSchema=True)  # Дані про користувачів

# 2. Видаляємо рядки з пропущеними значеннями
products_df_clean = products_df.dropna()  # Очищення даних про продукти
purchases_df_clean = purchases_df.dropna()  # Очищення даних про покупки
users_df_clean = users_df.dropna()  # Очищення даних про користувачів

# Додаємо обчислювану колонку "amount" як результат price * quantity
product_purchases = purchases_df_clean.join(products_df_clean, "product_id") \
    .withColumn("amount", col("price") * col("quantity"))

# 3. Загальна сума покупок за категоріями продуктів (з округленням)
total_purchases_by_category = product_purchases.groupBy("category") \
    .agg(round(sum("amount"), 2).alias("total_purchase"))  # Підрахунок і округлення

# 4. Сума покупок за категоріями продуктів для вікової групи 18-25 років (з округленням)
user_purchases = product_purchases.join(users_df_clean, "user_id")  # Об'єднання покупок з даними користувачів
age_filtered_purchases = user_purchases.filter((col("age") >= 18) & (col("age") <= 25))  # Фільтр за віком
total_purchases_by_category_age = age_filtered_purchases.groupBy("category") \
    .agg(round(sum("amount"), 2).alias("total_purchase_18_25"))  # Підрахунок і округлення

# 5. Частка покупок за категоріями товарів від загальної суми витрат для вікової групи 18-25 років
total_spent_18_25 = age_filtered_purchases.agg(sum("amount").alias("total_spent_18_25")).collect()[0]["total_spent_18_25"]
category_share = total_purchases_by_category_age.withColumn(
    "percentage_share", round((col("total_purchase_18_25") / total_spent_18_25) * 100, 2)  # Округлення до 2 знаків
)

# 6. Топ-3 категорії товарів з найбільшим відсотком витрат для вікової групи 18-25 років
top_3_categories = category_share.orderBy(col("percentage_share").desc()).limit(3)  # Вибір топ-3 категорій

# Вивід результатів
print("Загальна сума покупок за категоріями продуктів:")
total_purchases_by_category.show()

print("Сума покупок за категоріями продуктів для вікової групи 18-25 років:")
total_purchases_by_category_age.show()

print("Частка покупок за категоріями товарів від загальної суми витрат для вікової групи 18-25 років:")
category_share.show()

print("Топ-3 категорії товарів з найбільшим відсотком витрат для вікової групи 18-25 років:")
top_3_categories.show()

# Закриваємо сесію Spark
spark.stop()