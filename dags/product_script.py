import requests
import pandas as pd

url = "https://dummyjson.com/products"
response = requests.get(url)
data = response.json()["products"]
products_list = [] 
reviews_list = []

for product in data:
    products_info = {
        "id": product["id"],
        "title": product["title"],
        "description": product["description"],
        "category": product["category"],
        "price": product["price"],
        "discountPercentage": product["discountPercentage"],
        "rating": product.get("rating"),
        "stock": product.get("stock"),
        "brand": product.get("brand"),
        "sku": product.get("sku")
    }
    
    products_list.append(products_info)


    if "reviews" in product and product["reviews"]:
        for review in product["reviews"]:
            review_info = {
                "product_id": product["id"],
                "rating": review["rating"],
                "comment": review["comment"],
                "date": review["date"],
                "reviewer_name": review["reviewerName"],
                "reviewer_email": review["reviewerEmail"]
            }
            reviews_list.append(review_info)


products_df = pd.DataFrame(products_list)
reviews_df = pd.DataFrame(reviews_list)
print(products_df)
print(reviews_df)