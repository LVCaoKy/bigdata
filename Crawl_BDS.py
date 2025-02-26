import pandas as pd
from sqlalchemy import create_engine

# Cấu hình kết nối PostgreSQL
DB_USERNAME = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "127.0.0.1"  # Hoặc địa chỉ server PostgreSQL
DB_PORT = "5432"  # Cổng mặc định của PostgreSQL
DB_NAME = "my_db"
TABLE_NAME = "HOUSE_HCM"

# Đường dẫn file CSV
CSV_FILE = "batdongsan_merged.csv"

# Kết nối PostgreSQL
engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Đọc dữ liệu từ CSV
print("Đang đọc dữ liệu từ CSV...")
df = pd.read_csv(CSV_FILE, encoding="utf-8")

# Đưa dữ liệu vào PostgreSQL
df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)

print("Nhập dữ liệu thành công vào bảng HOUSE!")
