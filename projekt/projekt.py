import csv
import os
from datetime import datetime
from time import sleep

import oracledb


DB_HOST = "213.184.8.44"
DB_PORT = 1521
DB_SERVICE = "orcl"

DB_USER = "powirskim"
DB_PASSWORD = "Powir1234"


def connect_to_db():
    dsn = oracledb.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)


def log_audit(cursor, module, action, details):
    cursor.execute(
        """
        INSERT INTO audit_log(id, log_time, usr, module, action, details)
        VALUES (seq_audit_log.NEXTVAL, SYSTIMESTAMP, USER, :m, :a, :d)
        """,
        {"m": module, "a": action, "d": str(details)[:4000]},
    )


def parse_date(s):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


def exists(cursor, table_name, id_col, id_val):
    cursor.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE {id_col} = :v",
        {"v": id_val},
    )
    return cursor.fetchone()[0] > 0


def validate_data(row, table_name, cursor):
    errors = []

    def req(col):
        if str(row.get(col, "")).strip() == "":
            errors.append(f"{col} is required")

    if table_name == "P_CATEGORIES":
        req("category_id")
        req("name")

    elif table_name == "P_PRODUCTS":
        req("product_id")
        req("name")
        req("category_id")
        req("unit")

        try:
            cid = int(row["category_id"])
            if not exists(cursor, "p_categories", "category_id", cid):
                errors.append("category_id does not exist in p_categories")
        except:
            errors.append("category_id must be integer")

        a = str(row.get("active", "Y")).strip().upper()
        if a not in ("Y", "N"):
            errors.append("active must be Y or N")

    elif table_name == "P_SUPPLIERS":
        req("supplier_id")
        req("name")

    elif table_name == "P_CUSTOMERS":
        req("customer_id")
        req("name")

    elif table_name == "P_WAREHOUSES":
        req("warehouse_id")
        req("name")

    elif table_name == "P_PRODUCT_BATCHES":
        for c in ["batch_id", "product_id", "supplier_id", "warehouse_id",
                  "received_date", "buy_price", "qty_received"]:
            req(c)

        try:
            pid = int(row["product_id"])
            if not exists(cursor, "p_products", "product_id", pid):
                errors.append("product_id does not exist in p_products")
        except:
            errors.append("product_id must be integer")

        try:
            sid = int(row["supplier_id"])
            if not exists(cursor, "p_suppliers", "supplier_id", sid):
                errors.append("supplier_id does not exist in p_suppliers")
        except:
            errors.append("supplier_id must be integer")

        try:
            wid = int(row["warehouse_id"])
            if not exists(cursor, "p_warehouses", "warehouse_id", wid):
                errors.append("warehouse_id does not exist in p_warehouses")
        except:
            errors.append("warehouse_id must be integer")

        try:
            parse_date(row.get("received_date"))
        except:
            errors.append("received_date must be YYYY-MM-DD")

        try:
            parse_date(row.get("expire_date"))
        except:
            errors.append("expire_date must be YYYY-MM-DD or empty")

        try:
            if float(row.get("buy_price")) < 0:
                errors.append("buy_price must be >= 0")
        except:
            errors.append("buy_price must be number")

        try:
            if float(row.get("qty_received")) < 0:
                errors.append("qty_received must be >= 0")
        except:
            errors.append("qty_received must be number")

    return errors


def load_data_from_csv(file_path, table_name, columns):
    conn = connect_to_db()
    cursor = conn.cursor()

    good = 0
    bad = 0

    print(f"\n--- START {table_name} | plik: {file_path} ---")

    try:
        try:
            log_audit(cursor, "PY_LOADER", "START_FILE", f"{table_name} file={file_path}")
        except Exception as e:
            print(f"[WARN] Nie mogę logować do audit_log (start): {e}")

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader, start=1):
                data = {col: row.get(col, None) for col in columns}

                errors = validate_data(data, table_name, cursor)
                if errors:
                    bad += 1
                    print(f"[INVALID] {table_name} wiersz={i} errors={errors} data={data}")
                    try:
                        log_audit(cursor, "PY_LOADER", "ROW_INVALID",
                                  f"{table_name} row={i} errors={errors}")
                    except Exception:
                        pass
                    continue

                bind = dict(data)

                if table_name == "P_PRODUCT_BATCHES":
                    bind["received_date"] = parse_date(bind["received_date"])
                    bind["expire_date"] = parse_date(bind.get("expire_date"))
                    bind["buy_price"] = float(bind["buy_price"])
                    bind["qty_received"] = float(bind["qty_received"])

                for c in columns:
                    if c.endswith("_id") and bind.get(c) not in (None, ""):
                        bind[c] = int(bind[c])

                if "vat_rate" in columns and bind.get("vat_rate") not in (None, ""):
                    bind["vat_rate"] = float(bind["vat_rate"])

                if "active" in columns:
                    bind["active"] = str(bind.get("active", "Y")).strip().upper() or "Y"

                if i == 1 or i % 20 == 0:
                    print(f"[DEBUG] {table_name} row={i} bind={bind}")

                try:
                    cursor.execute(
                        f"""
                        INSERT INTO {table_name} ({', '.join(columns)})
                        VALUES ({', '.join([f':{c}' for c in columns])})
                        """,
                        bind
                    )
                    good += 1

                except Exception as e:
                    print("\n[DB_ERROR] INSERT nie wyszedł")
                    print("TABLE:", table_name)
                    print("ROW_NO:", i)
                    print("BIND:", bind)
                    print("ERROR:", e)
                    try:
                        log_audit(cursor, "PY_LOADER", "DB_ERROR",
                                  f"{table_name} row={i} bind={bind} err={e}")
                        conn.commit()
                    except Exception:
                        pass
                    raise

        conn.commit()

        try:
            log_audit(cursor, "PY_LOADER", "END_FILE", f"{table_name} good={good} bad={bad}")
            conn.commit()
        except Exception as e:
            print(f"[WARN] Nie mogę logować do audit_log (end): {e}")

    finally:
        cursor.close()
        conn.close()

    print(f"--- END {table_name} | good={good} bad={bad} ---")
    return good, bad


def load():
    jobs = [
        ("p_categories.csv", "P_CATEGORIES", ["category_id", "name"]),
        ("p_products.csv", "P_PRODUCTS", ["product_id", "name", "category_id", "unit", "vat_rate", "active"]),
        ("p_suppliers.csv", "P_SUPPLIERS", ["supplier_id", "name", "nip", "phone", "email", "active"]),
        ("p_customers.csv", "P_CUSTOMERS", ["customer_id", "name", "nip", "phone", "email", "active"]),
        ("p_warehouses.csv", "P_WAREHOUSES", ["warehouse_id", "name", "city"]),
        ("p_product_batches.csv", "P_PRODUCT_BATCHES",
         ["batch_id", "product_id", "supplier_id", "warehouse_id", "batch_code",
          "received_date", "expire_date", "buy_price", "qty_received"]),
    ]

    print("\n========== START LOAD ==========")
    for filename, table, cols in jobs:
        if not os.path.exists(filename):
            print(f"[SKIP] Brak pliku: {filename}")
            continue
        print(f"[RUN] {filename} -> {table}")
        load_data_from_csv(filename, table, cols)
    print("========== END LOAD ==========\n")


if __name__ == "__main__":
    while True:
        load()
        print("Zakończono operację")
        sleep(86400)