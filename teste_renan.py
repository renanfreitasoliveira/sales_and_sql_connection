import pandas as pd
from sqlalchemy import create_engine

db_user = 'postgres'
db_password = '-----'
db_host = 'localhost'
db_port = '5432'
db_name = 'postgres'

connection_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

#___________________________________________________________________________________________________________________________________________

try:
   
    engine = create_engine(connection_str)

    query_categories = """
        SELECT  category_id,
                category_name,
                description
        FROM    public.categories;"""

    query_customers = """
        SELECT  customer_id,
                company_name,
                contact_name,
                contact_title,
                city,
                country,
                phone,
                address,
                fax
        FROM    public.customers;"""

    query_employee_territories = """
        SELECT  employee_id,
                territory_id 
        FROM    public.employee_territories;"""

    query_employees = """
        SELECT  employee_id,
                last_name,
                first_name,
                title,
                title_of_courtesy,
                address,
                city,
                region,
                country,
                home_phone
        FROM    public.employees;"""

    query_orders = """
        SELECT  order_id,
                customer_id,
                employee_id,
                order_date,
                required_date,
                shipped_date,
                ship_via,
                ship_name,
                ship_address,
                ship_city,
                ship_region,
                ship_postal_code,
                ship_country
        FROM    public.orders;"""

    query_products = """
        SELECT  product_id,
                product_name,
                supplier_id,
                category_id,
                quantity_per_unit,
                unit_price, 
                units_in_stock,
                units_on_order,
                reorder_level,
                discontinued
        FROM    public.products;"""

    query_region = """
        SELECT  region_id,
                region_description 
        FROM    public.region;"""

    query_shippers = """
        SELECT  shipper_id,
                company_name,
                phone AS shipper_phone
        FROM    public.shippers;"""

    query_suppliers = """
        SELECT  supplier_id,
                company_name,
                contact_name,
                contact_title,
                address as supplier_address,
                city as supplier_city,
                region as supplier_region,
                country as supplier_country,
                phone as supplier_phone,
                fax as supplier_fax 
        FROM    public.suppliers;"""

    query_territories = """
        SELECT  territory_id,
                territory_description,
                region_id
        FROM    public.territories;"""
    
    #___________________________________________________________________________________________________________________________________________

    def convert_columns_to_string(df):
        return df.astype(str)

    customers = convert_columns_to_string(pd.read_sql_query(query_customers, engine))
    categories = convert_columns_to_string(pd.read_sql_query(query_categories, engine))
    employee_territories = convert_columns_to_string(pd.read_sql_query(query_employee_territories, engine))
    employees = convert_columns_to_string(pd.read_sql_query(query_employees, engine))
    orders = convert_columns_to_string(pd.read_sql_query(query_orders, engine))
    products = convert_columns_to_string(pd.read_sql_query(query_products, engine))
    region = convert_columns_to_string(pd.read_sql_query(query_region, engine))
    shippers = convert_columns_to_string(pd.read_sql_query(query_shippers, engine))
    suppliers = convert_columns_to_string(pd.read_sql_query(query_suppliers, engine))
    territories = convert_columns_to_string(pd.read_sql_query(query_territories, engine))

    pedido = convert_columns_to_string(pd.read_csv("---.csv", sep=','))
 
    #___________________________________________________________________________________________________________________________________________

    info_pedido = pd.merge(pedido, orders, on='order_id', how='left')\
    .merge(customers, on='customer_id', how='left') \
    .merge(employees, on='employee_id', how='left') \
    .merge(employee_territories, on='employee_id', how='left') \
    .merge(products, on='product_id', how='left') \
    .merge(categories, on='category_id', how='left') \
    .merge(territories, on='territory_id', how='left') \
    .merge(region, on='region_id', how='left') \
    .merge(shippers, left_on='ship_via', right_on='shipper_id', how='left') \
    .merge(suppliers, on='supplier_id', how='left')

    #___________________________________________________________________________________________________________________________________________

    info_pedido['unit_price_x'] = pd.to_numeric(info_pedido['unit_price_x'], errors='coerce')
    info_pedido['quantity'] = pd.to_numeric(info_pedido['quantity'], errors='coerce')
    info_pedido['discount'] = pd.to_numeric(info_pedido['discount'], errors='coerce')
    info_pedido['order_date'] = pd.to_datetime(info_pedido['order_date'], errors='coerce')
    info_pedido['shipped_date'] = pd.to_datetime(info_pedido['shipped_date'], errors='coerce')
    info_pedido['required_date'] = pd.to_datetime(info_pedido['required_date'], errors='coerce')
    #___________________________________________________________________________________________________________________________________________

    info_pedido['total_revenue'] = info_pedido['unit_price_x'] * info_pedido['quantity'] * (1 - info_pedido['discount'])
    info_pedido['days_to_ship'] = (info_pedido['shipped_date'] - info_pedido['order_date']).dt.days
    info_pedido['days_to_deliver'] = (info_pedido['required_date'] - info_pedido['order_date']).dt.days
    info_pedido['order_month'] = info_pedido['order_date'].dt.month
    info_pedido['order_year'] = info_pedido['order_date'].dt.year
    info_pedido['employee_full_name'] = info_pedido['first_name'] + ' ' + info_pedido['last_name']
    info_pedido['average_order_value'] = info_pedido.groupby('order_id')['total_revenue'].transform('sum') / info_pedido.groupby('order_id')['quantity'].transform('sum')
    info_pedido['customer_order_count'] = info_pedido.groupby('customer_id')['order_id'].transform('nunique')
    info_pedido['employee_order_count'] = info_pedido.groupby('employee_id')['order_id'].transform('nunique')
    info_pedido['product_order_count'] = info_pedido.groupby('product_id')['order_id'].transform('nunique')
    info_pedido['revenue_per_product'] = info_pedido.groupby('product_id')['total_revenue'].transform('sum')
    info_pedido['revenue_per_customer'] = info_pedido.groupby('customer_id')['total_revenue'].transform('sum')
    info_pedido['revenue_per_employee'] = info_pedido.groupby('employee_id')['total_revenue'].transform('sum')
    info_pedido['monthly_revenue'] = info_pedido.groupby(['order_year', 'order_month'])['total_revenue'].transform('sum')
    info_pedido['order_year_month'] = info_pedido['order_year'].astype(str) + info_pedido['order_month'].astype(str).str.zfill(2)

    info_pedido.to_sql('pedidos', engine, schema='public', if_exists='replace', index=False)

    #___________________________________________________________________________________________________________________________________________


except Exception as e:
    print(f"Ocorreu um erro: {e}")
