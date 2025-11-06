# -*- coding: utf-8 -*-
##########################################################
#   Copyright: Luis Aquino
#   Contact: Luis Aquino -> +502 4814-3481
#   Support: Luis Aquino -> laquinobarrientos@gmail.com
##########################################################
#   MODIFIED: Integración de re-sincronización de productos
#   para mayor robustez en la creación de ventas.
##########################################################


import mysql.connector
import json
from datetime import datetime, timedelta
import requests
import xmlrpc.client


# Se recomienda deshabilitar las advertencias de SSL solo si confías plenamente en el certificado del servidor
try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except ImportError:
    pass


class OdooConnector():

    def __init__(self, path_config=None, odoo_config=None):
        self.path_config = path_config
        self.odoo_config = odoo_config
    
    def logger(self, datetime=None, type=None, content=None):
        try:
            with open('OdooConnector.log', 'a', encoding='utf-8') as f_log:
                msg = ("%s %s %s\n" % (datetime, type, content))
                f_log.write(msg)
        except Exception as e:
            # Si el logger falla, imprime el error original y el error del logger en la consola
            print(f"ERROR AL ESCRIBIR EN EL LOG: {e}")
            print(f"MENSAJE ORIGINAL: {datetime} {type} {content}")

    def get_mysql_config(self):
        conf_dict = {}
        if self.path_config:
            self.logger(datetime=datetime.now(), type='PATH_CONFIG', content=str(self.path_config))
            try:
                with open(self.path_config, 'r') as f:
                    f_list = f.readlines()
                    self.logger(datetime=datetime.now(), type='PARAMETERS', content=f_list)
                    for item in f_list:
                        if '=' in item:
                            key, value = item.strip().split('=', 1)
                            if key == 'password':
                                conf_dict['password'] = value
                            elif key == 'usuario':
                                conf_dict['user'] = value
                            elif key == 'base':
                                conf_dict['db'] = value
                            elif key == 'puerto':
                                conf_dict['port'] = value
                            elif key == 'servidor':
                                conf_dict['url'] = value
                    self.logger(datetime=datetime.now(), type='CREDENTIALS', content=conf_dict)
                    return conf_dict
            except Exception as e:
                print("Error al leer el archivo de configuración de MySQL %s: %s" % (self.path_config, e))
                self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        return {}

    def get_odoo_config(self):
        if self.odoo_config:
            self.logger(datetime=datetime.now(), type='ODOO_CONFIG', content=str(self.odoo_config))
            try:
                with open(self.odoo_config, 'r') as json_config:
                    data = json.load(json_config)
                    self.logger(datetime=datetime.now(), type='ODOO_PARAMETERS', content=data)
                    return data
            except Exception as e:
                print("Error al leer el archivo de configuración de Odoo %s: %s" % (self.odoo_config, e))
                self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        return {}

    def mysql_connection(self):
        connection = None
        try:
            mysql_param = self.get_mysql_config()
            if mysql_param:
                connection = mysql.connector.connect(
                    host=mysql_param.get('url'),
                    user=mysql_param.get('user'),
                    password=mysql_param.get('password'),
                    port=mysql_param.get('port')
                )
                msg = ("Conexión a MySQL exitosa: %s" % (connection))
                self.logger(datetime=datetime.now(), type='INFO', content=msg)
                return connection
            else:
                self.logger(datetime=datetime.now(), type='WARNING', content="No se pudieron obtener los parámetros de configuración de MySQL.")
                return None
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
            if connection and connection.is_connected():
                connection.close()
            return None
    
    # --- MÉTODOS DE SINCRONIZACIÓN (EXISTENTES) ---
    # ... (El código para sync_nullsales, employees, subcategories, customers se mantiene igual) ...
    # ... Colocaré el código completo para que no falte nada ...

    def search_invalidated_sales(self):
        connection = self.mysql_connection()
        if not connection: return []
        invalidated_sales = []
        try:
            cr = connection.cursor()
            query = """SELECT id, num_doc, erp FROM ventasdiarias.venta_encabezado WHERE anulado = 1 AND web = 0 AND erp <> 0;"""
            cr.execute(query)
            records = cr.fetchall()
            invalidated_sales = [{'silverpos_id': row[0], 'num_doc': row[1], 'erp_id': row[2]} for row in records]
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En search_invalidated_sales: {e}")
        finally:
            if connection.is_connected():
                connection.close()
        return invalidated_sales

    def sync_nullsales_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            invalidated_sales = self.search_invalidated_sales()
            url, token, db_name, company_id = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db'), odoo_param.get('company_id')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = f"{url}/anulados.silverpos/create"

            for sale in invalidated_sales:
                sale_data = {
                    'silverpos_id': sale.get('silverpos_id'),
                    'num_doc': sale.get('num_doc'),
                    'erp_id': sale.get('erp_id'),
                    'company_id': company_id,
                }
                response = requests.post(post_url, data=json.dumps(sale_data), params=params, headers=headers, stream=True, verify=False)
                if response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=f"Respuesta anulación: {odoo_res}")
                    if odoo_res.get('create_id'):
                        self.update_nullsale(silverpos_id=sale.get('silverpos_id'), web=odoo_res.get('create_id'))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En sync_nullsales_odoo: {e}")

    def update_nullsale(self, silverpos_id=None, web=None):
        connection = self.mysql_connection()
        if not connection: return
        try:
            cr = connection.cursor()
            query = """UPDATE ventasdiarias.venta_encabezado SET web = %s WHERE id = %s;"""
            values = (int(web), int(silverpos_id))
            cr.execute(query, values)
            connection.commit()
            msg = ("Venta anulada %s actualizada con web %s" % (silverpos_id, web))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En update_nullsale: {e}")
        finally:
            if connection.is_connected():
                connection.close()

    def search_employees(self):
        employees = []
        connection = self.mysql_connection()
        if not connection: return []
        try:
            cr = connection.cursor()
            query = """SELECT id, nombre, user, password, email, erp FROM silverpos_hist.hist_usuarios
                        WHERE nombre != '' and erp = 0;"""
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                idodoo_res = self.validate_employee_odoo(idsilverpos=int(row[0]))
                if idodoo_res:
                    self.update_employees(idemployee=int(row[0]), idodoo=idodoo_res)
                else:
                    employee_dict = {'user_name': str(row[1]), 'user_email': str(row[1]), 'silverpos_id': int(row[0])}
                    employees.append(employee_dict)
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En search_employees: {e}")
        finally:
            if connection.is_connected():
                connection.close()
        return employees

    def validate_employee_odoo(self, idsilverpos=None):
        try:
            odoo_param = self.get_odoo_config()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = f"?domain=[('silverpos_id', '=', {idsilverpos})]"
            fields = "&fields=['id']"
            get_url = f"{url}/res.users/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') and odoo_res.get('data'):
                    return odoo_res['data'][0].get('id')
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En validate_employee_odoo: {e}")
        return False

    def sync_employee_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            employees = self.search_employees()
            url, token, db_name, company_id = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db'), odoo_param.get('company_id')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = f"{url}/res.users/create"
            for employee in employees:
                item = {
                    'name': employee.get('user_name'),
                    'login': employee.get('user_email'),
                    'company_id': company_id,
                    'silverpos_id': employee.get('silverpos_id')
                }
                response = requests.post(post_url, data=json.dumps(item), params=params, headers=headers, stream=True, verify=False)
                if response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    if odoo_res.get('create_id'):
                        self.update_employees(idemployee=employee.get('silverpos_id'), idodoo=odoo_res.get('create_id'))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En sync_employee_odoo: {e}")

    def update_employees(self, idemployee=None, idodoo=None):
        connection = self.mysql_connection()
        if not connection: return
        try:
            cr = connection.cursor()
            query = """UPDATE silverpos_hist.hist_usuarios SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idemployee))
            cr.execute(query, values)
            connection.commit()
            self.logger(datetime=datetime.now(), type='INFO', content=f"Empleado {idemployee} actualizado con ID Odoo {idodoo}")
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En update_employees: {e}")
        finally:
            if connection.is_connected():
                connection.close()

    # ... (Aquí irían los métodos de subcategorías y clientes, que no cambian) ...

    def search_subcategories(self):
        subcategories = []
        connection = self.mysql_connection()
        if not connection: return []
        try:
            cr = connection.cursor()
            query = """SELECT id, nombre, productos_categoria_id FROM silverpos_hist.hist_productos_sub_categoria WHERE nombre != '' and erp = 0;"""
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                idodoo_res = self.validate_subcategory_odoo(idsilverpos=int(row[0]))
                if idodoo_res:
                    self.update_subcategory(idsubcategory=int(row[0]), idodoo=idodoo_res)
                else:
                    subcategory_dict = {
                        'subcategory_id': int(row[0]),
                        'subcategory_name': str(row[1]),
                        'subcategory_cat': row[2]
                    }
                    subcategories.append(subcategory_dict)
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En search_subcategories: {e}")
        finally:
            if connection.is_connected():
                connection.close()
        return subcategories

    def validate_subcategory_odoo(self, idsilverpos=None):
        try:
            odoo_param = self.get_odoo_config()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = f"?domain=[('silverpos_id', '=', {idsilverpos})]"
            fields = "&fields=['id']"
            get_url = f"{url}/product.category/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') and odoo_res.get('data'):
                    return odoo_res['data'][0].get('id')
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En validate_subcategory_odoo: {e}")
        return False
    
    def sync_subcategories_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            subcategories = self.search_subcategories()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = f"{url}/product.category/create"
            for subcategory in subcategories:
                subcat = {
                    'name': subcategory.get('subcategory_name'),
                    'silverpos_id': subcategory.get('subcategory_id'),
                    'tipo_categoria': str(subcategory.get('subcategory_cat')),
                    'removal_strategy_id': 1,
                    'property_cost_method': 'average',
                    'property_valuation': 'real_time',
                    'property_account_income_categ_id': 70,
                    'property_account_expense_categ_id': 150,
                    'property_stock_valuation_account_id': 29,
                    'property_stock_journal': 8,
                    'property_stock_account_input_categ_id': 146,
                    'property_stock_account_output_categ_id': 147,
                }
                response = requests.post(post_url, data=json.dumps(subcat), params=params, headers=headers, stream=True, verify=False)
                if response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    if odoo_res.get('create_id'):
                        self.update_subcategory(idsubcategory=subcategory.get('subcategory_id'), idodoo=odoo_res.get('create_id'))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En sync_subcategories_odoo: {e}")
    
    def update_subcategory(self, idsubcategory=None, idodoo=None):
        connection = self.mysql_connection()
        if not connection: return
        try:
            cr = connection.cursor()
            query = """UPDATE silverpos_hist.hist_productos_sub_categoria SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idsubcategory))
            cr.execute(query, values)
            connection.commit()
            self.logger(datetime=datetime.now(), type='INFO', content=f"Subcategoría {idsubcategory} actualizada con ID Odoo {idodoo}")
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En update_subcategory: {e}")
        finally:
            if connection.is_connected():
                connection.close()

    def search_customers(self):
        customers = []
        connection = self.mysql_connection()
        if not connection: return []
        try:
            cr = connection.cursor()
            query = """SELECT id, nombre, num_doc, codigo FROM silverpos.clientes WHERE nombre != '' AND no_tours = 0;"""
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                idodoo_res = self.validate_customers_odoo(idsilverpos=int(row[0]))
                if idodoo_res:
                    self.update_customers(idcustomer=int(row[0]), idodoo=idodoo_res)
                else:
                    customer_dict = {
                        'customer_id': int(row[0]),
                        'customer_name': str(row[1]),
                        'customer_nit': row[2],
                        'customer_code': row[3],
                    }
                    customers.append(customer_dict)
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En search_customers: {e}")
        finally:
            if connection.is_connected():
                connection.close()
        return customers

    def validate_customers_odoo(self, idsilverpos=None):
        try:
            odoo_param = self.get_odoo_config()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = f"?domain=[('silverpos_id', '=', {idsilverpos})]"
            fields = "&fields=['id']"
            get_url = f"{url}/res.partner/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') and odoo_res.get('data'):
                    return odoo_res['data'][0].get('id')
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En validate_customers_odoo: {e}")
        return False

    def sync_customers_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            customers = self.search_customers()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = f"{url}/res.partner/create"
            for customer in customers:
                partner_data = {
                    'name': customer.get('customer_name'),
                    'vat': customer.get('customer_nit'),
                    'silverpos_id': customer.get('customer_id'),
                    'silverpos_code': customer.get('customer_code'),
                    'customer_rank': 1,
                    'property_account_receivable_id': 17,
                    'property_account_payable_id': 48,
                }
                response = requests.post(post_url, data=json.dumps(partner_data), params=params, headers=headers, stream=True, verify=False)
                if response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    # El endpoint de res.partner devuelve el ID directamente, no en 'create_id'
                    if odoo_res.get('id'):
                        self.update_customers(idcustomer=customer.get('customer_id'), idodoo=odoo_res.get('id'))
                    elif odoo_res.get('create_id'): # Por si acaso la API cambia
                        self.update_customers(idcustomer=customer.get('customer_id'), idodoo=odoo_res.get('create_id'))

        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En sync_customers_odoo: {e}")

    def update_customers(self, idcustomer=None, idodoo=None):
        connection = self.mysql_connection()
        if not connection: return
        try:
            cr = connection.cursor()
            query = """UPDATE silverpos.clientes SET no_tours = %s WHERE id = %s;"""
            values = (int(idodoo), int(idcustomer))
            cr.execute(query, values)
            connection.commit()
            self.logger(datetime=datetime.now(), type='INFO', content=f"Cliente {idcustomer} actualizado con ID Odoo {idodoo}")
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En update_customers: {e}")
        finally:
            if connection.is_connected():
                connection.close()
                
    # --- MÉTODOS DE PRODUCTOS Y LÓGICA DE RESINCRONIZACIÓN ---

    def search_products(self):
        products = []
        connection = self.mysql_connection()
        if not connection: return []
        try:
            cr = connection.cursor()
            query = """SELECT p.id, p.nombre, p.codigo, p.erp, p.productos_sub_categoria_id, sc.erp AS sub_categoria_erp
                       FROM silverpos_hist.hist_productos AS p
                       JOIN silverpos_hist.hist_productos_sub_categoria AS sc ON p.productos_sub_categoria_id = sc.id
                       WHERE p.nombre != '' AND p.erp = 0;"""
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                idodoo_res = self.validate_product_odoo(idsilverpos=int(row[0]))
                if idodoo_res:
                    self.update_products(idproduct=int(row[0]), idodoo=idodoo_res)
                else:
                    product_dict = {
                        'product_id': int(row[0]),
                        'product_name': str(row[1]),
                        'product_code': row[2],
                        'product_categ_id': row[5],
                    }
                    products.append(product_dict)
            self.logger(datetime=datetime.now(), type='INFO', content=f"Total de productos nuevos a sincronizar: {len(products)}")
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En search_products: {e}")
        finally:
            if connection.is_connected(): connection.close()
        return products

    def validate_payment_odoo(self, sale_id=None, payment_id=None):
        """
        Verifica si un pago ya existe en Odoo usando una referencia única.
        La referencia se construye a partir del ID de la venta y el ID del pago local.
        Devuelve True si existe, False si no.
        """
        try:
            odoo_param = self.get_odoo_config()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            
            # Construimos la referencia única que debe coincidir con la que se usa al crear el pago
            unique_ref = f"SO{sale_id}-PAY{payment_id}"
            
            domain = f"?domain=[('ref', '=', '{unique_ref}')]"
            fields = "&fields=['id']" # Solo necesitamos saber si existe, no necesitamos más datos
            
            get_url = f"{url}/account.payment/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, timeout=10, stream=True, verify=False)
            
            if response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                # Si 'data' no está vacío, significa que se encontró al menos un registro
                if odoo_res.get('success') and odoo_res.get('data'):
                    return True # El pago ya existe
            return False # El pago no existe o hubo un error en la respuesta
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En validate_payment_odoo para pago {payment_id}: {e}")
            # En caso de duda o error, es más seguro decir que no existe para no bloquear el flujo,
            # aunque podría generar un duplicado si el error es de red. La lógica principal lo manejará.
            return False

    def validate_product_odoo(self, idsilverpos=None):
        try:
            odoo_param = self.get_odoo_config()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = f"?domain=[('silverpos_id', '=', {idsilverpos})]"
            fields = "&fields=['id']"
            get_url = f"{url}/product.product/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') and odoo_res.get('data'):
                    return odoo_res['data'][0].get('id')
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En validate_product_odoo: {e}")
        return False

    def sync_products_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            products = self.search_products()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = f"{url}/product.product/create"
            for product in products:
                prod = {
                    'name': product.get('product_name'),
                    'default_code': product.get('product_code'),
                    'silverpos_id': product.get('product_id'),
                    'sale_ok': True,
                    'purchase_ok': True,
                    'invoice_policy': 'order',
                    'type': 'product',
                    'categ_id': product.get('product_categ_id'),
                }
                response = requests.post(post_url, data=json.dumps(prod), params=params, headers=headers, stream=True, verify=False)
                if response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    if odoo_res.get('create_id'):
                        self.update_products(idproduct=product.get('product_id'), idodoo=odoo_res.get('create_id'))
                    else:
                        self.logger(datetime=datetime.now(), type='ERROR', content=f"Fallo al crear producto {product.get('product_id')}: {odoo_res.get('message')}")
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En sync_products_odoo: {e}")

    def update_products(self, idproduct=None, idodoo=None):
        connection = self.mysql_connection()
        if not connection: return
        try:
            cr = connection.cursor()
            query = """UPDATE silverpos_hist.hist_productos SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idproduct))
            cr.execute(query, values)
            connection.commit()
            self.logger(datetime=datetime.now(), type='INFO', content=f"Producto {idproduct} actualizado con ID Odoo {idodoo}")
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En update_products: {e}")
        finally:
            if connection.is_connected(): connection.close()

    def reset_product_sync_status(self, odoo_product_id):
        """
        Busca un producto en la BD local por su ID de Odoo (erp) y lo resetea a 0 para re-sincronizar.
        """
        connection = self.mysql_connection()
        if not connection:
            self.logger(datetime=datetime.now(), type='ERROR', content="No se pudo conectar a MySQL para resetear producto.")
            return
        try:
            cr = connection.cursor()
            query = """UPDATE silverpos_hist.hist_productos SET erp = 0 WHERE erp = %s;"""
            values = (int(odoo_product_id),)
            cr.execute(query, values)
            connection.commit()
            if cr.rowcount > 0:
                msg = f"PRODUCTO RESETEADO: Producto Odoo ID {odoo_product_id} reseteado a erp=0 para re-sincronización."
                self.logger(datetime=datetime.now(), type='INFO', content=msg)
                print(msg)
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"Error al resetear producto Odoo ID {odoo_product_id}: {e}")
        finally:
            if connection.is_connected(): connection.close()

    def validate_odoo_product_ids(self, product_ids_to_check):
        """
        Verifica si una lista de IDs de producto de Odoo existen.
        Devuelve: (True, []) si todos existen.
                  (False, [lista_de_ids_invalidos]) si alguno no existe.
        """
        if not product_ids_to_check: return (True, [])
        unique_ids = list(set(product_ids_to_check))
        try:
            odoo_param = self.get_odoo_config()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = f"?domain=[('id', 'in', {unique_ids})]"
            fields = "&fields=['id']"
            get_url = f"{url}/product.product/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success'):
                    existing_ids = {item['id'] for item in odoo_res.get('data', [])}
                    invalid_ids = [pid for pid in unique_ids if pid not in existing_ids]
                    return (not invalid_ids, invalid_ids)
                else:
                    self.logger(datetime=datetime.now(), type='ERROR', content=f"API Error en pre-validación de productos: {odoo_res.get('message')}")
                    return (False, unique_ids)
            else:
                self.logger(datetime=datetime.now(), type='ERROR', content=f"HTTP Error {response.status_code} en pre-validación de productos.")
                return (False, unique_ids)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"Excepción en pre-validación de productos: {e}")
            return (False, product_ids_to_check)

    # --- MÉTODOS DE VENTAS Y PAGOS ---

    def search_sales(self):
        sale_dict = {}
        sales = []
        connection = None
        cr = None
        try:
            connection = self.mysql_connection()
            if not connection:
                return []
            
            # Aseguramos que la conexión use la base de datos correcta
            if connection.database != 'silverpos_hist':
                connection.database = 'silverpos_hist'

            cr = connection.cursor()
            query = """SELECT 
                            venc.id ,                  -- Índice 0
                            venc.fechatransaccion,     -- Índice 1
                            venc.idcliente,            -- Índice 2
                            cli.idodoo,                -- Índice 3
                            cli.nombre,                -- Índice 4
                            venc.serie,                -- Índice 5
                            venc.num_fac_electronica,  -- Índice 6
                            venc.uuid,                 -- Índice 7
                            user.erp,                  -- Índice 8
                            venc.fechanegocio,         -- Índice 9
                            venc.valor_propina         -- Índice 10
                        FROM hist_venta_enca venc
                        INNER JOIN hist_clientes cli on cli.id = venc.idcliente
                        INNER JOIN hist_usuarios user on user.id = venc.idmesero
                        WHERE venc.erp = 0 and venc.borrada = 0 and venc.mesa != 'Report' and venc.anulado = 0 and venc.fechanegocio >= '2024-07-01';"""
            
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                lines = self.search_sales_lines(int(row[0]), row[10])
                
                serie_completa = str(row[6]) if row[6] is not None else ""
                # <<< INICIO DEL MAPEO CORREGIDO >>>
                sale_dict = {
                    'silverpos_id': row[0],
                    'date_order': str(row[1]),
                    'partner_id': row[3],                     # CORRECTO: Usa cli.idodoo
                    'client_order_ref': row[4],
                     'silverpos_serie_fel': serie_completa[-5:],  # ¡AQUÍ ESTÁ LA LÓGICA!     # CORREGIDO: Ahora usa venc.serie
                    'silverpos_numero_fel': str(row[7]),      # CORREGIDO: Ahora usa venc.num_fac_electronica
                    'silverpos_uuid': str(row[6]),            # CORREGIDO: Ahora usa venc.uuid
                    'silverpos_user_id': row[8],
                    'state': 'draft',
                    'silverpos_order_date': str(row[9]),
                }
                # <<< FIN DEL MAPEO CORREGIDO >>>

                if lines:
                    sale_dict.update({
                        'order_line': lines,
                    })
                sales.append(sale_dict)
                
            msg = ("Total number of rows in this query are: %s" % (cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return sales or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection and connection.is_connected():
                if cr:
                    cr.close()
                connection.close()

    def search_sales_lines(self, idorder=None, valor_propina=0):
        lines = []
        connection = self.mysql_connection()
        if not connection: return []
        try:
            odoo_param = self.get_odoo_config()
            tax_mappings = odoo_param.get('tax_mappings', {})
            product_id_default = odoo_param.get('product_default')
            propina_plu = odoo_param.get('propina_plu')
            
            cr = connection.cursor()
            query = """SELECT
                            ln.id, ln.id_plu, plu.erp, ln.descripcion, ln.cantidad, ln.precio,
                            ln.tax1, ln.tax2, ln.tax3, ln.tax4, ln.tax5, ln.tax6, ln.tax7, ln.tax8, ln.tax9, ln.tax10,
                            ln.precioinicial, ln.descuento, ln.identificador
                        FROM silverpos_hist.hist_venta_deta_plus ln
                        INNER JOIN silverpos_hist.hist_productos plu ON plu.id = ln.id_plu
                        WHERE ln.id_enca = %s AND ln.precio > 0.00 AND borrado = 0;"""
            cr.execute(query, (idorder,))
            records = cr.fetchall()
            for row in records:
                quantity = float(row[4])
                if quantity == 0: continue
                
                price_untaxed = float(row[5])
                total_discount = float(row[17])
                tax_amount = sum(float(row[i] or 0) for i in range(6, 16))
                
                price_unit_with_tax = (price_untaxed + tax_amount) / quantity
                
                identifier = row[18]
                tax_id = [(6, 0, tax_mappings.get(str(identifier), []))]

                line_dict = {
                    'product_id': row[2] if row[2] != 0 else product_id_default,
                    'name': row[3],
                    'product_uom_qty': quantity,
                    'price_unit': round(price_unit_with_tax, 6),
                    'discount': (total_discount / price_untaxed) * 100 if price_untaxed > 0 else 0,
                    'tax_id': tax_id,
                }
                lines.append((0, 0, line_dict))

            if valor_propina and propina_plu:
                propina_line = {
                    'product_id': propina_plu,
                    'name': "Propina Sugerida",
                    'product_uom_qty': 1,
                    'price_unit': float(valor_propina),
                    'tax_id': []
                }
                lines.append((0, 0, propina_line))
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En search_sales_lines para orden {idorder}: {e}")
        finally:
            if connection.is_connected(): connection.close()
        return lines

    def sync_sales_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            sales = self.search_sales()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = f"{url}/sale.order/create"

            for so in sales:
                if not so.get('order_line'):
                    log_msg = f"OMITIDA Venta SilverPOS ID {so.get('silverpos_id')}: No tiene líneas de productos."
                    self.logger(datetime=datetime.now(), type='WARNING', content=log_msg)
                    print(log_msg)
                    continue

                product_ids_in_sale = [line[2]['product_id'] for line in so.get('order_line', []) if line[2].get('product_id')]
                is_valid, invalid_ids = self.validate_odoo_product_ids(product_ids_in_sale)

                if not is_valid:
                    log_msg = f"OMITIDA Venta SilverPOS ID {so.get('silverpos_id')}: Contiene productos inválidos o eliminados en Odoo. IDs: {invalid_ids}"
                    self.logger(datetime=datetime.now(), type='ERROR', content=log_msg)
                    print(log_msg)
                    for pid in invalid_ids:
                        self.reset_product_sync_status(pid)
                    continue
                
                print(f"Pre-validación exitosa. Procesando venta SilverPOS ID: {so.get('silverpos_id')}")
                
                so.update({
                    'warehouse_id': odoo_param.get('warehouse_id'),
                    'analytic_account_id': odoo_param.get('account_analytic_id'),
                    'company_id': odoo_param.get('company_id'),
                    'picking_type_id_mrp': odoo_param.get('picking_type_id_mrp'),
                    'picking_type_id_stock': odoo_param.get('picking_type_id_stock'),
                })
                so_payload = {k: v for k, v in so.items() if v is not None}
                
                response = requests.post(post_url, data=json.dumps(so_payload), params=params, headers=headers, stream=True, verify=False)
                
                self.logger(datetime=datetime.now(), type='DEBUG', content=f"Payload venta {so.get('silverpos_id')}: {json.dumps(so_payload)}")
                self.logger(datetime=datetime.now(), type='DEBUG', content=f"Respuesta venta {so.get('silverpos_id')}: {response.content.decode('utf-8')}")
                
                if response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    if odoo_res.get('success') and odoo_res.get('create_id'):
                        self.update_sales(idsale=so.get('silverpos_id'), idodoo=odoo_res.get('create_id'))
                    else:
                        self.logger(datetime=datetime.now(), type='ERROR', content=f"FALLO Venta {so.get('silverpos_id')}: {odoo_res.get('message')}")
                else:
                    self.logger(datetime=datetime.now(), type='ERROR', content=f"FALLO Venta {so.get('silverpos_id')}: HTTP {response.status_code} - {response.content.decode('utf-8')}")

        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"Excepción fatal en sync_sales_odoo: {e}")

    def update_sales(self, idsale=None, idodoo=None):
        connection = self.mysql_connection()
        if not connection: return
        try:
            cr = connection.cursor()
            query = """UPDATE silverpos_hist.hist_venta_enca SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idsale))
            cr.execute(query, values)
            connection.commit()
            self.logger(datetime=datetime.now(), type='INFO', content=f"Venta {idsale} actualizada con ID Odoo {idodoo}")
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En update_sales: {e}")
        finally:
            if connection.is_connected(): connection.close()
            
    def search_payments(self):
        payments = []
        connection = self.mysql_connection()
        if not connection: 
            return []
        
        try:
            # 2. Calcular las fechas dinámicas
            today = datetime.now()
            two_days_ago = today - timedelta(days=2)

            # Formatearlas como string en el formato 'YYYY-MM-DD' que entiende MySQL
            # Aunque el conector a menudo puede manejar objetos datetime, es más seguro ser explícito.
            start_date = two_days_ago.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')

            self.logger(datetime=datetime.now(), type='INFO', content=f"Buscando pagos entre {start_date} y {end_date}")

            cr = connection.cursor()
            
            # 3. Modificar la consulta para usar marcadores de posición (%s)
            # Usamos BETWEEN para hacer el rango más claro. `BETWEEN A AND B` es inclusivo.
            query = """SELECT 
                            pay.id, pay.valor as monto, pay.id_encaventa, pay.erp, enca.erp,
                            cli.no_tours, enca.fechanegocio, fpay.id as fpay
                        FROM silverpos_hist.hist_venta_deta_pagos pay
                        INNER JOIN silverpos_hist.hist_venta_enca enca ON enca.id = pay.id_encaventa
                        INNER JOIN silverpos.clientes cli ON cli.id = enca.idcliente
                        INNER JOIN silverpos_hist.hist_formas_de_pago fpay ON fpay.id = pay.id_forma_pago
                        WHERE 
                            enca.fechanegocio BETWEEN %s AND %s 
                            AND pay.erp = 0 
                            AND enca.erp != 0 
                            AND enca.borrada = 0 
                            AND enca.mesa != 'Report' 
                            AND enca.anulado = 0
                        ORDER BY enca.erp;"""
            
            # 4. Pasar las fechas como una tupla en el segundo argumento de execute()
            cr.execute(query, (start_date, end_date))
            
            records = cr.fetchall()
            for row in records:
                payment_dict = {
                    'pay_id': int(row[0]),
                    'pay_amount': float(row[1]),
                    'sale_id': int(row[4]),
                    'customer_id': int(row[5]),
                    'payment_date': str(row[6]),
                    'fpay': int(row[7]),
                }
                payments.append(payment_dict)
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En search_payments: {e}")
        finally:
            if connection.is_connected(): 
                connection.close()
                
        return payments

    def sync_payments_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            payments = self.search_payments()
            url, token, db_name = odoo_param.get('url'), odoo_param.get('token'), odoo_param.get('db')
            payments_ids = odoo_param.get('payments', {})
            company_id = odoo_param.get('company_id')
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = f"{url}/account.payment/create"

            for payment in payments:
                pay_id = payment.get('pay_id')
                sale_id = payment.get('sale_id')

                # --- VALIDACIÓN PREVIA ---
                if self.validate_payment_odoo(sale_id=sale_id, payment_id=pay_id):
                    log_msg = f"OMITIDO Pago SilverPOS ID {pay_id}: Ya existe en Odoo."
                    self.logger(datetime=datetime.now(), type='WARNING', content=log_msg)
                    print(log_msg)
                    # Opcional: Podrías actualizar el ERP local aquí si encuentras un pago que existe en Odoo
                    # pero no está marcado en tu BD. Por ahora, lo omitimos para mantenerlo simple.
                    continue

                # --- LÓGICA DE CREACIÓN (si la validación pasa) ---
                journal_id = payments_ids.get(str(payment.get('fpay')))
                if not journal_id:
                    self.logger(datetime=datetime.now(), type='WARNING', content=f"No se encontró mapeo de diario para forma de pago {payment.get('fpay')}. Omitiendo pago {pay_id}")
                    continue
                
                # Construimos la referencia única. ¡ESTO ES CLAVE!
                unique_ref = f"SO{sale_id}-PAY{pay_id}"

                pay_data = {
                    'payment_type': "inbound",
                    'partner_type': "customer",
                    'payment_method_id': 2,
                    'partner_id': payment.get('customer_id'),
                    'sale_id': sale_id,
                    'amount': payment.get('pay_amount', 0.00),
                    'journal_id': journal_id,
                    'date': payment.get('payment_date'),
                    'company_id': company_id,
                    'ref': unique_ref # Usamos la referencia única
                }

                print(f"Intentando crear pago para SilverPOS ID {pay_id} con referencia '{unique_ref}'")
                response = requests.post(post_url, data=json.dumps(pay_data), params=params, headers=headers, stream=True, verify=False)
                
                if response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=f"Respuesta creación pago {pay_id}: {odoo_res}")
                    if odoo_res.get('create_id'):
                        self.update_payments(idpayment=pay_id, idodoo=odoo_res.get('create_id'))
                    else:
                        self.logger(datetime=datetime.now(), type='ERROR', content=f"Fallo al crear pago {pay_id}: {odoo_res.get('message')}")
                else:
                    self.logger(datetime=datetime.now(), type='ERROR', content=f"Fallo al crear pago {pay_id}: HTTP {response.status_code} - {response.content.decode('utf-8')}")

        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"Excepción fatal en sync_payments_odoo: {e}")

    def update_payments(self, idpayment=None, idodoo=None):
        connection = self.mysql_connection()
        if not connection: return
        try:
            cr = connection.cursor()
            query = """UPDATE silverpos_hist.hist_venta_deta_pagos SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idpayment))
            cr.execute(query, values)
            connection.commit()
            self.logger(datetime=datetime.now(), type='INFO', content=f"Pago {idpayment} actualizado con ID Odoo {idodoo}")
            cr.close()
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=f"En update_payments: {e}")
        finally:
            if connection.is_connected(): connection.close()

if __name__ == '__main__':
    print("Iniciando conector Odoo-SilverPOS...")
    try:
        mysql_config_path = 'C:/dist/root/conexion_h.conf'
        odoo_config_path = 'C:/dist/root/config_connector.json'
        
        connector = OdooConnector(mysql_config_path, odoo_config_path)

        print("\n--- Sincronizando Ventas Anuladas ---")
        connector.sync_nullsales_odoo()
        
        print("\n--- Sincronizando Subcategorías ---")
        connector.sync_subcategories_odoo()
        
        print("\n--- Sincronizando Productos ---")
        connector.sync_products_odoo()
        
        print("\n--- Sincronizando Empleados ---")
        connector.sync_employee_odoo()
        
        print("\n--- Sincronizando Clientes ---")
        connector.sync_customers_odoo()
        
        print("\n--- Sincronizando Pedidos de Venta ---")
        connector.sync_sales_odoo()
        
        print("\n--- Sincronizando Pagos ---")
        connector.sync_payments_odoo()

        print("\nProceso de sincronización completado.")

    except Exception as e:
        print(f"ERROR CRÍTICO: El script no pudo completarse. Causa: {e}")
        # Si el conector se inicializó, intenta loguear el error
        if 'connector' in locals():
            connector.logger(datetime.now(), 'CRITICAL_ERROR', str(e))