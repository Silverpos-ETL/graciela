# -*- coding: utf-8 -*-
##########################################################
#   Copyright: Luis Aquino
#   Contact: Luis Aquino -> +502 4814-3481
#   Support: Luis Aquino -> laquinobarrientos@gmail.com
##########################################################



import mysql.connector
import json
from datetime import datetime
import requests
import xmlrpc.client

class OdooConnector():

    def __init__(self, path_config=None, odoo_config=None):
        self.path_config = path_config
        self.odoo_config = odoo_config
    
    def logger(self, datetime=None, type=None, content=None):
        f_log = open('OdooConnector.log', 'a')
        msg = ""
        try:
            msg = ("%s %s %s\n" %(datetime, type, content))
            f_log.write(msg)
        except Exception as e:
            msg = ("%s %s %s\n" %(datetime, 'ERROR', str(e)))
            f_log.write(msg)
        finally:
            f_log.close()

    def get_mysql_config(self):
        conf_dict = {}
        if self.path_config:
            print(self.path_config)
            self.logger(datetime=datetime.now(), type='PATH_CONFIG', content=str(self.path_config))
            f = open(self.path_config, 'r')
            try:
                #f = open(self.path_config, 'r')
                #print(list(f))
                f_list = list(f)
                self.logger(datetime=datetime.now(), type='PARAMETERS', content=f_list)
                for item in range(1, len(f_list)):
                    #print(f_list[item])
                    paramters = str(f_list[item]).split('=')
                    if paramters[0] == 'password':
                        conf_dict['password'] = paramters[1].rstrip("\n")
                    if paramters[0] == 'usuario':
                        conf_dict['user'] = paramters[1].rstrip("\n")
                    if paramters[0] == 'base':
                        conf_dict['db'] = paramters[1].rstrip("\n")
                    if paramters[0] == 'puerto':
                        conf_dict['port'] = paramters[1].rstrip("\n")
                    if paramters[0] == 'servidor':
                        conf_dict['url'] = paramters[1].rstrip("\n")
                print(conf_dict)
                self.logger(datetime=datetime.now(), type='CREDENTIALS', content=conf_dict)
                return conf_dict or {}
            except Exception as e:
                print("Error to read file %s: %s" %(self.path_config, e))
                self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
            finally:
                f.close()

    def get_odoo_config(self):
        if self.odoo_config:
            self.logger(datetime=datetime.now(), type='ODOO_CONFIG', content=str(self.odoo_config))
            json_config = open(self.odoo_config, 'r')
            try:
                data = json.load(json_config)
                print(data)
                self.logger(datetime=datetime.now(), type='ODOO_PARAMETERS', content=data)
                return data or {}
            except Exception as e:
                print("Error to read file %s: %s" %(self.path_config, e))
                self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
            finally:
                json_config.close()

    def mysql_connection(self):
        try:
            mysql_param = self.get_mysql_config()
            if mysql_param:
                url = mysql_param.get('url', False)
                user = mysql_param.get('user', False)
                password = mysql_param.get('password', False)
                db = mysql_param.get('db', False,)
                port = mysql_param.get('port', False)
                connection = mysql.connector.connect(host=url, user=user, passwd=password)
                #cr = connection.cursor()
                msg = ("MySql Connection successfully: %s" %(connection))
                self.logger(datetime=datetime.now(), type='INFO', content=msg)
                return connection
            else:
                self.logger(datetime=datetime.now(), type='WARNING', content="Couldn't to get MySql configuration")
                return False
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
 

    def search_invalidated_sales(self):
        connection = self.mysql_connection()
        cr = connection.cursor()
        query = """SELECT id, num_doc, erp FROM ventasdiarias.venta_encabezado WHERE anulado = 1 AND web = 0 AND erp <> 0;"""
        cr.execute(query)
        records = cr.fetchall()
        invalidated_sales = [{'silverpos_id': row[0], 'num_doc': row[1], 'erp_id': row[2]} for row in records]
        cr.close()
        connection.close()
        return invalidated_sales
    
    def validate_sale_odoo(self, silverpos_id=None):
        idodoo = False
        try:
            odoo_param = self.get_odoo_config()
            model = 'anulados.silverpos'
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)

            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = "?domain=[('silverpos_id', '=', %d), ('company_id', '=', %d)]" % (silverpos_id, company_id)
            fields = "&fields=['id', 'silverpos_id']"
            
            get_url = ("%s/%s/search%s%s" % (url, model, domain, fields))
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)

            if response and response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') == True:
                    for item in odoo_res.get('data', []):
                        idodoo = item.get('id', False)
                        return idodoo or False
            self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))

        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
            return False
        return False

    def sync_nullsales_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            invalidated_sales = self.search_invalidated_sales()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)

            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = "%s/anulados.silverpos/create" % (url)

            for sale in invalidated_sales:
                sale_data = {
                    'silverpos_id': sale.get('silverpos_id', False),
                    'num_doc': sale.get('num_doc', False),
                    'erp_id': sale.get('erp_id', False),
                    'company_id': company_id,
                   
                }
                
                response = requests.post(post_url, data=json.dumps(sale_data), params=params, headers=headers, stream=True, verify=False)
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='ERROR', content="asd")
                    self.logger(datetime=datetime.now(), type='ERROR', content=odoo_res)
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_nullsale(silverpos_id=sale.get('silverpos_id', False), web=odoo_res.get('create_id', False))

        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def update_nullsale(self, silverpos_id=None, web=None):
        try:
            connection = self.mysql_connection()
            query = """UPDATE ventasdiarias.venta_encabezado SET web = %s WHERE id = %s;"""
            values = (int(web), int(silverpos_id))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("Sale %s: is updated with web %s" % (silverpos_id, web))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def search_employees(self):
        employee_dict = {}
        employes = []
        try:
            connection = self.mysql_connection()
            query = """SELECT id, nombre, user, password, email, erp FROM silverpos_hist.hist_usuarios
                        WHERE nombre != '' and erp = 0;"""
            cr = connection.cursor()
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                print(row)
                idodoo_res = self.validate_employee_odoo(idsilverpos=int(row[0]))
                if idodoo_res:
                    self.update_employees(idemployee=int(row[0]), idodoo=idodoo_res)
                else:
                    employee_dict = {
                        'user_name': str(row[1]),
                        'user_email': str(row[1]),
                        'silverpos_id': int(row[0]),
                    }
                    employes.append(employee_dict)
            msg = ("Total number of rows in this query are: %s" %(cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return employes or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def sync_employee_odoo(self):
        products = []
        try:
            odoo_param = self.get_odoo_config()
            employees = self.search_employees()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = ("%s/res.users/create" %(url))

            self.logger(datetime=datetime.now(), type='INFO', content=f"post_url {post_url}")
            self.logger(datetime=datetime.now(), type='INFO', content=f"headers {headers}")
            self.logger(datetime=datetime.now(), type='INFO', content=f"params {params}")

            for employe in employees:
                item = {
                    'name': employe.get('user_name', False),
                    'login': employe.get('user_email', False),
                    'company_id': company_id,
                    'silverpos_id': employe.get('silverpos_id', False)
                }
                self.logger(datetime=datetime.now(), type='INFO', content=f"este es el directorio {item}")

                

                response  = requests.post(post_url, data=json.dumps(item), params=params, headers=headers, stream=True, verify=False)
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_employees(idemployee=employe.get('silverpos_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def validate_employee_odoo(self, idsilverpos=None):
        products = []
        idodoo = False
        try:
            odoo_param = self.get_odoo_config()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = f"?domain=[('silverpos_id', '=', {idsilverpos})]" 
            fields = "&fields=['id', 'silverpos_id']"
            get_url = ("%s/res.users/search%s%s" %(url, domain, fields))
            response  = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response and response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') == True:
                    for item in odoo_res.get('data', []):
                        idodoo = item.get('id', False)
                        print(idodoo)
                        return idodoo or False
                else:
                    print(idodoo)
                    return False
                self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def update_employees(self, idemployee=None, idodoo=None):
        product_dict = {}
        products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE silverpos_hist.hist_usuarios SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idemployee))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("SilverPos User %s: is update IdOdoo %s" %(idemployee, idodoo))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def search_subcategories(self):
        subcategories = []
        try:
            connection = self.mysql_connection()
            cr = connection.cursor()
            query = """SELECT id, nombre, productos_categoria_id FROM silverpos_hist.hist_productos_sub_categoria WHERE nombre != '' and erp = 0;"""
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                print(row)
                idodoo_res = self.validate_subcategory_odoo(idsilverpos=int(row[0]), name=str(row[1]))
                if idodoo_res:
                    self.update_subcategory(idsubcategory=int(row[0]), idodoo=idodoo_res)
                else:
                    subcategory_dict = {
                        'subcategory_id': int(row[0]),
                        'subcategory_name': str(row[1]),
                        'subcategory_cat': row[2]
                    }
                    subcategories.append(subcategory_dict)
            msg = "Total number of rows in this query are: %s" % cr.rowcount
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if cr:
                cr.close()
            if connection.is_connected():
                connection.close()
        return subcategories


    def validate_subcategory_odoo(self, idsilverpos=None, name=None):
        idodoo = False
        try:
            odoo_param = self.get_odoo_config()

            # Dado que Odoo usa el modelo 'product.category' para ambas categorías y subcategorías, mantenemos este modelo.
            model = 'product.category'

            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            #domain = "?domain=[('silverpos_id', '=', %d), ('silverpos_company_id', '=', %d)]" % (idsilverpos, company_id)
            domain = f"?domain=[('silverpos_id', '=', {idsilverpos})]" #, ('name', '=', '{name}')
            #domain = "?domain=[('silverpos_id', '=', %d)]" % (idsilverpos)
            fields = "&fields=['id', 'silverpos_id', 'name', 'tipo_categoria']"
            
            get_url = f"{url}/product.category/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
                    
            if response and response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') == True:
                    for item in odoo_res.get('data', []):
                        idodoo = item.get('id', False)
                        return idodoo or False

            self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))

        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
            return False
        return False  # Asegurarse de devolver un valor en todos los caminos.

    def update_subcategory(self, idsubcategory=None, idodoo=None):
        try:
            connection = self.mysql_connection()
            query = """UPDATE silverpos_hist.hist_productos_sub_categoria SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idsubcategory))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("SilverPos Subcategory %s: is updated with IdOdoo %s" % (idsubcategory, idodoo))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def sync_subcategories_odoo(self):
        try:
            odoo_param = self.get_odoo_config()
            subcategories = self.search_subcategories()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = "%s/product.category/create" % (url) 
            for subcategory in subcategories:
                tipo_categoria_value = str(subcategory.get('subcategory_cat'))
                subcat = {
                    'name': subcategory.get('subcategory_name', False),
                    'silverpos_id': subcategory.get('subcategory_id', False),
                    'tipo_categoria': tipo_categoria_value,
                    'removal_strategy_id': 1,
                    'property_cost_method': 'average',
                    'property_valuation': 'real_time',
                    'property_account_income_categ_id': 70, #cuenta de ingresos
                    'property_account_expense_categ_id': 150, #cuenta de gastos
                    'property_stock_valuation_account_id': 29, #cuenta de valoracion de inventario
                    'property_stock_journal': 8, #diario de stock
                    'property_stock_account_input_categ_id': 146, #cuenta entrada de stock
                    'property_stock_account_output_categ_id': 147, #cuenta de salida de stock
                    #'company_id': company_id,
                }
                
                response = requests.post(post_url, data=json.dumps(subcat), params=params, headers=headers, stream=True, verify=False)
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_subcategory(idsubcategory=subcategory.get('subcategory_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))



    def search_products(self):
        product_dict = {}
        products = []
        try:
            connection = self.mysql_connection()
          
            query = """SELECT 
                                p.id, 
                                p.nombre, 
                                p.codigo, 
                                p.erp, 
                                p.productos_sub_categoria_id,  
                                sc.erp AS sub_categoria_erp
                                
                            FROM 
                                silverpos_hist.hist_productos AS p
                            JOIN 
                                silverpos_hist.hist_productos_sub_categoria AS sc ON p.productos_sub_categoria_id = sc.id
                            WHERE 
                                p.nombre != '' AND p.erp = 0;"""
            cr = connection.cursor()
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                print(row)
                idodoo_res = self.validate_product_odoo(idsilverpos=int(row[0]))
                if idodoo_res:
                    self.update_products(idproduct=int(row[0]), idodoo=idodoo_res)
                else:
                    product_dict = {
                        'product_id': int(row[0]),
                        'product_name': str(row[1]),
                        'product_code': row[2],
                        'product_categ_id': row[5],
                        #'service_flag': row[6],
                    }
                    products.append(product_dict)
            msg = ("Total number of rows in this query are: %s" %(cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return products or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def validate_product_odoo(self, idsilverpos=None):
        products = []
        idodoo = False
        try:
            odoo_param = self.get_odoo_config()
            #products = self.search_products()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = "?domain=[('silverpos_id', '=', %d)]" % (idsilverpos)
            #domain = ("?domain=[('silverpos_id', '=', %d), ('company_id', '=', %d)]" % (idsilverpos, company_id))
            #domain = ("?domain=[('silverpos_id', '=', %d), ('silverpos_company_id', '=', %d)]" %(idsilverpos, company_id)) 
            fields = "&fields=['id', 'silverpos_id', 'display_name']"
            get_url = ("%s/product.product/search%s%s" %(url, domain, fields))
            response  = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response and response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') == True:
                    for item in odoo_res.get('data', []):
                        idodoo = item.get('id', False)
                        print(idodoo)
                        return idodoo or False
                else:
                    print(idodoo)
                    return False
                self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                #self.update_products(idproduct=product.get('product_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        
    def update_products(self, idproduct=None, idodoo=None):
        product_dict = {}
        products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE silverpos_hist.hist_productos SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idproduct))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("SilverPos Product %s: is update IdOdoo %s" %(idproduct, idodoo))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()
 
    def sync_products_odoo(self):
        products = []
        try:
            odoo_param = self.get_odoo_config()
            products = self.search_products()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            product_category = odoo_param.get('category_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = ("%s/product.product/create" %(url))
            for product in products:
                product_type = 'service' if product.get('service_flag') == 1 else 'product'
                prod = {
                    'name': product.get('product_name', False),
                    'default_code': product.get('product_code', False),
                    'silverpos_id': product.get('product_id', False),
                    'sale_ok': True,
                    'purchase_ok': True,
                    'invoice_policy': 'order',
                    #'company_id': company_id,
                    'type': product_type,
                    'categ_id': product.get('product_categ_id', False),
                }
                response  = requests.post(post_url, data=json.dumps(prod), params=params, headers=headers, stream=True, verify=False)
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_products(idproduct=product.get('product_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))


    def search_customers(self):
        customer_dict = {}
        customers = []
        try:
            connection = self.mysql_connection()

            query = """SELECT id, nombre, num_doc, codigo 
                    FROM silverpos.clientes 
                    WHERE nombre != '' AND no_tours = 0;"""
            cr = connection.cursor()
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                print(row)
                idodoo_res = self.validate_customers_odoo(idsilverpos=int(row[0]), name=str(row[1]), code=row[3])
                if idodoo_res:
                    self.update_customers(idcustomer=int(row[0]), idodoo=idodoo_res)
                else:
                    customer_dict = {
                        'customer_id': int(row[0]),
                        'customer_name': str(row[1]),
                        'customer_nit': row[2],
                        #'customer_code': row[3],
                    }
                    customers.append(customer_dict)
            msg = ("Total number of rows in this query are: %s" % (cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return customers or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()


    def validate_customers_odoo(self, idsilverpos=None, name=None, code=None):
        customers = []
        idodoo = False
        try:
            odoo_param = self.get_odoo_config()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            # Ajustamos el dominio para buscar por id, nombre y código
            domain = f"?domain=[('silverpos_id', '=', {idsilverpos}), ('name', '=', '{name}')]"
            # Ajustamos los campos a recuperar de Odoo
            fields = "&fields=['id', 'silverpos_id', 'name', 'vat']"
            get_url = f"{url}/res.partner/search{domain}{fields}"
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)
            if response and response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                if odoo_res.get('success') == True:
                    for item in odoo_res.get('data', []):
                        idodoo = item.get('id', False)
                        print(idodoo)
                        return idodoo or False
                else:
                    print(idodoo)
                    return False
                self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))


    def update_customers(self, idcustomer=None, idodoo=None):
        customers_dict = []
        customers = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE silverpos.clientes SET no_tours = %s WHERE id = %s;"""
            values = (int(idodoo), int(idcustomer))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("SilverPos Customer %s: is update no_tours %s" %(idcustomer, idodoo))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def sync_customers_odoo(self):
        products = []
        try:
            odoo_param = self.get_odoo_config()
            customers = self.search_customers()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = ("%s/res.partner/create" %(url))
            for customer in customers:
                prod = {
                    'name': customer.get('customer_name', False),
                    'vat': customer.get('customer_nit', False),
                    'silverpos_id': customer.get('customer_id', False),
                    'silverpos_code': customer.get('customer_code', False),
                    'customer_rank': 1,
                    'property_account_receivable_id': 17, #122101 Clientes Restaurante
                    'property_account_payable_id': 48, #22102 Acreedores locales

                }
                response  = requests.post(post_url, data=json.dumps(prod), params=params, headers=headers, stream=True, verify=False)
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_customers(idcustomer=customer.get('customer_id', False), idodoo=odoo_res.get('id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))


    def search_sales(self):
        sale_dict = {}
        sales = []
        try:
            connection = self.mysql_connection()
            query = """SELECT 
                        venc.id ,
                        venc.fechanegocio,
                        venc.idcliente,
                        cli.no_tours,
                        cli.nombre,
                        venc.serie,
                        venc.num_doc,
                        venc.uuid,
                        user.erp,
                        venc.anulado,
                        venc.cuenta_por_cobrar,
                        venc.valor_propina
                    FROM silverpos_hist.hist_venta_enca venc
                    INNER JOIN silverpos.clientes cli on cli.id = venc.idcliente
                    INNER JOIN silverpos_hist.hist_usuarios user on user.id = venc.idmesero
                    WHERE venc.erp = 0 and venc.borrada = 0 and venc.num_doc > 0 and venc.terminal > 0 and venc.mesa != 'Report' ;"""
            cr = connection.cursor()
            cr.execute(query)
            records = cr.fetchall()

            for row in records:
                print(row)
                lines = self.search_sales_lines(int(row[0]), row[11])

                state_value = 'cancel' if row[9] else 'draft'

                sale_dict = {
                    'silverpos_id': row[0],
                    'date_order': str(row[1]),
                    'partner_id': row[3],
                    'client_order_ref': row[4],
                    'silverpos_uuid': row[5],
                    'silverpos_serie_fel': row[6],
                    'silverpos_numero_fel': row[7],
                    'silverpos_user_id': row[8],
                    'state': state_value,
                    'silverpos_cxc': row[10],
                   # 'silverpos_tips': row[11],
                    'silverpos_order_date': str(row[1]),
                }
                if lines:
                    sale_dict.update({
                        'order_line': lines,
                    })
                sales.append(sale_dict)
            msg = ("Total number of rows in this query are: %s" %(cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return sales or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def search_sales_lines(self, idorder=None, valor_propina=0):
        lines = []
        try:
            odoo_param = self.get_odoo_config()
            tax_mappings = odoo_param.get('tax_mappings', {})
            
            connection = self.mysql_connection()
            product_id = odoo_param.get('product_default', False)
            query = """SELECT
                            ln.id,
                            ln.id_plu,
                            plu.erp,
                            ln.descripcion,
                            ln.cantidad,
                            ln.precio,
                            ln.tax1,
                            ln.tax2,
                            ln.tax3,
                            ln.tax4,
                            ln.tax5,
                            ln.tax6,
                            ln.tax7,
                            ln.tax8,
                            ln.tax9,
                            ln.tax10,
                            ln.precioinicial,
                            ln.descuento,
                            ln.identificador
                        FROM silverpos_hist.hist_venta_deta_plus ln
                        INNER JOIN silverpos_hist.hist_productos plu on plu.id = ln.id_plu
                        where ln.id_enca = %s and ln.precio > 0.00 and borrado = 0;"""
            data = (idorder,)
            cr = connection.cursor()
            cr.execute(query, data)
            records = cr.fetchall()
            for row in records:
                print(row)
                price_untaxed = float(row[5]) or 0.00  # Precio sin impuestos para toda la cantidad
                quantity = float(row[4])  # Cantidad (asegurado que no es 0)
                total_discount = float(row[17]) or 0.00  # Descuento total aplicado a toda la cantidad
                discount_per_unit = total_discount / quantity  # Calcula el descuento por unidad
                tax_amount = sum([float(row[i]) for i in range(6, 16)])  # Suma de impuestos por línea
                price_unit = round(((price_untaxed - discount_per_unit)+(tax_amount / quantity)), 6) or 0.00  # Precio unitario después del descuento
                valor_propina = (valor_propina)
                propina_plu = odoo_param.get('propina_plu', False)
                identifier = row[18]  # o la columna correspondiente para 'identificador'
                tax_id = [(6, 0, tax_mappings.get(identifier, []))]

                line_dict = {
                    'product_id': row[2] if row[2] != 0 else product_id,
                    'name': row[3],
                    'product_uom_qty': quantity or 0.00,
                    'price_unit': price_unit,
                   # 'discount': discount_per_unit or 0.00,
                    'tax_id': tax_id,
                }
                
                lines.append((0, 0, line_dict))

             # Chequeo y adición de la línea de propina después del bucle
           
            propina_line = {
                'product_id': propina_plu ,
                'name': "Propina Sugerida",
                'product_uom_qty': 1,
                'price_unit': valor_propina or 0,
                'tax_id': []  
            }
            if valor_propina != 0:
                lines.append((0, 0, propina_line))  # Añade la línea de propina a las líneas de pedido

            
            msg = ("Total number of rows in this query are: %s" %(cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return lines or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()
        return lines

    def sync_sales_odoo(self):
        sales = []
        try:
            odoo_param = self.get_odoo_config()
            sales = self.search_sales()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            warehouse_id = odoo_param.get('warehouse_id', False)
            picking_type_id_mrp = odoo_param.get('picking_type_id_mrp', False)
            picking_type_id_stock = odoo_param.get('picking_type_id_stock', False)
            company_id = odoo_param.get('company_id', False)
            account_analytic_id = odoo_param.get('account_analytic_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = ("%s/sale.order/create" %(url))
            for so in sales:
                if warehouse_id:
                    so.update({
                        'warehouse_id': warehouse_id,
                    })
                if account_analytic_id:
                    so.update({
                        'analytic_account_id': account_analytic_id,
                    })
                if  company_id:
                    so.update({
                        'company_id': company_id,
                    })

                if  picking_type_id_mrp:
                    so.update({
                        'picking_type_id_mrp': picking_type_id_mrp,
                    })
                
                if  picking_type_id_stock:
                    so.update({
                        'picking_type_id_stock': picking_type_id_stock,
                    })

                response  = requests.post(post_url, data=json.dumps(so), params=params, headers=headers, stream=True, verify=False)
                print(so)
                print(response.content.decode('utf-8'))
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_sales(idsale=so.get('silverpos_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def update_sales(self, idsale=None, idodoo=None):
        #product_dict = {}
        #products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE silverpos_hist.hist_venta_enca SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idsale))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("SilverPos Sale %s: is update IdOdoo %s" %(idsale, idodoo))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def search_payments(self):
        payment_dict = {}
        payments = []
        try:
            connection = self.mysql_connection()
            odoo_param = self.get_odoo_config()
            query = """SELECT 
                            pay.id,
                            pay.valor as monto,
                            pay.id_encaventa,
                            pay.erp,
                            enca.erp,
                            cli.no_tours,
                            enca.fechanegocio,
                            fpay.id as fpay
                        FROM silverpos_hist.hist_venta_deta_pagos pay
                        INNER JOIN silverpos_hist.hist_venta_enca enca on enca.id = pay.id_encaventa
                        INNER JOIN silverpos.clientes cli on cli.id = enca.idcliente
                        INNER JOIN silverpos_hist.hist_formas_de_pago fpay on fpay.id = pay.id_forma_pago
                        WHERE pay.erp = 0 and enca.erp != 0 and enca.borrada = 0 and enca.mesa != 'Report' and enca.anulado = 0
        
                        ORDER BY enca.erp;"""
            cr = connection.cursor()
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                print(row)
                payment_dict = {
                    'pay_id': int(row[0]),
                    'pay_amount': float(row[1]),
                    'sale_id': int(row[4]),
                    'customer_id': int(row[5]),
                    'payment_date': str(row[6]),
                    'fpay': int(row[7]),
                }
                payments.append(payment_dict)
            msg = ("Total number of rows in this query are: %s" %(cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return payments or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def sync_payments_odoo(self):
        payments = []
        try:
            odoo_param = self.get_odoo_config()
            payments = self.search_payments()
            self.logger(datetime=datetime.now(), type='XPAGOS', content=str(payments))
            url = odoo_param.get('url', False)
            self.logger(datetime=datetime.now(), type='XURL', content=str(url))
            token = odoo_param.get('token', False)
            self.logger(datetime=datetime.now(), type='XTOKEN', content=str(token))
            db_name = odoo_param.get('db', False)
            journal_id = odoo_param.get('cash', False)
            payments_ids = odoo_param.get('payments', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}

            
            post_url = ("%s/account.payment/create" %(url))
            self.logger(datetime=datetime.now(), type='XURL22', content=str(post_url))
            
            for payment in payments:
                #journal_id = 
                if payment.get('fpay', False):
                    journal_id = payments_ids[str(payment.get('fpay'))]
                prod = {
                    'payment_type': "inbound",
                    'partner_type': "customer",
                    'payment_method_id': 2,
                    'partner_id': payment.get('customer_id', False),
                    'sale_id': payment.get('sale_id', False),
                    'amount': payment.get('pay_amount', 0.00),
                    'journal_id': journal_id,
                    'date': payment.get('payment_date', False),
                    'company_id': company_id,
                    'ref': payment.get('sale_id', False)
                }
                self.logger(datetime=datetime.now(), type='Xheaders', content=str(headers))
                self.logger(datetime=datetime.now(), type='Xparametros', content=str(params))
                #self.logger(datetime=datetime.now(), type='XJSON', content=str(prod))
                self.logger(datetime=datetime.now(), type='ENDPOINT', content=str(post_url))
                response  = requests.post(post_url, data=json.dumps(prod), params=params, headers=headers, stream=True, verify=False)

                self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=odoo_res)
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_payments(idpayment=payment.get('pay_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def update_payments(self, idpayment=None, idodoo=None):
         
        try:
            connection = self.mysql_connection()
            query = """UPDATE silverpos_hist.hist_venta_deta_pagos SET erp = %s WHERE id = %s;"""
            values = (int(idodoo), int(idpayment))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("SilverPos Payment %s: is update IdOdoo %s" %(idsale, idodoo))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

OdooConnector()

connector = OdooConnector('C:/dist/root/conexion_h.conf', 'C:/dist/root/config_connector.json')
#connector.get_mysql_config()
#connector.get_odoo_config()
#cr = connector.mysql_connection()
#print(cr)
#products = connector.search_products()
#print(products)
connector.sync_nullsales_odoo()
connector.sync_subcategories_odoo()
connector.sync_products_odoo()
connector.sync_employee_odoo()
connector.sync_customers_odoo()
#connector.search_sales()
#connector.search_sales_lines(idorder=1)
connector.sync_sales_odoo()
connector.sync_payments_odoo()


#connector.validate_product_odoo(idsilverpos=139)
