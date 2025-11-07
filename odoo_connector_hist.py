# -*- coding: utf-8 -*-
##########################################################
#   Copyright: Luis Aquino
#   Contact: Luis Aquino -> +502 4814-3481
#   Support: Luis Aquino -> laquinobarrientos@gmail.com
##########################################################

#try:
import mysql.connector
import json
from datetime import datetime
import requests
#except Exception as e:

#f_log = open('OdooConnector.log', 'wb')

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
                connection = mysql.connector.connect(host=url, user=user, passwd=password, db=db)
                #cr = connection.cursor()
                msg = ("MySql Connection successfully: %s" %(connection))
                self.logger(datetime=datetime.now(), type='INFO', content=msg)
                return connection
            else:
                self.logger(datetime=datetime.now(), type='WARNING', content="Couldn't to get MySql configuration")
                return False
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
    
    def search_products(self):
        product_dict = {}
        products = []
        try:
            connection = self.mysql_connection()
            query = """SELECT id, nombre, codigo, erp FROM hist_productos 
                        WHERE nombre != '' and erp = 0;"""
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
            domain = ("?domain=[('silverpos_id','=',%s),('silverpos_company_id','=',%s)]" %(str(idsilverpos), str(company_id))) 
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
        
    def search_employees(self):
        employee_dict = {}
        employes = []
        try:
            connection = self.mysql_connection()
            query = """SELECT id, nombre, user, password, email, erp FROM hist_usuarios
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

    def update_employees(self, idemployee=None, idodoo=None):
        product_dict = {}
        products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE hist_usuarios SET erp = %s WHERE id = %s;"""
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

    def update_products(self, idproduct=None, idodoo=None):
        product_dict = {}
        products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE hist_productos SET erp = %s WHERE id = %s;"""
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
            domain = ("?domain=[('silverpos_id', '=', %d)]" %(idsilverpos)) 
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
            for employe in employees:
                item = {
                    'name': employe.get('user_name', False),
                    'login': employe.get('user_email', False),
                    'company_id': company_id,
                    'silverpos_id': employe.get('silverpos_id', False)
                }
                response  = requests.post(post_url, data=json.dumps(item), params=params, headers=headers, stream=True, verify=False)
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_employees(idemployee=employe.get('silverpos_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def sync_products_odoo(self):
        products = []
        try:
            odoo_param = self.get_odoo_config()
            products = self.search_products()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = ("%s/product.product/create" %(url))
            for product in products:
                prod = {
                    'name': product.get('product_name', False),
                    'default_code': product.get('product_code', False),
                    'silverpos_id': product.get('product_id', False),
                    'sale_ok': True,
                    'purchase_ok': False,
                    'invoice_policy': 'order',
                    'company_id': company_id,
                    'type': 'product',
					'detailed_type': 'product'
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
            query = """SELECT id, nombre, no_documento FROM hist_clientes 
                        WHERE nombre != '' and idodoo = 0;"""
            cr = connection.cursor()
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                print(row)
                #idodoo_res = self.validate_customers_odoo(nit=row[3])
                #if idodoo_res:
                #    self.update_customers(idcustomer=int(row[0]), idodoo=int(idodoo_res))
                #else:
                customer_dict = {
                    'customer_id': row[0],
                    'customer_name': str(row[1]),
                    'customer_nit': row[2],
                }
                customers.append(customer_dict)
            msg = ("Total number of rows in this query are: %s" %(cr.rowcount))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
            return customers or []
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def update_customers(self, idcustomer=None, idodoo=None):
        #product_dict = {}
        #products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE hist_clientes SET idodoo = %s WHERE id = %s;"""
            values = (int(idodoo), int(idcustomer))
            cr = connection.cursor()
            cr.execute(query, values)
            connection.commit()
            msg = ("SilverPos Customer %s: is update IdOdoo %s" %(idcustomer, idodoo))
            self.logger(datetime=datetime.now(), type='INFO', content=msg)
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
        finally:
            if connection.is_connected():
                connection.close()
                cr.close()

    def validate_customers_odoo(self, nit=None):
        products = []
        idodoo = False
        try:
            odoo_param = self.get_odoo_config()
            #products = self.search_products()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            domain = ("?domain=[('vat','=',%s)]" %(nit)) 
            fields = "&fields=['id', 'vat', 'name']"
            get_url = ("%s/res.partner/search%s%s" %(url, domain, fields))
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

    def sync_customers_odoo(self):
        products = []
        try:
            odoo_param = self.get_odoo_config()
            costumers = self.search_customers()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            post_url = ("%s/res.partner/create" %(url))
            for customer in costumers:
                prod = {
                    'name': customer.get('customer_name', False),
                    'vat': customer.get('customer_nit', False),
                    'silverpos_id': customer.get('customer_id', False),
                    'customer_rank': 1
                }
                response  = requests.post(post_url, data=json.dumps(prod), params=params, headers=headers, stream=True, verify=False)
                if response and response.status_code == 200:
                    odoo_res = json.loads(response.content.decode('utf-8'))
                    self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
                    self.update_customers(idcustomer=customer.get('customer_id', False), idodoo=odoo_res.get('create_id', False))
        except Exception as e:
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def search_sales(self):
        sale_dict = {}
        sales = []
        try:
            connection = self.mysql_connection()
            query = """SELECT 
                            venc.id ,
                            venc.fechatransaccion,
                            venc.idcliente,
                            cli.idodoo,
                            cli.nombre,
                            venc.serie,
                            venc.num_fac_electronica,
                            venc.uuid,
                            user.erp,
                            venc.fechanegocio,
                            venc.valor_propina
                        FROM hist_venta_enca venc
                        INNER JOIN hist_clientes cli on cli.id = venc.idcliente
                        INNER JOIN hist_usuarios user on user.id = venc.idmesero
                        WHERE venc.erp = 0 and venc.borrada = 0 and venc.mesa != 'Report' and venc.anulado = 0 and venc.fechanegocio >= '2024-07-01';"""
            cr = connection.cursor()
            cr.execute(query)
            records = cr.fetchall()
            for row in records:
                print(row)
                lines = self.search_sales_lines(int(row[0]), row[10])
                sale_dict = {
                    'silverpos_id': row[0],
                    'date_order': str(row[1]),
                    'partner_id': row[3],
                    'client_order_ref': row[4],
                    'silverpos_uuid': row[5],
                    'silverpos_serie_fel': row[6],
                    'silverpos_numero_fel': row[7],
                    'silverpos_user_id': row[8],
                    'state': 'draft',
                    'silverpos_order_date': str(row[9]),
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
                price_unit = round(((price_untaxed - discount_per_unit)+(tax_amount / quantity)),6 ) or 0.00  # Precio unitario después del descuento con impuesto
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
    
    
    
    # def search_sales_lines(self, idorder=None):
    #     line_dict = {}
    #     lines = []
    #     try:
    #         odoo_param = self.get_odoo_config()
    #         connection = self.mysql_connection()
    #         product_id = odoo_param.get('product_default', False)
    #         query = """SELECT
    #                         ln.id,
    #                         ln.id_plu,
    #                         plu.erp,
    #                         ln.descripcion,
    #                         ln.cantidad,
    #                         ln.precio,
    #                         ln.tax1,
    #                         ln.tax2,
    #                         ln.tax3,
    #                         ln.tax4,
    #                         ln.tax5,
    #                         ln.tax6,
    #                         ln.tax7,
    #                         ln.tax8,
    #                         ln.tax9,
    #                         ln.tax10,
    #                         ln.descuento
    #                     FROM hist_venta_deta_plus ln
    #                     INNER JOIN hist_productos plu on plu.id = ln.id_plu
    #                     where ln.id_enca = %s and ln.precio > 0.00;"""
    #         data = (idorder,)
    #         cr = connection.cursor()
    #         cr.execute(query, data)
    #         records = cr.fetchall()
    #         for row in records:
    #             print(row)
    #             price_untaxed = float(row[5])
    #             tax_amount = float(row[6] + row[7] + row[8] + row[9] + row[10] + row[11] + row[12] + row[13] + row[14] + row[15])
    #             quantity = float(row[4])
    #             price_unit = round((price_untaxed + (tax_amount / quantity if quantity > 0.00 else 1.00)), 4)
    #             line_dict = {
    #                 'product_id': row[2] if row[2] != 0 else product_id,
    #                 'name': row[3],
    #                 'product_uom_qty': quantity or 0.00,
    #                 'price_unit': float(price_unit) or 0.00,
    #             }
    #             lines.append((0, 0, line_dict))
    #         msg = ("Total number of rows in this query are: %s" %(cr.rowcount))
    #         self.logger(datetime=datetime.now(), type='INFO', content=msg)
    #         return lines or []
    #     except Exception as e:
    #         self.logger(datetime=datetime.now(), type='ERROR', content=str(e))
    #     finally:
    #         if connection.is_connected():
    #             connection.close()
    #             cr.close()

    #METODO MODIFICADO POR ELDER GIRON 01072025 PARA QUE VALIDE SI YA EXISTEN VENTAS CON EL SILVERPOS_ID A SUBIR
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
                # Verifica si la venta ya existe en Odoo usando el silverpos_id
                existing_sale = self.search_existing_sale(so.get('silverpos_id'))
                if existing_sale:
                    print(f"Venta ya existe en Odoo: {so.get('silverpos_id')}")
                    continue  # Si la venta ya existe, salta al siguiente

                # Añadir valores a la venta
                if warehouse_id:
                    so.update({
                        'warehouse_id': warehouse_id,
                    })
                if account_analytic_id:
                    so.update({
                        'analytic_account_id': account_analytic_id,
                    })
                if company_id:
                    so.update({
                        'company_id': company_id,
                    })
                if picking_type_id_mrp:
                    so.update({
                        'picking_type_id_mrp': picking_type_id_mrp,
                    })
                if picking_type_id_stock:
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

        except Exception as e:
            # Manejo de excepciones
            self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

       
#AGREGADO POR ELDER GIRON METODO PARA VALIDAR SI YA EXISTE SILVERPOS_ID EN ODOO PARA QUE NO ME VUELVA A SUBIR LA VENTA
    def search_existing_sale(self, silverpos_id):
        try:
            # Configuración de Odoo
            odoo_param = self.get_odoo_config()
            url = odoo_param.get('url', False)
            token = odoo_param.get('token', False)
            db_name = odoo_param.get('db', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}
            
            # Obtener la fecha de ayer
            yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
            
            # Construir el dominio de búsqueda con el filtro de fecha (fecha anterior)
            domain = f"?domain=[('silverpos_id', '=', {silverpos_id}), ('company_id', '=', {company_id}), ('date_order', '>=', '{yesterday}')]"
            fields = "&fields=['id']"  # Solamente necesitamos el 'id' de la venta
            get_url = f"{url}/sale.order/search{domain}{fields}"

            # Realizar la solicitud GET a la API de Odoo
            response = requests.get(get_url, params=params, headers=headers, stream=True, verify=False)

            # Verificar si la respuesta fue exitosa
            if response and response.status_code == 200:
                odoo_res = json.loads(response.content.decode('utf-8'))
                
                # Si la venta ya existe, Odoo debería devolver una lista de resultados
                if 'data' in odoo_res and len(odoo_res['data']) > 0:
                    return True  # Venta encontrada
                else:
                    return False  # No se encontró ninguna venta
            else:
                print(f"Error al verificar la venta en Odoo: {response.status_code} - {response.content.decode('utf-8')}")
                return False

        except Exception as e:
            print(f"Error al consultar Odoo: {str(e)}")
            return False

    def update_sales(self, idsale=None, idodoo=None):
        #product_dict = {}
        #products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE hist_venta_enca SET erp = %s WHERE id = %s;"""
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
    cli.idodoo,
    enca.fechanegocio,
    fpay.id as fpay
FROM silverpos_hist.hist_venta_deta_pagos pay
INNER JOIN hist_venta_enca enca on enca.id = pay.id_encaventa
INNER JOIN hist_clientes cli on cli.id = enca.idcliente
INNER JOIN silverpos_hist.hist_formas_de_pago fpay on fpay.id = pay.id_forma_pago
WHERE pay.erp = 0
  AND enca.erp != 0
  AND enca.borrada = 0
  AND enca.mesa != 'Report'
  AND enca.anulado = 0
  AND DATE(enca.fechanegocio) = CURDATE() - INTERVAL 4 DAY
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
            #self.logger(datetime=datetime.now(), type='XPAGOS', content=str(payments))
            url = odoo_param.get('url', False)
            #self.logger(datetime=datetime.now(), type='XURL', content=str(url))
            token = odoo_param.get('token', False)
            #self.logger(datetime=datetime.now(), type='XTOKEN', content=str(token))
            db_name = odoo_param.get('db', False)
            journal_id = odoo_param.get('cash', False)
            payments_ids = odoo_param.get('payments', False)
            company_id = odoo_param.get('company_id', False)
            params = {'api_key': token}
            headers = {'Accept': '*/*', 'db_name': db_name}

            
            post_url = ("%s/account.payment/create" %(url))
            #self.logger(datetime=datetime.now(), type='XURL22', content=str(post_url))
            
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
                    
                    'ref': str(payment.get('pay_id'))

                }
                #self.logger(datetime=datetime.now(), type='Xheaders', content=str(headers))
                #self.logger(datetime=datetime.now(), type='Xparametros', content=str(params))
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

    
    
    
    # def sync_payments_odoo(self):
    #     payments = []
    #     try:
    #         odoo_param = self.get_odoo_config()
    #         payments = self.search_payments()
    #         url = odoo_param.get('url', False)
    #         token = odoo_param.get('token', False)
    #         db_name = odoo_param.get('db', False)
    #         journal_id = odoo_param.get('cash', False)
    #         company_id = odoo_param.get('company_id', False)
    #         params = {'api_key': token}
    #         headers = {'Accept': '*/*', 'db_name': db_name}
    #         post_url = ("%s/account.payment/create" %(url))
    #         for payment in payments:
    #             prod = {
    #                 'payment_type': "inbound",
    #                 'partner_type': "customer",
    #                 'payment_method_id': 2,
    #                 'partner_id': payment.get('customer_id', False),
    #                 'sale_id': payment.get('sale_id', False),
    #                 'amount': payment.get('pay_amount', 0.00),
    #                 'journal_id': journal_id,
    #                 'payment_date': payment.get('payment_date', False),
    #                 'company_id': company_id,
    #                 'communication': payment.get('sale_id', False)
    #             }
    #             response  = requests.post(post_url, data=json.dumps(prod), params=params, headers=headers, stream=True, verify=False)
    #             if response and response.status_code == 200:
    #                 odoo_res = json.loads(response.content.decode('utf-8'))
    #                 self.logger(datetime=datetime.now(), type='INFO', content=response.content.decode('utf-8'))
    #                 self.update_payments(idpayment=payment.get('pay_id', False), idodoo=odoo_res.get('create_id', False))
    #     except Exception as e:
    #         self.logger(datetime=datetime.now(), type='ERROR', content=str(e))

    def update_payments(self, idpayment=None, idodoo=None):
        #product_dict = {}
        #products = []
        try:
            connection = self.mysql_connection()
            query = """UPDATE hist_venta_deta_pagos SET erp = %s WHERE id = %s;"""
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
connector.sync_products_odoo()
connector.sync_customers_odoo()
connector.sync_employee_odoo()
#connector.search_sales()
#connector.search_sales_lines(idorder=1)
connector.sync_sales_odoo()
connector.sync_payments_odoo()
#connector.validate_product_odoo(idsilverpos=139)
