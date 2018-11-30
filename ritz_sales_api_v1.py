#  Mike Booth and Nathan Laursen
#  Ritz Sales API now with Functions for added flavor


"""The API is divided into sections. The first contains the imports, connection string, connection functions, and
function declarations. Following that are the Ticket Sales, Concession Sales, Employee, Feature, and Sales sections.
These API sections pull data from the various tables in our database in differing ways we thought may be useful. The 
last sections, Box Office Reports, TotalSales, SalesByDate, and CandySalesByDate, pull from some of the SQL views in the
database. A previous version of this script had the file at over 600 lines of code. We chose to use the functions to
shorten the script since we were reusing the same code over and over in many places. Some of the functions are based off
what was in the sql_server_data_api.py example. We modified them as needed and explained what the code was doing."""



import pyodbc
from flask import Flask, g, render_template, abort, request, Response
import datetime


#  database connection
server =
database = 'database'
username = 'username'
password = 'password'
driver = '{ODBC Driver 17 for SQL Server}'

CONNECTION_STRING = f"DRIVER= {driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"

#  Setup Flask
app = Flask(__name__)


#  Convert Datetime objects from SQL for use in JSON if needed, not implemented
def my_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


#  Open Connection to Database
@app.before_request
def before_request():
    try:
        g.sql_conn = pyodbc.connect(CONNECTION_STRING, autocommit=True, timeout=30)
    except Exception:
        abort(500, "No database connection could be established.")


#  Close Connection to Database
@app.teardown_request
def teardown_request(exception):
    try:
        g.sql_conn.close()
    except AttributeError:
        pass


"""Default Page and Help Page"""


#  Default Page
@app.route('/')
def default_page():
    return render_template('index.html'), 200


#  Default Page
@app.route('/help')
def api_help():
    return render_template('ritz_theater_api_default.html'), 200


"""Query Functions"""


#  Execute Query and Store
def execute_query(query):
    curs = g.sql_conn.cursor()
    curs.execute(query)

    keys = [column[0] for column in curs.description]
    data = {}
    i = 0

    for row in curs.fetchall():
        data[i] = row
        i += 1
    return keys, data  # implicit cast to tuple?


#  Execute SQL queries and commits the changes to the database
def execute_commit_ops(query):
    curs = g.sql_conn.cursor()
    curs.execute(query)
    curs.commit()

    return 'success', 200
#  Creates a cursor variable, executes the query variable/parameter, commits the changes to the database, and returns a
#  200 code if successful.


"""Unused Query Function"""


# #  1000 Results Function
# def execute_1000(query):
#     curs = g.sql_conn.cursor()
#     curs.execute(query)
#     columns = [column[0] for column in curs.description]
#     data = []
#     i = 0
#     for row in curs.fetchall():
#         i += 1
#         if i > 1000:
#             break
#         data.append(dict(zip(columns, row)))
# #    return json.dumps(data, indent=4, sort_keys=True, default=str)
# #  creates a cursor variable, using the query parameter/variable, executes the query with the cursor and stores the
# #  result, then iterates over the result into a table and returns as a json. Added in the i iteration and break to
# #  meet the "up to 1000 entries" requirement.


"""API Section"""


#  Ticket Sales
#  Get All Ticket Sales
@app.route('/api/rs/sales/tickets', methods=['GET'])
def get_ticket_sales():
    query = """select * from Ticket_Sales"""
    query_result = execute_query(query)
    return Response(render_template('tickets.html', result=query_result[1], keys=query_result[0], mimetype='text/html'))


#  1000 Results API Function
# @app.route('/api/rs/sales/tickets', methods=['GET'])
# def get_1000_ticket_sales():
#     query = """select * from Ticket_Sales"""
#     return execute_1000(query)


#  GET Single Ticket Sale by ID. Will return a single item from your data, by ID.
@app.route('/api/rs/sales/tickets/<int:gross_id>', methods=['GET'])
def get_single_ticket_sale(gross_id):
    query = f"""select * from Ticket_Sales where GrossID = {gross_id}"""
    query_result = execute_query(query)
    return Response(render_template('ticket_id.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  (Add) Ticket Sale, Do not implement
# @app.route('/api/rs/sales/tickets', methods=['POST'])
# def insert_new_ticket_sale():
#     data = request.get_json()
#     query = f"""insert into Ticket_Sales VALUES {data['FeatureID']},{data['Date']},{data['DayOfWeek']},""" \
#             f"""{data['TimeOfShow']},{data['OpeningNumber']},{data['ClosingNumber']},{data['TicketPrice']}"""
#     query_result = execute_query(query)
#     return Response(render_template('add_tickets.html', result=query_result[1], keys=query_result[0],
#                                     mimetype='text/html'))


"""
#  DELETE Ticket Sale DO NOT USE!!!!!!! violates database referential integrity
@app.route('/api/rs/sales/ticket/<string:gross_id>', methods=['DELETE'])
def delete_ticket_sale(gross_id):
    query = f"DELETE FROM Ticket_Sales WHERE GrossID = {gross_id}"
    return execute_commit_ops(query)
"""


#  Concessions Sales
#  Get All Concession Sales
@app.route('/api/rs/sales/concession', methods=['GET'])
def get_concession_sales():
    query = """select * from Concession_Sales"""
    query_result = execute_query(query)
    return Response(render_template('concessions.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  GET Single Item Concessions
@app.route('/api/rs/sales/concession/<int:con_id>', methods=['GET'])
def get_single_concession_sale(con_id):
    query = f"""select * from Concession_Sales where ConSaleID = {con_id}"""
    query_result = execute_query(query)
    return Response(render_template('concessions_id.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


# # (Add) Concessions Entry, do not implement
# @app.route('/api/rs/sales/concession', methods=['POST'])
# def insert_new_concession_sale():
#     data = request.get_json()
#     query = f"""insert into Concession_Sales VALUES {data['Date']},{data['TotalSales']},{data['Time']},""" \
#             f"""{data['Screen']}"""
#     return execute_commit_ops(query)


"""
#  DELETE Concessions DO NOT USE!!!!!!! violates database referential integrity
@app.route('/api/rs/sales/concession/<string:con_id>', methods=['DELETE'])
def delete_concession_sale(con_id):
    query = f"DELETE FROM Concession_Sales WHERE ConSaleID = {con_id}"
    return execute_commit_ops(query)
"""


#  Employee Section
#  Get All Employees
@app.route('/api/rs/sales/employee', methods=['GET'])
def get_employees():
    query = """select * from Employee"""
    query_result = execute_query(query)
    return Response(render_template('employees.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


# GET Single Employee
@app.route('/api/rs/sales/employee/<int:emp_id>', methods=['GET'])
def get_single_employee(emp_id):
    query = f"""select * from Employee where EmployeeID = {emp_id}"""
    query_result = execute_query(query)
    return Response(render_template('employee_id.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


# #  (Add) Employee, do not implement
# @app.route('/api/rs/sales/employee', methods=['POST'])
# def insert_employee():
#     data = request.get_json()
#     query = f"""insert into Employee VALUES {data['FirstName']},{data['LastName']},{data['Phone']},""" \
#             f"""{data['CellPhone']},{data['Email']},{data['EmployeeType']},{data['Active']}"""
#     return execute_commit_ops(query)


"""
#  DELETE Employee DO NOT USE!!!!!!! violates database referential integrity
@app.route('/api/rs/sales/employee/<string:emp_id>', methods=['DELETE'])
def delete_employee(emp_id):
    query = f"DELETE FROM Employee WHERE EmployeeID = {emp_id}"
    return execute_commit_ops(query)
"""


#  Feature Section
#  Get All Features
@app.route('/api/rs/sales/feature', methods=['GET'])
def get_feature():
    query = """select * from Feature"""
    query_result = execute_query(query)
    return Response(render_template('features.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  GET Single Feature
@app.route('/api/rs/sales/feature/<int:feature_id>', methods=['GET'])
def get_single_feature(feature_id):
    query = f"""select * from Feature where FeatureID = {feature_id}"""
    query_result = execute_query(query)
    return Response(render_template('feature_id.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


# #  (Add) Feature, do not implement
# @app.route('/api/rs/sales/feature', methods=['POST'])
# def insert_new_feature():
#     data = request.get_json()
#     query = f"""insert into Feature VALUES {data['ShowTitle']},{data['YearReleased']},{data['Rating']},""" \
#             f"""{data['OpenDate']},{data['CloseDate']},{data['Distributor']},{data['Screen']}"""
#     return execute_commit_ops(query)


"""
#  DELETE Feature DO NOT USE!!!!!!! violates database referential integrity
@app.route('/api/rs/sales/feature/<string:feature_id>', methods=['DELETE'])
def delete_feature(feature_id):
    query = f"DELETE FROM Feature WHERE FeatureID = {feature_id}"
    return execute_commit_ops(query)
"""


#  Sales Section
#  Get All Sales
@app.route('/api/rs/sales/sales', methods=['GET'])
def get_sales():
    query = """select * from Sales"""
    query_result = execute_query(query)
    return Response(render_template('sales.html', result=query_result[1], keys=query_result[0], mimetype='text/html'))


# #  Add Sales Entry, do not implement
# @app.route('/api/rs/sales/sales', methods=['POST'])
# def insert_new_sale():
#     data = request.get_json()
#     query = f"""insert into Sales Values {data['Date']},{data['FeatureID']},{data['GrossID']},"""\
#             f"""{data['ManagerID']},{data['ConSaleID']}"""
#     return execute_commit_ops(query)

#  No Delete Function will be included for this section


"""API Reports Section"""


#  Box Office Report Section
#  All Reports
@app.route('/api/rs/sales/bo_report', methods=['GET'])
def get_report():
    query = """select * from BoxOfficeReport"""
    query_result = execute_query(query)
    return Response(render_template('all_box_reports.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  Box Office Report by Date Single
"""Enter Dates with the format MMDDYYYY including a 0 for single digit value months and dates"""
@app.route('/api/rs/sales/bo_report/<string:show_title>/<string:show_date>', methods=['GET'])
def get_single_bo_report(show_date, show_time, show_title):
    date_string = show_date[0:2] + '/' + show_date[2:4] + '/' + show_date[4:8]
    query = f"""select * from BoxOfficeReport where Date = '{date_string}' and ShowTime = '{show_time}' and"""\
            f"""ShowTitle like '{show_title}'"""
    query_result = execute_query(query)
    return Response(render_template('single_bo_report.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  Box Office Report for Date Range
"""Enter Dates with the format MMDDYYYY including a 0 for single digit value months and dates"""
@app.route('/api/rs/sales/bo_report/<string:first_date>/<string:last_date>/<string:show_title>', methods=['GET'])
def get_bo_report_range(first_date, last_date, show_title):
    first_date_string = first_date[0:2] + '/' + first_date[2:4] + '/' + first_date[4:8]
    last_date_string = last_date[0:2] + '/' + last_date[2:4] + '/' + last_date[4:8]
    query = f"""select * from BoxOfficeReport where Date between '{first_date_string}' and '{last_date_string}'"""\
            f"""and ShowTitle like '{show_title}'"""
    query_result = execute_query(query)
#  round Decimal results of currency columns to 2 places
    for i in range(0, len(query_result[1])):
        query_result[1][i][6] = round(query_result[1][i][6], 2)
        query_result[1][i][7] = round(query_result[1][i][7], 2)
        query_result[1][i][8] = round(query_result[1][i][8], 2)
    return Response(render_template('bo_report.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  Box Office Report by Feature
@app.route('/api/rs/sales/bo_report/<string:show_title>', methods=['GET'])
def get_single_bo_report_range(show_title):
    query = f"""select * from BoxOfficeReport where ShowTitle like '{show_title}'"""
    query_result = execute_query(query)
    return Response(render_template('bo_report_feature.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  Box Office Report By Rating
@app.route('/api/rs/sales/bo_report/<string:rating>', methods=['GET'])
def get_bo_report_rating(rating):
    query = f"""select * from BoxOfficeReport where Rating like '{rating}'"""
    query_result = execute_query(query)
    return Response(render_template('bo_report_rating.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  Box Office Report by Distributor
@app.route('/api/rs/sales/bo_report/<string:distributor>', methods=['GET'])
def get_bo_report_distributor(distributor):
    query = f"""select * from BoxOfficeReport where Distributor like '{distributor}'"""
    query_result = execute_query(query)
    return Response(render_template('bo_report_distributor.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  Other Views
#  TotalSales
@app.route('/api/rs/sales/total_sales', methods=['GET'])
def get_total_sales():
    query = f"""select * from TotalSales"""
    query_result = execute_query(query)
    return Response(render_template('total_sales.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  SalesByDate
@app.route('/api/rs/sales/sales_by_date', methods=['GET'])
def get_sales_by_date():
    query = f"""select * from SalesByDate"""
    query_result = execute_query(query)
    return Response(render_template('sales_by_date.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


#  CandySalesByDate
@app.route('/api/rs/sales/CandySalesByDate', methods=['GET'])
def get_candy_by_date():
    query = f"""select * from CandySalesByDate"""
    query_result = execute_query(query)
    return Response(render_template('candy_by_date.html', result=query_result[1], keys=query_result[0],
                                    mimetype='text/html'))


if __name__ == '__main__':
    app.run()
