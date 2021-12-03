import sys
import json
#from utils.connection_factory import ConnectionFactory
#from utils.formatter import iso_formatter
import logging
import pymysql.cursors
import pymysql


logging.getLogger().setLevel(logging.ERROR)
sys.path.append('../')

class ConnectionFactory:

  conn = None

  @staticmethod
  def open():
    if ConnectionFactory.conn is not None:
        try:
            with ConnectionFactory.conn.cursor() as cursor:
                sql = "SELECT now()"
                cursor.execute(sql)
                result = cursor.fetchall()

                return ConnectionFactory.conn
        except:
            ConnectionFactory.conn = None

    host = 'cassini-dev-db.cpraideugqca.ap-northeast-1.rds.amazonaws.com'
    user = 'cassini'
    password = 'k(NVD$Kg288u'
    database = 'cassini'
    charset = 'utf8mb4'
    program_name = 'AWS_LAMBDA_FUNCTION_NAME'

    ConnectionFactory.conn = pymysql.connect(host=host,
                    user=user,
                    db=database,
                    password=password,
                    charset=charset,
                    program_name=program_name,
                    cursorclass=pymysql.cursors.DictCursor)

    return ConnectionFactory.conn

def handler(event: dict):

    itemId = event['item_id']
    itemSQL = "SELECT ITEM_CODE, ITEM_NAME, ITEM_TYPE FROM MST_ITEM WHERE ITEM_ID = %s ;"

    conn = ConnectionFactory.open()
    try:
        with conn.cursor() as cursor:

            param = (itemId, )
            cursor.execute(itemSQL, param)
            itemData = cursor.fetchone()

    except Exception as e:
        logging.critical(e, exc_info=True)
        return e
    
    finally:
        cursor.close()
        conn.close()

    result = {}
    result['item'] = itemData

    response = {
        "statusCode": 200,
        "body": json.dumps(result).encode('utf-8')
    }

    return response
    
input = {
    "item_id": 473
}
r = handler(input)
print(r)
